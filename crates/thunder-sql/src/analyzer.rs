//! SQL Analyzer
//!
//! Performs semantic analysis on SQL statements and produces logical plans.
//! The analyzer validates table/column references, resolves names, and
//! type-checks expressions.

use async_trait::async_trait;
use sqlparser::ast::{self as sql_ast};
use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;

use crate::catalog::{Catalog, CatalogProvider};
use crate::parser::{
    convert_data_type, convert_expr, convert_join_type, convert_order_by, extract_table_name,
    is_aggregate_function, try_convert_aggregate,
};
use crate::types::TypeInference;
use crate::{Analyzer, AlterTableOp, Expr, LogicalPlan, SortExpr};

/// Convert a sqlparser ColumnDef to our internal ColumnDef
fn convert_column_def(col: &sql_ast::ColumnDef) -> Result<ColumnDef> {
    let data_type = convert_data_type(&col.data_type)?;
    let nullable = !col.options.iter().any(|opt| {
        matches!(
            opt.option,
            sql_ast::ColumnOption::NotNull | sql_ast::ColumnOption::Unique { is_primary: true, .. }
        )
    });
    Ok(ColumnDef {
        name: col.name.value.clone(),
        data_type,
        nullable,
        default: None,
    })
}

/// SQL Analyzer implementation
pub struct SqlAnalyzer {
    /// Catalog for schema resolution
    catalog: Arc<dyn CatalogProvider>,
}

impl SqlAnalyzer {
    /// Create a new analyzer with the given catalog
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog }
    }

    /// Create analyzer with a simple catalog
    pub fn with_catalog(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    /// Analyze a SELECT statement
    fn analyze_query(&self, query: &sql_ast::Query) -> Result<LogicalPlan> {
        // Handle CTEs if present
        if query.with.is_some() {
            return Err(Error::Sql(SqlError::UnsupportedFeature(
                "CTEs (WITH clause) not yet supported".to_string(),
            )));
        }

        // Process the main query body
        let plan = self.analyze_set_expr(query.body.as_ref())?;

        // Apply ORDER BY
        let plan = if !query.order_by.is_empty() {
            let order_exprs: Result<Vec<SortExpr>> = query
                .order_by
                .iter()
                .map(convert_order_by)
                .collect();

            LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_exprs?,
            }
        } else {
            plan
        };

        // Apply LIMIT/OFFSET
        let plan = if query.limit.is_some() || query.offset.is_some() {
            let limit = query
                .limit
                .as_ref()
                .and_then(|l| match l {
                    sql_ast::Expr::Value(sql_ast::Value::Number(n, _)) => n.parse().ok(),
                    _ => None,
                })
                .unwrap_or(usize::MAX);

            let offset = query
                .offset
                .as_ref()
                .and_then(|o| match &o.value {
                    sql_ast::Expr::Value(sql_ast::Value::Number(n, _)) => n.parse().ok(),
                    _ => None,
                })
                .unwrap_or(0);

            LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            }
        } else {
            plan
        };

        Ok(plan)
    }

    /// Analyze a set expression (SELECT, UNION, etc.)
    fn analyze_set_expr(&self, expr: &sql_ast::SetExpr) -> Result<LogicalPlan> {
        match expr {
            sql_ast::SetExpr::Select(select) => self.analyze_select(select),
            sql_ast::SetExpr::Values(values) => self.analyze_values(values),
            sql_ast::SetExpr::Query(query) => self.analyze_query(query),
            other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
                "Unsupported set expression: {:?}",
                other
            )))),
        }
    }

    /// Analyze a SELECT statement
    fn analyze_select(&self, select: &sql_ast::Select) -> Result<LogicalPlan> {
        // Start with the FROM clause
        let mut plan = if select.from.is_empty() {
            LogicalPlan::Empty
        } else {
            self.analyze_from(&select.from)?
        };

        // Get the schema for name resolution
        let schema = TypeInference::plan_schema(&plan)?;

        // Apply WHERE clause
        if let Some(where_clause) = &select.selection {
            let predicate = convert_expr(where_clause)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // Check for aggregates in the SELECT clause
        let has_aggregates = self.has_aggregates(&select.projection);
        let has_group_by = !matches!(&select.group_by, sql_ast::GroupByExpr::Expressions(exprs) if exprs.is_empty())
            && !matches!(&select.group_by, sql_ast::GroupByExpr::All);

        if has_aggregates || has_group_by {
            plan = self.analyze_aggregate(select, plan, &schema)?;
        } else {
            // Simple projection
            plan = self.analyze_projection(select, plan, &schema)?;
        }

        // Apply HAVING clause (after aggregation)
        if let Some(having) = &select.having {
            let predicate = convert_expr(having)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        Ok(plan)
    }

    /// Analyze FROM clause
    fn analyze_from(&self, from: &[sql_ast::TableWithJoins]) -> Result<LogicalPlan> {
        let mut plan = self.analyze_table_with_joins(&from[0])?;

        // Handle multiple tables in FROM (implicit cross join)
        for table in from.iter().skip(1) {
            let right = self.analyze_table_with_joins(table)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                condition: Expr::Literal(Value::Boolean(true)),
                join_type: crate::JoinType::Cross,
            };
        }

        Ok(plan)
    }

    /// Analyze a table with its joins
    fn analyze_table_with_joins(&self, table: &sql_ast::TableWithJoins) -> Result<LogicalPlan> {
        let mut plan = self.analyze_table_factor(&table.relation)?;

        for join in &table.joins {
            let right = self.analyze_table_factor(&join.relation)?;
            let join_type = convert_join_type(&join.join_operator)?;

            let condition = match &join.join_operator {
                sql_ast::JoinOperator::Inner(constraint)
                | sql_ast::JoinOperator::LeftOuter(constraint)
                | sql_ast::JoinOperator::RightOuter(constraint)
                | sql_ast::JoinOperator::FullOuter(constraint) => {
                    self.analyze_join_constraint(constraint)?
                }
                _ => Expr::Literal(Value::Boolean(true)),
            };

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                condition,
                join_type,
            };
        }

        Ok(plan)
    }

    /// Analyze a table factor (table reference, subquery, etc.)
    fn analyze_table_factor(&self, factor: &sql_ast::TableFactor) -> Result<LogicalPlan> {
        match factor {
            sql_ast::TableFactor::Table { name, alias, .. } => {
                let table_name = extract_table_name(name);
                let table_info = self
                    .catalog
                    .get_table(&table_name)
                    .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.clone())))?;

                Ok(LogicalPlan::Scan {
                    table: alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or(table_name),
                    schema: table_info.schema.clone(),
                    filter: None,
                    projection: None,
                })
            }

            sql_ast::TableFactor::Derived {
                subquery, ..
            } => {
                let plan = self.analyze_query(subquery)?;
                Ok(plan)
            }

            sql_ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.analyze_table_with_joins(table_with_joins),

            other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
                "Unsupported table factor: {:?}",
                other
            )))),
        }
    }

    /// Analyze a join constraint
    fn analyze_join_constraint(&self, constraint: &sql_ast::JoinConstraint) -> Result<Expr> {
        match constraint {
            sql_ast::JoinConstraint::On(expr) => convert_expr(expr),
            sql_ast::JoinConstraint::Using(columns) => {
                // Convert USING(col1, col2) to col1 = col1 AND col2 = col2
                let mut condition: Option<Expr> = None;
                for col in columns {
                    let col_name = col.value.clone();
                    let eq = Expr::BinaryOp {
                        left: Box::new(Expr::Column {
                            name: col_name.clone(),
                            index: None,
                        }),
                        op: crate::BinaryOperator::Eq,
                        right: Box::new(Expr::Column {
                            name: col_name,
                            index: None,
                        }),
                    };
                    condition = Some(match condition {
                        Some(c) => Expr::BinaryOp {
                            left: Box::new(c),
                            op: crate::BinaryOperator::And,
                            right: Box::new(eq),
                        },
                        None => eq,
                    });
                }
                Ok(condition.unwrap_or(Expr::Literal(Value::Boolean(true))))
            }
            sql_ast::JoinConstraint::Natural => {
                Ok(Expr::Literal(Value::Boolean(true)))
            }
            sql_ast::JoinConstraint::None => Ok(Expr::Literal(Value::Boolean(true))),
        }
    }

    /// Check if projection contains aggregate functions
    fn has_aggregates(&self, projection: &[sql_ast::SelectItem]) -> bool {
        for item in projection {
            match item {
                sql_ast::SelectItem::UnnamedExpr(expr)
                | sql_ast::SelectItem::ExprWithAlias { expr, .. } => {
                    if self.expr_has_aggregate(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Check if an expression contains aggregate functions
    fn expr_has_aggregate(&self, expr: &sql_ast::Expr) -> bool {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = crate::parser::object_name_to_string(&func.name);
                is_aggregate_function(&name)
            }
            sql_ast::Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            sql_ast::Expr::UnaryOp { expr, .. } => self.expr_has_aggregate(expr),
            sql_ast::Expr::Nested(inner) => self.expr_has_aggregate(inner),
            sql_ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                operand.as_ref().map_or(false, |e| self.expr_has_aggregate(e))
                    || conditions.iter().any(|e| self.expr_has_aggregate(e))
                    || results.iter().any(|e| self.expr_has_aggregate(e))
                    || else_result.as_ref().map_or(false, |e| self.expr_has_aggregate(e))
            }
            _ => false,
        }
    }

    /// Analyze aggregate query
    fn analyze_aggregate(
        &self,
        select: &sql_ast::Select,
        input: LogicalPlan,
        _schema: &Schema,
    ) -> Result<LogicalPlan> {
        // Extract GROUP BY expressions
        let group_by: Result<Vec<Expr>> = match &select.group_by {
            sql_ast::GroupByExpr::All => Ok(vec![]),
            sql_ast::GroupByExpr::Expressions(exprs) => {
                exprs.iter().map(convert_expr).collect()
            }
        };
        let group_by = group_by?;

        // Extract aggregate expressions from SELECT
        let mut aggregates = Vec::new();
        let mut output_exprs = Vec::new();
        let mut output_columns = Vec::new();

        for item in &select.projection {
            match item {
                sql_ast::SelectItem::UnnamedExpr(expr) => {
                    let (agg, out_expr, col_name) = self.extract_aggregate_expr(expr)?;
                    if let Some(a) = agg {
                        aggregates.push(a);
                    }
                    output_exprs.push(out_expr);
                    output_columns.push(ColumnDef::new(col_name, DataType::Null));
                }
                sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let (agg, out_expr, _) = self.extract_aggregate_expr(expr)?;
                    if let Some(a) = agg {
                        aggregates.push(a);
                    }
                    output_exprs.push(out_expr);
                    output_columns.push(ColumnDef::new(alias.value.clone(), DataType::Null));
                }
                sql_ast::SelectItem::Wildcard(_) => {
                    return Err(Error::Sql(SqlError::UnsupportedFeature(
                        "Wildcard in aggregate query".to_string(),
                    )));
                }
                _ => {}
            }
        }

        let output_schema = Schema::new(output_columns);

        Ok(LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by,
            aggregates,
            schema: output_schema,
        })
    }

    /// Extract aggregate expression from a SQL expression
    fn extract_aggregate_expr(
        &self,
        expr: &sql_ast::Expr,
    ) -> Result<(Option<crate::AggregateExpr>, Expr, String)> {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = crate::parser::object_name_to_string(&func.name);
                if let Some(agg) = try_convert_aggregate(func)? {
                    let col_name = format!("{}({})", name, if func.args.is_empty() { "*" } else { "..." });
                    Ok((Some(agg.clone()), convert_expr(expr)?, col_name))
                } else {
                    let col_name = name.clone();
                    Ok((None, convert_expr(expr)?, col_name))
                }
            }
            sql_ast::Expr::Identifier(ident) => {
                Ok((None, convert_expr(expr)?, ident.value.clone()))
            }
            _ => Ok((None, convert_expr(expr)?, "?".to_string())),
        }
    }

    /// Analyze simple projection (no aggregates)
    fn analyze_projection(
        &self,
        select: &sql_ast::Select,
        input: LogicalPlan,
        schema: &Schema,
    ) -> Result<LogicalPlan> {
        let mut exprs = Vec::new();
        let mut output_columns = Vec::new();

        for item in &select.projection {
            match item {
                sql_ast::SelectItem::Wildcard(_) => {
                    for col in &schema.columns {
                        exprs.push(Expr::Column {
                            name: col.name.clone(),
                            index: None,
                        });
                        output_columns.push(col.clone());
                    }
                }

                sql_ast::SelectItem::QualifiedWildcard(_, _) => {
                    for col in &schema.columns {
                        exprs.push(Expr::Column {
                            name: col.name.clone(),
                            index: None,
                        });
                        output_columns.push(col.clone());
                    }
                }

                sql_ast::SelectItem::UnnamedExpr(expr) => {
                    let converted = convert_expr(expr)?;
                    let col_name = self.expr_to_column_name(expr);
                    let data_type = TypeInference::infer_type(&converted, schema)
                        .unwrap_or(DataType::Null);
                    exprs.push(converted);
                    output_columns.push(ColumnDef::new(col_name, data_type));
                }

                sql_ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let converted = convert_expr(expr)?;
                    let data_type = TypeInference::infer_type(&converted, schema)
                        .unwrap_or(DataType::Null);
                    exprs.push(converted);
                    output_columns.push(ColumnDef::new(alias.value.clone(), data_type));
                }
            }
        }

        let output_schema = Schema::new(output_columns);

        Ok(LogicalPlan::Project {
            input: Box::new(input),
            exprs,
            schema: output_schema,
        })
    }

    /// Convert expression to a column name
    fn expr_to_column_name(&self, expr: &sql_ast::Expr) -> String {
        match expr {
            sql_ast::Expr::Identifier(ident) => ident.value.clone(),
            sql_ast::Expr::CompoundIdentifier(idents) => {
                idents.last().map(|i| i.value.clone()).unwrap_or_default()
            }
            sql_ast::Expr::Function(func) => {
                crate::parser::object_name_to_string(&func.name)
            }
            _ => "?column?".to_string(),
        }
    }

    /// Analyze VALUES clause
    fn analyze_values(&self, _values: &sql_ast::Values) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Empty)
    }

    /// Analyze INSERT statement
    fn analyze_insert(&self, table_name: &sql_ast::ObjectName, columns: &[sql_ast::Ident], source: &Option<Box<sql_ast::Query>>) -> Result<LogicalPlan> {
        let table_name_str = extract_table_name(table_name);

        // Verify table exists
        let table_info = self
            .catalog
            .get_table(&table_name_str)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name_str.clone())))?;

        // Get column list
        let col_names: Vec<String> = if columns.is_empty() {
            table_info.schema.column_names().into_iter().map(String::from).collect()
        } else {
            columns.iter().map(|c| c.value.clone()).collect()
        };

        // Validate columns exist
        for col in &col_names {
            if table_info.schema.column_by_name(col).is_none() {
                return Err(Error::Sql(SqlError::ColumnNotFound(col.clone())));
            }
        }

        // Extract values
        let values = match source.as_ref().map(|s| s.body.as_ref()) {
            Some(sql_ast::SetExpr::Values(vals)) => {
                let rows: Result<Vec<Vec<Expr>>> = vals
                    .rows
                    .iter()
                    .map(|row| row.iter().map(convert_expr).collect())
                    .collect();
                rows?
            }
            _ => vec![],
        };

        Ok(LogicalPlan::Insert {
            table: table_name_str,
            columns: col_names,
            values,
        })
    }

    /// Analyze UPDATE statement
    fn analyze_update(&self, table: &sql_ast::TableWithJoins, assignments: &[sql_ast::Assignment], selection: &Option<sql_ast::Expr>) -> Result<LogicalPlan> {
        let table_name = match &table.relation {
            sql_ast::TableFactor::Table { name, .. } => extract_table_name(name),
            _ => {
                return Err(Error::Sql(SqlError::UnsupportedFeature(
                    "Complex UPDATE target".to_string(),
                )))
            }
        };

        // Verify table exists
        self.catalog
            .get_table(&table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.clone())))?;

        // Convert assignments
        let converted_assignments: Result<Vec<(String, Expr)>> = assignments
            .iter()
            .map(|a| {
                let col_name = a.id.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                let value = convert_expr(&a.value)?;
                Ok((col_name, value))
            })
            .collect();

        // Convert WHERE clause
        let filter = selection
            .as_ref()
            .map(convert_expr)
            .transpose()?;

        Ok(LogicalPlan::Update {
            table: table_name,
            assignments: converted_assignments?,
            filter,
        })
    }

    /// Analyze DELETE statement
    fn analyze_delete(&self, from: &[sql_ast::TableWithJoins], selection: &Option<sql_ast::Expr>) -> Result<LogicalPlan> {
        let table_name = from
            .first()
            .and_then(|t| match &t.relation {
                sql_ast::TableFactor::Table { name, .. } => Some(extract_table_name(name)),
                _ => None,
            })
            .ok_or_else(|| Error::Sql(SqlError::ParseError("Missing table in DELETE".to_string())))?;

        // Verify table exists
        self.catalog
            .get_table(&table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.clone())))?;

        // Convert WHERE clause
        let filter = selection
            .as_ref()
            .map(convert_expr)
            .transpose()?;

        Ok(LogicalPlan::Delete {
            table: table_name,
            filter,
        })
    }

    /// Analyze CREATE TABLE statement
    fn analyze_create_table(&self, name: &sql_ast::ObjectName, columns: &[sql_ast::ColumnDef], if_not_exists: bool) -> Result<LogicalPlan> {
        let table_name = extract_table_name(name);

        // Check if table already exists (unless IF NOT EXISTS)
        if !if_not_exists && self.catalog.get_table(&table_name).is_some() {
            return Err(Error::AlreadyExists("Table".to_string(), table_name));
        }

        // Convert column definitions
        let converted_columns: Result<Vec<ColumnDef>> = columns
            .iter()
            .map(|col| {
                let data_type = convert_data_type(&col.data_type)?;
                let nullable = !col.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        sql_ast::ColumnOption::NotNull | sql_ast::ColumnOption::Unique { is_primary: true, .. }
                    )
                });
                Ok(ColumnDef {
                    name: col.name.value.clone(),
                    data_type,
                    nullable,
                    default: None,
                })
            })
            .collect();

        Ok(LogicalPlan::CreateTable {
            name: table_name,
            columns: converted_columns?,
            if_not_exists,
        })
    }

    /// Analyze DROP TABLE statement
    fn analyze_drop_table(&self, names: &[sql_ast::ObjectName], if_exists: bool) -> Result<LogicalPlan> {
        let table_name = names
            .first()
            .map(extract_table_name)
            .ok_or_else(|| Error::Sql(SqlError::ParseError("Missing table name in DROP".to_string())))?;

        if !if_exists && self.catalog.get_table(&table_name).is_none() {
            return Err(Error::Sql(SqlError::TableNotFound(table_name)));
        }

        Ok(LogicalPlan::DropTable {
            name: table_name,
            if_exists,
        })
    }
}

#[async_trait]
impl Analyzer for SqlAnalyzer {
    async fn analyze(&self, stmt: sql_ast::Statement) -> Result<LogicalPlan> {
        match stmt {
            sql_ast::Statement::Query(query) => self.analyze_query(&query),
            sql_ast::Statement::Insert { table_name, columns, source, .. } => {
                self.analyze_insert(&table_name, &columns, &source)
            }
            sql_ast::Statement::Update { table, assignments, selection, .. } => {
                self.analyze_update(&table, &assignments, &selection)
            }
            sql_ast::Statement::Delete { from, selection, .. } => {
                self.analyze_delete(&from, &selection)
            }
            sql_ast::Statement::CreateTable { name, columns, if_not_exists, .. } => {
                self.analyze_create_table(&name, &columns, if_not_exists)
            }
            sql_ast::Statement::Drop {
                object_type: sql_ast::ObjectType::Table,
                names,
                if_exists,
                ..
            } => self.analyze_drop_table(&names, if_exists),

            // CREATE INDEX
            sql_ast::Statement::CreateIndex {
                name: ref idx_name,
                table_name: ref tbl_name,
                columns: ref idx_columns,
                unique,
                if_not_exists,
                ..
            } => {
                let index_name = idx_name.as_ref()
                    .map(|n| extract_table_name(n))
                    .unwrap_or_else(|| "unnamed_idx".to_string());
                let table_name = extract_table_name(tbl_name);
                let columns: Vec<String> = idx_columns.iter()
                    .map(|c| c.expr.to_string())
                    .collect();
                Ok(LogicalPlan::CreateIndex {
                    name: index_name,
                    table: table_name,
                    columns,
                    unique,
                    if_not_exists,
                })
            }

            // DROP INDEX
            sql_ast::Statement::Drop {
                object_type: sql_ast::ObjectType::Index,
                names,
                if_exists,
                ..
            } => {
                let index_name = names
                    .first()
                    .map(extract_table_name)
                    .ok_or_else(|| Error::Sql(SqlError::ParseError("Missing index name in DROP INDEX".to_string())))?;
                Ok(LogicalPlan::DropIndex {
                    name: index_name,
                    if_exists,
                })
            }

            // ALTER TABLE
            sql_ast::Statement::AlterTable { name, operations, .. } => {
                let table_name = extract_table_name(&name);
                let ops = operations.iter().map(|op| {
                    match op {
                        sql_ast::AlterTableOperation::AddColumn { column_def, if_not_exists, .. } => {
                            let col = convert_column_def(column_def)?;
                            let _ = if_not_exists; // handled at execution level
                            Ok(AlterTableOp::AddColumn { column: col })
                        }
                        sql_ast::AlterTableOperation::DropColumn { column_name, if_exists, .. } => {
                            let _ = if_exists;
                            Ok(AlterTableOp::DropColumn { name: column_name.value.clone() })
                        }
                        sql_ast::AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                            Ok(AlterTableOp::RenameColumn {
                                old_name: old_column_name.value.clone(),
                                new_name: new_column_name.value.clone(),
                            })
                        }
                        sql_ast::AlterTableOperation::RenameTable { table_name: ref new_name } => {
                            Ok(AlterTableOp::RenameTable {
                                new_name: new_name.to_string(),
                            })
                        }
                        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
                            "ALTER TABLE operation: {:?}", other
                        )))),
                    }
                }).collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::AlterTable {
                    table: table_name,
                    operations: ops,
                })
            }

            // EXPLAIN / EXPLAIN ANALYZE
            sql_ast::Statement::Explain {
                statement, analyze, verbose, ..
            } => {
                let inner_plan = self.analyze(*statement.clone()).await?;
                Ok(LogicalPlan::Explain {
                    plan: Box::new(inner_plan),
                    analyze,
                    verbose,
                })
            }

            // ANALYZE
            sql_ast::Statement::Analyze { table_name, .. } => {
                let table = Some(extract_table_name(&table_name));
                Ok(LogicalPlan::AnalyzeTable { table })
            }

            other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
                "Statement: {:?}",
                other
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::parse_sql;

    fn test_analyzer() -> SqlAnalyzer {
        let catalog = Arc::new(Catalog::new());

        // Register test tables
        let users_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("email", DataType::String),
        ]);
        catalog.register_table_with_pk("users", users_schema, vec![0]).unwrap();

        let orders_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
            ColumnDef::new("status", DataType::String),
        ]);
        catalog.register_table_with_pk("orders", orders_schema, vec![0]).unwrap();

        SqlAnalyzer::with_catalog(catalog)
    }

    #[tokio::test]
    async fn test_analyze_simple_select() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT id, name FROM users").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    #[tokio::test]
    async fn test_analyze_select_star() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT * FROM users").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Project { exprs, .. } = plan {
            assert_eq!(exprs.len(), 4);
        } else {
            panic!("Expected Project plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_select_with_where() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT id, name FROM users WHERE age > 18").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Project { input, .. } = plan {
            assert!(matches!(*input, LogicalPlan::Filter { .. }));
        } else {
            panic!("Expected Project plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_aggregate() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT COUNT(*), AVG(age) FROM users").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        assert!(matches!(plan, LogicalPlan::Aggregate { .. }));
    }

    #[tokio::test]
    async fn test_analyze_group_by() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT status, COUNT(*) FROM orders GROUP BY status").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Aggregate { group_by, .. } = plan {
            assert_eq!(group_by.len(), 1);
        } else {
            panic!("Expected Aggregate plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_join() {
        let analyzer = test_analyzer();

        let stmts = parse_sql(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Project { input, .. } = plan {
            assert!(matches!(*input, LogicalPlan::Join { .. }));
        } else {
            panic!("Expected Project plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_order_by() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT id, name FROM users ORDER BY name DESC").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        assert!(matches!(plan, LogicalPlan::Sort { .. }));
    }

    #[tokio::test]
    async fn test_analyze_limit_offset() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 5").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Limit { limit, offset, .. } = plan {
            assert_eq!(limit, 10);
            assert_eq!(offset, 5);
        } else {
            panic!("Expected Limit plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_insert() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Insert { table, columns, values } = plan {
            assert_eq!(table, "users");
            assert_eq!(columns.len(), 3);
            assert_eq!(values.len(), 1);
        } else {
            panic!("Expected Insert plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_update() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Update { table, assignments, filter } = plan {
            assert_eq!(table, "users");
            assert_eq!(assignments.len(), 1);
            assert!(filter.is_some());
        } else {
            panic!("Expected Update plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_delete() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("DELETE FROM users WHERE age < 18").unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::Delete { table, filter } = plan {
            assert_eq!(table, "users");
            assert!(filter.is_some());
        } else {
            panic!("Expected Delete plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_create_table() {
        let analyzer = test_analyzer();

        let stmts = parse_sql(
            "CREATE TABLE products (id BIGINT NOT NULL, name VARCHAR(100), price DECIMAL(10, 2))",
        )
        .unwrap();
        let plan = analyzer.analyze(stmts[0].clone()).await.unwrap();

        if let LogicalPlan::CreateTable { name, columns, .. } = plan {
            assert_eq!(name, "products");
            assert_eq!(columns.len(), 3);
        } else {
            panic!("Expected CreateTable plan");
        }
    }

    #[tokio::test]
    async fn test_analyze_table_not_found() {
        let analyzer = test_analyzer();

        let stmts = parse_sql("SELECT * FROM nonexistent").unwrap();
        let result = analyzer.analyze(stmts[0].clone()).await;

        assert!(result.is_err());
    }
}
