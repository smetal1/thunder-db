//! Query Planning Utilities
//!
//! Provides utilities for working with logical plans, including:
//! - Plan visitors and transformers
//! - Cost estimation
//! - Plan analysis

use std::collections::HashSet;
use thunder_common::prelude::*;

use crate::catalog::CatalogProvider;
use crate::{Expr, LogicalPlan, BinaryOperator};

/// Visitor trait for traversing logical plans
pub trait PlanVisitor {
    /// Called before visiting children
    fn pre_visit(&mut self, _plan: &LogicalPlan) -> bool {
        true // continue by default
    }

    /// Called after visiting children
    fn post_visit(&mut self, _plan: &LogicalPlan) {}
}

/// Walk a plan tree with a visitor
pub fn walk_plan<V: PlanVisitor>(plan: &LogicalPlan, visitor: &mut V) {
    if !visitor.pre_visit(plan) {
        return;
    }

    match plan {
        LogicalPlan::Scan { .. } => {}
        LogicalPlan::Project { input, .. } => walk_plan(input, visitor),
        LogicalPlan::Filter { input, .. } => walk_plan(input, visitor),
        LogicalPlan::Join { left, right, .. } => {
            walk_plan(left, visitor);
            walk_plan(right, visitor);
        }
        LogicalPlan::Aggregate { input, .. } => walk_plan(input, visitor),
        LogicalPlan::Sort { input, .. } => walk_plan(input, visitor),
        LogicalPlan::Limit { input, .. } => walk_plan(input, visitor),
        LogicalPlan::Insert { .. } => {}
        LogicalPlan::Update { .. } => {}
        LogicalPlan::Delete { .. } => {}
        LogicalPlan::CreateTable { .. } => {}
        LogicalPlan::DropTable { .. } => {}
        LogicalPlan::CreateIndex { .. } => {}
        LogicalPlan::DropIndex { .. } => {}
        LogicalPlan::AlterTable { .. } => {}
        LogicalPlan::Explain { plan: inner, .. } => walk_plan(inner, visitor),
        LogicalPlan::AnalyzeTable { .. } => {}
        LogicalPlan::Empty => {}
    }

    visitor.post_visit(plan);
}

/// Transformer trait for rewriting logical plans
pub trait PlanTransformer {
    /// Transform a plan node, returning a new plan
    fn transform(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

/// Apply a transformer to a plan tree (bottom-up)
pub fn transform_plan<T: PlanTransformer>(plan: LogicalPlan, transformer: &T) -> Result<LogicalPlan> {
    let transformed = match plan {
        LogicalPlan::Scan { .. } => plan,

        LogicalPlan::Project { input, exprs, schema } => {
            let new_input = transform_plan(*input, transformer)?;
            LogicalPlan::Project {
                input: Box::new(new_input),
                exprs,
                schema,
            }
        }

        LogicalPlan::Filter { input, predicate } => {
            let new_input = transform_plan(*input, transformer)?;
            LogicalPlan::Filter {
                input: Box::new(new_input),
                predicate,
            }
        }

        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            let new_left = transform_plan(*left, transformer)?;
            let new_right = transform_plan(*right, transformer)?;
            LogicalPlan::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                condition,
                join_type,
            }
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let new_input = transform_plan(*input, transformer)?;
            LogicalPlan::Aggregate {
                input: Box::new(new_input),
                group_by,
                aggregates,
                schema,
            }
        }

        LogicalPlan::Sort { input, order_by } => {
            let new_input = transform_plan(*input, transformer)?;
            LogicalPlan::Sort {
                input: Box::new(new_input),
                order_by,
            }
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let new_input = transform_plan(*input, transformer)?;
            LogicalPlan::Limit {
                input: Box::new(new_input),
                limit,
                offset,
            }
        }

        other => other,
    };

    transformer.transform(transformed)
}

/// Visitor for collecting table names used in a plan
pub struct TableCollector {
    pub tables: HashSet<String>,
}

impl TableCollector {
    pub fn new() -> Self {
        Self {
            tables: HashSet::new(),
        }
    }

    pub fn collect(plan: &LogicalPlan) -> HashSet<String> {
        let mut collector = Self::new();
        walk_plan(plan, &mut collector);
        collector.tables
    }
}

impl Default for TableCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanVisitor for TableCollector {
    fn pre_visit(&mut self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan { table, .. } => {
                self.tables.insert(table.clone());
            }
            LogicalPlan::Insert { table, .. } => {
                self.tables.insert(table.clone());
            }
            LogicalPlan::Update { table, .. } => {
                self.tables.insert(table.clone());
            }
            LogicalPlan::Delete { table, .. } => {
                self.tables.insert(table.clone());
            }
            _ => {}
        }
        true
    }
}

/// Visitor for collecting column references in a plan
pub struct ColumnCollector {
    pub columns: HashSet<String>,
}

impl ColumnCollector {
    pub fn new() -> Self {
        Self {
            columns: HashSet::new(),
        }
    }

    pub fn collect_from_expr(expr: &Expr) -> HashSet<String> {
        let mut collector = Self::new();
        collector.visit_expr(expr);
        collector.columns
    }

    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column { name, .. } => {
                self.columns.insert(name.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr(left);
                self.visit_expr(right);
            }
            Expr::UnaryOp { expr, .. } => {
                self.visit_expr(expr);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    self.visit_expr(arg);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.visit_expr(op);
                }
                for (cond, then) in when_clauses {
                    self.visit_expr(cond);
                    self.visit_expr(then);
                }
                if let Some(e) = else_clause {
                    self.visit_expr(e);
                }
            }
            Expr::IsNull(e) | Expr::IsNotNull(e) => {
                self.visit_expr(e);
            }
            Expr::InList { expr, list, .. } => {
                self.visit_expr(expr);
                for item in list {
                    self.visit_expr(item);
                }
            }
            Expr::Between { expr, low, high, .. } => {
                self.visit_expr(expr);
                self.visit_expr(low);
                self.visit_expr(high);
            }
            Expr::Subquery(_plan) => {
                // Would need to recursively collect from subquery
            }
            Expr::Cast { expr, .. } => {
                self.visit_expr(expr);
            }
            Expr::Literal(_) | Expr::Wildcard => {}
        }
    }
}

impl Default for ColumnCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Cost estimation for query plans
#[derive(Debug, Clone, Default)]
pub struct PlanCost {
    /// Estimated number of rows
    pub rows: f64,
    /// Estimated CPU cost
    pub cpu: f64,
    /// Estimated I/O cost
    pub io: f64,
    /// Estimated memory cost
    pub memory: f64,
}

impl PlanCost {
    pub fn new(rows: f64, cpu: f64, io: f64, memory: f64) -> Self {
        Self { rows, cpu, io, memory }
    }

    /// Total cost (weighted sum)
    pub fn total(&self) -> f64 {
        self.cpu + self.io * 10.0 + self.memory * 0.1
    }
}

/// Cost estimator for logical plans
pub struct CostEstimator<'a> {
    catalog: &'a dyn CatalogProvider,
}

impl<'a> CostEstimator<'a> {
    pub fn new(catalog: &'a dyn CatalogProvider) -> Self {
        Self { catalog }
    }

    /// Estimate the cost of a plan
    pub fn estimate(&self, plan: &LogicalPlan) -> PlanCost {
        match plan {
            LogicalPlan::Scan { table, filter,  .. } => {
                let table_info = self.catalog.get_table(table);
                let base_rows = table_info
                    .map(|t| t.stats.row_count as f64)
                    .unwrap_or(1000.0);

                // Apply selectivity estimate for filter
                let selectivity = filter
                    .as_ref()
                    .map(|f| self.estimate_selectivity(f))
                    .unwrap_or(1.0);

                let rows = base_rows * selectivity;
                let io = base_rows; // Full table scan
                let cpu = rows;

                PlanCost::new(rows, cpu, io, 0.0)
            }

            LogicalPlan::Project { input, exprs, .. } => {
                let input_cost = self.estimate(input);
                // Projection is cheap - just adds CPU cost per row
                PlanCost::new(
                    input_cost.rows,
                    input_cost.cpu + input_cost.rows * exprs.len() as f64 * 0.1,
                    input_cost.io,
                    input_cost.memory,
                )
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_cost = self.estimate(input);
                let selectivity = self.estimate_selectivity(predicate);

                PlanCost::new(
                    input_cost.rows * selectivity,
                    input_cost.cpu + input_cost.rows, // Filter check per row
                    input_cost.io,
                    input_cost.memory,
                )
            }

            LogicalPlan::Join { left, right, join_type, .. } => {
                let left_cost = self.estimate(left);
                let right_cost = self.estimate(right);

                // Simple nested loop join estimate
                // Real optimizer would consider hash join, merge join, etc.
                let rows = match join_type {
                    crate::JoinType::Inner => left_cost.rows * right_cost.rows * 0.1,
                    crate::JoinType::Left => left_cost.rows,
                    crate::JoinType::Right => right_cost.rows,
                    crate::JoinType::Full => left_cost.rows + right_cost.rows,
                    crate::JoinType::Cross => left_cost.rows * right_cost.rows,
                };

                let cpu = left_cost.rows * right_cost.rows; // Comparison cost
                let io = left_cost.io + right_cost.io;
                let memory = right_cost.rows * 100.0; // Assume hash table for right side

                PlanCost::new(rows, cpu, io, memory)
            }

            LogicalPlan::Aggregate { input, group_by, .. } => {
                let input_cost = self.estimate(input);

                // Estimate distinct groups
                let groups = if group_by.is_empty() {
                    1.0
                } else {
                    (input_cost.rows / 10.0).max(1.0) // Rough estimate
                };

                PlanCost::new(
                    groups,
                    input_cost.cpu + input_cost.rows * 2.0, // Hashing + aggregation
                    input_cost.io,
                    input_cost.rows * 50.0, // Hash table
                )
            }

            LogicalPlan::Sort { input, order_by: _ } => {
                let input_cost = self.estimate(input);
                let n = input_cost.rows;

                // Sort cost is O(n log n)
                let sort_cpu = if n > 0.0 {
                    n * n.log2()
                } else {
                    0.0
                };

                PlanCost::new(
                    n,
                    input_cost.cpu + sort_cpu,
                    input_cost.io,
                    n * 100.0, // Memory for sorting
                )
            }

            LogicalPlan::Limit { input, limit, .. } => {
                let input_cost = self.estimate(input);
                let rows = (input_cost.rows).min(*limit as f64);

                PlanCost::new(
                    rows,
                    input_cost.cpu,
                    input_cost.io,
                    input_cost.memory,
                )
            }

            _ => PlanCost::default(),
        }
    }

    /// Estimate selectivity of a predicate (0.0 to 1.0)
    fn estimate_selectivity(&self, expr: &Expr) -> f64 {
        match expr {
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Eq => 0.1,      // Equality is selective
                BinaryOperator::NotEq => 0.9,
                BinaryOperator::Lt | BinaryOperator::Gt => 0.33,
                BinaryOperator::LtEq | BinaryOperator::GtEq => 0.5,
                BinaryOperator::And => 0.25,    // Compound condition
                BinaryOperator::Or => 0.75,
                BinaryOperator::Like => 0.2,
                _ => 0.5,
            },
            Expr::IsNull(_) => 0.05,
            Expr::IsNotNull(_) => 0.95,
            Expr::InList { list, .. } => (list.len() as f64 * 0.1).min(0.8),
            Expr::Between { .. } => 0.25,
            Expr::Literal(Value::Boolean(true)) => 1.0,
            Expr::Literal(Value::Boolean(false)) => 0.0,
            _ => 0.5,
        }
    }
}

/// Pretty print a logical plan
pub fn format_plan(plan: &LogicalPlan, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    let mut result = String::new();

    match plan {
        LogicalPlan::Scan {
            table,
            filter,
            projection,
            ..
        } => {
            result.push_str(&format!("{}Scan: {}", prefix, table));
            if let Some(proj) = projection {
                result.push_str(&format!(" [cols: {:?}]", proj));
            }
            if let Some(f) = filter {
                result.push_str(&format!(" [filter: {:?}]", f));
            }
            result.push('\n');
        }

        LogicalPlan::Project { input, exprs, .. } => {
            result.push_str(&format!("{}Project: {} exprs\n", prefix, exprs.len()));
            result.push_str(&format_plan(input, indent + 1));
        }

        LogicalPlan::Filter { input, predicate } => {
            result.push_str(&format!("{}Filter: {:?}\n", prefix, predicate));
            result.push_str(&format_plan(input, indent + 1));
        }

        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            result.push_str(&format!("{}Join: {:?} on {:?}\n", prefix, join_type, condition));
            result.push_str(&format_plan(left, indent + 1));
            result.push_str(&format_plan(right, indent + 1));
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            result.push_str(&format!(
                "{}Aggregate: {} groups, {} aggs\n",
                prefix,
                group_by.len(),
                aggregates.len()
            ));
            result.push_str(&format_plan(input, indent + 1));
        }

        LogicalPlan::Sort { input, order_by } => {
            result.push_str(&format!("{}Sort: {} keys\n", prefix, order_by.len()));
            result.push_str(&format_plan(input, indent + 1));
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            result.push_str(&format!("{}Limit: {} offset {}\n", prefix, limit, offset));
            result.push_str(&format_plan(input, indent + 1));
        }

        LogicalPlan::Insert { table, columns, values } => {
            result.push_str(&format!(
                "{}Insert: {} ({} cols, {} rows)\n",
                prefix,
                table,
                columns.len(),
                values.len()
            ));
        }

        LogicalPlan::Update { table, assignments, filter: _ } => {
            result.push_str(&format!(
                "{}Update: {} ({} assignments)\n",
                prefix,
                table,
                assignments.len()
            ));
        }

        LogicalPlan::Delete { table, filter: _ } => {
            result.push_str(&format!("{}Delete: {}\n", prefix, table));
        }

        LogicalPlan::CreateTable { name, columns, .. } => {
            result.push_str(&format!("{}CreateTable: {} ({} cols)\n", prefix, name, columns.len()));
        }

        LogicalPlan::DropTable { name, .. } => {
            result.push_str(&format!("{}DropTable: {}\n", prefix, name));
        }

        LogicalPlan::CreateIndex { name, table, columns, unique, .. } => {
            result.push_str(&format!(
                "{}CreateIndex: {} on {} ({:?}) unique={}\n",
                prefix, name, table, columns, unique
            ));
        }

        LogicalPlan::DropIndex { name, .. } => {
            result.push_str(&format!("{}DropIndex: {}\n", prefix, name));
        }

        LogicalPlan::AlterTable { table, operations } => {
            result.push_str(&format!("{}AlterTable: {} ({} ops)\n", prefix, table, operations.len()));
        }

        LogicalPlan::Explain { plan: inner, analyze, verbose } => {
            result.push_str(&format!("{}Explain: analyze={} verbose={}\n", prefix, analyze, verbose));
            result.push_str(&format_plan(inner, indent + 1));
        }

        LogicalPlan::AnalyzeTable { table } => {
            result.push_str(&format!("{}Analyze: {:?}\n", prefix, table));
        }

        LogicalPlan::Empty => {
            result.push_str(&format!("{}Empty\n", prefix));
        }
    }

    result
}

/// Check if a plan contains any subqueries
pub fn has_subquery(plan: &LogicalPlan) -> bool {
    struct SubqueryChecker {
        found: bool,
    }

    impl PlanVisitor for SubqueryChecker {
        fn pre_visit(&mut self, plan: &LogicalPlan) -> bool {
            if let LogicalPlan::Filter { predicate, .. } = plan {
                if expr_has_subquery(predicate) {
                    self.found = true;
                    return false;
                }
            }
            if let LogicalPlan::Project { exprs, .. } = plan {
                for expr in exprs {
                    if expr_has_subquery(expr) {
                        self.found = true;
                        return false;
                    }
                }
            }
            !self.found
        }
    }

    let mut checker = SubqueryChecker { found: false };
    walk_plan(plan, &mut checker);
    checker.found
}

fn expr_has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_has_subquery(left) || expr_has_subquery(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_subquery(expr),
        Expr::Function { args, .. } => args.iter().any(expr_has_subquery),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_ref().map_or(false, |e| expr_has_subquery(e))
                || when_clauses.iter().any(|(c, t)| expr_has_subquery(c) || expr_has_subquery(t))
                || else_clause.as_ref().map_or(false, |e| expr_has_subquery(e))
        }
        Expr::InList { expr, list, .. } => {
            expr_has_subquery(expr) || list.iter().any(expr_has_subquery)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use std::sync::Arc;

    fn test_catalog() -> Arc<Catalog> {
        let catalog = Arc::new(Catalog::new());
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
        ]);
        catalog.register_table("users", schema).unwrap();

        // Update stats
        catalog
            .update_table_stats(
                "users",
                crate::catalog::TableStats {
                    row_count: 10000,
                    size_bytes: 1_000_000,
                    last_updated: 0,
                },
            )
            .unwrap();

        catalog
    }

    #[test]
    fn test_table_collector() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    schema: Schema::empty(),
                    filter: None,
                    projection: None,
                }),
                right: Box::new(LogicalPlan::Scan {
                    table: "orders".to_string(),
                    schema: Schema::empty(),
                    filter: None,
                    projection: None,
                }),
                condition: Expr::Literal(Value::Boolean(true)),
                join_type: crate::JoinType::Inner,
            }),
            exprs: vec![],
            schema: Schema::empty(),
        };

        let tables = TableCollector::collect(&plan);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_column_collector() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "id".to_string(),
                index: None,
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Column {
                name: "min_id".to_string(),
                index: None,
            }),
        };

        let columns = ColumnCollector::collect_from_expr(&expr);
        assert!(columns.contains("id"));
        assert!(columns.contains("min_id"));
    }

    #[test]
    fn test_cost_estimation() {
        let catalog = test_catalog();
        let estimator = CostEstimator::new(catalog.as_ref());

        let scan = LogicalPlan::Scan {
            table: "users".to_string(),
            schema: Schema::empty(),
            filter: None,
            projection: None,
        };

        let cost = estimator.estimate(&scan);
        assert_eq!(cost.rows, 10000.0);
    }

    #[test]
    fn test_cost_with_filter() {
        let catalog = test_catalog();
        let estimator = CostEstimator::new(catalog.as_ref());

        let scan = LogicalPlan::Scan {
            table: "users".to_string(),
            schema: Schema::empty(),
            filter: Some(Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "id".to_string(),
                    index: None,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(Value::Int64(1))),
            })),
            projection: None,
        };

        let cost = estimator.estimate(&scan);
        assert!(cost.rows < 10000.0); // Filter reduces rows
    }

    #[test]
    fn test_format_plan() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    schema: Schema::empty(),
                    filter: None,
                    projection: None,
                }),
                predicate: Expr::Literal(Value::Boolean(true)),
            }),
            exprs: vec![Expr::Column {
                name: "id".to_string(),
                index: None,
            }],
            schema: Schema::empty(),
        };

        let formatted = format_plan(&plan, 0);
        assert!(formatted.contains("Project"));
        assert!(formatted.contains("Filter"));
        assert!(formatted.contains("Scan"));
    }

    #[test]
    fn test_selectivity_estimation() {
        let catalog = test_catalog();
        let estimator = CostEstimator::new(catalog.as_ref());

        // Equality is selective
        let eq_sel = estimator.estimate_selectivity(&Expr::BinaryOp {
            left: Box::new(Expr::Column { name: "id".to_string(), index: None }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Value::Int64(1))),
        });
        assert!(eq_sel < 0.5);

        // IS NULL is very selective
        let null_sel = estimator.estimate_selectivity(&Expr::IsNull(Box::new(
            Expr::Column { name: "x".to_string(), index: None },
        )));
        assert!(null_sel < 0.1);
    }
}
