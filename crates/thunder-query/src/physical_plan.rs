//! Physical Planner
//!
//! Converts logical plans to physical plans, selecting the best execution
//! strategy based on cost estimation and available indexes.

use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;
use thunder_sql::{
    catalog::{CatalogProvider, IndexInfo, IndexType},
    AggregateExpr, Expr, JoinType, LogicalPlan,
};

use crate::{PhysicalPlan, ScanRange};

/// Physical planner that converts logical plans to physical plans
pub struct DefaultPhysicalPlanner {
    /// Catalog for looking up table/index info
    catalog: Arc<dyn CatalogProvider>,
    /// Planner configuration
    config: PlannerConfig,
}

/// Configuration for the physical planner
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Threshold for switching from nested loop to hash join
    pub hash_join_threshold: usize,
    /// Threshold for switching from hash to merge join
    pub merge_join_threshold: usize,
    /// Whether to prefer index scans
    pub prefer_index_scan: bool,
    /// Memory limit for hash operations
    pub hash_memory_limit: usize,
    /// Enable parallel execution
    pub enable_parallel: bool,
    /// Number of partitions for parallel execution
    pub num_partitions: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            hash_join_threshold: 1000,
            merge_join_threshold: 100_000,
            prefer_index_scan: true,
            hash_memory_limit: 64 * 1024 * 1024, // 64MB
            enable_parallel: true,
            num_partitions: 4,
        }
    }
}

impl DefaultPhysicalPlanner {
    /// Create a new physical planner
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            catalog,
            config: PlannerConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(catalog: Arc<dyn CatalogProvider>, config: PlannerConfig) -> Self {
        Self { catalog, config }
    }

    /// Convert a logical plan to a physical plan
    pub fn plan(&self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        match logical {
            LogicalPlan::Scan {
                table,
                schema,
                filter,
                projection,
            } => self.plan_scan(&table, schema, filter, projection),

            LogicalPlan::Filter { input, predicate } => {
                let input_plan = self.plan(*input)?;
                Ok(PhysicalPlan::Filter {
                    input: Box::new(input_plan),
                    predicate,
                })
            }

            LogicalPlan::Project { input, exprs, schema } => {
                let input_plan = self.plan(*input)?;
                Ok(PhysicalPlan::Project {
                    input: Box::new(input_plan),
                    exprs,
                    schema,
                })
            }

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => self.plan_join(*left, *right, condition, join_type),

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => self.plan_aggregate(*input, group_by, aggregates, schema),

            LogicalPlan::Sort { input, order_by } => {
                let input_plan = self.plan(*input)?;
                Ok(PhysicalPlan::Sort {
                    input: Box::new(input_plan),
                    order_by,
                })
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_plan = self.plan(*input)?;
                Ok(PhysicalPlan::Limit {
                    input: Box::new(input_plan),
                    limit,
                    offset,
                })
            }

            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => self.plan_insert(&table, columns, values),

            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => self.plan_update(&table, assignments, filter),

            LogicalPlan::Delete { table, filter } => self.plan_delete(&table, filter),

            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::AnalyzeTable { .. } => {
                // DDL/utility operations are handled separately by the engine
                Err(Error::Internal(
                    "DDL/utility operations should be handled by schema manager".to_string(),
                ))
            }

            LogicalPlan::Empty => Ok(PhysicalPlan::Empty {
                schema: Schema::empty(),
            }),
        }
    }

    /// Plan a table scan operation
    fn plan_scan(
        &self,
        table_name: &str,
        schema: Schema,
        filter: Option<Box<Expr>>,
        _projection: Option<Vec<usize>>,
    ) -> Result<PhysicalPlan> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        // Check if we can use an index
        if self.config.prefer_index_scan {
            if let Some(filter_expr) = &filter {
                if let Some(index_scan) =
                    self.try_plan_index_scan(&table_info.id, &schema, filter_expr)?
                {
                    return Ok(index_scan);
                }
            }
        }

        // Default to sequential scan
        Ok(PhysicalPlan::SeqScan {
            table_id: table_info.id,
            schema,
            filter: filter.map(|f| *f),
        })
    }

    /// Try to plan an index scan
    fn try_plan_index_scan(
        &self,
        table_id: &TableId,
        schema: &Schema,
        filter: &Expr,
    ) -> Result<Option<PhysicalPlan>> {
        let indexes = self.catalog.get_table_indexes(*table_id);

        // Find a suitable index
        for index in indexes {
            if let Some(scan_range) = self.extract_scan_range(filter, &index, schema) {
                return Ok(Some(PhysicalPlan::IndexScan {
                    table_id: *table_id,
                    index_id: index.id,
                    schema: schema.clone(),
                    range: Some(scan_range),
                }));
            }
        }

        Ok(None)
    }

    /// Extract a scan range from a filter expression
    fn extract_scan_range(
        &self,
        filter: &Expr,
        index: &IndexInfo,
        schema: &Schema,
    ) -> Option<ScanRange> {
        // Only support B-tree indexes for range scans
        if index.index_type != IndexType::BTree {
            return None;
        }

        // Only support single-column indexes for now
        if index.columns.len() != 1 {
            return None;
        }

        let index_col_idx = index.columns[0].column_index;
        let index_col_name = schema
            .columns
            .get(index_col_idx)
            .map(|c| c.name.as_str())?;

        // Look for simple equality or range predicates
        match filter {
            Expr::BinaryOp { left, op, right } => {
                // Check if left is the indexed column
                if let Expr::Column { name, .. } = left.as_ref() {
                    if name == index_col_name {
                        if let Expr::Literal(value) = right.as_ref() {
                            let key = self.value_to_bytes(value)?;
                            return Some(match op {
                                thunder_sql::BinaryOperator::Eq => ScanRange {
                                    start: Some(key.clone()),
                                    end: Some(key),
                                    start_inclusive: true,
                                    end_inclusive: true,
                                },
                                thunder_sql::BinaryOperator::Gt => ScanRange {
                                    start: Some(key),
                                    end: None,
                                    start_inclusive: false,
                                    end_inclusive: false,
                                },
                                thunder_sql::BinaryOperator::GtEq => ScanRange {
                                    start: Some(key),
                                    end: None,
                                    start_inclusive: true,
                                    end_inclusive: false,
                                },
                                thunder_sql::BinaryOperator::Lt => ScanRange {
                                    start: None,
                                    end: Some(key),
                                    start_inclusive: false,
                                    end_inclusive: false,
                                },
                                thunder_sql::BinaryOperator::LtEq => ScanRange {
                                    start: None,
                                    end: Some(key),
                                    start_inclusive: false,
                                    end_inclusive: true,
                                },
                                _ => return None,
                            });
                        }
                    }
                }
                None
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    return None;
                }
                if let Expr::Column { name, .. } = expr.as_ref() {
                    if name == index_col_name {
                        if let (Expr::Literal(low_val), Expr::Literal(high_val)) =
                            (low.as_ref(), high.as_ref())
                        {
                            let start_key = self.value_to_bytes(low_val)?;
                            let end_key = self.value_to_bytes(high_val)?;
                            return Some(ScanRange {
                                start: Some(start_key),
                                end: Some(end_key),
                                start_inclusive: true,
                                end_inclusive: true,
                            });
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Convert a value to bytes for index comparison
    fn value_to_bytes(&self, value: &Value) -> Option<Vec<u8>> {
        Some(match value {
            Value::Int64(n) => n.to_be_bytes().to_vec(),
            Value::Int32(n) => n.to_be_bytes().to_vec(),
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
            _ => return None,
        })
    }

    /// Plan a join operation
    fn plan_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Result<PhysicalPlan> {
        let left_plan = self.plan(left)?;
        let right_plan = self.plan(right)?;

        // Try to extract equi-join keys
        if let Some((left_keys, right_keys)) = self.extract_join_keys(&condition) {
            // Estimate sizes to choose join strategy
            let left_size = self.estimate_plan_size(&left_plan);
            let right_size = self.estimate_plan_size(&right_plan);

            // Choose join strategy based on size
            if left_size < self.config.hash_join_threshold
                && right_size < self.config.hash_join_threshold
            {
                // Nested loop for small tables
                return Ok(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    condition,
                    join_type,
                });
            } else if left_size > self.config.merge_join_threshold
                || right_size > self.config.merge_join_threshold
            {
                // Merge join for very large sorted data
                return Ok(PhysicalPlan::MergeJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    left_keys,
                    right_keys,
                    join_type,
                });
            } else {
                // Hash join for medium-sized tables
                return Ok(PhysicalPlan::HashJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    left_keys,
                    right_keys,
                    join_type,
                });
            }
        }

        // Fall back to nested loop join for non-equi joins
        Ok(PhysicalPlan::NestedLoopJoin {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            condition,
            join_type,
        })
    }

    /// Extract join keys from an equi-join condition
    fn extract_join_keys(&self, condition: &Expr) -> Option<(Vec<usize>, Vec<usize>)> {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        self.collect_join_keys(condition, &mut left_keys, &mut right_keys);

        if left_keys.is_empty() {
            None
        } else {
            Some((left_keys, right_keys))
        }
    }

    /// Recursively collect join keys from AND conditions
    fn collect_join_keys(
        &self,
        condition: &Expr,
        left_keys: &mut Vec<usize>,
        right_keys: &mut Vec<usize>,
    ) {
        match condition {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    thunder_sql::BinaryOperator::And => {
                        self.collect_join_keys(left, left_keys, right_keys);
                        self.collect_join_keys(right, left_keys, right_keys);
                    }
                    thunder_sql::BinaryOperator::Eq => {
                        // Check for column = column pattern
                        if let (
                            Expr::Column { index: Some(l_idx), .. },
                            Expr::Column { index: Some(r_idx), .. },
                        ) = (left.as_ref(), right.as_ref())
                        {
                            left_keys.push(*l_idx);
                            right_keys.push(*r_idx);
                        }
                        // Also handle name-based column references
                        else if let (
                            Expr::Column { name: _l_name, .. },
                            Expr::Column { name: _r_name, .. },
                        ) = (left.as_ref(), right.as_ref())
                        {
                            // Use simple heuristic: assume first reference is left table
                            // This is a simplification - real planner would track table aliases
                            left_keys.push(left_keys.len());
                            right_keys.push(right_keys.len());
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    /// Plan an aggregate operation
    fn plan_aggregate(
        &self,
        input: LogicalPlan,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
    ) -> Result<PhysicalPlan> {
        let input_plan = self.plan(input)?;

        // Convert group_by expressions to column indices
        let group_by_indices: Vec<usize> = group_by
            .iter()
            .filter_map(|expr| {
                if let Expr::Column { index: Some(idx), .. } = expr {
                    Some(*idx)
                } else if let Expr::Column { name, .. } = expr {
                    // Find column index by name
                    self.find_column_index(&input_plan, name)
                } else {
                    None
                }
            })
            .collect();

        Ok(PhysicalPlan::HashAggregate {
            input: Box::new(input_plan),
            group_by: group_by_indices,
            aggregates,
            schema,
        })
    }

    /// Find column index by name in a plan's schema
    fn find_column_index(&self, plan: &PhysicalPlan, name: &str) -> Option<usize> {
        let schema = self.plan_schema(plan).ok()?;
        schema.column_by_name(name).map(|(i, _)| i)
    }

    /// Get schema of a physical plan
    fn plan_schema(&self, plan: &PhysicalPlan) -> Result<Schema> {
        match plan {
            PhysicalPlan::SeqScan { schema, .. } => Ok(schema.clone()),
            PhysicalPlan::IndexScan { schema, .. } => Ok(schema.clone()),
            PhysicalPlan::Project { schema, .. } => Ok(schema.clone()),
            PhysicalPlan::HashAggregate { schema, .. } => Ok(schema.clone()),
            PhysicalPlan::Filter { input, .. } => self.plan_schema(input),
            PhysicalPlan::Sort { input, .. } => self.plan_schema(input),
            PhysicalPlan::Limit { input, .. } => self.plan_schema(input),
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::MergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                let left_schema = self.plan_schema(left)?;
                let right_schema = self.plan_schema(right)?;
                let mut columns = left_schema.columns;
                columns.extend(right_schema.columns);
                Ok(Schema::new(columns))
            }
            PhysicalPlan::Values { schema, .. } => Ok(schema.clone()),
            PhysicalPlan::Empty { schema } => Ok(schema.clone()),
            PhysicalPlan::Exchange { input, .. } => self.plan_schema(input),
            PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => {
                Ok(Schema::empty())
            }
        }
    }

    /// Estimate the output size of a plan
    fn estimate_plan_size(&self, plan: &PhysicalPlan) -> usize {
        match plan {
            PhysicalPlan::SeqScan {  .. } => {
                // Default estimate - real implementation would use catalog stats
                10000
            }
            PhysicalPlan::IndexScan { table_id, range, .. } => {
                // Index scans are typically more selective
                let base_size = self.estimate_plan_size(&PhysicalPlan::SeqScan {
                    table_id: *table_id,
                    schema: Schema::empty(),
                    filter: None,
                });
                if range.is_some() {
                    base_size / 10 // Assume 10% selectivity for range scans
                } else {
                    base_size
                }
            }
            PhysicalPlan::Filter { input, .. } => {
                // Assume 50% selectivity for filters
                self.estimate_plan_size(input) / 2
            }
            PhysicalPlan::Project { input, .. } => self.estimate_plan_size(input),
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::MergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // Estimate join output size
                let left_size = self.estimate_plan_size(left);
                let right_size = self.estimate_plan_size(right);
                (left_size * right_size) / 100 // Assume 1% match rate
            }
            PhysicalPlan::HashAggregate { input, group_by, .. } => {
                let input_size = self.estimate_plan_size(input);
                if group_by.is_empty() {
                    1 // Scalar aggregate
                } else {
                    input_size / 10 // Assume 10% unique groups
                }
            }
            PhysicalPlan::Sort { input, .. } => self.estimate_plan_size(input),
            PhysicalPlan::Limit { limit, .. } => *limit,
            PhysicalPlan::Values { values, .. } => values.len(),
            PhysicalPlan::Empty { .. } => 0,
            _ => 10000,
        }
    }

    /// Plan an INSERT operation
    fn plan_insert(
        &self,
        table_name: &str,
        _columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    ) -> Result<PhysicalPlan> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        // Convert expressions to values
        let value_rows: Vec<Vec<Value>> = values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| {
                        if let Expr::Literal(v) = expr {
                            v.clone()
                        } else {
                            Value::Null // Simplified - real implementation would evaluate
                        }
                    })
                    .collect()
            })
            .collect();

        let input = PhysicalPlan::Values {
            values: value_rows,
            schema: table_info.schema.clone(),
        };

        Ok(PhysicalPlan::Insert {
            table_id: table_info.id,
            input: Box::new(input),
        })
    }

    /// Plan an UPDATE operation
    fn plan_update(
        &self,
        table_name: &str,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    ) -> Result<PhysicalPlan> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        // Convert assignments to column indices
        let indexed_assignments: Vec<(usize, Expr)> = assignments
            .into_iter()
            .filter_map(|(col_name, expr)| {
                table_info
                    .schema
                    .column_by_name(&col_name)
                    .map(|(i, _)| (i, expr))
            })
            .collect();

        // Create scan with filter
        let scan = PhysicalPlan::SeqScan {
            table_id: table_info.id,
            schema: table_info.schema.clone(),
            filter,
        };

        Ok(PhysicalPlan::Update {
            table_id: table_info.id,
            input: Box::new(scan),
            assignments: indexed_assignments,
        })
    }

    /// Plan a DELETE operation
    fn plan_delete(&self, table_name: &str, filter: Option<Expr>) -> Result<PhysicalPlan> {
        let table_info = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::Sql(SqlError::TableNotFound(table_name.to_string())))?;

        // Create scan with filter
        let scan = PhysicalPlan::SeqScan {
            table_id: table_info.id,
            schema: table_info.schema.clone(),
            filter,
        };

        Ok(PhysicalPlan::Delete {
            table_id: table_info.id,
            input: Box::new(scan),
        })
    }
}


impl crate::PhysicalPlanner for DefaultPhysicalPlanner {
    fn plan(&self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        DefaultPhysicalPlanner::plan(self, logical)
    }
}

/// Physical plan visitor for analysis and optimization
pub trait PhysicalPlanVisitor {
    fn visit(&mut self, _plan: &PhysicalPlan) -> bool {
        true
    }
}

/// Walk a physical plan tree
pub fn walk_physical_plan<V: PhysicalPlanVisitor>(plan: &PhysicalPlan, visitor: &mut V) {
    if !visitor.visit(plan) {
        return;
    }

    match plan {
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Exchange { input, .. }
        | PhysicalPlan::Insert { input, .. }
        | PhysicalPlan::Update { input, .. }
        | PhysicalPlan::Delete { input, .. } => {
            walk_physical_plan(input, visitor);
        }
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::MergeJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            walk_physical_plan(left, visitor);
            walk_physical_plan(right, visitor);
        }
        PhysicalPlan::SeqScan { .. }
        | PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::Values { .. }
        | PhysicalPlan::Empty { .. } => {}
    }
}

/// Format a physical plan for display
pub fn format_physical_plan(plan: &PhysicalPlan, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    let mut output = String::new();

    match plan {
        PhysicalPlan::SeqScan { table_id, filter, .. } => {
            output.push_str(&format!("{}SeqScan [table_id={}]", prefix, table_id.0));
            if filter.is_some() {
                output.push_str(" (with filter)");
            }
        }
        PhysicalPlan::IndexScan { table_id, index_id, range, .. } => {
            output.push_str(&format!(
                "{}IndexScan [table_id={}, index_id={}]",
                prefix, table_id.0, index_id.0
            ));
            if range.is_some() {
                output.push_str(" (with range)");
            }
        }
        PhysicalPlan::Filter { input, .. } => {
            output.push_str(&format!("{}Filter\n", prefix));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Project { input, exprs, .. } => {
            output.push_str(&format!("{}Project [{} exprs]\n", prefix, exprs.len()));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::HashJoin { left, right, join_type, .. } => {
            output.push_str(&format!("{}HashJoin [{:?}]\n", prefix, join_type));
            output.push_str(&format_physical_plan(left, indent + 1));
            output.push_str("\n");
            output.push_str(&format_physical_plan(right, indent + 1));
        }
        PhysicalPlan::MergeJoin { left, right, join_type, .. } => {
            output.push_str(&format!("{}MergeJoin [{:?}]\n", prefix, join_type));
            output.push_str(&format_physical_plan(left, indent + 1));
            output.push_str("\n");
            output.push_str(&format_physical_plan(right, indent + 1));
        }
        PhysicalPlan::NestedLoopJoin { left, right, join_type, .. } => {
            output.push_str(&format!("{}NestedLoopJoin [{:?}]\n", prefix, join_type));
            output.push_str(&format_physical_plan(left, indent + 1));
            output.push_str("\n");
            output.push_str(&format_physical_plan(right, indent + 1));
        }
        PhysicalPlan::HashAggregate { input, group_by, aggregates, .. } => {
            output.push_str(&format!(
                "{}HashAggregate [group_by={:?}, aggs={}]\n",
                prefix,
                group_by,
                aggregates.len()
            ));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Sort { input, order_by } => {
            output.push_str(&format!("{}Sort [{} keys]\n", prefix, order_by.len()));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Limit { input, limit, offset } => {
            output.push_str(&format!("{}Limit [limit={}, offset={}]\n", prefix, limit, offset));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Exchange { input, partitioning } => {
            output.push_str(&format!("{}Exchange [{:?}]\n", prefix, partitioning));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Values { values, .. } => {
            output.push_str(&format!("{}Values [{} rows]", prefix, values.len()));
        }
        PhysicalPlan::Empty { .. } => {
            output.push_str(&format!("{}Empty", prefix));
        }
        PhysicalPlan::Insert { table_id, input } => {
            output.push_str(&format!("{}Insert [table_id={}]\n", prefix, table_id.0));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Update { table_id, input, assignments } => {
            output.push_str(&format!(
                "{}Update [table_id={}, {} assignments]\n",
                prefix,
                table_id.0,
                assignments.len()
            ));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
        PhysicalPlan::Delete { table_id, input } => {
            output.push_str(&format!("{}Delete [table_id={}]\n", prefix, table_id.0));
            output.push_str(&format_physical_plan(input, indent + 1));
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use thunder_sql::catalog::Catalog;

    fn test_catalog() -> Arc<Catalog> {
        let catalog = Catalog::new();

        let users_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        catalog
            .register_table_with_pk("users", users_schema, vec![0])
            .unwrap();

        let orders_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        catalog
            .register_table_with_pk("orders", orders_schema, vec![0])
            .unwrap();

        Arc::new(catalog)
    }

    #[test]
    fn test_plan_simple_scan() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog.clone());

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        let logical = LogicalPlan::Scan {
            table: "users".to_string(),
            schema,
            filter: None,
            projection: None,
        };

        let physical = planner.plan(logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn test_plan_filter() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        let logical = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema,
                filter: None,
                projection: None,
            }),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "age".to_string(),
                    index: None,
                }),
                op: thunder_sql::BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Value::Int32(25))),
            },
        };

        let physical = planner.plan(logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn test_plan_project() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let input_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        let output_schema = Schema::new(vec![
            ColumnDef::new("name", DataType::String),
        ]);

        let logical = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema: input_schema,
                filter: None,
                projection: None,
            }),
            exprs: vec![Expr::Column {
                name: "name".to_string(),
                index: None,
            }],
            schema: output_schema,
        };

        let physical = planner.plan(logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::Project { .. }));
    }

    #[test]
    fn test_plan_join() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let users_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
        ]);

        let orders_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema: users_schema,
                filter: None,
                projection: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "orders".to_string(),
                schema: orders_schema,
                filter: None,
                projection: None,
            }),
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "id".to_string(),
                    index: Some(0),
                }),
                op: thunder_sql::BinaryOperator::Eq,
                right: Box::new(Expr::Column {
                    name: "user_id".to_string(),
                    index: Some(1),
                }),
            },
            join_type: JoinType::Inner,
        };

        let physical = planner.plan(logical).unwrap();
        // Should be hash join for medium-sized tables (default estimate)
        assert!(matches!(
            physical,
            PhysicalPlan::HashJoin { .. } | PhysicalPlan::NestedLoopJoin { .. }
        ));
    }

    #[test]
    fn test_plan_aggregate() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        let output_schema = Schema::new(vec![
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("count", DataType::Int64),
        ]);

        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema,
                filter: None,
                projection: None,
            }),
            group_by: vec![Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            }],
            aggregates: vec![AggregateExpr {
                func: AggregateFunction::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
            }],
            schema: output_schema,
        };

        let physical = planner.plan(logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::HashAggregate { .. }));
    }

    #[test]
    fn test_plan_sort_limit() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ]);

        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    schema,
                    filter: None,
                    projection: None,
                }),
                order_by: vec![SortExpr {
                    expr: Expr::Column {
                        name: "age".to_string(),
                        index: None,
                    },
                    asc: false,
                    nulls_first: false,
                }],
            }),
            limit: 10,
            offset: 0,
        };

        let physical = planner.plan(logical).unwrap();
        assert!(matches!(physical, PhysicalPlan::Limit { .. }));

        if let PhysicalPlan::Limit { input, .. } = physical {
            assert!(matches!(*input, PhysicalPlan::Sort { .. }));
        }
    }

    #[test]
    fn test_format_plan() {
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Sort {
                input: Box::new(PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::SeqScan {
                        table_id: TableId(1),
                        schema: Schema::empty(),
                        filter: None,
                    }),
                    predicate: Expr::Literal(Value::Boolean(true)),
                }),
                order_by: vec![],
            }),
            limit: 10,
            offset: 0,
        };

        let formatted = format_physical_plan(&plan, 0);
        assert!(formatted.contains("Limit"));
        assert!(formatted.contains("Sort"));
        assert!(formatted.contains("Filter"));
        assert!(formatted.contains("SeqScan"));
    }

    #[test]
    fn test_estimate_size() {
        let catalog = test_catalog();
        let planner = DefaultPhysicalPlanner::new(catalog);

        let scan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: Schema::empty(),
            filter: None,
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan.clone()),
            predicate: Expr::Literal(Value::Boolean(true)),
        };

        let limit = PhysicalPlan::Limit {
            input: Box::new(filter.clone()),
            limit: 10,
            offset: 0,
        };

        let scan_size = planner.estimate_plan_size(&scan);
        let filter_size = planner.estimate_plan_size(&filter);
        let limit_size = planner.estimate_plan_size(&limit);

        // Filter should reduce estimated size
        assert!(filter_size <= scan_size);
        // Limit should cap the size
        assert_eq!(limit_size, 10);
    }
}
