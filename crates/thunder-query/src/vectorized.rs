//! Vectorized Execution
//!
//! Provides batch-oriented query execution for improved CPU cache utilization
//! and SIMD optimization opportunities. Processes data in batches rather than
//! row-by-row for better performance.

use std::collections::HashMap;
use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;
use thunder_sql::{AggregateExpr, AggregateFunction, BinaryOperator, Expr, SortExpr, UnaryOperator};

use crate::{ExecutionStats, PhysicalPlan, RecordBatch};
use crate::executor::{Aggregator, ExprEvaluator};

/// Batch size for vectorized execution
pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// A column vector for vectorized processing
#[derive(Debug, Clone)]
pub struct ColumnVector {
    /// Data type of the column
    pub data_type: DataType,
    /// Values in the column
    pub values: Vec<Value>,
    /// Null bitmap (true = null)
    pub nulls: Vec<bool>,
}

impl ColumnVector {
    /// Create a new column vector
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            values: Vec::new(),
            nulls: Vec::new(),
        }
    }

    /// Create with capacity
    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        Self {
            data_type,
            values: Vec::with_capacity(capacity),
            nulls: Vec::with_capacity(capacity),
        }
    }

    /// Create from values
    pub fn from_values(data_type: DataType, values: Vec<Value>) -> Self {
        let nulls: Vec<bool> = values.iter().map(|v| v.is_null()).collect();
        Self {
            data_type,
            values,
            nulls,
        }
    }

    /// Push a value
    pub fn push(&mut self, value: Value) {
        self.nulls.push(value.is_null());
        self.values.push(value);
    }

    /// Get value at index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Check if value at index is null
    pub fn is_null(&self, index: usize) -> bool {
        self.nulls.get(index).copied().unwrap_or(true)
    }

    /// Length of the column
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Clear the column
    pub fn clear(&mut self) {
        self.values.clear();
        self.nulls.clear();
    }
}

/// A batch of column vectors
#[derive(Debug, Clone)]
pub struct VectorBatch {
    /// Schema of the batch
    pub schema: Schema,
    /// Column vectors
    pub columns: Vec<ColumnVector>,
    /// Number of rows
    pub num_rows: usize,
}

impl VectorBatch {
    /// Create an empty batch
    pub fn new(schema: Schema) -> Self {
        let columns: Vec<ColumnVector> = schema
            .columns
            .iter()
            .map(|col| ColumnVector::new(col.data_type.clone()))
            .collect();

        Self {
            schema,
            columns,
            num_rows: 0,
        }
    }

    /// Create from rows
    pub fn from_rows(schema: Schema, rows: &[Row]) -> Self {
        let mut columns: Vec<ColumnVector> = schema
            .columns
            .iter()
            .map(|col| ColumnVector::with_capacity(col.data_type.clone(), rows.len()))
            .collect();

        for row in rows {
            for (i, value) in row.values.iter().enumerate() {
                if let Some(col) = columns.get_mut(i) {
                    col.push(value.clone());
                }
            }
        }

        Self {
            schema,
            columns,
            num_rows: rows.len(),
        }
    }

    /// Convert to rows
    pub fn to_rows(&self) -> Vec<Row> {
        let mut rows = Vec::with_capacity(self.num_rows);
        for i in 0..self.num_rows {
            let values: Vec<Value> = self
                .columns
                .iter()
                .map(|col| col.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            rows.push(Row::new(values));
        }
        rows
    }

    /// Get a row at index
    pub fn get_row(&self, index: usize) -> Option<Row> {
        if index >= self.num_rows {
            return None;
        }
        let values: Vec<Value> = self
            .columns
            .iter()
            .map(|col| col.get(index).cloned().unwrap_or(Value::Null))
            .collect();
        Some(Row::new(values))
    }

    /// Get column by index
    pub fn column(&self, index: usize) -> Option<&ColumnVector> {
        self.columns.get(index)
    }

    /// Get column by name
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnVector> {
        self.schema
            .column_by_name(name)
            .map(|(i, _)| &self.columns[i])
    }

    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Append a row
    pub fn append_row(&mut self, row: &Row) {
        for (i, value) in row.values.iter().enumerate() {
            if let Some(col) = self.columns.get_mut(i) {
                col.push(value.clone());
            }
        }
        self.num_rows += 1;
    }

    /// Convert to RecordBatch
    pub fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::new(self.schema.clone(), self.to_rows())
    }
}

/// Vectorized expression evaluator
pub struct VectorizedEvaluator;

impl VectorizedEvaluator {
    /// Evaluate an expression on a batch, returning a column vector
    pub fn evaluate(expr: &Expr, batch: &VectorBatch) -> Result<ColumnVector> {
        match expr {
            Expr::Column { name, index } => {
                if let Some(idx) = index {
                    batch
                        .column(*idx)
                        .cloned()
                        .ok_or_else(|| Error::Internal(format!("Column index {} not found", idx)))
                } else {
                    batch
                        .column_by_name(name)
                        .cloned()
                        .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(name.clone())))
                }
            }

            Expr::Literal(value) => {
                // Broadcast literal to all rows
                let mut col = ColumnVector::with_capacity(value.data_type(), batch.num_rows);
                for _ in 0..batch.num_rows {
                    col.push(value.clone());
                }
                Ok(col)
            }

            Expr::BinaryOp { left, op, right } => {
                let left_col = Self::evaluate(left, batch)?;
                let right_col = Self::evaluate(right, batch)?;
                Self::eval_binary_vector(&left_col, op, &right_col)
            }

            Expr::UnaryOp { op, expr } => {
                let col = Self::evaluate(expr, batch)?;
                Self::eval_unary_vector(op, &col)
            }

            Expr::IsNull(inner) => {
                let col = Self::evaluate(inner, batch)?;
                let mut result = ColumnVector::with_capacity(DataType::Boolean, batch.num_rows);
                for i in 0..col.len() {
                    result.push(Value::Boolean(col.is_null(i)));
                }
                Ok(result)
            }

            Expr::IsNotNull(inner) => {
                let col = Self::evaluate(inner, batch)?;
                let mut result = ColumnVector::with_capacity(DataType::Boolean, batch.num_rows);
                for i in 0..col.len() {
                    result.push(Value::Boolean(!col.is_null(i)));
                }
                Ok(result)
            }

            Expr::Cast { expr, data_type } => {
                let col = Self::evaluate(expr, batch)?;
                Self::cast_vector(&col, data_type)
            }

            // For complex expressions, fall back to row-by-row evaluation
            _ => {
                let mut result = ColumnVector::new(DataType::Null);
                for i in 0..batch.num_rows {
                    let row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));
                    let value = ExprEvaluator::evaluate(expr, &row, &batch.schema)
                        .unwrap_or(Value::Null);
                    if result.data_type == DataType::Null {
                        result.data_type = value.data_type();
                    }
                    result.push(value);
                }
                Ok(result)
            }
        }
    }

    /// Evaluate binary operation on vectors
    fn eval_binary_vector(
        left: &ColumnVector,
        op: &BinaryOperator,
        right: &ColumnVector,
    ) -> Result<ColumnVector> {
        let len = left.len().min(right.len());
        let result_type = match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Like
            | BinaryOperator::ILike => DataType::Boolean,
            BinaryOperator::Concat => DataType::String,
            _ => left.data_type.clone(),
        };

        let mut result = ColumnVector::with_capacity(result_type, len);

        for i in 0..len {
            let l = left.get(i).cloned().unwrap_or(Value::Null);
            let r = right.get(i).cloned().unwrap_or(Value::Null);

            // Fast path for null handling
            if l.is_null() || r.is_null() {
                match op {
                    BinaryOperator::And if matches!(l, Value::Boolean(false)) || matches!(r, Value::Boolean(false)) => {
                        result.push(Value::Boolean(false));
                    }
                    BinaryOperator::Or if matches!(l, Value::Boolean(true)) || matches!(r, Value::Boolean(true)) => {
                        result.push(Value::Boolean(true));
                    }
                    _ => result.push(Value::Null),
                }
                continue;
            }

            // Fast path for integers
            let value = match (&l, op, &r) {
                // Integer arithmetic (inline for performance)
                (Value::Int64(a), BinaryOperator::Add, Value::Int64(b)) => Value::Int64(a + b),
                (Value::Int64(a), BinaryOperator::Sub, Value::Int64(b)) => Value::Int64(a - b),
                (Value::Int64(a), BinaryOperator::Mul, Value::Int64(b)) => Value::Int64(a * b),
                (Value::Int64(a), BinaryOperator::Div, Value::Int64(b)) if *b != 0 => Value::Int64(a / b),
                (Value::Int64(a), BinaryOperator::Mod, Value::Int64(b)) if *b != 0 => Value::Int64(a % b),

                // Integer comparisons
                (Value::Int64(a), BinaryOperator::Eq, Value::Int64(b)) => Value::Boolean(a == b),
                (Value::Int64(a), BinaryOperator::NotEq, Value::Int64(b)) => Value::Boolean(a != b),
                (Value::Int64(a), BinaryOperator::Lt, Value::Int64(b)) => Value::Boolean(a < b),
                (Value::Int64(a), BinaryOperator::LtEq, Value::Int64(b)) => Value::Boolean(a <= b),
                (Value::Int64(a), BinaryOperator::Gt, Value::Int64(b)) => Value::Boolean(a > b),
                (Value::Int64(a), BinaryOperator::GtEq, Value::Int64(b)) => Value::Boolean(a >= b),

                // Float arithmetic
                (Value::Float64(a), BinaryOperator::Add, Value::Float64(b)) => Value::Float64(a + b),
                (Value::Float64(a), BinaryOperator::Sub, Value::Float64(b)) => Value::Float64(a - b),
                (Value::Float64(a), BinaryOperator::Mul, Value::Float64(b)) => Value::Float64(a * b),
                (Value::Float64(a), BinaryOperator::Div, Value::Float64(b)) if *b != 0.0 => Value::Float64(a / b),

                // Float comparisons
                (Value::Float64(a), BinaryOperator::Eq, Value::Float64(b)) => {
                    Value::Boolean((a - b).abs() < f64::EPSILON)
                }
                (Value::Float64(a), BinaryOperator::Lt, Value::Float64(b)) => Value::Boolean(a < b),
                (Value::Float64(a), BinaryOperator::LtEq, Value::Float64(b)) => Value::Boolean(a <= b),
                (Value::Float64(a), BinaryOperator::Gt, Value::Float64(b)) => Value::Boolean(a > b),
                (Value::Float64(a), BinaryOperator::GtEq, Value::Float64(b)) => Value::Boolean(a >= b),

                // Boolean logic
                (Value::Boolean(a), BinaryOperator::And, Value::Boolean(b)) => Value::Boolean(*a && *b),
                (Value::Boolean(a), BinaryOperator::Or, Value::Boolean(b)) => Value::Boolean(*a || *b),

                // String operations
                (Value::String(a), BinaryOperator::Eq, Value::String(b)) => Value::Boolean(a == b),
                (Value::String(a), BinaryOperator::NotEq, Value::String(b)) => Value::Boolean(a != b),
                (Value::String(a), BinaryOperator::Concat, Value::String(b)) => {
                    Value::String(Arc::from(format!("{}{}", a, b).as_str()))
                }

                // Fall back to generic evaluation
                _ => {
                    use crate::executor::ExprEvaluator;
                    let schema = Schema::empty();
                    let row = Row::new(vec![l.clone(), r.clone()]);
                    let expr = Expr::BinaryOp {
                        left: Box::new(Expr::Literal(l)),
                        op: op.clone(),
                        right: Box::new(Expr::Literal(r)),
                    };
                    ExprEvaluator::evaluate(&expr, &row, &schema).unwrap_or(Value::Null)
                }
            };

            result.push(value);
        }

        Ok(result)
    }

    /// Evaluate unary operation on vector
    fn eval_unary_vector(op: &UnaryOperator, col: &ColumnVector) -> Result<ColumnVector> {
        let mut result = ColumnVector::with_capacity(col.data_type.clone(), col.len());

        for i in 0..col.len() {
            let v = col.get(i).cloned().unwrap_or(Value::Null);

            if v.is_null() {
                result.push(Value::Null);
                continue;
            }

            let value = match (op, &v) {
                (UnaryOperator::Not, Value::Boolean(b)) => Value::Boolean(!b),
                (UnaryOperator::Neg, Value::Int64(n)) => Value::Int64(-n),
                (UnaryOperator::Neg, Value::Int32(n)) => Value::Int32(-n),
                (UnaryOperator::Neg, Value::Float64(n)) => Value::Float64(-n),
                (UnaryOperator::Plus, _) => v,
                _ => Value::Null,
            };

            result.push(value);
        }

        Ok(result)
    }

    /// Cast vector to target type
    fn cast_vector(col: &ColumnVector, target: &DataType) -> Result<ColumnVector> {
        let mut result = ColumnVector::with_capacity(target.clone(), col.len());

        for i in 0..col.len() {
            let v = col.get(i).cloned().unwrap_or(Value::Null);

            let value = if v.is_null() {
                Value::Null
            } else {
                match target {
                    DataType::Int64 => Value::Int64(v.as_i64().unwrap_or(0)),
                    DataType::Int32 => Value::Int32(v.as_i64().unwrap_or(0) as i32),
                    DataType::Float64 => Value::Float64(v.as_f64().unwrap_or(0.0)),
                    DataType::String => Value::String(Arc::from(v.to_string().as_str())),
                    DataType::Boolean => Value::Boolean(v.as_bool().unwrap_or(false)),
                    _ => v,
                }
            };

            result.push(value);
        }

        Ok(result)
    }
}

/// Vectorized operator for filter
pub struct VectorizedFilter;

impl VectorizedFilter {
    /// Apply filter to a batch
    pub fn filter(batch: &VectorBatch, predicate: &Expr) -> Result<VectorBatch> {
        let mask = VectorizedEvaluator::evaluate(predicate, batch)?;

        let mut result = VectorBatch::new(batch.schema.clone());

        for i in 0..batch.num_rows {
            if let Some(Value::Boolean(true)) = mask.get(i) {
                if let Some(row) = batch.get_row(i) {
                    result.append_row(&row);
                }
            }
        }

        Ok(result)
    }

    /// Compute selection vector (indices of matching rows)
    pub fn compute_selection(batch: &VectorBatch, predicate: &Expr) -> Result<Vec<usize>> {
        let mask = VectorizedEvaluator::evaluate(predicate, batch)?;

        let selection: Vec<usize> = (0..batch.num_rows)
            .filter(|&i| matches!(mask.get(i), Some(Value::Boolean(true))))
            .collect();

        Ok(selection)
    }
}

/// Vectorized operator for projection
pub struct VectorizedProject;

impl VectorizedProject {
    /// Apply projection to a batch
    pub fn project(batch: &VectorBatch, exprs: &[Expr], schema: Schema) -> Result<VectorBatch> {
        let columns: Vec<ColumnVector> = exprs
            .iter()
            .map(|expr| VectorizedEvaluator::evaluate(expr, batch))
            .collect::<Result<Vec<_>>>()?;

        Ok(VectorBatch {
            schema,
            columns,
            num_rows: batch.num_rows,
        })
    }
}

/// Vectorized hash aggregation
pub struct VectorizedHashAggregate {
    /// Group-by column indices
    group_by: Vec<usize>,
    /// Aggregate expressions
    aggregates: Vec<AggregateExpr>,
    /// Output schema
    schema: Schema,
    /// Hash table: string key -> (original values, aggregators)
    hash_table: HashMap<String, (Vec<Value>, Vec<Aggregator>)>,
}

impl VectorizedHashAggregate {
    /// Create new hash aggregate
    pub fn new(group_by: Vec<usize>, aggregates: Vec<AggregateExpr>, schema: Schema) -> Self {
        Self {
            group_by,
            aggregates,
            schema,
            hash_table: HashMap::new(),
        }
    }

    /// Convert values to string key for hashing
    fn values_to_key(values: &[Value]) -> String {
        values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|")
    }

    /// Process a batch
    pub fn accumulate(&mut self, batch: &VectorBatch, input_schema: &Schema) -> Result<()> {
        for i in 0..batch.num_rows {
            // Build group key
            let key_values: Vec<Value> = self
                .group_by
                .iter()
                .map(|&idx| {
                    batch
                        .columns
                        .get(idx)
                        .and_then(|c| c.get(i).cloned())
                        .unwrap_or(Value::Null)
                })
                .collect();
            let key = Self::values_to_key(&key_values);

            // Get or create aggregators
            let (_, aggs) = self.hash_table.entry(key).or_insert_with(|| {
                (key_values, self.aggregates.iter().map(Aggregator::new).collect())
            });

            // Get row for expression evaluation
            let row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));

            // Accumulate values
            for (agg, agg_expr) in aggs.iter_mut().zip(self.aggregates.iter()) {
                let arg_values: Vec<Value> = agg_expr
                    .args
                    .iter()
                    .map(|arg| ExprEvaluator::evaluate(arg, &row, input_schema).unwrap_or(Value::Null))
                    .collect();
                agg.accumulate(&arg_values);
            }
        }

        Ok(())
    }

    /// Finalize and get result batch
    pub fn finalize(&self) -> VectorBatch {
        let mut result = VectorBatch::new(self.schema.clone());

        for (_, (key_values, aggs)) in &self.hash_table {
            let mut values = key_values.clone();
            for agg in aggs {
                values.push(agg.finalize());
            }
            result.append_row(&Row::new(values));
        }

        result
    }

    /// Reset state
    pub fn reset(&mut self) {
        self.hash_table.clear();
    }
}

/// Vectorized sort operator
pub struct VectorizedSort;

impl VectorizedSort {
    /// Sort a batch by order expressions
    pub fn sort(batch: &VectorBatch, order_by: &[SortExpr]) -> Result<VectorBatch> {
        if batch.is_empty() {
            return Ok(batch.clone());
        }

        // Create index array
        let mut indices: Vec<usize> = (0..batch.num_rows).collect();

        // Sort indices based on order expressions
        indices.sort_by(|&a, &b| {
            for sort_expr in order_by {
                let row_a = batch.get_row(a).unwrap_or_else(|| Row::new(vec![]));
                let row_b = batch.get_row(b).unwrap_or_else(|| Row::new(vec![]));

                let val_a = ExprEvaluator::evaluate(&sort_expr.expr, &row_a, &batch.schema)
                    .unwrap_or(Value::Null);
                let val_b = ExprEvaluator::evaluate(&sort_expr.expr, &row_b, &batch.schema)
                    .unwrap_or(Value::Null);

                let cmp = ExprEvaluator::compare_values(&val_a, &val_b);
                let cmp = if sort_expr.asc { cmp } else { cmp.reverse() };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Build result batch using sorted indices
        let mut result = VectorBatch::new(batch.schema.clone());
        for &idx in &indices {
            if let Some(row) = batch.get_row(idx) {
                result.append_row(&row);
            }
        }

        Ok(result)
    }
}

/// Vectorized hash join
pub struct VectorizedHashJoin {
    /// Build side hash table - using String key for hashing
    hash_table: HashMap<String, Vec<Row>>,
    /// Left key indices
    left_keys: Vec<usize>,
    /// Right key indices
    right_keys: Vec<usize>,
}

impl VectorizedHashJoin {
    /// Create new hash join
    pub fn new(left_keys: Vec<usize>, right_keys: Vec<usize>) -> Self {
        Self {
            hash_table: HashMap::new(),
            left_keys,
            right_keys,
        }
    }

    /// Convert values to string key for hashing
    fn values_to_key(values: &[Value]) -> String {
        values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|")
    }

    /// Build hash table from right side
    pub fn build(&mut self, batch: &VectorBatch) {
        for i in 0..batch.num_rows {
            let key_values: Vec<Value> = self
                .right_keys
                .iter()
                .map(|&idx| {
                    batch
                        .columns
                        .get(idx)
                        .and_then(|c| c.get(i).cloned())
                        .unwrap_or(Value::Null)
                })
                .collect();
            let key = Self::values_to_key(&key_values);

            let row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));
            self.hash_table.entry(key).or_default().push(row);
        }
    }

    /// Probe with left side
    pub fn probe(&self, batch: &VectorBatch, output_schema: Schema) -> VectorBatch {
        let mut result = VectorBatch::new(output_schema);

        for i in 0..batch.num_rows {
            let key_values: Vec<Value> = self
                .left_keys
                .iter()
                .map(|&idx| {
                    batch
                        .columns
                        .get(idx)
                        .and_then(|c| c.get(i).cloned())
                        .unwrap_or(Value::Null)
                })
                .collect();
            let key = Self::values_to_key(&key_values);

            if let Some(matches) = self.hash_table.get(&key) {
                let left_row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));
                for right_row in matches {
                    let mut combined = left_row.values.clone();
                    combined.extend(right_row.values.clone());
                    result.append_row(&Row::new(combined));
                }
            }
        }

        result
    }
}

/// Vectorized execution engine
pub struct VectorizedExecutor {
    /// Batch size
    batch_size: usize,
    /// Execution stats
    stats: ExecutionStats,
}

impl Default for VectorizedExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorizedExecutor {
    /// Create new vectorized executor
    pub fn new() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            stats: ExecutionStats::default(),
        }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Execute a plan on a batch
    pub fn execute_batch(&mut self, plan: &PhysicalPlan, input: VectorBatch) -> Result<VectorBatch> {
        match plan {
            PhysicalPlan::Filter { predicate, .. } => {
                VectorizedFilter::filter(&input, predicate)
            }
            PhysicalPlan::Project { exprs, schema, .. } => {
                VectorizedProject::project(&input, exprs, schema.clone())
            }
            PhysicalPlan::Sort { order_by, .. } => {
                VectorizedSort::sort(&input, order_by)
            }
            PhysicalPlan::Limit { limit, offset, .. } => {
                let rows = input.to_rows();
                let limited: Vec<Row> = rows.into_iter().skip(*offset).take(*limit).collect();
                Ok(VectorBatch::from_rows(input.schema, &limited))
            }
            _ => {
                // For other operators, fall back to row-by-row
                Ok(input)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ])
    }

    fn test_batch() -> VectorBatch {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![
                Value::Int64(1),
                Value::String(Arc::from("Alice")),
                Value::Int32(30),
            ]),
            Row::new(vec![
                Value::Int64(2),
                Value::String(Arc::from("Bob")),
                Value::Int32(25),
            ]),
            Row::new(vec![
                Value::Int64(3),
                Value::String(Arc::from("Charlie")),
                Value::Int32(35),
            ]),
        ];
        VectorBatch::from_rows(schema, &rows)
    }

    #[test]
    fn test_column_vector() {
        let mut col = ColumnVector::new(DataType::Int64);
        col.push(Value::Int64(1));
        col.push(Value::Int64(2));
        col.push(Value::Null);

        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Some(&Value::Int64(1)));
        assert!(!col.is_null(0));
        assert!(col.is_null(2));
    }

    #[test]
    fn test_vector_batch_from_rows() {
        let batch = test_batch();

        assert_eq!(batch.num_rows, 3);
        assert_eq!(batch.columns.len(), 3);

        let row = batch.get_row(0).unwrap();
        assert_eq!(row.get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_vectorized_filter() {
        let batch = test_batch();

        // age > 28
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Int32(28))),
        };

        let result = VectorizedFilter::filter(&batch, &predicate).unwrap();
        assert_eq!(result.num_rows, 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_vectorized_project() {
        let batch = test_batch();

        let output_schema = Schema::new(vec![
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("double_age", DataType::Int32),
        ]);

        let exprs = vec![
            Expr::Column {
                name: "name".to_string(),
                index: Some(1),
            },
            Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "age".to_string(),
                    index: Some(2),
                }),
                op: BinaryOperator::Mul,
                right: Box::new(Expr::Literal(Value::Int32(2))),
            },
        ];

        let result = VectorizedProject::project(&batch, &exprs, output_schema).unwrap();
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_vectorized_sort() {
        let batch = test_batch();

        let order_by = vec![SortExpr {
            expr: Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            },
            asc: true,
            nulls_first: false,
        }];

        let result = VectorizedSort::sort(&batch, &order_by).unwrap();

        let row0 = result.get_row(0).unwrap();
        let row1 = result.get_row(1).unwrap();
        let row2 = result.get_row(2).unwrap();

        assert_eq!(row0.get(2), Some(&Value::Int32(25))); // Bob
        assert_eq!(row1.get(2), Some(&Value::Int32(30))); // Alice
        assert_eq!(row2.get(2), Some(&Value::Int32(35))); // Charlie
    }

    #[test]
    fn test_vectorized_hash_aggregate() {
        let batch = test_batch();

        let agg_schema = Schema::new(vec![
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("count", DataType::Int64),
        ]);

        let mut agg = VectorizedHashAggregate::new(
            vec![2], // group by age
            vec![AggregateExpr {
                func: AggregateFunction::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
            }],
            agg_schema,
        );

        agg.accumulate(&batch, &batch.schema).unwrap();
        let result = agg.finalize();

        assert_eq!(result.num_rows, 3); // Three distinct ages
    }

    #[test]
    fn test_vectorized_hash_join() {
        let left_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
        ]);

        let right_schema = Schema::new(vec![
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        let left_batch = VectorBatch::from_rows(
            left_schema.clone(),
            &[
                Row::new(vec![Value::Int64(1), Value::String(Arc::from("Alice"))]),
                Row::new(vec![Value::Int64(2), Value::String(Arc::from("Bob"))]),
            ],
        );

        let right_batch = VectorBatch::from_rows(
            right_schema.clone(),
            &[
                Row::new(vec![Value::Int64(1), Value::Float64(100.0)]),
                Row::new(vec![Value::Int64(1), Value::Float64(200.0)]),
                Row::new(vec![Value::Int64(2), Value::Float64(50.0)]),
            ],
        );

        let mut join = VectorizedHashJoin::new(vec![0], vec![0]);
        join.build(&right_batch);

        let output_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        let result = join.probe(&left_batch, output_schema);
        assert_eq!(result.num_rows, 3); // Alice has 2 orders, Bob has 1
    }

    #[test]
    fn test_binary_vector_operations() {
        let a = ColumnVector::from_values(
            DataType::Int64,
            vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)],
        );
        let b = ColumnVector::from_values(
            DataType::Int64,
            vec![Value::Int64(5), Value::Int64(10), Value::Int64(15)],
        );

        let result = VectorizedEvaluator::eval_binary_vector(&a, &BinaryOperator::Add, &b).unwrap();
        assert_eq!(result.get(0), Some(&Value::Int64(15)));
        assert_eq!(result.get(1), Some(&Value::Int64(30)));
        assert_eq!(result.get(2), Some(&Value::Int64(45)));

        let result = VectorizedEvaluator::eval_binary_vector(&a, &BinaryOperator::Gt, &b).unwrap();
        assert_eq!(result.get(0), Some(&Value::Boolean(true)));
        assert_eq!(result.get(1), Some(&Value::Boolean(true)));
        assert_eq!(result.get(2), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_null_handling_in_vectors() {
        let a = ColumnVector::from_values(
            DataType::Int64,
            vec![Value::Int64(10), Value::Null, Value::Int64(30)],
        );
        let b = ColumnVector::from_values(
            DataType::Int64,
            vec![Value::Int64(5), Value::Int64(10), Value::Null],
        );

        let result = VectorizedEvaluator::eval_binary_vector(&a, &BinaryOperator::Add, &b).unwrap();
        assert_eq!(result.get(0), Some(&Value::Int64(15)));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn test_selection_vector() {
        let batch = test_batch();

        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            }),
            op: BinaryOperator::GtEq,
            right: Box::new(Expr::Literal(Value::Int32(30))),
        };

        let selection = VectorizedFilter::compute_selection(&batch, &predicate).unwrap();
        assert_eq!(selection, vec![0, 2]); // Indices of Alice and Charlie
    }
}
