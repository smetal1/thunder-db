//! Physical Plan Executor
//!
//! Implements execution of physical query plans with support for:
//! - Sequential and index scans
//! - Joins (hash, merge, nested loop)
//! - Aggregation (hash-based)
//! - Sorting and limiting
//! - Filtering and projection
//! - DML operations (insert, update, delete)

use std::collections::HashMap;
use std::sync::Arc;

use futures::executor::block_on;
use parking_lot::RwLock;
use tracing::debug;

use thunder_common::prelude::*;
use thunder_query::{ExecutionStats, PhysicalPlan, RecordBatch, ScanRange};
use thunder_sql::{AggregateExpr, AggregateFunction, BinaryOperator, Expr, JoinType, SortExpr, UnaryOperator};
use thunder_storage::{BTree, BTreeConfig, BufferPoolImpl, RowIterator, RowStoreImpl, Snapshot};

// ============================================================================
// Value Key (for hashing)
// ============================================================================

/// A hashable key derived from Values (for grouping/joining)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ValueKey(Vec<ValueKeyPart>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ValueKeyPart {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64), // Use bits for float comparison
    String(String),
    Binary(Vec<u8>),
}

impl ValueKey {
    fn from_values(values: &[Value]) -> Self {
        ValueKey(values.iter().map(ValueKeyPart::from_value).collect())
    }
}

impl ValueKeyPart {
    fn from_value(val: &Value) -> Self {
        match val {
            Value::Null => ValueKeyPart::Null,
            Value::Boolean(b) => ValueKeyPart::Bool(*b),
            Value::Int8(i) => ValueKeyPart::Int(*i as i64),
            Value::Int16(i) => ValueKeyPart::Int(*i as i64),
            Value::Int32(i) => ValueKeyPart::Int(*i as i64),
            Value::Int64(i) => ValueKeyPart::Int(*i),
            Value::Float32(f) => ValueKeyPart::Float((*f as f64).to_bits()),
            Value::Float64(f) => ValueKeyPart::Float(f.to_bits()),
            Value::Decimal(v, _) => ValueKeyPart::Int(*v as i64),
            Value::String(s) => ValueKeyPart::String(s.to_string()),
            Value::Binary(b) => ValueKeyPart::Binary(b.to_vec()),
            Value::Date(d) => ValueKeyPart::Int(*d as i64),
            Value::Time(t) => ValueKeyPart::Int(*t),
            Value::Timestamp(t) => ValueKeyPart::Int(*t),
            Value::TimestampTz(t, _) => ValueKeyPart::Int(*t),
            Value::Uuid(u) => ValueKeyPart::Binary(u.to_vec()),
            Value::Json(j) => ValueKeyPart::String(j.to_string()),
            Value::Vector(v) => ValueKeyPart::String(format!("{:?}", v)),
            Value::Array(a) => ValueKeyPart::String(format!("{:?}", a)),
        }
    }
}

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates expressions against rows
pub struct ExprEvaluator {
    /// Column name to index mapping for current context
    columns: HashMap<String, usize>,
}

impl ExprEvaluator {
    pub fn new(schema: &Schema) -> Self {
        let columns = schema.columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self { columns }
    }

    pub fn with_columns(columns: HashMap<String, usize>) -> Self {
        Self { columns }
    }

    /// Evaluate an expression against a row
    pub fn evaluate(&self, expr: &Expr, row: &Row) -> Result<Value> {
        match expr {
            Expr::Column { name, index } => {
                // First try index if available
                if let Some(idx) = index {
                    return Ok(row.values.get(*idx).cloned().unwrap_or(Value::Null));
                }
                // Fall back to name lookup
                let idx = self.columns.get(name)
                    .ok_or_else(|| Error::Internal(format!("Column '{}' not found", name)))?;
                Ok(row.values.get(*idx).cloned().unwrap_or(Value::Null))
            }
            Expr::Literal(value) => Ok(value.clone()),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate(left, row)?;
                let right_val = self.evaluate(right, row)?;
                self.eval_binary_op(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate(expr, row)?;
                self.eval_unary_op(op, &val)
            }
            Expr::IsNull(expr) => {
                let val = self.evaluate(expr, row)?;
                Ok(Value::Boolean(matches!(val, Value::Null)))
            }
            Expr::IsNotNull(expr) => {
                let val = self.evaluate(expr, row)?;
                Ok(Value::Boolean(!matches!(val, Value::Null)))
            }
            Expr::Between { expr, low, high, negated } => {
                let val = self.evaluate(expr, row)?;
                let low_val = self.evaluate(low, row)?;
                let high_val = self.evaluate(high, row)?;
                let in_range = self.compare(&val, &low_val)? >= 0
                    && self.compare(&val, &high_val)? <= 0;
                Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
            }
            Expr::InList { expr, list, negated } => {
                let val = self.evaluate(expr, row)?;
                let mut found = false;
                for item in list {
                    let item_val = self.evaluate(item, row)?;
                    if self.values_equal(&val, &item_val) {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Boolean(if *negated { !found } else { found }))
            }
            Expr::Case { operand, when_clauses, else_clause } => {
                if let Some(op_expr) = operand {
                    let op_val = self.evaluate(op_expr, row)?;
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.evaluate(when_expr, row)?;
                        if self.values_equal(&op_val, &when_val) {
                            return self.evaluate(then_expr, row);
                        }
                    }
                } else {
                    for (when_expr, then_expr) in when_clauses {
                        let when_val = self.evaluate(when_expr, row)?;
                        if self.value_to_bool(&when_val)? {
                            return self.evaluate(then_expr, row);
                        }
                    }
                }
                if let Some(else_expr) = else_clause {
                    self.evaluate(else_expr, row)
                } else {
                    Ok(Value::Null)
                }
            }
            Expr::Cast { expr, data_type } => {
                let val = self.evaluate(expr, row)?;
                self.cast_value(&val, data_type)
            }
            Expr::Function { name, args } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| self.evaluate(a, row))
                    .collect::<Result<_>>()?;
                self.eval_function(name, &arg_vals)
            }
            Expr::Wildcard => Err(Error::Internal("Cannot evaluate wildcard".into())),
            Expr::Subquery(_) => Err(Error::Internal("Subquery evaluation not supported".into())),
        }
    }

    fn eval_binary_op(&self, left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
        // Handle NULL propagation for most operations
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            match op {
                BinaryOperator::And => {
                    if let Value::Boolean(false) = left {
                        return Ok(Value::Boolean(false));
                    }
                    if let Value::Boolean(false) = right {
                        return Ok(Value::Boolean(false));
                    }
                    return Ok(Value::Null);
                }
                BinaryOperator::Or => {
                    if let Value::Boolean(true) = left {
                        return Ok(Value::Boolean(true));
                    }
                    if let Value::Boolean(true) = right {
                        return Ok(Value::Boolean(true));
                    }
                    return Ok(Value::Null);
                }
                _ => return Ok(Value::Null),
            }
        }

        match op {
            BinaryOperator::Eq => Ok(Value::Boolean(self.values_equal(left, right))),
            BinaryOperator::NotEq => Ok(Value::Boolean(!self.values_equal(left, right))),
            BinaryOperator::Lt => Ok(Value::Boolean(self.compare(left, right)? < 0)),
            BinaryOperator::LtEq => Ok(Value::Boolean(self.compare(left, right)? <= 0)),
            BinaryOperator::Gt => Ok(Value::Boolean(self.compare(left, right)? > 0)),
            BinaryOperator::GtEq => Ok(Value::Boolean(self.compare(left, right)? >= 0)),
            BinaryOperator::And => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Boolean(l && r))
            }
            BinaryOperator::Or => {
                let l = self.value_to_bool(left)?;
                let r = self.value_to_bool(right)?;
                Ok(Value::Boolean(l || r))
            }
            BinaryOperator::Add => self.numeric_op(left, right, |a, b| a + b, |a, b| a + b),
            BinaryOperator::Sub => self.numeric_op(left, right, |a, b| a - b, |a, b| a - b),
            BinaryOperator::Mul => self.numeric_op(left, right, |a, b| a * b, |a, b| a * b),
            BinaryOperator::Div => {
                let is_zero = match right {
                    Value::Int64(0) => true,
                    Value::Float64(f) if *f == 0.0 => true,
                    _ => false,
                };
                if is_zero {
                    return Err(Error::Internal("Division by zero".into()));
                }
                self.numeric_op(left, right, |a, b| a / b, |a, b| a / b)
            }
            BinaryOperator::Mod => self.numeric_op(left, right, |a, b| a % b, |a, b| a % b),
            BinaryOperator::Concat => {
                let l = self.value_to_string(left);
                let r = self.value_to_string(right);
                Ok(Value::String(format!("{}{}", l, r).into()))
            }
            BinaryOperator::Like | BinaryOperator::ILike => {
                let matches = self.like_match(left, right, matches!(op, BinaryOperator::ILike))?;
                Ok(Value::Boolean(matches))
            }
        }
    }

    fn eval_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value> {
        if matches!(val, Value::Null) {
            return Ok(Value::Null);
        }
        match op {
            UnaryOperator::Not => {
                let b = self.value_to_bool(val)?;
                Ok(Value::Boolean(!b))
            }
            UnaryOperator::Neg => {
                match val {
                    Value::Int64(i) => Ok(Value::Int64(-i)),
                    Value::Float64(f) => Ok(Value::Float64(-f)),
                    _ => Err(Error::Internal("Cannot negate non-numeric value".into())),
                }
            }
            UnaryOperator::Plus => Ok(val.clone()),
        }
    }

    fn numeric_op<F1, F2>(&self, left: &Value, right: &Value, int_op: F1, float_op: F2) -> Result<Value>
    where
        F1: Fn(i64, i64) -> i64,
        F2: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(int_op(*l, *r))),
            (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(float_op(*l, *r))),
            (Value::Int64(l), Value::Float64(r)) => Ok(Value::Float64(float_op(*l as f64, *r))),
            (Value::Float64(l), Value::Int64(r)) => Ok(Value::Float64(float_op(*l, *r as f64))),
            _ => Err(Error::Internal("Invalid numeric operation".into())),
        }
    }

    fn compare(&self, left: &Value, right: &Value) -> Result<i32> {
        match (left, right) {
            (Value::Null, Value::Null) => Ok(0),
            (Value::Null, _) => Ok(-1),
            (_, Value::Null) => Ok(1),
            (Value::Boolean(l), Value::Boolean(r)) => Ok(l.cmp(r) as i32),
            (Value::Int64(l), Value::Int64(r)) => Ok(l.cmp(r) as i32),
            (Value::Float64(l), Value::Float64(r)) => {
                Ok(l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::Int64(l), Value::Float64(r)) => {
                Ok((*l as f64).partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::Float64(l), Value::Int64(r)) => {
                Ok(l.partial_cmp(&(*r as f64)).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (Value::String(l), Value::String(r)) => Ok(l.cmp(r) as i32),
            (Value::Binary(l), Value::Binary(r)) => Ok(l.cmp(r) as i32),
            (Value::Timestamp(l), Value::Timestamp(r)) => Ok(l.cmp(r) as i32),
            _ => Err(Error::Internal(format!(
                "Cannot compare {:?} with {:?}", left, right
            ))),
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        self.compare(left, right).map(|c| c == 0).unwrap_or(false)
    }

    fn value_to_bool(&self, val: &Value) -> Result<bool> {
        match val {
            Value::Boolean(b) => Ok(*b),
            Value::Int64(i) => Ok(*i != 0),
            Value::Null => Ok(false),
            _ => Err(Error::Internal("Cannot convert to boolean".into())),
        }
    }

    fn value_to_i64(&self, val: &Value) -> Result<i64> {
        match val {
            Value::Int64(i) => Ok(*i),
            Value::Float64(f) => Ok(*f as i64),
            Value::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            _ => Err(Error::Internal("Cannot convert to integer".into())),
        }
    }

    fn value_to_string(&self, val: &Value) -> String {
        match val {
            Value::String(s) => s.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            Value::Binary(b) => format!("{:?}", b),
            Value::Timestamp(t) => t.to_string(),
            Value::Json(j) => j.to_string(),
            Value::Array(a) => format!("{:?}", a),
            _ => format!("{:?}", val),
        }
    }

    fn cast_value(&self, val: &Value, target: &DataType) -> Result<Value> {
        if matches!(val, Value::Null) {
            return Ok(Value::Null);
        }
        match target {
            DataType::Int64 => Ok(Value::Int64(self.value_to_i64(val)?)),
            DataType::Float64 => {
                match val {
                    Value::Int64(i) => Ok(Value::Float64(*i as f64)),
                    Value::Float64(f) => Ok(Value::Float64(*f)),
                    Value::String(s) => s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| Error::Internal("Invalid float".into())),
                    _ => Err(Error::Internal("Cannot cast to float".into())),
                }
            }
            DataType::String => Ok(Value::String(self.value_to_string(val).into())),
            DataType::Boolean => Ok(Value::Boolean(self.value_to_bool(val)?)),
            _ => Err(Error::Internal(format!("Unsupported cast to {:?}", target))),
        }
    }

    fn eval_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        match name.to_uppercase().as_str() {
            "UPPER" => {
                let s = args.first().map(|v| self.value_to_string(v)).unwrap_or_default();
                Ok(Value::String(s.to_uppercase().into()))
            }
            "LOWER" => {
                let s = args.first().map(|v| self.value_to_string(v)).unwrap_or_default();
                Ok(Value::String(s.to_lowercase().into()))
            }
            "LENGTH" | "LEN" => {
                let s = args.first().map(|v| self.value_to_string(v)).unwrap_or_default();
                Ok(Value::Int64(s.len() as i64))
            }
            "COALESCE" => {
                for arg in args {
                    if !matches!(arg, Value::Null) {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                if args.len() >= 2 && self.values_equal(&args[0], &args[1]) {
                    Ok(Value::Null)
                } else {
                    Ok(args.first().cloned().unwrap_or(Value::Null))
                }
            }
            "ABS" => {
                match args.first() {
                    Some(Value::Int64(i)) => Ok(Value::Int64(i.abs())),
                    Some(Value::Float64(f)) => Ok(Value::Float64(f.abs())),
                    _ => Ok(Value::Null),
                }
            }
            "ROUND" => {
                match args.first() {
                    Some(Value::Float64(f)) => {
                        let decimals = args.get(1)
                            .and_then(|v| match v { Value::Int64(i) => Some(*i), _ => None })
                            .unwrap_or(0);
                        let factor = 10_f64.powi(decimals as i32);
                        Ok(Value::Float64((f * factor).round() / factor))
                    }
                    Some(Value::Int64(i)) => Ok(Value::Int64(*i)),
                    _ => Ok(Value::Null),
                }
            }
            "SUBSTR" | "SUBSTRING" => {
                let s = args.first().map(|v| self.value_to_string(v)).unwrap_or_default();
                let start = args.get(1).and_then(|v| match v { Value::Int64(i) => Some(*i as usize), _ => None }).unwrap_or(1);
                let len = args.get(2).and_then(|v| match v { Value::Int64(i) => Some(*i as usize), _ => None });
                let start_idx = start.saturating_sub(1);
                match len {
                    Some(l) => Ok(Value::String(s.chars().skip(start_idx).take(l).collect::<String>().into())),
                    None => Ok(Value::String(s.chars().skip(start_idx).collect::<String>().into())),
                }
            }
            "CONCAT" => {
                let result: String = args.iter().map(|v| self.value_to_string(v)).collect();
                Ok(Value::String(result.into()))
            }
            "NOW" | "CURRENT_TIMESTAMP" => {
                Ok(Value::Timestamp(chrono::Utc::now().timestamp_micros()))
            }
            _ => Err(Error::Internal(format!("Unknown function: {}", name))),
        }
    }

    fn like_match(&self, val: &Value, pattern: &Value, case_insensitive: bool) -> Result<bool> {
        let s = self.value_to_string(val);
        let pat = self.value_to_string(pattern);

        let (s, pat) = if case_insensitive {
            (s.to_lowercase(), pat.to_lowercase())
        } else {
            (s, pat)
        };

        if !pat.contains('%') && !pat.contains('_') {
            return Ok(s == pat);
        }

        let parts: Vec<&str> = pat.split('%').collect();
        if parts.len() == 1 {
            return Ok(s.len() == pat.len() &&
                s.chars().zip(pat.chars()).all(|(c, p)| p == '_' || c == p));
        }

        let mut pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            if i == 0 {
                if !s.starts_with(part) {
                    return Ok(false);
                }
                pos = part.len();
            } else if i == parts.len() - 1 {
                if !s[pos..].ends_with(part) {
                    return Ok(false);
                }
            } else {
                if let Some(idx) = s[pos..].find(part) {
                    pos += idx + part.len();
                } else {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

// ============================================================================
// Aggregate Accumulator
// ============================================================================

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub struct AggregateAccumulator {
    function: AggregateFunction,
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
    first: Option<Value>,
    last: Option<Value>,
    values: Vec<Value>,
    string_parts: Vec<String>,
}

impl AggregateAccumulator {
    pub fn new(function: AggregateFunction) -> Self {
        Self {
            function,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            first: None,
            last: None,
            values: Vec::new(),
            string_parts: Vec::new(),
        }
    }

    pub fn accumulate(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }

        self.count += 1;

        match self.function {
            AggregateFunction::Sum | AggregateFunction::Avg => {
                if let Some(n) = self.value_to_f64(value) {
                    self.sum += n;
                }
            }
            AggregateFunction::Min => {
                let should_update = match &self.min {
                    None => true,
                    Some(current) => self.compare(value, current) < 0,
                };
                if should_update {
                    self.min = Some(value.clone());
                }
            }
            AggregateFunction::Max => {
                let should_update = match &self.max {
                    None => true,
                    Some(current) => self.compare(value, current) > 0,
                };
                if should_update {
                    self.max = Some(value.clone());
                }
            }
            AggregateFunction::Count => {}
            AggregateFunction::First => {
                if self.first.is_none() {
                    self.first = Some(value.clone());
                }
            }
            AggregateFunction::Last => {
                self.last = Some(value.clone());
            }
            AggregateFunction::ArrayAgg => {
                self.values.push(value.clone());
            }
            AggregateFunction::StringAgg => {
                self.string_parts.push(self.value_to_string(value));
            }
        }
    }

    pub fn accumulate_count_star(&mut self) {
        self.count += 1;
    }

    pub fn finalize(&self) -> Value {
        match self.function {
            AggregateFunction::Count => Value::Int64(self.count),
            AggregateFunction::Sum => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float64(self.sum)
                }
            }
            AggregateFunction::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float64(self.sum / self.count as f64)
                }
            }
            AggregateFunction::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateFunction::Max => self.max.clone().unwrap_or(Value::Null),
            AggregateFunction::First => self.first.clone().unwrap_or(Value::Null),
            AggregateFunction::Last => self.last.clone().unwrap_or(Value::Null),
            AggregateFunction::ArrayAgg => Value::Array(self.values.clone().into()),
            AggregateFunction::StringAgg => Value::String(self.string_parts.join(",").into()),
        }
    }

    fn value_to_f64(&self, val: &Value) -> Option<f64> {
        match val {
            Value::Int64(i) => Some(*i as f64),
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }

    fn value_to_string(&self, val: &Value) -> String {
        match val {
            Value::String(s) => s.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.to_string(),
            _ => format!("{:?}", val),
        }
    }

    fn compare(&self, a: &Value, b: &Value) -> i32 {
        match (a, b) {
            (Value::Int64(l), Value::Int64(r)) => l.cmp(r) as i32,
            (Value::Float64(l), Value::Float64(r)) => {
                l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal) as i32
            }
            (Value::String(l), Value::String(r)) => l.cmp(r) as i32,
            (Value::Timestamp(l), Value::Timestamp(r)) => l.cmp(r) as i32,
            _ => 0,
        }
    }
}

// ============================================================================
// Table Storage (In-Memory)
// ============================================================================

/// In-memory table storage with optional index support
pub struct TableStorage {
    tables: RwLock<HashMap<TableId, TableData>>,
    indexes: RwLock<HashMap<IndexId, Arc<BTree>>>,
    /// Maps (table_id, index_id) to the column indices that make up the index key
    index_columns: RwLock<HashMap<(TableId, IndexId), Vec<usize>>>,
    next_row_id: std::sync::atomic::AtomicU64,
}

struct TableData {
    schema: Schema,
    rows: Vec<(RowId, Row)>,
    /// Secondary index: maps row_id to position in rows vec for fast lookup
    row_index: HashMap<RowId, usize>,
}

impl TableStorage {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            index_columns: RwLock::new(HashMap::new()),
            next_row_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn create_table(&self, table_id: TableId, schema: Schema) {
        self.tables.write().insert(table_id, TableData {
            schema,
            rows: Vec::new(),
            row_index: HashMap::new(),
        });
    }

    /// Create an index on a table
    pub fn create_index(
        &self,
        table_id: TableId,
        index_id: IndexId,
        column_indices: Vec<usize>,
        buffer_pool: Arc<BufferPoolImpl>,
    ) -> Result<()> {
        let btree = BTree::new(index_id, buffer_pool, BTreeConfig::default())?;
        self.indexes.write().insert(index_id, Arc::new(btree));
        self.index_columns.write().insert((table_id, index_id), column_indices);
        Ok(())
    }

    /// Get an index by ID
    pub fn get_index(&self, index_id: IndexId) -> Option<Arc<BTree>> {
        self.indexes.read().get(&index_id).cloned()
    }

    /// Get the column indices for an index
    pub fn get_index_columns(&self, table_id: TableId, index_id: IndexId) -> Option<Vec<usize>> {
        self.index_columns.read().get(&(table_id, index_id)).cloned()
    }

    /// Get a row by RowId
    pub fn get_row(&self, table_id: TableId, row_id: RowId) -> Option<Row> {
        let tables = self.tables.read();
        let table = tables.get(&table_id)?;
        let pos = table.row_index.get(&row_id)?;
        table.rows.get(*pos).map(|(_, row)| row.clone())
    }

    /// Get multiple rows by RowIds
    pub fn get_rows(&self, table_id: TableId, row_ids: &[RowId]) -> Vec<Row> {
        let tables = self.tables.read();
        let Some(table) = tables.get(&table_id) else {
            return Vec::new();
        };

        row_ids.iter()
            .filter_map(|row_id| {
                table.row_index.get(row_id)
                    .and_then(|pos| table.rows.get(*pos))
                    .map(|(_, row)| row.clone())
            })
            .collect()
    }

    pub fn get_schema(&self, table_id: TableId) -> Option<Schema> {
        self.tables.read().get(&table_id).map(|t| t.schema.clone())
    }

    pub fn scan(&self, table_id: TableId) -> Vec<Row> {
        self.tables.read()
            .get(&table_id)
            .map(|t| t.rows.iter().map(|(_, r)| r.clone()).collect())
            .unwrap_or_default()
    }

    pub fn insert(&self, table_id: TableId, rows: Vec<Row>) -> Result<u64> {
        let mut tables = self.tables.write();
        let table = tables.get_mut(&table_id)
            .ok_or_else(|| Error::Internal("Table not found".into()))?;

        let count = rows.len() as u64;
        for row in rows {
            let row_id = RowId(self.next_row_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
            let pos = table.rows.len();
            table.rows.push((row_id, row));
            table.row_index.insert(row_id, pos);
        }
        Ok(count)
    }

    pub fn update<F>(&self, table_id: TableId, predicate: F, updates: &[(usize, Value)]) -> Result<u64>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write();
        let table = tables.get_mut(&table_id)
            .ok_or_else(|| Error::Internal("Table not found".into()))?;

        let mut count = 0u64;
        for (_, row) in &mut table.rows {
            if predicate(row) {
                for (col_idx, value) in updates {
                    if *col_idx < row.values.len() {
                        row.values[*col_idx] = value.clone();
                    }
                }
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn delete<F>(&self, table_id: TableId, predicate: F) -> Result<u64>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write();
        let table = tables.get_mut(&table_id)
            .ok_or_else(|| Error::Internal("Table not found".into()))?;

        let before = table.rows.len();
        table.rows.retain(|(_, row)| !predicate(row));
        let deleted = before - table.rows.len();

        // Rebuild row_index after deletion (positions have changed)
        if deleted > 0 {
            table.row_index.clear();
            for (pos, (row_id, _)) in table.rows.iter().enumerate() {
                table.row_index.insert(*row_id, pos);
            }
        }
        Ok(deleted as u64)
    }
}

impl Default for TableStorage {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Physical Plan Executor
// ============================================================================

/// Executes physical query plans
pub struct PhysicalExecutor {
    /// In-memory storage for transient data
    storage: Arc<TableStorage>,
    /// Optional persistent row store
    row_store: Option<Arc<RowStoreImpl>>,
    /// Current transaction snapshot for visibility
    snapshot: Option<Snapshot>,
}

impl PhysicalExecutor {
    pub fn new(storage: Arc<TableStorage>) -> Self {
        Self {
            storage,
            row_store: None,
            snapshot: None,
        }
    }

    /// Create an executor with persistent storage
    pub fn with_row_store(storage: Arc<TableStorage>, row_store: Arc<RowStoreImpl>) -> Self {
        Self {
            storage,
            row_store: Some(row_store),
            snapshot: None,
        }
    }

    /// Set the transaction snapshot for visibility checking
    pub fn set_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot = Some(snapshot);
    }

    /// Execute a physical plan and return results
    pub async fn execute(&self, plan: PhysicalPlan, _txn_id: TxnId) -> Result<ExecutionResult> {
        self.execute_with_snapshot(plan, _txn_id, None).await
    }

    /// Execute a physical plan with a specific snapshot for MVCC visibility
    pub async fn execute_with_snapshot(
        &self,
        plan: PhysicalPlan,
        _txn_id: TxnId,
        snapshot: Option<Snapshot>,
    ) -> Result<ExecutionResult> {
        // Create a temporary executor with the snapshot if provided
        let executor = if let Some(snap) = snapshot {
            PhysicalExecutor {
                storage: self.storage.clone(),
                row_store: self.row_store.clone(),
                snapshot: Some(snap),
            }
        } else {
            PhysicalExecutor {
                storage: self.storage.clone(),
                row_store: self.row_store.clone(),
                snapshot: self.snapshot.clone(),
            }
        };

        let start = std::time::Instant::now();
        let (batches, rows_affected, schema) = executor.execute_plan(plan)?;

        let stats = ExecutionStats {
            rows_read: batches.iter().map(|b| b.num_rows() as u64).sum(),
            rows_written: rows_affected.unwrap_or(0),
            bytes_read: 0,
            bytes_written: 0,
            execution_time_ms: start.elapsed().as_millis() as u64,
        };

        Ok(ExecutionResult {
            schema,
            batches,
            rows_affected,
            stats,
        })
    }

    fn execute_plan(&self, plan: PhysicalPlan) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        match plan {
            PhysicalPlan::SeqScan { table_id, schema, filter } => {
                self.execute_seq_scan(table_id, schema, filter)
            }
            PhysicalPlan::IndexScan { table_id, index_id, schema, range } => {
                self.execute_index_scan(table_id, index_id, schema, range)
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.execute_filter(*input, predicate)
            }
            PhysicalPlan::Project { input, exprs, schema } => {
                self.execute_project(*input, exprs, schema)
            }
            PhysicalPlan::HashJoin { left, right, left_keys, right_keys, join_type } => {
                self.execute_hash_join(*left, *right, left_keys, right_keys, join_type)
            }
            PhysicalPlan::MergeJoin { left, right, left_keys, right_keys, join_type } => {
                self.execute_merge_join(*left, *right, left_keys, right_keys, join_type)
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                self.execute_nested_loop_join(*left, *right, condition, join_type)
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregates, schema } => {
                self.execute_hash_aggregate(*input, group_by, aggregates, schema)
            }
            PhysicalPlan::Sort { input, order_by } => {
                self.execute_sort(*input, order_by)
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                self.execute_limit(*input, limit, offset)
            }
            PhysicalPlan::Values { values, schema } => {
                self.execute_values(values, schema)
            }
            PhysicalPlan::Insert { table_id, input } => {
                self.execute_insert(table_id, *input)
            }
            PhysicalPlan::Update { table_id, input, assignments } => {
                self.execute_update(table_id, *input, assignments)
            }
            PhysicalPlan::Delete { table_id, input } => {
                self.execute_delete(table_id, *input)
            }
            PhysicalPlan::Exchange { input, .. } => {
                self.execute_plan(*input)
            }
            PhysicalPlan::Empty { schema } => {
                Ok((vec![], None, schema))
            }
        }
    }

    fn execute_seq_scan(
        &self,
        table_id: TableId,
        schema: Schema,
        filter: Option<Expr>,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        debug!("Executing sequential scan on table {:?}", table_id);

        // Try persistent storage first, fall back to in-memory
        let rows: Vec<Row> = if let (Some(row_store), Some(snapshot)) = (&self.row_store, &self.snapshot) {
            // Use persistent row store with MVCC visibility
            let mut iterator = row_store.scan(table_id, snapshot)?;
            let mut result = Vec::new();
            while let Some(row_result) = iterator.next() {
                match row_result {
                    Ok((_, row)) => result.push(row),
                    Err(e) => {
                        debug!("Error reading row: {}", e);
                        // Continue scanning despite individual row errors
                    }
                }
            }
            result
        } else {
            // Use in-memory storage
            self.storage.scan(table_id)
        };

        let evaluator = ExprEvaluator::new(&schema);

        let filtered_rows: Vec<Row> = if let Some(pred) = filter {
            rows.into_iter()
                .filter(|row| {
                    evaluator.evaluate(&pred, row)
                        .and_then(|v| match v {
                            Value::Boolean(b) => Ok(b),
                            Value::Null => Ok(false),
                            _ => Err(Error::Internal("Filter must return boolean".into())),
                        })
                        .unwrap_or(false)
                })
                .collect()
        } else {
            rows
        };

        let batch = RecordBatch::new(schema.clone(), filtered_rows);
        Ok((vec![batch], None, schema))
    }

    fn execute_index_scan(
        &self,
        table_id: TableId,
        index_id: IndexId,
        schema: Schema,
        range: Option<ScanRange>,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        debug!("Executing index scan on table {:?} using index {:?}", table_id, index_id);

        // Try to get the index
        let Some(btree) = self.storage.get_index(index_id) else {
            // Fall back to sequential scan if index not found
            debug!("Index {:?} not found, falling back to sequential scan", index_id);
            return self.execute_seq_scan(table_id, schema, None);
        };

        // Perform the index scan based on the range
        let row_ids: Vec<RowId> = if let Some(scan_range) = range {
            // Use B+Tree range scan
            let start_key = scan_range.start.as_deref().unwrap_or(&[]);
            let end_key = scan_range.end.as_deref().unwrap_or(&[0xFF; 32]);

            let results = btree.range(start_key, end_key)?;

            // Filter results based on inclusivity
            results.into_iter()
                .filter(|(key, _)| {
                    let key_slice = key.as_ref();
                    let start_ok = match &scan_range.start {
                        Some(start) => {
                            if scan_range.start_inclusive {
                                key_slice >= start.as_slice()
                            } else {
                                key_slice > start.as_slice()
                            }
                        }
                        None => true,
                    };
                    let end_ok = match &scan_range.end {
                        Some(end) => {
                            if scan_range.end_inclusive {
                                key_slice <= end.as_slice()
                            } else {
                                key_slice < end.as_slice()
                            }
                        }
                        None => true,
                    };
                    start_ok && end_ok
                })
                .map(|(_, row_id)| row_id)
                .collect()
        } else {
            // Full index scan - scan all entries
            let results = btree.range(&[], &[0xFF; 32])?;
            results.into_iter().map(|(_, row_id)| row_id).collect()
        };

        // Fetch the rows using the RowIds - try persistent storage first
        let rows: Vec<Row> = if let (Some(row_store), Some(snapshot)) = (&self.row_store, &self.snapshot) {
            // Use persistent storage with MVCC visibility
            let mut result = Vec::with_capacity(row_ids.len());
            for row_id in row_ids {
                // Block on async get (in production, executor should be fully async)
                match block_on(row_store.get(table_id, snapshot, row_id)) {
                    Ok(Some(row)) => result.push(row),
                    Ok(None) => {
                        debug!("Row {:?} not found or not visible", row_id);
                    }
                    Err(e) => {
                        debug!("Error fetching row {:?}: {}", row_id, e);
                    }
                }
            }
            result
        } else {
            // Use in-memory storage
            self.storage.get_rows(table_id, &row_ids)
        };

        let batch = RecordBatch::new(schema.clone(), rows);
        Ok((vec![batch], None, schema))
    }

    fn execute_filter(
        &self,
        input: PhysicalPlan,
        predicate: Expr,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, schema) = self.execute_plan(input)?;
        let evaluator = ExprEvaluator::new(&schema);

        let filtered_batches: Vec<RecordBatch> = batches
            .into_iter()
            .map(|batch| {
                let filtered_rows: Vec<Row> = batch.rows
                    .into_iter()
                    .filter(|row| {
                        evaluator.evaluate(&predicate, row)
                            .and_then(|v| match v {
                                Value::Boolean(b) => Ok(b),
                                Value::Null => Ok(false),
                                _ => Ok(false),
                            })
                            .unwrap_or(false)
                    })
                    .collect();
                RecordBatch::new(batch.schema, filtered_rows)
            })
            .collect();

        Ok((filtered_batches, None, schema))
    }

    fn execute_project(
        &self,
        input: PhysicalPlan,
        exprs: Vec<Expr>,
        output_schema: Schema,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, input_schema) = self.execute_plan(input)?;
        let evaluator = ExprEvaluator::new(&input_schema);

        let projected_batches: Vec<RecordBatch> = batches
            .into_iter()
            .map(|batch| {
                let projected_rows: Vec<Row> = batch.rows
                    .into_iter()
                    .filter_map(|row| {
                        let values: Result<Vec<Value>> = exprs
                            .iter()
                            .map(|expr| evaluator.evaluate(expr, &row))
                            .collect();
                        values.ok().map(Row::new)
                    })
                    .collect();
                RecordBatch::new(output_schema.clone(), projected_rows)
            })
            .collect();

        Ok((projected_batches, None, output_schema))
    }

    fn execute_hash_join(
        &self,
        left: PhysicalPlan,
        right: PhysicalPlan,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (left_batches, _, left_schema) = self.execute_plan(left)?;
        let (right_batches, _, right_schema) = self.execute_plan(right)?;

        let mut combined_columns = left_schema.columns.clone();
        combined_columns.extend(right_schema.columns.clone());
        let output_schema = Schema::new(combined_columns);

        let left_rows: Vec<Row> = left_batches.into_iter().flat_map(|b| b.rows).collect();
        let right_rows: Vec<Row> = right_batches.into_iter().flat_map(|b| b.rows).collect();

        // Build hash table using ValueKey
        let mut hash_table: HashMap<ValueKey, Vec<usize>> = HashMap::new();
        for (idx, row) in right_rows.iter().enumerate() {
            let key_values: Vec<Value> = right_keys.iter()
                .map(|&i| row.values.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key = ValueKey::from_values(&key_values);
            hash_table.entry(key).or_default().push(idx);
        }

        let mut result_rows = Vec::new();
        let mut right_matched: std::collections::HashSet<usize> = std::collections::HashSet::new();

        for left_row in &left_rows {
            let key_values: Vec<Value> = left_keys.iter()
                .map(|&i| left_row.values.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key = ValueKey::from_values(&key_values);

            let matches = hash_table.get(&key);
            let mut had_match = false;

            if let Some(right_indices) = matches {
                for &right_idx in right_indices {
                    had_match = true;
                    right_matched.insert(right_idx);
                    let mut combined = left_row.values.clone();
                    combined.extend(right_rows[right_idx].values.clone());
                    result_rows.push(Row::new(combined));
                }
            }

            if !had_match && matches!(join_type, JoinType::Left | JoinType::Full) {
                let mut combined = left_row.values.clone();
                combined.extend(vec![Value::Null; right_schema.columns.len()]);
                result_rows.push(Row::new(combined));
            }
        }

        if matches!(join_type, JoinType::Right | JoinType::Full) {
            for (idx, right_row) in right_rows.iter().enumerate() {
                if !right_matched.contains(&idx) {
                    let mut combined = vec![Value::Null; left_schema.columns.len()];
                    combined.extend(right_row.values.clone());
                    result_rows.push(Row::new(combined));
                }
            }
        }

        let batch = RecordBatch::new(output_schema.clone(), result_rows);
        Ok((vec![batch], None, output_schema))
    }

    /// Execute a merge join on sorted inputs.
    ///
    /// Both inputs are first sorted by the join keys, then merged using a
    /// two-pointer approach. This is efficient for large, pre-sorted datasets
    /// or when sort-merge is cheaper than hash join.
    fn execute_merge_join(
        &self,
        left: PhysicalPlan,
        right: PhysicalPlan,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        // First, get the child results
        let (left_batches, _, left_schema) = self.execute_plan(left)?;
        let (right_batches, _, right_schema) = self.execute_plan(right)?;

        // Build output schema
        let mut combined_columns = left_schema.columns.clone();
        combined_columns.extend(right_schema.columns.clone());
        let output_schema = Schema::new(combined_columns);

        // Collect all rows
        let mut left_rows: Vec<Row> = left_batches.into_iter().flat_map(|b| b.rows).collect();
        let mut right_rows: Vec<Row> = right_batches.into_iter().flat_map(|b| b.rows).collect();

        // Sort both sides by join keys
        let sort_by_keys = |rows: &mut Vec<Row>, keys: &[usize]| {
            rows.sort_by(|a, b| {
                for &key_idx in keys {
                    let a_val = a.values.get(key_idx).cloned().unwrap_or(Value::Null);
                    let b_val = b.values.get(key_idx).cloned().unwrap_or(Value::Null);
                    let cmp = Self::compare_values(&a_val, &b_val);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        };

        sort_by_keys(&mut left_rows, &left_keys);
        sort_by_keys(&mut right_rows, &right_keys);

        let mut result_rows = Vec::new();
        let mut right_matched: std::collections::HashSet<usize> = std::collections::HashSet::new();

        let mut left_idx = 0;
        let mut right_idx = 0;

        while left_idx < left_rows.len() && right_idx < right_rows.len() {
            let left_key: Vec<Value> = left_keys.iter()
                .map(|&i| left_rows[left_idx].values.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let right_key: Vec<Value> = right_keys.iter()
                .map(|&i| right_rows[right_idx].values.get(i).cloned().unwrap_or(Value::Null))
                .collect();

            match Self::compare_key_vectors(&left_key, &right_key) {
                std::cmp::Ordering::Less => {
                    // Left key is smaller - emit null-padded for LEFT/FULL join
                    if matches!(join_type, JoinType::Left | JoinType::Full) {
                        let mut combined = left_rows[left_idx].values.clone();
                        combined.extend(vec![Value::Null; right_schema.columns.len()]);
                        result_rows.push(Row::new(combined));
                    }
                    left_idx += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Right key is smaller - emit null-padded for RIGHT/FULL join
                    if matches!(join_type, JoinType::Right | JoinType::Full) {
                        let mut combined = vec![Value::Null; left_schema.columns.len()];
                        combined.extend(right_rows[right_idx].values.clone());
                        result_rows.push(Row::new(combined));
                    }
                    right_idx += 1;
                }
                std::cmp::Ordering::Equal => {
                    // Keys match - find all matching rows on both sides
                    let left_start = left_idx;
                    let right_start = right_idx;

                    // Find extent of matching left rows
                    while left_idx < left_rows.len() {
                        let key: Vec<Value> = left_keys.iter()
                            .map(|&i| left_rows[left_idx].values.get(i).cloned().unwrap_or(Value::Null))
                            .collect();
                        if Self::compare_key_vectors(&key, &left_key) != std::cmp::Ordering::Equal {
                            break;
                        }
                        left_idx += 1;
                    }

                    // Find extent of matching right rows
                    while right_idx < right_rows.len() {
                        let key: Vec<Value> = right_keys.iter()
                            .map(|&i| right_rows[right_idx].values.get(i).cloned().unwrap_or(Value::Null))
                            .collect();
                        if Self::compare_key_vectors(&key, &right_key) != std::cmp::Ordering::Equal {
                            break;
                        }
                        right_idx += 1;
                    }

                    // Emit cartesian product of matching groups
                    for li in left_start..left_idx {
                        for ri in right_start..right_idx {
                            right_matched.insert(ri);
                            let mut combined = left_rows[li].values.clone();
                            combined.extend(right_rows[ri].values.clone());
                            result_rows.push(Row::new(combined));
                        }
                    }
                }
            }
        }

        // Handle remaining left rows for LEFT/FULL join
        if matches!(join_type, JoinType::Left | JoinType::Full) {
            while left_idx < left_rows.len() {
                let mut combined = left_rows[left_idx].values.clone();
                combined.extend(vec![Value::Null; right_schema.columns.len()]);
                result_rows.push(Row::new(combined));
                left_idx += 1;
            }
        }

        // Handle remaining/unmatched right rows for RIGHT/FULL join
        if matches!(join_type, JoinType::Right | JoinType::Full) {
            for (idx, right_row) in right_rows.iter().enumerate() {
                if !right_matched.contains(&idx) {
                    let mut combined = vec![Value::Null; left_schema.columns.len()];
                    combined.extend(right_row.values.clone());
                    result_rows.push(Row::new(combined));
                }
            }
        }

        let batch = RecordBatch::new(output_schema.clone(), result_rows);
        Ok((vec![batch], None, output_schema))
    }

    /// Compare two values for ordering
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
            (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
            (Value::Float64(l), Value::Float64(r)) => {
                l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Int64(l), Value::Float64(r)) => {
                (*l as f64).partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::Float64(l), Value::Int64(r)) => {
                l.partial_cmp(&(*r as f64)).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(l), Value::String(r)) => l.cmp(r),
            (Value::Binary(l), Value::Binary(r)) => l.cmp(r),
            (Value::Timestamp(l), Value::Timestamp(r)) => l.cmp(r),
            _ => std::cmp::Ordering::Equal,
        }
    }

    /// Compare two key vectors
    fn compare_key_vectors(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
        for (av, bv) in a.iter().zip(b.iter()) {
            let cmp = Self::compare_values(av, bv);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }

    fn execute_nested_loop_join(
        &self,
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (left_batches, _, left_schema) = self.execute_plan(left)?;
        let (right_batches, _, right_schema) = self.execute_plan(right)?;

        let mut combined_columns = left_schema.columns.clone();
        combined_columns.extend(right_schema.columns.clone());
        let output_schema = Schema::new(combined_columns);

        let mut col_map: HashMap<String, usize> = HashMap::new();
        for (i, col) in left_schema.columns.iter().enumerate() {
            col_map.insert(col.name.clone(), i);
        }
        for (i, col) in right_schema.columns.iter().enumerate() {
            col_map.insert(col.name.clone(), left_schema.columns.len() + i);
        }
        let evaluator = ExprEvaluator::with_columns(col_map);

        let left_rows: Vec<Row> = left_batches.into_iter().flat_map(|b| b.rows).collect();
        let right_rows: Vec<Row> = right_batches.into_iter().flat_map(|b| b.rows).collect();

        let mut result_rows = Vec::new();
        let mut right_matched: std::collections::HashSet<usize> = std::collections::HashSet::new();

        for left_row in &left_rows {
            let mut had_match = false;

            for (right_idx, right_row) in right_rows.iter().enumerate() {
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                let combined_row = Row::new(combined_values.clone());

                let matches = evaluator.evaluate(&condition, &combined_row)
                    .and_then(|v| match v {
                        Value::Boolean(b) => Ok(b),
                        Value::Null => Ok(false),
                        _ => Ok(false),
                    })
                    .unwrap_or(false);

                if matches {
                    had_match = true;
                    right_matched.insert(right_idx);
                    result_rows.push(Row::new(combined_values));
                }
            }

            if !had_match && matches!(join_type, JoinType::Left | JoinType::Full) {
                let mut combined = left_row.values.clone();
                combined.extend(vec![Value::Null; right_schema.columns.len()]);
                result_rows.push(Row::new(combined));
            }
        }

        if matches!(join_type, JoinType::Right | JoinType::Full) {
            for (idx, right_row) in right_rows.iter().enumerate() {
                if !right_matched.contains(&idx) {
                    let mut combined = vec![Value::Null; left_schema.columns.len()];
                    combined.extend(right_row.values.clone());
                    result_rows.push(Row::new(combined));
                }
            }
        }

        let batch = RecordBatch::new(output_schema.clone(), result_rows);
        Ok((vec![batch], None, output_schema))
    }

    fn execute_hash_aggregate(
        &self,
        input: PhysicalPlan,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        output_schema: Schema,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, input_schema) = self.execute_plan(input)?;
        let evaluator = ExprEvaluator::new(&input_schema);

        // Use ValueKey for grouping
        let mut groups: HashMap<ValueKey, Vec<AggregateAccumulator>> = HashMap::new();

        for batch in batches {
            for row in batch.rows {
                let key_values: Vec<Value> = group_by.iter()
                    .map(|&i| row.values.get(i).cloned().unwrap_or(Value::Null))
                    .collect();
                let key = ValueKey::from_values(&key_values);

                let accumulators = groups.entry(key).or_insert_with(|| {
                    aggregates.iter()
                        .map(|agg| AggregateAccumulator::new(agg.func))
                        .collect()
                });

                for (acc, agg_expr) in accumulators.iter_mut().zip(aggregates.iter()) {
                    if agg_expr.args.is_empty() || matches!(agg_expr.args.first(), Some(Expr::Wildcard)) {
                        // COUNT(*)
                        acc.accumulate_count_star();
                    } else if let Some(arg) = agg_expr.args.first() {
                        if let Ok(val) = evaluator.evaluate(arg, &row) {
                            acc.accumulate(&val);
                        }
                    }
                }
            }
        }

        // Handle no groups case for aggregates
        if groups.is_empty() && group_by.is_empty() {
            let accumulators: Vec<AggregateAccumulator> = aggregates.iter()
                .map(|agg| AggregateAccumulator::new(agg.func))
                .collect();
            groups.insert(ValueKey(vec![]), accumulators);
        }

        // Reconstruct values from keys
        let result_rows: Vec<Row> = groups.into_iter()
            .map(|(key, accumulators)| {
                let mut values: Vec<Value> = key.0.into_iter().map(|kp| {
                    match kp {
                        ValueKeyPart::Null => Value::Null,
                        ValueKeyPart::Bool(b) => Value::Boolean(b),
                        ValueKeyPart::Int(i) => Value::Int64(i),
                        ValueKeyPart::Float(f) => Value::Float64(f64::from_bits(f)),
                        ValueKeyPart::String(s) => Value::String(s.into()),
                        ValueKeyPart::Binary(b) => Value::Binary(b.into()),
                    }
                }).collect();
                values.extend(accumulators.iter().map(|acc| acc.finalize()));
                Row::new(values)
            })
            .collect();

        let batch = RecordBatch::new(output_schema.clone(), result_rows);
        Ok((vec![batch], None, output_schema))
    }

    fn execute_sort(
        &self,
        input: PhysicalPlan,
        order_by: Vec<SortExpr>,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, schema) = self.execute_plan(input)?;
        let evaluator = ExprEvaluator::new(&schema);

        let mut all_rows: Vec<Row> = batches.into_iter().flat_map(|b| b.rows).collect();

        all_rows.sort_by(|a, b| {
            for sort_expr in &order_by {
                let a_val = evaluator.evaluate(&sort_expr.expr, a).unwrap_or(Value::Null);
                let b_val = evaluator.evaluate(&sort_expr.expr, b).unwrap_or(Value::Null);

                let cmp = match (&a_val, &b_val) {
                    (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                    (Value::Null, _) => if sort_expr.nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    },
                    (_, Value::Null) => if sort_expr.nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    },
                    (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
                    (Value::Float64(l), Value::Float64(r)) => {
                        l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (Value::String(l), Value::String(r)) => l.cmp(r),
                    (Value::Timestamp(l), Value::Timestamp(r)) => l.cmp(r),
                    _ => std::cmp::Ordering::Equal,
                };

                let ordered_cmp = if sort_expr.asc { cmp } else { cmp.reverse() };
                if ordered_cmp != std::cmp::Ordering::Equal {
                    return ordered_cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        let batch = RecordBatch::new(schema.clone(), all_rows);
        Ok((vec![batch], None, schema))
    }

    fn execute_limit(
        &self,
        input: PhysicalPlan,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, schema) = self.execute_plan(input)?;

        let all_rows: Vec<Row> = batches.into_iter().flat_map(|b| b.rows).collect();
        let limited_rows: Vec<Row> = all_rows.into_iter()
            .skip(offset)
            .take(limit)
            .collect();

        let batch = RecordBatch::new(schema.clone(), limited_rows);
        Ok((vec![batch], None, schema))
    }

    fn execute_values(
        &self,
        values: Vec<Vec<Value>>,
        schema: Schema,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let rows: Vec<Row> = values.into_iter()
            .map(Row::new)
            .collect();
        let batch = RecordBatch::new(schema.clone(), rows);
        Ok((vec![batch], None, schema))
    }

    fn execute_insert(
        &self,
        table_id: TableId,
        input: PhysicalPlan,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let (batches, _, schema) = self.execute_plan(input)?;

        let rows: Vec<Row> = batches.into_iter().flat_map(|b| b.rows).collect();
        let row_count = rows.len() as u64;

        // Write to persistent storage if available
        if let Some(row_store) = &self.row_store {
            // Get txn_id from snapshot, or use TxnId(0) for auto-commit
            let txn_id = self.snapshot.as_ref()
                .map(|s| s.txn_id)
                .unwrap_or(TxnId(0));

            for row in rows {
                block_on(row_store.insert(table_id, txn_id, row))?;
            }
        } else {
            // Fall back to in-memory storage
            self.storage.insert(table_id, rows)?;
        }

        Ok((vec![], Some(row_count), schema))
    }

    fn execute_update(
        &self,
        table_id: TableId,
        input: PhysicalPlan,
        assignments: Vec<(usize, Expr)>,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let schema = self.storage.get_schema(table_id)
            .ok_or_else(|| Error::Internal("Table not found".into()))?;
        let evaluator = ExprEvaluator::new(&schema);

        let (input_batches, _, _) = self.execute_plan(input)?;
        let rows_to_match: Vec<Row> = input_batches.into_iter().flat_map(|b| b.rows).collect();

        // Update using persistent storage if available
        if let (Some(row_store), Some(snapshot)) = (&self.row_store, &self.snapshot) {
            let txn_id = snapshot.txn_id;
            let mut total_updated = 0u64;

            // Scan to find RowIds that match rows to update
            let mut iterator = row_store.scan(table_id, snapshot)?;
            let mut rows_to_update: Vec<(RowId, Row)> = Vec::new();

            while let Some(row_result) = iterator.next() {
                if let Ok((row_id, row)) = row_result {
                    // Check if this row matches any row to update
                    if rows_to_match.iter().any(|match_row| row.values == match_row.values) {
                        rows_to_update.push((row_id, row));
                    }
                }
            }

            // Apply updates to each matched row
            for (row_id, mut row) in rows_to_update {
                // Apply assignments to create new row
                for (col_idx, expr) in &assignments {
                    if let Ok(new_value) = evaluator.evaluate(expr, &row) {
                        if *col_idx < row.values.len() {
                            row.values[*col_idx] = new_value;
                        }
                    }
                }

                // Update in persistent storage (MVCC: delete old + insert new)
                block_on(row_store.update(table_id, txn_id, row_id, row))?;
                total_updated += 1;
            }

            Ok((vec![], Some(total_updated), schema))
        } else {
            // Fall back to in-memory storage
            let mut total_updated = 0u64;

            for row in &rows_to_match {
                let updates: Vec<(usize, Value)> = assignments.iter()
                    .filter_map(|(col_idx, expr)| {
                        evaluator.evaluate(expr, row).ok().map(|v| (*col_idx, v))
                    })
                    .collect();

                let count = self.storage.update(table_id, |_r| true, &updates)?;
                total_updated += count;
                break;
            }

            Ok((vec![], Some(total_updated), schema))
        }
    }

    fn execute_delete(
        &self,
        table_id: TableId,
        input: PhysicalPlan,
    ) -> Result<(Vec<RecordBatch>, Option<u64>, Schema)> {
        let schema = self.storage.get_schema(table_id)
            .ok_or_else(|| Error::Internal("Table not found".into()))?;

        // Get rows to delete from input plan
        let (input_batches, _, _) = self.execute_plan(input)?;
        let rows_to_delete: Vec<Row> = input_batches.into_iter().flat_map(|b| b.rows).collect();

        // Delete from persistent storage if available
        if let (Some(row_store), Some(snapshot)) = (&self.row_store, &self.snapshot) {
            let txn_id = snapshot.txn_id;
            let mut count = 0u64;

            // Scan to find RowIds that match rows to delete
            let mut iterator = row_store.scan(table_id, snapshot)?;
            let mut row_ids_to_delete = Vec::new();

            while let Some(row_result) = iterator.next() {
                if let Ok((row_id, row)) = row_result {
                    // Check if this row matches any row to delete
                    if rows_to_delete.iter().any(|del_row| row.values == del_row.values) {
                        row_ids_to_delete.push(row_id);
                    }
                }
            }

            // Delete each matched row
            for row_id in row_ids_to_delete {
                block_on(row_store.delete(table_id, txn_id, row_id))?;
                count += 1;
            }

            Ok((vec![], Some(count), schema))
        } else {
            // Fall back to in-memory storage
            let count = self.storage.delete(table_id, |row| {
                rows_to_delete.iter().any(|del_row| {
                    row.values == del_row.values
                })
            })?;

            Ok((vec![], Some(count), schema))
        }
    }
}

// ============================================================================
// Execution Result Helper
// ============================================================================

use thunder_query::ExecutionResult;

/// Helper function to convert ExecutionResult to rows
pub fn execution_result_to_rows(result: &ExecutionResult) -> Vec<Row> {
    result.batches.iter().flat_map(|b| b.rows.clone()).collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int64),
        ])
    }

    #[test]
    fn test_expr_evaluator_literal() {
        let schema = create_test_schema();
        let evaluator = ExprEvaluator::new(&schema);
        let row = Row::new(vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(30)]);

        let result = evaluator.evaluate(&Expr::Literal(Value::Int64(42)), &row).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_expr_evaluator_column() {
        let schema = create_test_schema();
        let evaluator = ExprEvaluator::new(&schema);
        let row = Row::new(vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(30)]);

        let result = evaluator.evaluate(
            &Expr::Column { name: "name".into(), index: None },
            &row
        ).unwrap();
        assert_eq!(result, Value::String("Alice".into()));
    }

    #[test]
    fn test_expr_evaluator_binary_op() {
        let schema = create_test_schema();
        let evaluator = ExprEvaluator::new(&schema);
        let row = Row::new(vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(30)]);

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column { name: "age".into(), index: None }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Int64(25))),
        };

        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_aggregate_accumulator_count() {
        let mut acc = AggregateAccumulator::new(AggregateFunction::Count);
        acc.accumulate(&Value::Int64(1));
        acc.accumulate(&Value::Int64(2));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::Int64(3));

        assert_eq!(acc.finalize(), Value::Int64(3));
    }

    #[test]
    fn test_aggregate_accumulator_sum() {
        let mut acc = AggregateAccumulator::new(AggregateFunction::Sum);
        acc.accumulate(&Value::Int64(10));
        acc.accumulate(&Value::Int64(20));
        acc.accumulate(&Value::Int64(30));

        assert_eq!(acc.finalize(), Value::Float64(60.0));
    }

    #[test]
    fn test_table_storage_basic() {
        let storage = TableStorage::new();
        let schema = create_test_schema();
        let table_id = TableId(1);

        storage.create_table(table_id, schema.clone());

        let rows = vec![
            Row::new(vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(30)]),
            Row::new(vec![Value::Int64(2), Value::String("Bob".into()), Value::Int64(25)]),
        ];
        let inserted = storage.insert(table_id, rows).unwrap();
        assert_eq!(inserted, 2);

        let scanned = storage.scan(table_id);
        assert_eq!(scanned.len(), 2);
    }

    #[tokio::test]
    async fn test_executor_values() {
        let storage = Arc::new(TableStorage::new());
        let executor = PhysicalExecutor::new(storage);

        let schema = Schema::new(vec![
            ColumnDef::new("a", DataType::Int64),
            ColumnDef::new("b", DataType::String),
        ]);

        let plan = PhysicalPlan::Values {
            values: vec![
                vec![Value::Int64(1), Value::String("one".into())],
                vec![Value::Int64(2), Value::String("two".into())],
            ],
            schema: schema.clone(),
        };

        let result = executor.execute(plan, TxnId(1)).await.unwrap();
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_executor_filter() {
        let storage = Arc::new(TableStorage::new());
        let executor = PhysicalExecutor::new(storage);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("value", DataType::Int64),
        ]);

        let values_plan = PhysicalPlan::Values {
            values: vec![
                vec![Value::Int64(1), Value::Int64(10)],
                vec![Value::Int64(2), Value::Int64(20)],
                vec![Value::Int64(3), Value::Int64(30)],
            ],
            schema: schema.clone(),
        };

        let filter_plan = PhysicalPlan::Filter {
            input: Box::new(values_plan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column { name: "value".into(), index: None }),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Value::Int64(15))),
            },
        };

        let result = executor.execute(filter_plan, TxnId(1)).await.unwrap();
        assert_eq!(execution_result_to_rows(&result).len(), 2);
    }

    #[tokio::test]
    async fn test_executor_sort() {
        let storage = Arc::new(TableStorage::new());
        let executor = PhysicalExecutor::new(storage);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);

        let values_plan = PhysicalPlan::Values {
            values: vec![
                vec![Value::Int64(3)],
                vec![Value::Int64(1)],
                vec![Value::Int64(2)],
            ],
            schema: schema.clone(),
        };

        let sort_plan = PhysicalPlan::Sort {
            input: Box::new(values_plan),
            order_by: vec![SortExpr {
                expr: Expr::Column { name: "id".into(), index: None },
                asc: true,
                nulls_first: false,
            }],
        };

        let result = executor.execute(sort_plan, TxnId(1)).await.unwrap();
        let rows = execution_result_to_rows(&result);
        assert_eq!(rows[0].values[0], Value::Int64(1));
        assert_eq!(rows[1].values[0], Value::Int64(2));
        assert_eq!(rows[2].values[0], Value::Int64(3));
    }

    #[tokio::test]
    async fn test_executor_limit() {
        let storage = Arc::new(TableStorage::new());
        let executor = PhysicalExecutor::new(storage);

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);

        let values_plan = PhysicalPlan::Values {
            values: (1..=10).map(|i| vec![Value::Int64(i)]).collect(),
            schema: schema.clone(),
        };

        let limit_plan = PhysicalPlan::Limit {
            input: Box::new(values_plan),
            limit: 3,
            offset: 2,
        };

        let result = executor.execute(limit_plan, TxnId(1)).await.unwrap();
        let rows = execution_result_to_rows(&result);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], Value::Int64(3));
    }
}
