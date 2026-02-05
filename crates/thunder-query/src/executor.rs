//! Query Executor
//!
//! Provides Volcano-style iterator execution for physical plans.
//! Each operator implements a pull-based iterator model where
//! consumers call `next()` to get the next batch of results.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thunder_common::error::{QueryError, SqlError};
use thunder_common::prelude::*;
use thunder_sql::{AggregateExpr, AggregateFunction, BinaryOperator, Expr, JoinType, SortExpr, UnaryOperator};

/// Tracks memory allocation during query execution and enforces limits.
///
/// Memory-intensive operators (sort, hash join, hash aggregate) must register
/// their allocations. If the cumulative allocation exceeds the configured limit,
/// the query fails with `OutOfMemory`.
pub struct MemoryTracker {
    allocated: AtomicUsize,
    limit: usize,
}

impl MemoryTracker {
    /// Create a new memory tracker with the given byte limit.
    pub fn new(limit: usize) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            limit,
        }
    }

    /// Try to allocate `bytes`. Returns `Err(OutOfMemory)` if the limit would be exceeded.
    pub fn allocate(&self, bytes: usize) -> Result<()> {
        let prev = self.allocated.fetch_add(bytes, Ordering::Relaxed);
        let new = prev + bytes;
        if new > self.limit {
            self.allocated.fetch_sub(bytes, Ordering::Relaxed);
            return Err(Error::Query(QueryError::OutOfMemory));
        }
        Ok(())
    }

    /// Release `bytes` of previously allocated memory.
    pub fn deallocate(&self, bytes: usize) {
        self.allocated.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Current allocated bytes.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }
}

use crate::{ExecutionResult, ExecutionStats, PhysicalPlan, RecordBatch};

/// Expression evaluator for runtime evaluation
pub struct ExprEvaluator;

impl ExprEvaluator {
    /// Evaluate an expression against a row
    pub fn evaluate(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
        match expr {
            Expr::Column { name, index } => {
                if let Some(idx) = index {
                    row.get(*idx).cloned().ok_or_else(|| {
                        Error::Internal(format!("Column index {} out of bounds", idx))
                    })
                } else {
                    let idx = schema
                        .column_by_name(name)
                        .map(|(i, _)| i)
                        .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(name.clone())))?;
                    row.get(idx).cloned().ok_or_else(|| {
                        Error::Internal(format!("Column '{}' not found in row", name))
                    })
                }
            }

            Expr::Literal(value) => Ok(value.clone()),

            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate(left, row, schema)?;
                let right_val = Self::evaluate(right, row, schema)?;
                Self::eval_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = Self::evaluate(expr, row, schema)?;
                Self::eval_unary_op(op, &val)
            }

            Expr::Function { name, args } => {
                let arg_values: Result<Vec<Value>> = args
                    .iter()
                    .map(|arg| Self::evaluate(arg, row, schema))
                    .collect();
                Self::eval_function(name, &arg_values?)
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let operand_val = operand
                    .as_ref()
                    .map(|e| Self::evaluate(e, row, schema))
                    .transpose()?;

                for (when_expr, then_expr) in when_clauses {
                    let when_val = Self::evaluate(when_expr, row, schema)?;

                    let matches = match &operand_val {
                        Some(op) => Self::values_equal(op, &when_val),
                        None => when_val.as_bool().unwrap_or(false),
                    };

                    if matches {
                        return Self::evaluate(then_expr, row, schema);
                    }
                }

                if let Some(else_expr) = else_clause {
                    Self::evaluate(else_expr, row, schema)
                } else {
                    Ok(Value::Null)
                }
            }

            Expr::IsNull(inner) => {
                let val = Self::evaluate(inner, row, schema)?;
                Ok(Value::Boolean(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = Self::evaluate(inner, row, schema)?;
                Ok(Value::Boolean(!val.is_null()))
            }

            Expr::InList { expr, list, negated } => {
                let val = Self::evaluate(expr, row, schema)?;
                let mut found = false;
                for item in list {
                    let item_val = Self::evaluate(item, row, schema)?;
                    if Self::values_equal(&val, &item_val) {
                        found = true;
                        break;
                    }
                }
                Ok(Value::Boolean(if *negated { !found } else { found }))
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = Self::evaluate(expr, row, schema)?;
                let low_val = Self::evaluate(low, row, schema)?;
                let high_val = Self::evaluate(high, row, schema)?;

                let in_range = Self::compare_values(&val, &low_val) != std::cmp::Ordering::Less
                    && Self::compare_values(&val, &high_val) != std::cmp::Ordering::Greater;

                Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
            }

            Expr::Cast { expr, data_type } => {
                let val = Self::evaluate(expr, row, schema)?;
                Self::cast_value(&val, data_type)
            }

            Expr::Subquery(_) => Err(Error::Internal(
                "Subqueries must be evaluated separately".to_string(),
            )),

            Expr::Wildcard => Err(Error::Internal(
                "Wildcard cannot be evaluated".to_string(),
            )),
        }
    }

    /// Evaluate binary operation
    fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
        // Handle NULL propagation
        if left.is_null() || right.is_null() {
            return match op {
                BinaryOperator::And => {
                    // false AND NULL = false
                    if let Value::Boolean(false) = left {
                        return Ok(Value::Boolean(false));
                    }
                    if let Value::Boolean(false) = right {
                        return Ok(Value::Boolean(false));
                    }
                    Ok(Value::Null)
                }
                BinaryOperator::Or => {
                    // true OR NULL = true
                    if let Value::Boolean(true) = left {
                        return Ok(Value::Boolean(true));
                    }
                    if let Value::Boolean(true) = right {
                        return Ok(Value::Boolean(true));
                    }
                    Ok(Value::Null)
                }
                _ => Ok(Value::Null),
            };
        }

        match op {
            // Arithmetic
            BinaryOperator::Add => Self::numeric_op(left, right, |a, b| a + b, |a, b| a + b),
            BinaryOperator::Sub => Self::numeric_op(left, right, |a, b| a - b, |a, b| a - b),
            BinaryOperator::Mul => Self::numeric_op(left, right, |a, b| a * b, |a, b| a * b),
            BinaryOperator::Div => {
                // Check for division by zero
                if let Some(r) = right.as_f64() {
                    if r == 0.0 {
                        return Err(Error::Query(QueryError::DivisionByZero));
                    }
                }
                Self::numeric_op(left, right, |a, b| a / b, |a, b| a / b)
            }
            BinaryOperator::Mod => Self::numeric_op(left, right, |a, b| a % b, |a, b| a % b),

            // Comparison
            BinaryOperator::Eq => Ok(Value::Boolean(Self::values_equal(left, right))),
            BinaryOperator::NotEq => Ok(Value::Boolean(!Self::values_equal(left, right))),
            BinaryOperator::Lt => {
                Ok(Value::Boolean(Self::compare_values(left, right) == std::cmp::Ordering::Less))
            }
            BinaryOperator::LtEq => {
                Ok(Value::Boolean(Self::compare_values(left, right) != std::cmp::Ordering::Greater))
            }
            BinaryOperator::Gt => {
                Ok(Value::Boolean(Self::compare_values(left, right) == std::cmp::Ordering::Greater))
            }
            BinaryOperator::GtEq => {
                Ok(Value::Boolean(Self::compare_values(left, right) != std::cmp::Ordering::Less))
            }

            // Logical
            BinaryOperator::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Boolean(l && r))
            }
            BinaryOperator::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Boolean(l || r))
            }

            // String operations
            BinaryOperator::Concat => {
                let l = left.to_string();
                let r = right.to_string();
                Ok(Value::String(Arc::from(format!("{}{}", l, r).as_str())))
            }

            BinaryOperator::Like => {
                let text = left.to_string();
                let pattern = right.to_string();
                Ok(Value::Boolean(Self::like_match(&text, &pattern, false)))
            }

            BinaryOperator::ILike => {
                let text = left.to_string().to_lowercase();
                let pattern = right.to_string().to_lowercase();
                Ok(Value::Boolean(Self::like_match(&text, &pattern, false)))
            }
        }
    }

    /// Evaluate unary operation
    fn eval_unary_op(op: &UnaryOperator, val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::Null);
        }

        match op {
            UnaryOperator::Not => {
                let b = val.as_bool().unwrap_or(false);
                Ok(Value::Boolean(!b))
            }
            UnaryOperator::Neg => match val {
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Int32(n) => Ok(Value::Int32(-n)),
                Value::Float64(n) => Ok(Value::Float64(-n)),
                Value::Float32(n) => Ok(Value::Float32(-n)),
                _ => Err(Error::Internal("Cannot negate non-numeric value".to_string())),
            },
            UnaryOperator::Plus => Ok(val.clone()),
        }
    }

    /// Evaluate a function call
    fn eval_function(name: &str, args: &[Value]) -> Result<Value> {
        let name_upper = name.to_uppercase();

        match name_upper.as_str() {
            // String functions
            "UPPER" => {
                let s = args.first().map(|v| v.to_string()).unwrap_or_default();
                Ok(Value::String(Arc::from(s.to_uppercase().as_str())))
            }
            "LOWER" => {
                let s = args.first().map(|v| v.to_string()).unwrap_or_default();
                Ok(Value::String(Arc::from(s.to_lowercase().as_str())))
            }
            "LENGTH" => {
                let s = args.first().map(|v| v.to_string()).unwrap_or_default();
                Ok(Value::Int32(s.chars().count() as i32))
            }
            "CONCAT" => {
                let s: String = args.iter().map(|v| v.to_string()).collect();
                Ok(Value::String(Arc::from(s.as_str())))
            }
            "SUBSTRING" | "SUBSTR" => {
                let s = args.first().map(|v| v.to_string()).unwrap_or_default();
                let start = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
                let len = args.get(2).and_then(|v| v.as_i64());

                let start_idx = start.saturating_sub(1); // SQL is 1-indexed
                let chars: Vec<char> = s.chars().collect();
                let end_idx = len.map(|l| (start_idx + l as usize).min(chars.len())).unwrap_or(chars.len());
                let result: String = chars[start_idx.min(chars.len())..end_idx].iter().collect();
                Ok(Value::String(Arc::from(result.as_str())))
            }
            "TRIM" => {
                let s = args.first().map(|v| v.to_string()).unwrap_or_default();
                Ok(Value::String(Arc::from(s.trim())))
            }

            // Numeric functions
            "ABS" => {
                let v = args.first().ok_or_else(|| Error::Internal("ABS requires argument".to_string()))?;
                match v {
                    Value::Int64(n) => Ok(Value::Int64(n.abs())),
                    Value::Int32(n) => Ok(Value::Int32(n.abs())),
                    Value::Float64(n) => Ok(Value::Float64(n.abs())),
                    Value::Float32(n) => Ok(Value::Float32(n.abs())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(Error::Internal("ABS requires numeric argument".to_string())),
                }
            }
            "CEIL" | "CEILING" => {
                let v = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                Ok(Value::Int64(v.ceil() as i64))
            }
            "FLOOR" => {
                let v = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                Ok(Value::Int64(v.floor() as i64))
            }
            "ROUND" => {
                let v = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let multiplier = 10f64.powi(precision);
                Ok(Value::Float64((v * multiplier).round() / multiplier))
            }
            "SQRT" => {
                let v = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                Ok(Value::Float64(v.sqrt()))
            }
            "POWER" | "POW" => {
                let base = args.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                let exp = args.get(1).and_then(|v| v.as_f64()).unwrap_or(1.0);
                Ok(Value::Float64(base.powf(exp)))
            }

            // Null handling
            "COALESCE" => {
                for arg in args {
                    if !arg.is_null() {
                        return Ok(arg.clone());
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                if args.len() >= 2 && Self::values_equal(&args[0], &args[1]) {
                    Ok(Value::Null)
                } else {
                    Ok(args.first().cloned().unwrap_or(Value::Null))
                }
            }

            // Date/time functions
            "NOW" | "CURRENT_TIMESTAMP" => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                Ok(Value::Timestamp(now))
            }

            // Unknown function
            _ => Err(Error::Internal(format!("Unknown function: {}", name))),
        }
    }

    /// Helper for numeric operations
    fn numeric_op<F, G>(left: &Value, right: &Value, int_op: F, float_op: G) -> Result<Value>
    where
        F: Fn(i64, i64) -> i64,
        G: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(int_op(*l, *r))),
            (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(int_op(*l as i64, *r as i64) as i32)),
            (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(float_op(*l, *r))),
            (Value::Float32(l), Value::Float32(r)) => Ok(Value::Float32(float_op(*l as f64, *r as f64) as f32)),
            // Mixed types - promote to float
            (l, r) => {
                let lf = l.as_f64().unwrap_or(0.0);
                let rf = r.as_f64().unwrap_or(0.0);
                Ok(Value::Float64(float_op(lf, rf)))
            }
        }
    }

    /// Check if two values are equal
    fn values_equal(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, Value::Null) => true, // For comparison purposes
            (Value::Boolean(l), Value::Boolean(r)) => l == r,
            (Value::Int64(l), Value::Int64(r)) => l == r,
            (Value::Int32(l), Value::Int32(r)) => l == r,
            (Value::Float64(l), Value::Float64(r)) => (l - r).abs() < f64::EPSILON,
            (Value::String(l), Value::String(r)) => l == r,
            // Handle cross-type comparisons
            (l, r) => {
                if let (Some(lf), Some(rf)) = (l.as_f64(), r.as_f64()) {
                    (lf - rf).abs() < f64::EPSILON
                } else {
                    l.to_string() == r.to_string()
                }
            }
        }
    }

    /// Compare two values
    pub fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
            (Value::Int32(l), Value::Int32(r)) => l.cmp(r),
            (Value::Float64(l), Value::Float64(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
            (Value::String(l), Value::String(r)) => l.cmp(r),
            (Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
            // Cross-type comparison via float
            (l, r) => {
                if let (Some(lf), Some(rf)) = (l.as_f64(), r.as_f64()) {
                    lf.partial_cmp(&rf).unwrap_or(Ordering::Equal)
                } else {
                    l.to_string().cmp(&r.to_string())
                }
            }
        }
    }

    /// SQL LIKE pattern matching
    fn like_match(text: &str, pattern: &str, escape: bool) -> bool {
        let mut text_chars = text.chars().peekable();
        let mut pattern_chars = pattern.chars().peekable();

        while let Some(p) = pattern_chars.next() {
            match p {
                '%' => {
                    // Match any sequence
                    if pattern_chars.peek().is_none() {
                        return true;
                    }
                    // Try matching rest of pattern from each position
                    let remaining_pattern: String = pattern_chars.collect();
                    while text_chars.peek().is_some() {
                        let remaining_text: String = text_chars.clone().collect();
                        if Self::like_match(&remaining_text, &remaining_pattern, escape) {
                            return true;
                        }
                        text_chars.next();
                    }
                    return Self::like_match("", &remaining_pattern, escape);
                }
                '_' => {
                    // Match any single character
                    if text_chars.next().is_none() {
                        return false;
                    }
                }
                c => {
                    // Match literal character
                    match text_chars.next() {
                        Some(tc) if tc == c => {}
                        _ => return false,
                    }
                }
            }
        }

        text_chars.peek().is_none()
    }

    /// Cast a value to a target type
    fn cast_value(value: &Value, target: &DataType) -> Result<Value> {
        if value.is_null() {
            return Ok(Value::Null);
        }

        match target {
            DataType::Boolean => {
                let b = match value {
                    Value::Boolean(b) => *b,
                    Value::Int64(n) => *n != 0,
                    Value::Int32(n) => *n != 0,
                    Value::String(s) => s.eq_ignore_ascii_case("true") || s.as_ref() == "1",
                    _ => return Err(Error::Internal("Cannot cast to boolean".to_string())),
                };
                Ok(Value::Boolean(b))
            }
            DataType::Int64 => {
                let n = value.as_i64().ok_or_else(|| {
                    Error::Internal(format!("Cannot cast {:?} to Int64", value))
                })?;
                Ok(Value::Int64(n))
            }
            DataType::Int32 => {
                let n = value.as_i64().ok_or_else(|| {
                    Error::Internal(format!("Cannot cast {:?} to Int32", value))
                })?;
                Ok(Value::Int32(n as i32))
            }
            DataType::Float64 => {
                let n = value.as_f64().ok_or_else(|| {
                    Error::Internal(format!("Cannot cast {:?} to Float64", value))
                })?;
                Ok(Value::Float64(n))
            }
            DataType::String | DataType::Varchar(_) => {
                Ok(Value::String(Arc::from(value.to_string().as_str())))
            }
            _ => Err(Error::Internal(format!(
                "Unsupported cast to {:?}",
                target
            ))),
        }
    }
}

/// Aggregator for computing aggregate functions
pub struct Aggregator {
    /// Aggregate function
    pub func: AggregateFunction,
    /// Whether to use DISTINCT
    pub distinct: bool,
    /// State for aggregation
    state: AggregateState,
}

/// Aggregate state
enum AggregateState {
    Count(i64),
    Sum { sum: f64, has_value: bool },
    Avg { sum: f64, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    ArrayAgg(Vec<Value>),
    StringAgg { values: Vec<String>, separator: Option<String> },
}

impl Aggregator {
    /// Create a new aggregator
    pub fn new(expr: &AggregateExpr) -> Self {
        let state = match expr.func {
            AggregateFunction::Count => AggregateState::Count(0),
            AggregateFunction::Sum => AggregateState::Sum { sum: 0.0, has_value: false },
            AggregateFunction::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => AggregateState::Min(None),
            AggregateFunction::Max => AggregateState::Max(None),
            AggregateFunction::First => AggregateState::First(None),
            AggregateFunction::Last => AggregateState::Last(None),
            AggregateFunction::ArrayAgg => AggregateState::ArrayAgg(Vec::new()),
            AggregateFunction::StringAgg => AggregateState::StringAgg {
                values: Vec::new(),
                separator: None,
            },
        };

        Self {
            func: expr.func.clone(),
            distinct: expr.distinct,
            state,
        }
    }

    /// Accumulate a value
    pub fn accumulate(&mut self, values: &[Value]) {
        // Skip NULL values for most aggregates
        let value = values.first().cloned().unwrap_or(Value::Null);

        match &mut self.state {
            AggregateState::Count(count) => {
                // COUNT(*) counts all, COUNT(expr) counts non-null
                if values.is_empty() || !value.is_null() {
                    *count += 1;
                }
            }
            AggregateState::Sum { sum, has_value } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *has_value = true;
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(v) = value.as_f64() {
                    *sum += v;
                    *count += 1;
                }
            }
            AggregateState::Min(min) => {
                if !value.is_null() {
                    *min = Some(match min.take() {
                        None => value,
                        Some(current) => {
                            if ExprEvaluator::compare_values(&value, &current)
                                == std::cmp::Ordering::Less
                            {
                                value
                            } else {
                                current
                            }
                        }
                    });
                }
            }
            AggregateState::Max(max) => {
                if !value.is_null() {
                    *max = Some(match max.take() {
                        None => value,
                        Some(current) => {
                            if ExprEvaluator::compare_values(&value, &current)
                                == std::cmp::Ordering::Greater
                            {
                                value
                            } else {
                                current
                            }
                        }
                    });
                }
            }
            AggregateState::First(first) => {
                if first.is_none() && !value.is_null() {
                    *first = Some(value);
                }
            }
            AggregateState::Last(last) => {
                if !value.is_null() {
                    *last = Some(value);
                }
            }
            AggregateState::ArrayAgg(arr) => {
                arr.push(value);
            }
            AggregateState::StringAgg { values, separator } => {
                if !value.is_null() {
                    values.push(value.to_string());
                }
                if separator.is_none() && values.len() > 1 {
                    *separator = Some(",".to_string());
                }
            }
        }
    }

    /// Get the final result
    pub fn finalize(&self) -> Value {
        match &self.state {
            AggregateState::Count(count) => Value::Int64(*count),
            AggregateState::Sum { sum, has_value } => {
                if *has_value {
                    Value::Float64(*sum)
                } else {
                    Value::Null
                }
            }
            AggregateState::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float64(*sum / *count as f64)
                } else {
                    Value::Null
                }
            }
            AggregateState::Min(val) => val.clone().unwrap_or(Value::Null),
            AggregateState::Max(val) => val.clone().unwrap_or(Value::Null),
            AggregateState::First(val) => val.clone().unwrap_or(Value::Null),
            AggregateState::Last(val) => val.clone().unwrap_or(Value::Null),
            AggregateState::ArrayAgg(arr) => {
                // For simplicity, convert to JSON-like string
                let s: String = arr.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
                Value::String(Arc::from(format!("[{}]", s).as_str()))
            }
            AggregateState::StringAgg { values, separator } => {
                let sep = separator.as_deref().unwrap_or(",");
                Value::String(Arc::from(values.join(sep).as_str()))
            }
        }
    }

    /// Reset the aggregator state
    pub fn reset(&mut self) {
        self.state = match self.func {
            AggregateFunction::Count => AggregateState::Count(0),
            AggregateFunction::Sum => AggregateState::Sum { sum: 0.0, has_value: false },
            AggregateFunction::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => AggregateState::Min(None),
            AggregateFunction::Max => AggregateState::Max(None),
            AggregateFunction::First => AggregateState::First(None),
            AggregateFunction::Last => AggregateState::Last(None),
            AggregateFunction::ArrayAgg => AggregateState::ArrayAgg(Vec::new()),
            AggregateFunction::StringAgg => AggregateState::StringAgg {
                values: Vec::new(),
                separator: None,
            },
        };
    }
}

/// Query execution context
pub struct ExecutionContext {
    /// Batch size for vectorized execution
    pub batch_size: usize,
    /// Memory limit in bytes
    pub memory_limit: usize,
    /// Memory tracker for enforcing limits
    pub memory_tracker: Arc<MemoryTracker>,
    /// Statistics
    pub stats: ExecutionStats,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        let memory_limit = 256 * 1024 * 1024; // 256MB
        Self {
            batch_size: 1024,
            memory_limit,
            memory_tracker: Arc::new(MemoryTracker::new(memory_limit)),
            stats: ExecutionStats::default(),
        }
    }
}

/// In-memory query executor
pub struct MemoryExecutor {
    /// Data storage (table_id -> rows)
    tables: HashMap<TableId, Vec<Row>>,
    /// Schema storage
    schemas: HashMap<TableId, Schema>,
    /// Memory tracker for enforcing per-query memory limits
    memory_tracker: Arc<MemoryTracker>,
}

impl Default for MemoryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryExecutor {
    /// Create a new memory executor
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            schemas: HashMap::new(),
            memory_tracker: Arc::new(MemoryTracker::new(256 * 1024 * 1024)),
        }
    }

    /// Create a new memory executor with a specific memory tracker
    pub fn with_memory_tracker(memory_tracker: Arc<MemoryTracker>) -> Self {
        Self {
            tables: HashMap::new(),
            schemas: HashMap::new(),
            memory_tracker,
        }
    }

    /// Register a table with data
    pub fn register_table(&mut self, table_id: TableId, schema: Schema, rows: Vec<Row>) {
        self.schemas.insert(table_id, schema);
        self.tables.insert(table_id, rows);
    }

    /// Execute a physical plan
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<Row>> {
        match plan {
            PhysicalPlan::SeqScan { table_id, schema, filter } => {
                self.execute_seq_scan(*table_id, schema, filter.as_ref())
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.execute_filter(input, predicate)
            }
            PhysicalPlan::Project { input, exprs, schema } => {
                self.execute_project(input, exprs, schema)
            }
            PhysicalPlan::Sort { input, order_by } => {
                self.execute_sort(input, order_by)
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                self.execute_limit(input, *limit, *offset)
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregates, schema } => {
                self.execute_hash_aggregate(input, group_by, aggregates, schema)
            }
            PhysicalPlan::HashJoin { left, right, left_keys, right_keys, join_type } => {
                self.execute_hash_join(left, right, left_keys, right_keys, *join_type)
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                self.execute_nested_loop_join(left, right, condition, *join_type)
            }
            PhysicalPlan::Values { values, .. } => {
                Ok(values.iter().map(|v| Row::new(v.clone())).collect())
            }
            PhysicalPlan::Empty { .. } => Ok(vec![]),
            _ => Err(Error::Internal(format!(
                "Unsupported physical plan: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    fn execute_seq_scan(
        &self,
        table_id: TableId,
        schema: &Schema,
        filter: Option<&Expr>,
    ) -> Result<Vec<Row>> {
        let rows = self.tables.get(&table_id).cloned().unwrap_or_default();
        let table_schema = self.schemas.get(&table_id).unwrap_or(schema);

        if let Some(predicate) = filter {
            rows.into_iter()
                .filter(|row| {
                    ExprEvaluator::evaluate(predicate, row, table_schema)
                        .map(|v| v.as_bool().unwrap_or(false))
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(Ok)
                .collect()
        } else {
            Ok(rows)
        }
    }

    fn execute_filter(&self, input: &PhysicalPlan, predicate: &Expr) -> Result<Vec<Row>> {
        let input_rows = self.execute(input)?;
        let schema = self.plan_schema(input)?;

        input_rows
            .into_iter()
            .filter(|row| {
                ExprEvaluator::evaluate(predicate, row, &schema)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
            .map(Ok)
            .collect()
    }

    fn execute_project(
        &self,
        input: &PhysicalPlan,
        exprs: &[Expr],
        _output_schema: &Schema,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute(input)?;
        let input_schema = self.plan_schema(input)?;

        input_rows
            .into_iter()
            .map(|row| {
                let values: Result<Vec<Value>> = exprs
                    .iter()
                    .map(|expr| ExprEvaluator::evaluate(expr, &row, &input_schema))
                    .collect();
                Ok(Row::new(values?))
            })
            .collect()
    }

    /// Estimate the in-memory size of a set of rows (bytes).
    fn estimate_rows_size(rows: &[Row]) -> usize {
        // Conservative estimate: 64 bytes base + 32 bytes per value
        rows.len() * (64 + rows.first().map(|r| r.len() * 32).unwrap_or(0))
    }

    fn execute_sort(&self, input: &PhysicalPlan, order_by: &[SortExpr]) -> Result<Vec<Row>> {
        let mut rows = self.execute(input)?;
        let schema = self.plan_schema(input)?;

        // Track memory for the sort buffer
        let estimated_bytes = Self::estimate_rows_size(&rows);
        self.memory_tracker.allocate(estimated_bytes)?;

        rows.sort_by(|a, b| {
            for sort_expr in order_by {
                let a_val = ExprEvaluator::evaluate(&sort_expr.expr, a, &schema).unwrap_or(Value::Null);
                let b_val = ExprEvaluator::evaluate(&sort_expr.expr, b, &schema).unwrap_or(Value::Null);

                let cmp = ExprEvaluator::compare_values(&a_val, &b_val);

                let cmp = if sort_expr.asc { cmp } else { cmp.reverse() };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    fn execute_limit(&self, input: &PhysicalPlan, limit: usize, offset: usize) -> Result<Vec<Row>> {
        let rows = self.execute(input)?;
        Ok(rows.into_iter().skip(offset).take(limit).collect())
    }

    fn execute_hash_aggregate(
        &self,
        input: &PhysicalPlan,
        group_by: &[usize],
        aggregates: &[AggregateExpr],
        _output_schema: &Schema,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute(input)?;
        let input_schema = self.plan_schema(input)?;

        // Helper to convert values to a string key for hashing
        fn values_to_key(values: &[Value]) -> String {
            values
                .iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join("|")
        }

        // Group rows by key - use String key since Value doesn't implement Hash
        let mut groups: HashMap<String, (Vec<Value>, Vec<Aggregator>)> = HashMap::new();
        // Track memory for groups: each group key ~128 bytes + aggregator state ~64 bytes per agg
        let per_group_bytes = 128 + aggregates.len() * 64;
        let mut tracked_groups: usize = 0;

        for row in input_rows {
            // Build group key
            let key_values: Vec<Value> = group_by
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key_str = values_to_key(&key_values);

            // Get or create aggregators for this group
            let is_new = !groups.contains_key(&key_str);
            let (_, aggs) = groups.entry(key_str).or_insert_with(|| {
                (key_values, aggregates.iter().map(Aggregator::new).collect())
            });

            // Track memory for new groups
            if is_new {
                tracked_groups += 1;
                if tracked_groups % 1000 == 0 {
                    // Batch-check memory every 1000 groups
                    self.memory_tracker.allocate(per_group_bytes * 1000)?;
                }
            }

            // Accumulate values
            for (agg, agg_expr) in aggs.iter_mut().zip(aggregates.iter()) {
                let arg_values: Vec<Value> = agg_expr
                    .args
                    .iter()
                    .map(|arg| ExprEvaluator::evaluate(arg, &row, &input_schema).unwrap_or(Value::Null))
                    .collect();
                agg.accumulate(&arg_values);
            }
        }

        // Build result rows
        let mut result_rows = Vec::new();
        for (_key_str, (key_values, aggs)) in groups {
            let mut values = key_values;
            for agg in aggs {
                values.push(agg.finalize());
            }
            result_rows.push(Row::new(values));
        }

        Ok(result_rows)
    }

    fn execute_hash_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        left_keys: &[usize],
        right_keys: &[usize],
        join_type: JoinType,
    ) -> Result<Vec<Row>> {
        let left_rows = self.execute(left)?;
        let right_rows = self.execute(right)?;
        let left_schema = self.plan_schema(left)?;
        let right_schema = self.plan_schema(right)?;

        // Helper to convert values to a string key for hashing
        fn values_to_key(values: &[Value]) -> String {
            values
                .iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>()
                .join("|")
        }

        // Track memory for the hash table (built from right side)
        let hash_table_bytes = Self::estimate_rows_size(&right_rows);
        self.memory_tracker.allocate(hash_table_bytes)?;

        // Build hash table from right side - use String key since Value doesn't implement Hash
        let mut hash_table: HashMap<String, Vec<Row>> = HashMap::new();
        for row in &right_rows {
            let key_values: Vec<Value> = right_keys
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key_str = values_to_key(&key_values);
            hash_table.entry(key_str).or_default().push(row.clone());
        }

        let mut result = Vec::new();
        let right_nulls: Vec<Value> = (0..right_schema.len()).map(|_| Value::Null).collect();

        for left_row in &left_rows {
            let key_values: Vec<Value> = left_keys
                .iter()
                .map(|&i| left_row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key_str = values_to_key(&key_values);

            if let Some(matches) = hash_table.get(&key_str) {
                for right_row in matches {
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.clone());
                    result.push(Row::new(values));
                }
            } else if join_type == JoinType::Left || join_type == JoinType::Full {
                let mut values = left_row.values.clone();
                values.extend(right_nulls.clone());
                result.push(Row::new(values));
            }
        }

        // Handle right outer join
        if join_type == JoinType::Right || join_type == JoinType::Full {
            let left_nulls: Vec<Value> = (0..left_schema.len()).map(|_| Value::Null).collect();

            for right_row in &right_rows {
                let key_values: Vec<Value> = right_keys
                    .iter()
                    .map(|&i| right_row.get(i).cloned().unwrap_or(Value::Null))
                    .collect();
                let key_str = values_to_key(&key_values);

                // Check if this right row was matched
                let matched = left_rows.iter().any(|left_row| {
                    let left_key: Vec<Value> = left_keys
                        .iter()
                        .map(|&i| left_row.get(i).cloned().unwrap_or(Value::Null))
                        .collect();
                    let left_key_str = values_to_key(&left_key);
                    key_str == left_key_str
                });

                if !matched {
                    let mut values = left_nulls.clone();
                    values.extend(right_row.values.clone());
                    result.push(Row::new(values));
                }
            }
        }

        Ok(result)
    }

    fn execute_nested_loop_join(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &Expr,
        join_type: JoinType,
    ) -> Result<Vec<Row>> {
        let left_rows = self.execute(left)?;
        let right_rows = self.execute(right)?;
        let left_schema = self.plan_schema(left)?;
        let right_schema = self.plan_schema(right)?;

        // Create combined schema for condition evaluation
        let mut combined_columns = left_schema.columns.clone();
        combined_columns.extend(right_schema.columns.clone());
        let combined_schema = Schema::new(combined_columns);

        let mut result = Vec::new();
        let right_nulls: Vec<Value> = (0..right_schema.len()).map(|_| Value::Null).collect();

        for left_row in &left_rows {
            let mut matched = false;

            for right_row in &right_rows {
                // Create combined row for condition evaluation
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                let combined_row = Row::new(combined_values.clone());

                // Evaluate join condition
                let matches = ExprEvaluator::evaluate(condition, &combined_row, &combined_schema)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false);

                if matches {
                    result.push(combined_row);
                    matched = true;
                }
            }

            // Handle left outer join
            if !matched && (join_type == JoinType::Left || join_type == JoinType::Full) {
                let mut values = left_row.values.clone();
                values.extend(right_nulls.clone());
                result.push(Row::new(values));
            }
        }

        // Handle right outer join (similar to hash join)
        if join_type == JoinType::Right || join_type == JoinType::Full {
            let left_nulls: Vec<Value> = (0..left_schema.len()).map(|_| Value::Null).collect();

            for right_row in &right_rows {
                let matched = left_rows.iter().any(|left_row| {
                    let mut combined_values = left_row.values.clone();
                    combined_values.extend(right_row.values.clone());
                    let combined_row = Row::new(combined_values);

                    ExprEvaluator::evaluate(condition, &combined_row, &combined_schema)
                        .map(|v| v.as_bool().unwrap_or(false))
                        .unwrap_or(false)
                });

                if !matched {
                    let mut values = left_nulls.clone();
                    values.extend(right_row.values.clone());
                    result.push(Row::new(values));
                }
            }
        }

        Ok(result)
    }

    /// Get the schema of a physical plan
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
}

/// Async query executor trait implementation
pub struct AsyncExecutor {
    memory_executor: MemoryExecutor,
}

impl AsyncExecutor {
    pub fn new() -> Self {
        Self {
            memory_executor: MemoryExecutor::new(),
        }
    }

    pub fn with_tables(tables: HashMap<TableId, (Schema, Vec<Row>)>) -> Self {
        let mut executor = Self::new();
        for (table_id, (schema, rows)) in tables {
            executor.memory_executor.register_table(table_id, schema, rows);
        }
        executor
    }
}

impl Default for AsyncExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl crate::Executor for AsyncExecutor {
    async fn execute(&self, plan: PhysicalPlan, _txn_id: TxnId) -> Result<ExecutionResult> {
        let start = std::time::Instant::now();

        let rows = self.memory_executor.execute(&plan)?;
        let schema = self.memory_executor.plan_schema(&plan)?;

        let stats = ExecutionStats {
            rows_read: rows.len() as u64,
            execution_time_ms: start.elapsed().as_millis() as u64,
            ..Default::default()
        };

        Ok(ExecutionResult {
            schema: schema.clone(),
            batches: vec![RecordBatch::new(schema, rows)],
            rows_affected: None,
            stats,
        })
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

    fn test_row(id: i64, name: &str, age: i32) -> Row {
        Row::new(vec![
            Value::Int64(id),
            Value::String(Arc::from(name)),
            Value::Int32(age),
        ])
    }

    #[test]
    fn test_evaluate_literal() {
        let schema = test_schema();
        let row = test_row(1, "Alice", 30);

        let expr = Expr::Literal(Value::Int64(42));
        let result = ExprEvaluator::evaluate(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_evaluate_column() {
        let schema = test_schema();
        let row = test_row(1, "Alice", 30);

        let expr = Expr::Column {
            name: "name".to_string(),
            index: None,
        };
        let result = ExprEvaluator::evaluate(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::String(Arc::from("Alice")));
    }

    #[test]
    fn test_evaluate_binary_op() {
        let schema = test_schema();
        let row = test_row(1, "Alice", 30);

        // age > 25
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: None,
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Int32(25))),
        };
        let result = ExprEvaluator::evaluate(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_like() {
        let schema = test_schema();
        let row = test_row(1, "Alice", 30);

        // name LIKE 'A%'
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "name".to_string(),
                index: None,
            }),
            op: BinaryOperator::Like,
            right: Box::new(Expr::Literal(Value::String(Arc::from("A%")))),
        };
        let result = ExprEvaluator::evaluate(&expr, &row, &schema).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_seq_scan() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![
                test_row(1, "Alice", 30),
                test_row(2, "Bob", 25),
                test_row(3, "Charlie", 35),
            ],
        );

        let plan = PhysicalPlan::SeqScan {
            table_id,
            schema: schema.clone(),
            filter: None,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_filter() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![
                test_row(1, "Alice", 30),
                test_row(2, "Bob", 25),
                test_row(3, "Charlie", 35),
            ],
        );

        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "age".to_string(),
                    index: None,
                }),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Value::Int32(28))),
            },
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_project() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![test_row(1, "Alice", 30)],
        );

        let output_schema = Schema::new(vec![
            ColumnDef::new("name", DataType::String),
        ]);

        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            exprs: vec![Expr::Column {
                name: "name".to_string(),
                index: None,
            }],
            schema: output_schema,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
        assert_eq!(result[0].get(0).unwrap(), &Value::String(Arc::from("Alice")));
    }

    #[test]
    fn test_sort() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![
                test_row(1, "Alice", 30),
                test_row(2, "Bob", 25),
                test_row(3, "Charlie", 35),
            ],
        );

        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            order_by: vec![SortExpr {
                expr: Expr::Column {
                    name: "age".to_string(),
                    index: None,
                },
                asc: true,
                nulls_first: false,
            }],
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].get(2).unwrap(), &Value::Int32(25)); // Bob first
        assert_eq!(result[1].get(2).unwrap(), &Value::Int32(30)); // Alice second
        assert_eq!(result[2].get(2).unwrap(), &Value::Int32(35)); // Charlie third
    }

    #[test]
    fn test_limit() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![
                test_row(1, "Alice", 30),
                test_row(2, "Bob", 25),
                test_row(3, "Charlie", 35),
            ],
        );

        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            limit: 2,
            offset: 0,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_hash_aggregate() {
        let mut executor = MemoryExecutor::new();
        let schema = test_schema();
        let table_id = TableId(1);

        executor.register_table(
            table_id,
            schema.clone(),
            vec![
                test_row(1, "Alice", 30),
                test_row(2, "Bob", 30),
                test_row(3, "Charlie", 25),
            ],
        );

        let output_schema = Schema::new(vec![
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("count", DataType::Int64),
        ]);

        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id,
                schema: schema.clone(),
                filter: None,
            }),
            group_by: vec![2], // group by age
            aggregates: vec![AggregateExpr {
                func: AggregateFunction::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
            }],
            schema: output_schema,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 2); // Two groups: age 30 and age 25
    }

    #[test]
    fn test_hash_join() {
        let mut executor = MemoryExecutor::new();

        let users_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
        ]);

        let orders_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        let users_id = TableId(1);
        let orders_id = TableId(2);

        executor.register_table(
            users_id,
            users_schema.clone(),
            vec![
                Row::new(vec![Value::Int64(1), Value::String(Arc::from("Alice"))]),
                Row::new(vec![Value::Int64(2), Value::String(Arc::from("Bob"))]),
            ],
        );

        executor.register_table(
            orders_id,
            orders_schema.clone(),
            vec![
                Row::new(vec![Value::Int64(1), Value::Int64(1), Value::Float64(100.0)]),
                Row::new(vec![Value::Int64(2), Value::Int64(1), Value::Float64(200.0)]),
                Row::new(vec![Value::Int64(3), Value::Int64(2), Value::Float64(50.0)]),
            ],
        );

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_id: users_id,
                schema: users_schema,
                filter: None,
            }),
            right: Box::new(PhysicalPlan::SeqScan {
                table_id: orders_id,
                schema: orders_schema,
                filter: None,
            }),
            left_keys: vec![0], // users.id
            right_keys: vec![1], // orders.user_id
            join_type: JoinType::Inner,
        };

        let result = executor.execute(&plan).unwrap();
        assert_eq!(result.len(), 3); // Alice has 2 orders, Bob has 1
    }

    #[test]
    fn test_aggregator() {
        // Test COUNT
        let mut count_agg = Aggregator::new(&AggregateExpr {
            func: AggregateFunction::Count,
            args: vec![],
            distinct: false,
        });
        count_agg.accumulate(&[Value::Int64(1)]);
        count_agg.accumulate(&[Value::Int64(2)]);
        count_agg.accumulate(&[Value::Null]);
        assert_eq!(count_agg.finalize(), Value::Int64(2));

        // Test SUM
        let mut sum_agg = Aggregator::new(&AggregateExpr {
            func: AggregateFunction::Sum,
            args: vec![],
            distinct: false,
        });
        sum_agg.accumulate(&[Value::Int64(10)]);
        sum_agg.accumulate(&[Value::Int64(20)]);
        sum_agg.accumulate(&[Value::Int64(30)]);
        assert_eq!(sum_agg.finalize(), Value::Float64(60.0));

        // Test AVG
        let mut avg_agg = Aggregator::new(&AggregateExpr {
            func: AggregateFunction::Avg,
            args: vec![],
            distinct: false,
        });
        avg_agg.accumulate(&[Value::Float64(10.0)]);
        avg_agg.accumulate(&[Value::Float64(20.0)]);
        avg_agg.accumulate(&[Value::Float64(30.0)]);
        assert_eq!(avg_agg.finalize(), Value::Float64(20.0));

        // Test MIN/MAX
        let mut min_agg = Aggregator::new(&AggregateExpr {
            func: AggregateFunction::Min,
            args: vec![],
            distinct: false,
        });
        min_agg.accumulate(&[Value::Int64(30)]);
        min_agg.accumulate(&[Value::Int64(10)]);
        min_agg.accumulate(&[Value::Int64(20)]);
        assert_eq!(min_agg.finalize(), Value::Int64(10));
    }

    #[test]
    fn test_like_pattern() {
        assert!(ExprEvaluator::like_match("hello", "h%", false));
        assert!(ExprEvaluator::like_match("hello", "%o", false));
        assert!(ExprEvaluator::like_match("hello", "h_llo", false));
        assert!(ExprEvaluator::like_match("hello", "%ll%", false));
        assert!(!ExprEvaluator::like_match("hello", "x%", false));
        assert!(ExprEvaluator::like_match("hello", "%", false));
        assert!(ExprEvaluator::like_match("hello", "hello", false));
    }

    #[tokio::test]
    async fn test_async_executor() {
        let schema = test_schema();
        let table_id = TableId(1);

        let mut tables = HashMap::new();
        tables.insert(
            table_id,
            (
                schema.clone(),
                vec![
                    test_row(1, "Alice", 30),
                    test_row(2, "Bob", 25),
                ],
            ),
        );

        let executor = AsyncExecutor::with_tables(tables);

        let plan = PhysicalPlan::SeqScan {
            table_id,
            schema,
            filter: None,
        };

        let result = crate::Executor::execute(&executor, plan, TxnId(1)).await.unwrap();
        assert_eq!(result.batches[0].num_rows(), 2);
    }
}
