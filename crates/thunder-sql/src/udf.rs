//! User-Defined Functions (UDFs)
//!
//! Provides support for registering and executing user-defined functions:
//! - Scalar functions (one output per input row)
//! - Aggregate functions (one output per group)
//! - Table functions (return a table)

use dashmap::DashMap;
use std::fmt;
use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;

/// A scalar function that operates on a single row
pub trait ScalarUdf: Send + Sync {
    /// Name of the function
    fn name(&self) -> &str;

    /// Return type of the function
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Execute the function on input values
    fn execute(&self, args: &[Value]) -> Result<Value>;

    /// Number of arguments (None = variadic)
    fn num_args(&self) -> Option<usize> {
        None
    }
}

/// A user-defined aggregate function
pub trait AggregateUdf: Send + Sync {
    /// Name of the function
    fn name(&self) -> &str;

    /// Return type of the function
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Create a new accumulator
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

/// Accumulator for aggregate functions
pub trait Accumulator: Send + Sync {
    /// Add a value to the accumulator
    fn accumulate(&mut self, values: &[Value]) -> Result<()>;

    /// Merge another accumulator into this one
    fn merge(&mut self, other: &dyn Accumulator) -> Result<()>;

    /// Get the final result
    fn finalize(&self) -> Result<Value>;

    /// Reset the accumulator
    fn reset(&mut self);
}

/// Registry for user-defined functions
pub struct UdfRegistry {
    /// Scalar functions
    scalar_functions: DashMap<String, Arc<dyn ScalarUdf>>,
    /// Aggregate functions
    aggregate_functions: DashMap<String, Arc<dyn AggregateUdf>>,
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl UdfRegistry {
    /// Create a new UDF registry with built-in functions
    pub fn new() -> Self {
        let registry = Self {
            scalar_functions: DashMap::new(),
            aggregate_functions: DashMap::new(),
        };

        // Register built-in scalar functions
        registry.register_scalar(Arc::new(UpperFunction));
        registry.register_scalar(Arc::new(LowerFunction));
        registry.register_scalar(Arc::new(LengthFunction));
        registry.register_scalar(Arc::new(ConcatFunction));
        registry.register_scalar(Arc::new(SubstringFunction));
        registry.register_scalar(Arc::new(TrimFunction));
        registry.register_scalar(Arc::new(AbsFunction));
        registry.register_scalar(Arc::new(CeilFunction));
        registry.register_scalar(Arc::new(FloorFunction));
        registry.register_scalar(Arc::new(RoundFunction));
        registry.register_scalar(Arc::new(CoalesceFunction));
        registry.register_scalar(Arc::new(NullIfFunction));
        registry.register_scalar(Arc::new(NowFunction));

        // Register built-in aggregate functions
        registry.register_aggregate(Arc::new(CountAgg));
        registry.register_aggregate(Arc::new(SumAgg));
        registry.register_aggregate(Arc::new(AvgAgg));
        registry.register_aggregate(Arc::new(MinAgg));
        registry.register_aggregate(Arc::new(MaxAgg));

        registry
    }

    /// Register a scalar function
    pub fn register_scalar(&self, func: Arc<dyn ScalarUdf>) {
        self.scalar_functions
            .insert(func.name().to_uppercase(), func);
    }

    /// Register an aggregate function
    pub fn register_aggregate(&self, func: Arc<dyn AggregateUdf>) {
        self.aggregate_functions
            .insert(func.name().to_uppercase(), func);
    }

    /// Get a scalar function by name
    pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn ScalarUdf>> {
        self.scalar_functions.get(&name.to_uppercase()).map(|r| r.clone())
    }

    /// Get an aggregate function by name
    pub fn get_aggregate(&self, name: &str) -> Option<Arc<dyn AggregateUdf>> {
        self.aggregate_functions
            .get(&name.to_uppercase())
            .map(|r| r.clone())
    }

    /// Check if a function is a scalar function
    pub fn is_scalar(&self, name: &str) -> bool {
        self.scalar_functions.contains_key(&name.to_uppercase())
    }

    /// Check if a function is an aggregate function
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.aggregate_functions.contains_key(&name.to_uppercase())
    }

    /// List all scalar functions
    pub fn list_scalar_functions(&self) -> Vec<String> {
        self.scalar_functions.iter().map(|r| r.key().clone()).collect()
    }

    /// List all aggregate functions
    pub fn list_aggregate_functions(&self) -> Vec<String> {
        self.aggregate_functions
            .iter()
            .map(|r| r.key().clone())
            .collect()
    }

    /// Execute a scalar function
    pub fn execute_scalar(&self, name: &str, args: &[Value]) -> Result<Value> {
        let func = self
            .get_scalar(name)
            .ok_or_else(|| Error::NotFound("Function".to_string(), name.to_string()))?;

        // Check argument count
        if let Some(expected) = func.num_args() {
            if args.len() != expected {
                return Err(Error::InvalidArgument(format!(
                    "Function {} expects {} arguments, got {}",
                    name,
                    expected,
                    args.len()
                )));
            }
        }

        func.execute(args)
    }
}

impl fmt::Debug for UdfRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdfRegistry")
            .field("scalar_functions", &self.scalar_functions.len())
            .field("aggregate_functions", &self.aggregate_functions.len())
            .finish()
    }
}

// =============================================================================
// Built-in Scalar Functions
// =============================================================================

/// UPPER(string) - convert to uppercase
pub struct UpperFunction;

impl ScalarUdf for UpperFunction {
    fn name(&self) -> &str {
        "UPPER"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::String(s)) => Ok(Value::String(Arc::from(s.to_uppercase().as_str()))),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "UPPER requires a string argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// LOWER(string) - convert to lowercase
pub struct LowerFunction;

impl ScalarUdf for LowerFunction {
    fn name(&self) -> &str {
        "LOWER"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::String(s)) => Ok(Value::String(Arc::from(s.to_lowercase().as_str()))),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "LOWER requires a string argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// LENGTH(string) - get string length
pub struct LengthFunction;

impl ScalarUdf for LengthFunction {
    fn name(&self) -> &str {
        "LENGTH"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::String(s)) => Ok(Value::Int32(s.chars().count() as i32)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "LENGTH requires a string argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// CONCAT(string, ...) - concatenate strings
pub struct ConcatFunction;

impl ScalarUdf for ConcatFunction {
    fn name(&self) -> &str {
        "CONCAT"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        let mut result = String::new();
        for arg in args {
            match arg {
                Value::String(s) => result.push_str(s),
                Value::Null => {} // Skip nulls
                other => result.push_str(&other.to_string()),
            }
        }
        Ok(Value::String(Arc::from(result.as_str())))
    }
}

/// SUBSTRING(string, start, length) - extract substring
pub struct SubstringFunction;

impl ScalarUdf for SubstringFunction {
    fn name(&self) -> &str {
        "SUBSTRING"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        let s = match args.get(0) {
            Some(Value::String(s)) => s,
            Some(Value::Null) => return Ok(Value::Null),
            _ => {
                return Err(Error::Sql(SqlError::InvalidExpression(
                    "SUBSTRING requires a string first argument".to_string(),
                )))
            }
        };

        let start = match args.get(1).and_then(|v| v.as_i64()) {
            Some(n) => (n.max(1) - 1) as usize, // SQL is 1-indexed
            None => 0,
        };

        let len = match args.get(2).and_then(|v| v.as_i64()) {
            Some(n) => Some(n.max(0) as usize),
            None => None,
        };

        let chars: Vec<char> = s.chars().collect();
        let end = match len {
            Some(l) => (start + l).min(chars.len()),
            None => chars.len(),
        };

        let result: String = chars[start.min(chars.len())..end].iter().collect();
        Ok(Value::String(Arc::from(result.as_str())))
    }
}

/// TRIM(string) - remove leading/trailing whitespace
pub struct TrimFunction;

impl ScalarUdf for TrimFunction {
    fn name(&self) -> &str {
        "TRIM"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::String(s)) => Ok(Value::String(Arc::from(s.trim()))),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "TRIM requires a string argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// ABS(number) - absolute value
pub struct AbsFunction;

impl ScalarUdf for AbsFunction {
    fn name(&self) -> &str {
        "ABS"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            Error::Sql(SqlError::InvalidExpression("ABS requires an argument".to_string()))
        })
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
            Some(Value::Int32(n)) => Ok(Value::Int32(n.abs())),
            Some(Value::Float64(n)) => Ok(Value::Float64(n.abs())),
            Some(Value::Float32(n)) => Ok(Value::Float32(n.abs())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "ABS requires a numeric argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// CEIL(number) - ceiling
pub struct CeilFunction;

impl ScalarUdf for CeilFunction {
    fn name(&self) -> &str {
        "CEIL"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Float64(n)) => Ok(Value::Int64(n.ceil() as i64)),
            Some(Value::Float32(n)) => Ok(Value::Int64(n.ceil() as i64)),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Int32(n)) => Ok(Value::Int64(*n as i64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "CEIL requires a numeric argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// FLOOR(number) - floor
pub struct FloorFunction;

impl ScalarUdf for FloorFunction {
    fn name(&self) -> &str {
        "FLOOR"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Float64(n)) => Ok(Value::Int64(n.floor() as i64)),
            Some(Value::Float32(n)) => Ok(Value::Int64(n.floor() as i64)),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Int32(n)) => Ok(Value::Int64(*n as i64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "FLOOR requires a numeric argument".to_string(),
            ))),
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// ROUND(number, [precision]) - round
pub struct RoundFunction;

impl ScalarUdf for RoundFunction {
    fn name(&self) -> &str {
        "ROUND"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        let precision = args
            .get(1)
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        let multiplier = 10f64.powi(precision);

        match args.first() {
            Some(Value::Float64(n)) => Ok(Value::Float64((n * multiplier).round() / multiplier)),
            Some(Value::Float32(n)) => {
                Ok(Value::Float64(((*n as f64) * multiplier).round() / multiplier))
            }
            Some(Value::Int64(n)) => Ok(Value::Float64(*n as f64)),
            Some(Value::Int32(n)) => Ok(Value::Float64(*n as f64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(Error::Sql(SqlError::InvalidExpression(
                "ROUND requires a numeric argument".to_string(),
            ))),
        }
    }
}

/// COALESCE(value, ...) - return first non-null value
pub struct CoalesceFunction;

impl ScalarUdf for CoalesceFunction {
    fn name(&self) -> &str {
        "COALESCE"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .iter()
            .find(|t| **t != DataType::Null)
            .cloned()
            .ok_or_else(|| {
                Error::Sql(SqlError::InvalidExpression(
                    "COALESCE requires at least one non-null type".to_string(),
                ))
            })
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }
}

/// NULLIF(value1, value2) - return null if value1 = value2
pub struct NullIfFunction;

impl ScalarUdf for NullIfFunction {
    fn name(&self) -> &str {
        "NULLIF"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            Error::Sql(SqlError::InvalidExpression(
                "NULLIF requires arguments".to_string(),
            ))
        })
    }

    fn execute(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidArgument(
                "NULLIF requires exactly 2 arguments".to_string(),
            ));
        }

        if args[0] == args[1] {
            Ok(Value::Null)
        } else {
            Ok(args[0].clone())
        }
    }

    fn num_args(&self) -> Option<usize> {
        Some(2)
    }
}

/// NOW() - current timestamp
pub struct NowFunction;

impl ScalarUdf for NowFunction {
    fn name(&self) -> &str {
        "NOW"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp)
    }

    fn execute(&self, _args: &[Value]) -> Result<Value> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        Ok(Value::Timestamp(now))
    }

    fn num_args(&self) -> Option<usize> {
        Some(0)
    }
}

// =============================================================================
// Built-in Aggregate Functions
// =============================================================================

/// COUNT aggregate
pub struct CountAgg;

impl AggregateUdf for CountAgg {
    fn name(&self) -> &str {
        "COUNT"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { count: 0 })
    }
}

struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, values: &[Value]) -> Result<()> {
        // COUNT(*) counts all rows, COUNT(expr) counts non-null
        if values.is_empty() || !values[0].is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other_count) = other.finalize()?.as_i64() {
            self.count += other_count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::Int64(self.count))
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

/// SUM aggregate
pub struct SumAgg;

impl AggregateUdf for SumAgg {
    fn name(&self) -> &str {
        "SUM"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(DataType::Int64)
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(DataType::Float64),
            Some(other) => Err(Error::Sql(SqlError::InvalidExpression(format!(
                "SUM does not support {:?}",
                other
            )))),
            None => Ok(DataType::Int64),
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumAccumulator {
            sum: 0.0,
            has_value: false,
        })
    }
}

struct SumAccumulator {
    sum: f64,
    has_value: bool,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, values: &[Value]) -> Result<()> {
        if let Some(v) = values.first().and_then(|v| v.as_f64()) {
            self.sum += v;
            self.has_value = true;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(v) = other.finalize()?.as_f64() {
            self.sum += v;
            self.has_value = true;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.has_value {
            Ok(Value::Float64(self.sum))
        } else {
            Ok(Value::Null)
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.has_value = false;
    }
}

/// AVG aggregate
pub struct AvgAgg;

impl AggregateUdf for AvgAgg {
    fn name(&self) -> &str {
        "AVG"
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator { sum: 0.0, count: 0 })
    }
}

struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, values: &[Value]) -> Result<()> {
        if let Some(v) = values.first().and_then(|v| v.as_f64()) {
            self.sum += v;
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        // For proper merging, we'd need to track sum and count separately
        // This is a simplified version
        if let Some(v) = other.finalize()?.as_f64() {
            self.sum += v;
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.count > 0 {
            Ok(Value::Float64(self.sum / self.count as f64))
        } else {
            Ok(Value::Null)
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

/// MIN aggregate
pub struct MinAgg;

impl AggregateUdf for MinAgg {
    fn name(&self) -> &str {
        "MIN"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            Error::Sql(SqlError::InvalidExpression("MIN requires an argument".to_string()))
        })
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinAccumulator { min: None })
    }
}

struct MinAccumulator {
    min: Option<Value>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, values: &[Value]) -> Result<()> {
        if let Some(v) = values.first() {
            if v.is_null() {
                return Ok(());
            }

            self.min = Some(match &self.min {
                None => v.clone(),
                Some(current) => {
                    if compare_values(v, current) == std::cmp::Ordering::Less {
                        v.clone()
                    } else {
                        current.clone()
                    }
                }
            });
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other_val = other.finalize()?;
        if !other_val.is_null() {
            self.accumulate(&[other_val])?;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.min.clone().unwrap_or(Value::Null))
    }

    fn reset(&mut self) {
        self.min = None;
    }
}

/// MAX aggregate
pub struct MaxAgg;

impl AggregateUdf for MaxAgg {
    fn name(&self) -> &str {
        "MAX"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            Error::Sql(SqlError::InvalidExpression("MAX requires an argument".to_string()))
        })
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxAccumulator { max: None })
    }
}

struct MaxAccumulator {
    max: Option<Value>,
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, values: &[Value]) -> Result<()> {
        if let Some(v) = values.first() {
            if v.is_null() {
                return Ok(());
            }

            self.max = Some(match &self.max {
                None => v.clone(),
                Some(current) => {
                    if compare_values(v, current) == std::cmp::Ordering::Greater {
                        v.clone()
                    } else {
                        current.clone()
                    }
                }
            });
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other_val = other.finalize()?;
        if !other_val.is_null() {
            self.accumulate(&[other_val])?;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.max.clone().unwrap_or(Value::Null))
    }

    fn reset(&mut self) {
        self.max = None;
    }
}

/// Compare two values
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper_function() {
        let func = UpperFunction;
        let result = func
            .execute(&[Value::String(Arc::from("hello"))])
            .unwrap();
        assert_eq!(result, Value::String(Arc::from("HELLO")));
    }

    #[test]
    fn test_lower_function() {
        let func = LowerFunction;
        let result = func
            .execute(&[Value::String(Arc::from("HELLO"))])
            .unwrap();
        assert_eq!(result, Value::String(Arc::from("hello")));
    }

    #[test]
    fn test_length_function() {
        let func = LengthFunction;
        let result = func
            .execute(&[Value::String(Arc::from("hello"))])
            .unwrap();
        assert_eq!(result, Value::Int32(5));
    }

    #[test]
    fn test_concat_function() {
        let func = ConcatFunction;
        let result = func
            .execute(&[
                Value::String(Arc::from("hello")),
                Value::String(Arc::from(" ")),
                Value::String(Arc::from("world")),
            ])
            .unwrap();
        assert_eq!(result, Value::String(Arc::from("hello world")));
    }

    #[test]
    fn test_substring_function() {
        let func = SubstringFunction;
        let result = func
            .execute(&[
                Value::String(Arc::from("hello world")),
                Value::Int64(7),
                Value::Int64(5),
            ])
            .unwrap();
        assert_eq!(result, Value::String(Arc::from("world")));
    }

    #[test]
    fn test_abs_function() {
        let func = AbsFunction;
        assert_eq!(func.execute(&[Value::Int64(-42)]).unwrap(), Value::Int64(42));
        assert_eq!(
            func.execute(&[Value::Float64(-3.14)]).unwrap(),
            Value::Float64(3.14)
        );
    }

    #[test]
    fn test_coalesce_function() {
        let func = CoalesceFunction;
        let result = func
            .execute(&[Value::Null, Value::Null, Value::Int64(42)])
            .unwrap();
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn test_nullif_function() {
        let func = NullIfFunction;
        assert_eq!(
            func.execute(&[Value::Int64(1), Value::Int64(1)]).unwrap(),
            Value::Null
        );
        assert_eq!(
            func.execute(&[Value::Int64(1), Value::Int64(2)]).unwrap(),
            Value::Int64(1)
        );
    }

    #[test]
    fn test_count_accumulator() {
        let mut acc = CountAccumulator { count: 0 };
        acc.accumulate(&[Value::Int64(1)]).unwrap();
        acc.accumulate(&[Value::Int64(2)]).unwrap();
        acc.accumulate(&[Value::Null]).unwrap(); // Should not count
        assert_eq!(acc.finalize().unwrap(), Value::Int64(2));
    }

    #[test]
    fn test_sum_accumulator() {
        let mut acc = SumAccumulator {
            sum: 0.0,
            has_value: false,
        };
        acc.accumulate(&[Value::Int64(10)]).unwrap();
        acc.accumulate(&[Value::Int64(20)]).unwrap();
        acc.accumulate(&[Value::Int64(30)]).unwrap();
        assert_eq!(acc.finalize().unwrap(), Value::Float64(60.0));
    }

    #[test]
    fn test_avg_accumulator() {
        let mut acc = AvgAccumulator { sum: 0.0, count: 0 };
        acc.accumulate(&[Value::Float64(10.0)]).unwrap();
        acc.accumulate(&[Value::Float64(20.0)]).unwrap();
        acc.accumulate(&[Value::Float64(30.0)]).unwrap();
        assert_eq!(acc.finalize().unwrap(), Value::Float64(20.0));
    }

    #[test]
    fn test_min_max_accumulators() {
        let mut min_acc = MinAccumulator { min: None };
        min_acc.accumulate(&[Value::Int64(30)]).unwrap();
        min_acc.accumulate(&[Value::Int64(10)]).unwrap();
        min_acc.accumulate(&[Value::Int64(20)]).unwrap();
        assert_eq!(min_acc.finalize().unwrap(), Value::Int64(10));

        let mut max_acc = MaxAccumulator { max: None };
        max_acc.accumulate(&[Value::Int64(30)]).unwrap();
        max_acc.accumulate(&[Value::Int64(10)]).unwrap();
        max_acc.accumulate(&[Value::Int64(20)]).unwrap();
        assert_eq!(max_acc.finalize().unwrap(), Value::Int64(30));
    }

    #[test]
    fn test_udf_registry() {
        let registry = UdfRegistry::new();

        // Test scalar function lookup
        assert!(registry.get_scalar("UPPER").is_some());
        assert!(registry.get_scalar("upper").is_some()); // Case insensitive
        assert!(registry.is_scalar("LOWER"));

        // Test aggregate function lookup
        assert!(registry.get_aggregate("COUNT").is_some());
        assert!(registry.is_aggregate("SUM"));

        // Test execution
        let result = registry
            .execute_scalar("UPPER", &[Value::String(Arc::from("test"))])
            .unwrap();
        assert_eq!(result, Value::String(Arc::from("TEST")));
    }

    #[test]
    fn test_round_function() {
        let func = RoundFunction;

        // Round to integer
        let result = func.execute(&[Value::Float64(3.7)]).unwrap();
        assert_eq!(result, Value::Float64(4.0));

        // Round to 2 decimal places
        let result = func
            .execute(&[Value::Float64(3.14159), Value::Int64(2)])
            .unwrap();
        assert_eq!(result, Value::Float64(3.14));
    }
}
