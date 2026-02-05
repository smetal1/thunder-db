//! SQL Type System
//!
//! Provides type inference, type coercion, and type validation for SQL expressions.

use thunder_common::error::SqlError;
use thunder_common::prelude::*;

/// Type coercion rules for binary operations
pub struct TypeCoercion;

impl TypeCoercion {
    /// Get the result type of a binary operation
    pub fn binary_op_result_type(
        left: &DataType,
        right: &DataType,
        op: &super::BinaryOperator,
    ) -> Result<DataType> {
        use super::BinaryOperator::*;

        match op {
            // Comparison operators always return boolean
            Eq | NotEq | Lt | LtEq | Gt | GtEq | Like | ILike => Ok(DataType::Boolean),

            // Logical operators require boolean operands
            And | Or => {
                if *left == DataType::Boolean && *right == DataType::Boolean {
                    Ok(DataType::Boolean)
                } else {
                    Err(Error::Sql(SqlError::InvalidExpression(format!(
                        "Cannot apply logical operator to {:?} and {:?}",
                        left, right
                    ))))
                }
            }

            // Arithmetic operators
            Add | Sub | Mul | Div | Mod => Self::numeric_result_type(left, right),

            // String concatenation
            Concat => {
                if left.is_string() && right.is_string() {
                    Ok(DataType::String)
                } else {
                    Err(Error::Sql(SqlError::InvalidExpression(format!(
                        "Cannot concatenate {:?} and {:?}",
                        left, right
                    ))))
                }
            }
        }
    }

    /// Get the result type of numeric operations
    fn numeric_result_type(left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        // If either is null, result is null
        if *left == Null || *right == Null {
            return Ok(Null);
        }

        // Both must be numeric
        if !left.is_numeric() || !right.is_numeric() {
            return Err(Error::Sql(SqlError::InvalidExpression(format!(
                "Cannot perform arithmetic on {:?} and {:?}",
                left, right
            ))));
        }

        // Promote to the wider type
        match (left, right) {
            // Float64 dominates
            (Float64, _) | (_, Float64) => Ok(Float64),
            // Float32 next
            (Float32, _) | (_, Float32) => Ok(Float64), // Promote to f64 for precision
            // Decimal preserves precision
            (Decimal { precision: p1, scale: s1 }, Decimal { precision: p2, scale: s2 }) => {
                Ok(Decimal {
                    precision: (*p1).max(*p2),
                    scale: (*s1).max(*s2),
                })
            }
            (Decimal { precision, scale }, _) | (_, Decimal { precision, scale }) => {
                Ok(Decimal {
                    precision: *precision,
                    scale: *scale,
                })
            }
            // Int64 dominates among integers
            (Int64, _) | (_, Int64) => Ok(Int64),
            (Int32, _) | (_, Int32) => Ok(Int64), // Promote to avoid overflow
            (Int16, _) | (_, Int16) => Ok(Int32),
            (Int8, Int8) => Ok(Int16),
            _ => Err(Error::Sql(SqlError::InvalidExpression(format!(
                "Unsupported numeric operation between {:?} and {:?}",
                left, right
            )))),
        }
    }

    /// Check if a type can be coerced to another type
    pub fn can_coerce(from: &DataType, to: &DataType) -> bool {
        if from == to {
            return true;
        }

        use DataType::*;
        match (from, to) {
            // Null can be coerced to anything
            (Null, _) => true,

            // Integer promotions
            (Int8, Int16 | Int32 | Int64 | Float32 | Float64) => true,
            (Int16, Int32 | Int64 | Float32 | Float64) => true,
            (Int32, Int64 | Float32 | Float64) => true,
            (Int64, Float64) => true,
            (Float32, Float64) => true,

            // Integer to decimal
            (Int8 | Int16 | Int32 | Int64, Decimal { .. }) => true,

            // String types are interchangeable
            (String, Varchar(_) | Char(_)) => true,
            (Varchar(_), String | Char(_)) => true,
            (Char(_), String | Varchar(_)) => true,

            // Binary types
            (Binary, FixedBinary(_)) => true,
            (FixedBinary(_), Binary) => true,

            // Timestamp can be coerced to date
            (Timestamp, Date) => true,
            (TimestampTz, Date | Timestamp) => true,

            _ => false,
        }
    }

    /// Find a common type that both types can be coerced to
    pub fn common_type(left: &DataType, right: &DataType) -> Option<DataType> {
        if left == right {
            return Some(left.clone());
        }

        // Try coercing in both directions
        if Self::can_coerce(left, right) {
            return Some(right.clone());
        }
        if Self::can_coerce(right, left) {
            return Some(left.clone());
        }

        // Special case: find a common numeric type
        if left.is_numeric() && right.is_numeric() {
            return Self::numeric_result_type(left, right).ok();
        }

        None
    }
}

/// Type inference for expressions
pub struct TypeInference;

impl TypeInference {
    /// Infer the type of an expression given a schema context
    pub fn infer_type(expr: &super::Expr, schema: &Schema) -> Result<DataType> {
        use super::Expr::*;

        match expr {
            Column { name, index } => {
                if let Some(idx) = index {
                    schema
                        .column(*idx)
                        .map(|c| c.data_type.clone())
                        .ok_or_else(|| {
                            Error::Sql(SqlError::ColumnNotFound(format!(
                                "Column index {} out of bounds",
                                idx
                            )))
                        })
                } else {
                    schema
                        .column_by_name(name)
                        .map(|(_, c)| c.data_type.clone())
                        .ok_or_else(|| Error::Sql(SqlError::ColumnNotFound(name.clone())))
                }
            }

            Literal(value) => Ok(value.data_type()),

            BinaryOp { left, op, right } => {
                let left_type = Self::infer_type(left, schema)?;
                let right_type = Self::infer_type(right, schema)?;
                TypeCoercion::binary_op_result_type(&left_type, &right_type, op)
            }

            UnaryOp { op, expr } => {
                let expr_type = Self::infer_type(expr, schema)?;
                match op {
                    super::UnaryOperator::Not => {
                        if expr_type == DataType::Boolean {
                            Ok(DataType::Boolean)
                        } else {
                            Err(Error::Sql(SqlError::InvalidExpression(format!(
                                "NOT requires boolean, got {:?}",
                                expr_type
                            ))))
                        }
                    }
                    super::UnaryOperator::Neg | super::UnaryOperator::Plus => {
                        if expr_type.is_numeric() {
                            Ok(expr_type)
                        } else {
                            Err(Error::Sql(SqlError::InvalidExpression(format!(
                                "Unary +/- requires numeric type, got {:?}",
                                expr_type
                            ))))
                        }
                    }
                }
            }

            Function { name, args } => Self::infer_function_type(name, args, schema),

            Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Result type is the common type of all THEN expressions and ELSE
                let mut result_type = if let Some((_, then_expr)) = when_clauses.first() {
                    Self::infer_type(then_expr, schema)?
                } else {
                    DataType::Null
                };

                for (_, then_expr) in when_clauses.iter().skip(1) {
                    let then_type = Self::infer_type(then_expr, schema)?;
                    result_type = TypeCoercion::common_type(&result_type, &then_type)
                        .ok_or_else(|| {
                            Error::Sql(SqlError::InvalidExpression(format!(
                                "Incompatible types in CASE: {:?} and {:?}",
                                result_type, then_type
                            )))
                        })?;
                }

                if let Some(else_expr) = else_clause {
                    let else_type = Self::infer_type(else_expr, schema)?;
                    result_type = TypeCoercion::common_type(&result_type, &else_type)
                        .ok_or_else(|| {
                            Error::Sql(SqlError::InvalidExpression(format!(
                                "Incompatible ELSE type: {:?} vs {:?}",
                                result_type, else_type
                            )))
                        })?;
                }

                // Validate operand if present
                if let Some(op) = operand {
                    Self::infer_type(op, schema)?;
                }

                Ok(result_type)
            }

            IsNull(_) | IsNotNull(_) => Ok(DataType::Boolean),

            InList { expr, list, .. } => {
                let expr_type = Self::infer_type(expr, schema)?;
                for item in list {
                    let item_type = Self::infer_type(item, schema)?;
                    if TypeCoercion::common_type(&expr_type, &item_type).is_none() {
                        return Err(Error::Sql(SqlError::InvalidExpression(format!(
                            "IN list type mismatch: {:?} vs {:?}",
                            expr_type, item_type
                        ))));
                    }
                }
                Ok(DataType::Boolean)
            }

            Between {
                expr, low, high, ..
            } => {
                let expr_type = Self::infer_type(expr, schema)?;
                let low_type = Self::infer_type(low, schema)?;
                let high_type = Self::infer_type(high, schema)?;

                if TypeCoercion::common_type(&expr_type, &low_type).is_none()
                    || TypeCoercion::common_type(&expr_type, &high_type).is_none()
                {
                    return Err(Error::Sql(SqlError::InvalidExpression(format!(
                        "BETWEEN type mismatch: {:?} BETWEEN {:?} AND {:?}",
                        expr_type, low_type, high_type
                    ))));
                }
                Ok(DataType::Boolean)
            }

            Subquery(plan) => {
                // Get the schema of the subquery result
                let subquery_schema = Self::plan_schema(plan)?;
                if subquery_schema.len() != 1 {
                    return Err(Error::Sql(SqlError::InvalidExpression(
                        "Scalar subquery must return exactly one column".to_string(),
                    )));
                }
                Ok(subquery_schema.columns[0].data_type.clone())
            }

            Cast { data_type, .. } => Ok(data_type.clone()),

            Wildcard => Err(Error::Sql(SqlError::InvalidExpression(
                "Cannot infer type of wildcard".to_string(),
            ))),
        }
    }

    /// Infer the return type of a function call
    fn infer_function_type(name: &str, args: &[super::Expr], schema: &Schema) -> Result<DataType> {
        let name_upper = name.to_uppercase();

        // Type of first argument (if any)
        let arg_type = if let Some(arg) = args.first() {
            Some(Self::infer_type(arg, schema)?)
        } else {
            None
        };

        match name_upper.as_str() {
            // Aggregate functions
            "COUNT" => Ok(DataType::Int64),
            "SUM" => arg_type.map_or(Ok(DataType::Null), |t| {
                if t.is_numeric() {
                    Ok(t)
                } else {
                    Err(Error::Sql(SqlError::InvalidExpression(format!(
                        "SUM requires numeric type, got {:?}",
                        t
                    ))))
                }
            }),
            "AVG" => Ok(DataType::Float64),
            "MIN" | "MAX" => arg_type.ok_or_else(|| {
                Error::Sql(SqlError::InvalidExpression(
                    "MIN/MAX requires an argument".to_string(),
                ))
            }),

            // String functions
            "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE" => Ok(DataType::String),
            "LENGTH" | "CHAR_LENGTH" | "OCTET_LENGTH" => Ok(DataType::Int32),
            "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "REPLACE" | "CONCAT" => Ok(DataType::String),
            "POSITION" | "STRPOS" => Ok(DataType::Int32),

            // Numeric functions
            "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" => {
                arg_type.ok_or_else(|| {
                    Error::Sql(SqlError::InvalidExpression(format!(
                        "{} requires an argument",
                        name_upper
                    )))
                })
            }
            "SQRT" | "EXP" | "LN" | "LOG" | "LOG10" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS"
            | "ATAN" | "SINH" | "COSH" | "TANH" => Ok(DataType::Float64),
            "POWER" | "MOD" => Ok(DataType::Float64),
            "RANDOM" => Ok(DataType::Float64),

            // Date/time functions
            "NOW" | "CURRENT_TIMESTAMP" => Ok(DataType::Timestamp),
            "CURRENT_DATE" => Ok(DataType::Date),
            "CURRENT_TIME" => Ok(DataType::Time),
            "DATE_TRUNC" => Ok(DataType::Timestamp),
            "DATE_PART" | "EXTRACT" => Ok(DataType::Float64),
            "AGE" => Ok(DataType::Interval),
            "TO_CHAR" => Ok(DataType::String),
            "TO_DATE" => Ok(DataType::Date),
            "TO_TIMESTAMP" => Ok(DataType::Timestamp),

            // Null handling
            "COALESCE" | "NULLIF" | "IFNULL" => {
                // Return type of first non-null argument type
                for arg in args {
                    let t = Self::infer_type(arg, schema)?;
                    if t != DataType::Null {
                        return Ok(t);
                    }
                }
                Ok(DataType::Null)
            }

            // Type casting
            "CAST" => {
                // CAST(expr AS type) - handled elsewhere
                Ok(DataType::String) // fallback
            }

            // JSON functions
            "JSON_EXTRACT" | "JSON_EXTRACT_PATH" => Ok(DataType::Json),
            "JSON_EXTRACT_PATH_TEXT" | "JSON_TYPEOF" => Ok(DataType::String),
            "JSON_ARRAY_LENGTH" => Ok(DataType::Int32),

            // Array functions
            "ARRAY_LENGTH" | "ARRAY_UPPER" | "ARRAY_LOWER" => Ok(DataType::Int32),
            "ARRAY_AGG" => arg_type
                .map(|t| DataType::Array(Box::new(t)))
                .ok_or_else(|| {
                    Error::Sql(SqlError::InvalidExpression(
                        "ARRAY_AGG requires an argument".to_string(),
                    ))
                }),
            "UNNEST" => {
                if let Some(DataType::Array(inner)) = arg_type {
                    Ok(*inner)
                } else {
                    Err(Error::Sql(SqlError::InvalidExpression(
                        "UNNEST requires an array argument".to_string(),
                    )))
                }
            }

            // UUID functions
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => Ok(DataType::Uuid),

            // Boolean functions
            "BOOL_AND" | "BOOL_OR" | "EVERY" => Ok(DataType::Boolean),

            // Vector functions
            "VECTOR_DIMS" => Ok(DataType::Int32),
            "VECTOR_NORM" | "COSINE_DISTANCE" | "L2_DISTANCE" | "INNER_PRODUCT" => {
                Ok(DataType::Float64)
            }

            // Unknown function - return null type and let runtime handle it
            _ => Ok(DataType::Null),
        }
    }

    /// Get the schema of a logical plan
    pub fn plan_schema(plan: &super::LogicalPlan) -> Result<Schema> {
        use super::LogicalPlan::*;

        match plan {
            Scan { schema, .. } => Ok(schema.clone()),
            Project { schema, .. } => Ok(schema.clone()),
            Filter { input, .. } => Self::plan_schema(input),
            Join { left, right, .. } => {
                let left_schema = Self::plan_schema(left)?;
                let right_schema = Self::plan_schema(right)?;
                let mut columns = left_schema.columns;
                columns.extend(right_schema.columns);
                Ok(Schema::new(columns))
            }
            Aggregate { schema, .. } => Ok(schema.clone()),
            Sort { input, .. } => Self::plan_schema(input),
            Limit { input, .. } => Self::plan_schema(input),
            Insert { .. } | Update { .. } | Delete { .. } | CreateTable { .. } | DropTable { .. }
            | CreateIndex { .. } | DropIndex { .. } | AlterTable { .. } | AnalyzeTable { .. } => {
                Ok(Schema::empty())
            }
            Explain { .. } => {
                Ok(Schema::new(vec![
                    ColumnDef::new("QUERY PLAN", DataType::String),
                ]))
            }
            Empty => Ok(Schema::empty()),
        }
    }
}

/// Validate types in expressions
pub struct TypeValidator;

impl TypeValidator {
    /// Validate that an expression is type-correct
    pub fn validate(expr: &super::Expr, schema: &Schema) -> Result<()> {
        // Simply try to infer the type - if it succeeds, the expression is valid
        TypeInference::infer_type(expr, schema)?;
        Ok(())
    }

    /// Validate a row against a schema
    pub fn validate_row(row: &Row, schema: &Schema) -> Result<()> {
        if row.len() != schema.len() {
            return Err(Error::Sql(SqlError::InvalidExpression(format!(
                "Row has {} columns but schema has {}",
                row.len(),
                schema.len()
            ))));
        }

        for (i, (value, column)) in row.values.iter().zip(schema.columns.iter()).enumerate() {
            let value_type = value.data_type();

            // Null is always allowed if column is nullable
            if value.is_null() {
                if !column.nullable {
                    return Err(Error::Sql(SqlError::InvalidExpression(format!(
                        "Column '{}' does not allow NULL",
                        column.name
                    ))));
                }
                continue;
            }

            // Check type compatibility
            if !TypeCoercion::can_coerce(&value_type, &column.data_type) {
                return Err(Error::Sql(SqlError::InvalidExpression(format!(
                    "Column '{}' (index {}) expects {:?} but got {:?}",
                    column.name, i, column.data_type, value_type
                ))));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryOperator, Expr};

    fn test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("salary", DataType::Float64),
            ColumnDef::new("active", DataType::Boolean),
        ])
    }

    #[test]
    fn test_coercion_numeric() {
        assert!(TypeCoercion::can_coerce(&DataType::Int32, &DataType::Int64));
        assert!(TypeCoercion::can_coerce(&DataType::Int64, &DataType::Float64));
        assert!(!TypeCoercion::can_coerce(&DataType::String, &DataType::Int64));
    }

    #[test]
    fn test_coercion_string() {
        assert!(TypeCoercion::can_coerce(&DataType::String, &DataType::Varchar(255)));
        assert!(TypeCoercion::can_coerce(&DataType::Varchar(100), &DataType::String));
    }

    #[test]
    fn test_common_type() {
        assert_eq!(
            TypeCoercion::common_type(&DataType::Int32, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            TypeCoercion::common_type(&DataType::Int32, &DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(
            TypeCoercion::common_type(&DataType::String, &DataType::Int64),
            None
        );
    }

    #[test]
    fn test_binary_op_result_type() {
        assert_eq!(
            TypeCoercion::binary_op_result_type(
                &DataType::Int32,
                &DataType::Int32,
                &BinaryOperator::Eq
            )
            .unwrap(),
            DataType::Boolean
        );

        assert_eq!(
            TypeCoercion::binary_op_result_type(
                &DataType::Int32,
                &DataType::Int64,
                &BinaryOperator::Add
            )
            .unwrap(),
            DataType::Int64
        );

        assert_eq!(
            TypeCoercion::binary_op_result_type(
                &DataType::Boolean,
                &DataType::Boolean,
                &BinaryOperator::And
            )
            .unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn test_infer_literal_type() {
        let schema = test_schema();

        let expr = Expr::Literal(Value::Int64(42));
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::Int64
        );

        let expr = Expr::Literal(Value::String("hello".into()));
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::String
        );
    }

    #[test]
    fn test_infer_column_type() {
        let schema = test_schema();

        let expr = Expr::Column {
            name: "id".to_string(),
            index: None,
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::Int64
        );

        let expr = Expr::Column {
            name: "name".to_string(),
            index: None,
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::String
        );
    }

    #[test]
    fn test_infer_binary_op_type() {
        let schema = test_schema();

        // age + salary => Float64
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: None,
            }),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Column {
                name: "salary".to_string(),
                index: None,
            }),
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::Float64
        );

        // age > 18 => Boolean
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: None,
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Int32(18))),
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn test_infer_function_type() {
        let schema = test_schema();

        let expr = Expr::Function {
            name: "COUNT".to_string(),
            args: vec![Expr::Column {
                name: "id".to_string(),
                index: None,
            }],
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::Int64
        );

        let expr = Expr::Function {
            name: "UPPER".to_string(),
            args: vec![Expr::Column {
                name: "name".to_string(),
                index: None,
            }],
        };
        assert_eq!(
            TypeInference::infer_type(&expr, &schema).unwrap(),
            DataType::String
        );
    }

    #[test]
    fn test_validate_row() {
        let schema = test_schema();

        // Valid row
        let row = Row::new(vec![
            Value::Int64(1),
            Value::String("Alice".into()),
            Value::Int32(30),
            Value::Float64(50000.0),
            Value::Boolean(true),
        ]);
        assert!(TypeValidator::validate_row(&row, &schema).is_ok());

        // Invalid: wrong number of columns
        let row = Row::new(vec![Value::Int64(1)]);
        assert!(TypeValidator::validate_row(&row, &schema).is_err());

        // Invalid: NULL in NOT NULL column
        let row = Row::new(vec![
            Value::Null,
            Value::String("Alice".into()),
            Value::Int32(30),
            Value::Float64(50000.0),
            Value::Boolean(true),
        ]);
        assert!(TypeValidator::validate_row(&row, &schema).is_err());
    }

    #[test]
    fn test_null_coercion() {
        // Null can be coerced to anything
        assert!(TypeCoercion::can_coerce(&DataType::Null, &DataType::Int64));
        assert!(TypeCoercion::can_coerce(&DataType::Null, &DataType::String));
        assert!(TypeCoercion::can_coerce(&DataType::Null, &DataType::Boolean));
    }
}
