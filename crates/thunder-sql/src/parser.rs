//! SQL Parser Extensions
//!
//! Provides helper functions for working with sqlparser AST and converting
//! sqlparser types to ThunderDB's internal representation.

use sqlparser::ast::{self as sql_ast, Ident};
use std::sync::Arc;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;

use crate::{AggregateExpr, AggregateFunction, BinaryOperator, Expr, JoinType, SortExpr, UnaryOperator};

/// Hex encoding helper (inline to avoid dependency)
fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, String> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    if s.len() % 2 != 0 {
        return Err("Invalid hex string length".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| format!("Invalid hex: {}", e))
        })
        .collect()
}

/// Convert a sqlparser Ident to a string
pub fn ident_to_string(ident: &Ident) -> String {
    ident.value.clone()
}

/// Convert a vec of Idents to a qualified name string
pub fn object_name_to_string(name: &sql_ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract just the table name from an ObjectName (last part)
pub fn extract_table_name(name: &sql_ast::ObjectName) -> String {
    name.0
        .last()
        .map(|i| i.value.clone())
        .unwrap_or_default()
}

/// Convert sqlparser DataType to ThunderDB DataType
pub fn convert_data_type(sql_type: &sql_ast::DataType) -> Result<DataType> {
    use sql_ast::DataType as SqlType;

    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),

        SqlType::TinyInt(_) => Ok(DataType::Int8),
        SqlType::SmallInt(_) => Ok(DataType::Int16),
        SqlType::Int(_) | SqlType::Integer(_) | SqlType::MediumInt(_) => Ok(DataType::Int32),
        SqlType::BigInt(_) => Ok(DataType::Int64),

        SqlType::Real => Ok(DataType::Float32),
        SqlType::Float(_) | SqlType::Double | SqlType::DoublePrecision => Ok(DataType::Float64),

        SqlType::Decimal(info) | SqlType::Numeric(info) => {
            let (precision, scale) = match info {
                sql_ast::ExactNumberInfo::None => (38, 0),
                sql_ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sql_ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
            };
            Ok(DataType::Decimal { precision, scale })
        }

        SqlType::Char(len) | SqlType::Character(len) => {
            let n = len.as_ref().map(|l| {
                match l {
                    sql_ast::CharacterLength::IntegerLength { length, .. } => *length,
                    sql_ast::CharacterLength::Max => u64::MAX,
                }
            }).unwrap_or(1);
            Ok(DataType::Char(n as u32))
        }

        SqlType::Varchar(len) | SqlType::CharacterVarying(len) => {
            if let Some(l) = len {
                let n = match l {
                    sql_ast::CharacterLength::IntegerLength { length, .. } => *length,
                    sql_ast::CharacterLength::Max => u64::MAX,
                };
                Ok(DataType::Varchar(n as u32))
            } else {
                Ok(DataType::String)
            }
        }

        SqlType::Nvarchar(len) => {
            if let Some(n) = len {
                Ok(DataType::Varchar(*n as u32))
            } else {
                Ok(DataType::String)
            }
        }

        SqlType::Text | SqlType::String(_) | SqlType::Clob(_) => Ok(DataType::String),

        SqlType::Binary(len) => {
            if let Some(l) = len {
                Ok(DataType::FixedBinary(*l as u32))
            } else {
                Ok(DataType::Binary)
            }
        }

        SqlType::Varbinary(_) | SqlType::Blob(_) | SqlType::Bytea => Ok(DataType::Binary),

        SqlType::Date => Ok(DataType::Date),
        SqlType::Time(_, _) => Ok(DataType::Time),
        SqlType::Timestamp(_, tz) => {
            if *tz == sql_ast::TimezoneInfo::WithTimeZone
                || *tz == sql_ast::TimezoneInfo::Tz
            {
                Ok(DataType::TimestampTz)
            } else {
                Ok(DataType::Timestamp)
            }
        }
        SqlType::Interval => Ok(DataType::Interval),

        SqlType::Uuid => Ok(DataType::Uuid),
        SqlType::JSON => Ok(DataType::Json),
        SqlType::JSONB => Ok(DataType::Jsonb),

        SqlType::Array(inner) => {
            if let sql_ast::ArrayElemTypeDef::AngleBracket(inner_type) = inner {
                let inner_dt = convert_data_type(inner_type)?;
                Ok(DataType::Array(Box::new(inner_dt)))
            } else if let sql_ast::ArrayElemTypeDef::SquareBracket(inner_type) = inner {
                let inner_dt = convert_data_type(inner_type)?;
                Ok(DataType::Array(Box::new(inner_dt)))
            } else {
                Ok(DataType::Array(Box::new(DataType::Null)))
            }
        }

        SqlType::Custom(name, args) => {
            let type_name = object_name_to_string(name).to_uppercase();
            match type_name.as_str() {
                "VECTOR" => {
                    let dim = args
                        .first()
                        .and_then(|a| {
                            a.to_string().parse::<u32>().ok()
                        })
                        .unwrap_or(0);
                    Ok(DataType::Vector(dim))
                }
                _ => Err(Error::Sql(SqlError::UnsupportedFeature(type_name))),
            }
        }

        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
            "{:?}",
            other
        )))),
    }
}

/// Convert sqlparser Expr to ThunderDB Expr
pub fn convert_expr(sql_expr: &sql_ast::Expr) -> Result<Expr> {
    use sql_ast::Expr as SqlExpr;

    match sql_expr {
        SqlExpr::Identifier(ident) => Ok(Expr::Column {
            name: ident.value.clone(),
            index: None,
        }),

        SqlExpr::CompoundIdentifier(idents) => {
            // For now, use the last part as the column name
            let name = idents
                .last()
                .map(|i| i.value.clone())
                .unwrap_or_default();
            Ok(Expr::Column { name, index: None })
        }

        SqlExpr::Value(value) => convert_value(value),

        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(right)?),
        }),

        SqlExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: convert_unary_op(op)?,
            expr: Box::new(convert_expr(expr)?),
        }),

        SqlExpr::Nested(inner) => convert_expr(inner),

        SqlExpr::Function(func) => convert_function(func),

        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand = operand
                .as_ref()
                .map(|e| convert_expr(e))
                .transpose()?
                .map(Box::new);

            let when_clauses: Result<Vec<_>> = conditions
                .iter()
                .zip(results.iter())
                .map(|(cond, result)| {
                    Ok((convert_expr(cond)?, convert_expr(result)?))
                })
                .collect();

            let else_clause = else_result
                .as_ref()
                .map(|e| convert_expr(e))
                .transpose()?
                .map(Box::new);

            Ok(Expr::Case {
                operand,
                when_clauses: when_clauses?,
                else_clause,
            })
        }

        SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(convert_expr(expr)?))),

        SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(convert_expr(expr)?))),

        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let items: Result<Vec<_>> = list.iter().map(convert_expr).collect();
            Ok(Expr::InList {
                expr: Box::new(convert_expr(expr)?),
                list: items?,
                negated: *negated,
            })
        }

        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(expr)?),
            low: Box::new(convert_expr(low)?),
            high: Box::new(convert_expr(high)?),
            negated: *negated,
        }),

        SqlExpr::Cast {
            expr,
            data_type,
            ..
        } => Ok(Expr::Cast {
            expr: Box::new(convert_expr(expr)?),
            data_type: convert_data_type(data_type)?,
        }),

        SqlExpr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let op = if *negated {
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::BinaryOp {
                        left: Box::new(convert_expr(expr)?),
                        op: BinaryOperator::Like,
                        right: Box::new(convert_expr(pattern)?),
                    }),
                }
            } else {
                Expr::BinaryOp {
                    left: Box::new(convert_expr(expr)?),
                    op: BinaryOperator::Like,
                    right: Box::new(convert_expr(pattern)?),
                }
            };
            Ok(op)
        }

        SqlExpr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let op = if *negated {
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::BinaryOp {
                        left: Box::new(convert_expr(expr)?),
                        op: BinaryOperator::ILike,
                        right: Box::new(convert_expr(pattern)?),
                    }),
                }
            } else {
                Expr::BinaryOp {
                    left: Box::new(convert_expr(expr)?),
                    op: BinaryOperator::ILike,
                    right: Box::new(convert_expr(pattern)?),
                }
            };
            Ok(op)
        }

        SqlExpr::Wildcard => Ok(Expr::Wildcard),

        SqlExpr::QualifiedWildcard(_) => Ok(Expr::Wildcard),

        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
            "{:?}",
            other
        )))),
    }
}

/// Convert sqlparser Value to ThunderDB Expr::Literal
fn convert_value(value: &sql_ast::Value) -> Result<Expr> {
    use sql_ast::Value as SqlValue;

    let v = match value {
        SqlValue::Null => Value::Null,
        SqlValue::Boolean(b) => Value::Boolean(*b),
        SqlValue::Number(n, _) => {
            // Try to parse as integer first, then float
            if let Ok(i) = n.parse::<i64>() {
                Value::Int64(i)
            } else if let Ok(f) = n.parse::<f64>() {
                Value::Float64(f)
            } else {
                return Err(Error::Sql(SqlError::ParseError(format!(
                    "Invalid number: {}",
                    n
                ))));
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Value::String(Arc::from(s.as_str()))
        }
        SqlValue::EscapedStringLiteral(s) => Value::String(Arc::from(s.as_str())),
        SqlValue::HexStringLiteral(s) => {
            let bytes = hex_decode(s)
                .map_err(|e| Error::Sql(SqlError::ParseError(format!("Invalid hex: {}", e))))?;
            Value::Binary(Arc::from(bytes))
        }
        SqlValue::DollarQuotedString(s) => Value::String(Arc::from(s.value.as_str())),
        SqlValue::Placeholder(_) => {
            return Err(Error::Sql(SqlError::UnsupportedFeature(
                "Placeholders not yet supported".to_string(),
            )));
        }
        other => {
            return Err(Error::Sql(SqlError::UnsupportedFeature(format!(
                "Unsupported value: {:?}",
                other
            ))));
        }
    };

    Ok(Expr::Literal(v))
}

/// Convert sqlparser BinaryOperator to ThunderDB BinaryOperator
fn convert_binary_op(op: &sql_ast::BinaryOperator) -> Result<BinaryOperator> {
    use sql_ast::BinaryOperator as SqlOp;

    match op {
        SqlOp::Plus => Ok(BinaryOperator::Add),
        SqlOp::Minus => Ok(BinaryOperator::Sub),
        SqlOp::Multiply => Ok(BinaryOperator::Mul),
        SqlOp::Divide => Ok(BinaryOperator::Div),
        SqlOp::Modulo => Ok(BinaryOperator::Mod),
        SqlOp::Eq => Ok(BinaryOperator::Eq),
        SqlOp::NotEq => Ok(BinaryOperator::NotEq),
        SqlOp::Lt => Ok(BinaryOperator::Lt),
        SqlOp::LtEq => Ok(BinaryOperator::LtEq),
        SqlOp::Gt => Ok(BinaryOperator::Gt),
        SqlOp::GtEq => Ok(BinaryOperator::GtEq),
        SqlOp::And => Ok(BinaryOperator::And),
        SqlOp::Or => Ok(BinaryOperator::Or),
        SqlOp::StringConcat => Ok(BinaryOperator::Concat),
        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
            "Unsupported binary operator: {:?}",
            other
        )))),
    }
}

/// Convert sqlparser UnaryOperator to ThunderDB UnaryOperator
fn convert_unary_op(op: &sql_ast::UnaryOperator) -> Result<UnaryOperator> {
    use sql_ast::UnaryOperator as SqlOp;

    match op {
        SqlOp::Not => Ok(UnaryOperator::Not),
        SqlOp::Minus => Ok(UnaryOperator::Neg),
        SqlOp::Plus => Ok(UnaryOperator::Plus),
        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
            "Unsupported unary operator: {:?}",
            other
        )))),
    }
}

/// Convert sqlparser Function to ThunderDB Expr
fn convert_function(func: &sql_ast::Function) -> Result<Expr> {
    let name = object_name_to_string(&func.name);

    let args: Result<Vec<Expr>> = func
        .args
        .iter()
        .filter_map(|arg| match arg {
            sql_ast::FunctionArg::Unnamed(arg_expr) => {
                match arg_expr {
                    sql_ast::FunctionArgExpr::Expr(e) => Some(convert_expr(e)),
                    sql_ast::FunctionArgExpr::Wildcard => Some(Ok(Expr::Wildcard)),
                    _ => None,
                }
            }
            sql_ast::FunctionArg::Named { arg, .. } => {
                match arg {
                    sql_ast::FunctionArgExpr::Expr(e) => Some(convert_expr(e)),
                    sql_ast::FunctionArgExpr::Wildcard => Some(Ok(Expr::Wildcard)),
                    _ => None,
                }
            }
        })
        .collect();

    Ok(Expr::Function {
        name,
        args: args?,
    })
}

/// Convert sqlparser JoinConstraint type to ThunderDB JoinType
pub fn convert_join_type(join_type: &sql_ast::JoinOperator) -> Result<JoinType> {
    use sql_ast::JoinOperator;

    match join_type {
        JoinOperator::Inner(_) => Ok(JoinType::Inner),
        JoinOperator::LeftOuter(_) | JoinOperator::LeftSemi(_) | JoinOperator::LeftAnti(_) => {
            Ok(JoinType::Left)
        }
        JoinOperator::RightOuter(_) | JoinOperator::RightSemi(_) | JoinOperator::RightAnti(_) => {
            Ok(JoinType::Right)
        }
        JoinOperator::FullOuter(_) => Ok(JoinType::Full),
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {
            Ok(JoinType::Cross)
        }
        #[allow(unreachable_patterns)]
        other => Err(Error::Sql(SqlError::UnsupportedFeature(format!(
            "Unsupported join type: {:?}",
            other
        )))),
    }
}

/// Convert sqlparser OrderByExpr to ThunderDB SortExpr
pub fn convert_order_by(order_by: &sql_ast::OrderByExpr) -> Result<SortExpr> {
    Ok(SortExpr {
        expr: convert_expr(&order_by.expr)?,
        asc: order_by.asc.unwrap_or(true),
        nulls_first: order_by.nulls_first.unwrap_or(false),
    })
}

/// Check if an expression is an aggregate function
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "FIRST" | "LAST" | "ARRAY_AGG" | "STRING_AGG"
            | "BOOL_AND" | "BOOL_OR" | "EVERY" | "LISTAGG"
    )
}

/// Convert a function expression to an AggregateExpr if it's an aggregate function
pub fn try_convert_aggregate(func: &sql_ast::Function) -> Result<Option<AggregateExpr>> {
    let name = object_name_to_string(&func.name).to_uppercase();

    let agg_func = match name.as_str() {
        "COUNT" => Some(AggregateFunction::Count),
        "SUM" => Some(AggregateFunction::Sum),
        "AVG" => Some(AggregateFunction::Avg),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        "FIRST" => Some(AggregateFunction::First),
        "LAST" => Some(AggregateFunction::Last),
        "ARRAY_AGG" => Some(AggregateFunction::ArrayAgg),
        "STRING_AGG" | "LISTAGG" => Some(AggregateFunction::StringAgg),
        _ => None,
    };

    match agg_func {
        Some(func_type) => {
            let args: Result<Vec<Expr>> = func
                .args
                .iter()
                .filter_map(|arg| match arg {
                    sql_ast::FunctionArg::Unnamed(arg_expr) => {
                        match arg_expr {
                            sql_ast::FunctionArgExpr::Expr(e) => Some(convert_expr(e)),
                            sql_ast::FunctionArgExpr::Wildcard => Some(Ok(Expr::Wildcard)),
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .collect();

            Ok(Some(AggregateExpr {
                func: func_type,
                args: args?,
                distinct: func.distinct,
            }))
        }
        None => Ok(None),
    }
}

/// Extract column names from a SELECT clause
pub fn extract_select_columns(select: &sql_ast::Select) -> Vec<String> {
    select
        .projection
        .iter()
        .filter_map(|item| match item {
            sql_ast::SelectItem::UnnamedExpr(expr) => match expr {
                sql_ast::Expr::Identifier(ident) => Some(ident.value.clone()),
                sql_ast::Expr::CompoundIdentifier(idents) => {
                    idents.last().map(|i| i.value.clone())
                }
                _ => None,
            },
            sql_ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            sql_ast::SelectItem::Wildcard(_) => None,
            sql_ast::SelectItem::QualifiedWildcard(_, _) => None,
        })
        .collect()
}

/// Extract table name from a FROM clause
pub fn extract_from_table(from: &[sql_ast::TableWithJoins]) -> Option<String> {
    from.first().and_then(|t| match &t.relation {
        sql_ast::TableFactor::Table { name, .. } => Some(extract_table_name(name)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sql;

    #[test]
    fn test_convert_data_type() {
        assert_eq!(convert_data_type(&sql_ast::DataType::Int(None)).unwrap(), DataType::Int32);
        assert_eq!(convert_data_type(&sql_ast::DataType::BigInt(None)).unwrap(), DataType::Int64);
        assert_eq!(convert_data_type(&sql_ast::DataType::Boolean).unwrap(), DataType::Boolean);
        assert_eq!(convert_data_type(&sql_ast::DataType::Text).unwrap(), DataType::String);
    }

    #[test]
    fn test_convert_simple_expr() {
        let expr = sql_ast::Expr::Identifier(Ident::new("col1"));
        let result = convert_expr(&expr).unwrap();
        assert!(matches!(result, Expr::Column { name, .. } if name == "col1"));
    }

    #[test]
    fn test_convert_binary_expr() {
        let expr = sql_ast::Expr::BinaryOp {
            left: Box::new(sql_ast::Expr::Value(sql_ast::Value::Number("1".to_string(), false))),
            op: sql_ast::BinaryOperator::Plus,
            right: Box::new(sql_ast::Expr::Value(sql_ast::Value::Number("2".to_string(), false))),
        };

        let result = convert_expr(&expr).unwrap();
        assert!(matches!(result, Expr::BinaryOp { op: BinaryOperator::Add, .. }));
    }

    #[test]
    fn test_convert_value() {
        let expr = sql_ast::Expr::Value(sql_ast::Value::Number("42".to_string(), false));
        let result = convert_expr(&expr).unwrap();
        assert!(matches!(result, Expr::Literal(Value::Int64(42))));

        let expr = sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString("hello".to_string()));
        let result = convert_expr(&expr).unwrap();
        assert!(matches!(result, Expr::Literal(Value::String(_))));
    }

    #[test]
    fn test_is_aggregate_function() {
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("sum"));
        assert!(is_aggregate_function("AVG"));
        assert!(!is_aggregate_function("UPPER"));
        assert!(!is_aggregate_function("custom_func"));
    }

    #[test]
    fn test_extract_table_name() {
        let stmts = parse_sql("SELECT * FROM users").unwrap();
        if let sql_ast::Statement::Query(query) = &stmts[0] {
            if let sql_ast::SetExpr::Select(select) = query.body.as_ref() {
                let table = extract_from_table(&select.from);
                assert_eq!(table, Some("users".to_string()));
            }
        }
    }

    #[test]
    fn test_convert_order_by() {
        let order = sql_ast::OrderByExpr {
            expr: sql_ast::Expr::Identifier(Ident::new("col1")),
            asc: Some(false),
            nulls_first: Some(true),
        };

        let result = convert_order_by(&order).unwrap();
        assert!(!result.asc);
        assert!(result.nulls_first);
    }

    #[test]
    fn test_convert_join_type() {
        let inner = sql_ast::JoinOperator::Inner(sql_ast::JoinConstraint::None);
        assert_eq!(convert_join_type(&inner).unwrap(), JoinType::Inner);

        let left = sql_ast::JoinOperator::LeftOuter(sql_ast::JoinConstraint::None);
        assert_eq!(convert_join_type(&left).unwrap(), JoinType::Left);

        let cross = sql_ast::JoinOperator::CrossJoin;
        assert_eq!(convert_join_type(&cross).unwrap(), JoinType::Cross);
    }
}
