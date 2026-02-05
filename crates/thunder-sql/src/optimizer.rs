//! Query Optimizer
//!
//! Implements query optimization rules for logical plans:
//! - Predicate pushdown
//! - Projection pushdown
//! - Constant folding
//! - Join reordering
//! - Common subexpression elimination

use std::collections::HashSet;
use thunder_common::prelude::*;

use crate::planner::{transform_plan, ColumnCollector, PlanTransformer};
use crate::{BinaryOperator, Expr, LogicalPlan, Optimizer, UnaryOperator};

/// Rule-based query optimizer
pub struct RuleBasedOptimizer {
    /// Optimization rules to apply
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl Default for RuleBasedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleBasedOptimizer {
    /// Create a new optimizer with default rules
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(ConstantFoldingRule),
                Box::new(SimplifyExpressionsRule),
                Box::new(PredicatePushdownRule),
                Box::new(ProjectionPushdownRule),
                Box::new(EliminateEmptyRule),
                Box::new(MergeLimitsRule),
            ],
        }
    }

    /// Add a custom optimization rule
    pub fn add_rule(&mut self, rule: Box<dyn OptimizationRule>) {
        self.rules.push(rule);
    }

    /// Apply all optimization rules to a plan
    fn apply_rules(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
        // Apply rules in order, iteratively until no more changes
        let max_iterations = 10;

        for _ in 0..max_iterations {
            let original = format!("{:?}", plan);

            for rule in &self.rules {
                plan = rule.apply(plan)?;
            }

            // Check if plan changed
            if format!("{:?}", plan) == original {
                break;
            }
        }

        Ok(plan)
    }
}

impl Optimizer for RuleBasedOptimizer {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.apply_rules(plan)
    }
}

/// An optimization rule
pub trait OptimizationRule: Send + Sync {
    /// Apply this rule to a plan
    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan>;

    /// Name of this rule for debugging
    fn name(&self) -> &str;
}

/// Constant folding: evaluate constant expressions at compile time
pub struct ConstantFoldingRule;

impl OptimizationRule for ConstantFoldingRule {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        transform_plan(plan, &ConstantFolder)
    }
}

struct ConstantFolder;

impl PlanTransformer for ConstantFolder {
    fn transform(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let folded = fold_expr(predicate);

                // If filter is always true, eliminate it
                if let Expr::Literal(Value::Boolean(true)) = &folded {
                    return Ok(*input);
                }

                // If filter is always false, return empty
                if let Expr::Literal(Value::Boolean(false)) = &folded {
                    return Ok(LogicalPlan::Empty);
                }

                Ok(LogicalPlan::Filter {
                    input,
                    predicate: folded,
                })
            }

            LogicalPlan::Project { input, exprs, schema } => {
                let folded_exprs: Vec<Expr> = exprs.into_iter().map(fold_expr).collect();
                Ok(LogicalPlan::Project {
                    input,
                    exprs: folded_exprs,
                    schema,
                })
            }

            other => Ok(other),
        }
    }
}

/// Fold constant expressions
fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = fold_expr(*left);
            let right = fold_expr(*right);

            // Try to fold if both sides are literals
            if let (Expr::Literal(l), Expr::Literal(r)) = (&left, &right) {
                if let Some(result) = fold_binary_op(l, &op, r) {
                    return Expr::Literal(result);
                }
            }

            // Special cases
            match (&left, &op, &right) {
                // x AND true => x, x AND false => false
                (_, BinaryOperator::And, Expr::Literal(Value::Boolean(true))) => left,
                (Expr::Literal(Value::Boolean(true)), BinaryOperator::And, _) => right,
                (_, BinaryOperator::And, Expr::Literal(Value::Boolean(false))) => {
                    Expr::Literal(Value::Boolean(false))
                }
                (Expr::Literal(Value::Boolean(false)), BinaryOperator::And, _) => {
                    Expr::Literal(Value::Boolean(false))
                }

                // x OR true => true, x OR false => x
                (_, BinaryOperator::Or, Expr::Literal(Value::Boolean(true))) => {
                    Expr::Literal(Value::Boolean(true))
                }
                (Expr::Literal(Value::Boolean(true)), BinaryOperator::Or, _) => {
                    Expr::Literal(Value::Boolean(true))
                }
                (_, BinaryOperator::Or, Expr::Literal(Value::Boolean(false))) => left,
                (Expr::Literal(Value::Boolean(false)), BinaryOperator::Or, _) => right,

                // x + 0 => x, x * 1 => x, x * 0 => 0
                (_, BinaryOperator::Add, Expr::Literal(Value::Int64(0))) => left,
                (Expr::Literal(Value::Int64(0)), BinaryOperator::Add, _) => right,
                (_, BinaryOperator::Mul, Expr::Literal(Value::Int64(1))) => left,
                (Expr::Literal(Value::Int64(1)), BinaryOperator::Mul, _) => right,
                (_, BinaryOperator::Mul, Expr::Literal(Value::Int64(0))) => {
                    Expr::Literal(Value::Int64(0))
                }
                (Expr::Literal(Value::Int64(0)), BinaryOperator::Mul, _) => {
                    Expr::Literal(Value::Int64(0))
                }

                _ => Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
            }
        }

        Expr::UnaryOp { op, expr } => {
            let expr = fold_expr(*expr);

            // Fold NOT on boolean literals
            if let (UnaryOperator::Not, Expr::Literal(Value::Boolean(b))) = (&op, &expr) {
                return Expr::Literal(Value::Boolean(!b));
            }

            // Fold double negation
            if let (
                UnaryOperator::Not,
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: inner,
                },
            ) = (&op, &expr)
            {
                return (**inner).clone();
            }

            Expr::UnaryOp {
                op,
                expr: Box::new(expr),
            }
        }

        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand = operand.map(|e| Box::new(fold_expr(*e)));
            let when_clauses: Vec<_> = when_clauses
                .into_iter()
                .map(|(cond, then)| (fold_expr(cond), fold_expr(then)))
                .collect();
            let else_clause = else_clause.map(|e| Box::new(fold_expr(*e)));

            // If first WHEN condition is always true, return its THEN
            if let Some((Expr::Literal(Value::Boolean(true)), then)) = when_clauses.first() {
                return then.clone();
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            }
        }

        Expr::IsNull(inner) => {
            let inner = fold_expr(*inner);
            if let Expr::Literal(Value::Null) = inner {
                return Expr::Literal(Value::Boolean(true));
            }
            if let Expr::Literal(_) = inner {
                return Expr::Literal(Value::Boolean(false));
            }
            Expr::IsNull(Box::new(inner))
        }

        Expr::IsNotNull(inner) => {
            let inner = fold_expr(*inner);
            if let Expr::Literal(Value::Null) = inner {
                return Expr::Literal(Value::Boolean(false));
            }
            if let Expr::Literal(_) = inner {
                return Expr::Literal(Value::Boolean(true));
            }
            Expr::IsNotNull(Box::new(inner))
        }

        other => other,
    }
}

/// Fold binary operation on two literal values
fn fold_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Option<Value> {
    match (left, op, right) {
        // Integer arithmetic
        (Value::Int64(l), BinaryOperator::Add, Value::Int64(r)) => Some(Value::Int64(l + r)),
        (Value::Int64(l), BinaryOperator::Sub, Value::Int64(r)) => Some(Value::Int64(l - r)),
        (Value::Int64(l), BinaryOperator::Mul, Value::Int64(r)) => Some(Value::Int64(l * r)),
        (Value::Int64(l), BinaryOperator::Div, Value::Int64(r)) if *r != 0 => {
            Some(Value::Int64(l / r))
        }
        (Value::Int64(l), BinaryOperator::Mod, Value::Int64(r)) if *r != 0 => {
            Some(Value::Int64(l % r))
        }

        // Float arithmetic
        (Value::Float64(l), BinaryOperator::Add, Value::Float64(r)) => Some(Value::Float64(l + r)),
        (Value::Float64(l), BinaryOperator::Sub, Value::Float64(r)) => Some(Value::Float64(l - r)),
        (Value::Float64(l), BinaryOperator::Mul, Value::Float64(r)) => Some(Value::Float64(l * r)),
        (Value::Float64(l), BinaryOperator::Div, Value::Float64(r)) if *r != 0.0 => {
            Some(Value::Float64(l / r))
        }

        // Integer comparisons
        (Value::Int64(l), BinaryOperator::Eq, Value::Int64(r)) => Some(Value::Boolean(l == r)),
        (Value::Int64(l), BinaryOperator::NotEq, Value::Int64(r)) => Some(Value::Boolean(l != r)),
        (Value::Int64(l), BinaryOperator::Lt, Value::Int64(r)) => Some(Value::Boolean(l < r)),
        (Value::Int64(l), BinaryOperator::LtEq, Value::Int64(r)) => Some(Value::Boolean(l <= r)),
        (Value::Int64(l), BinaryOperator::Gt, Value::Int64(r)) => Some(Value::Boolean(l > r)),
        (Value::Int64(l), BinaryOperator::GtEq, Value::Int64(r)) => Some(Value::Boolean(l >= r)),

        // Boolean operations
        (Value::Boolean(l), BinaryOperator::And, Value::Boolean(r)) => {
            Some(Value::Boolean(*l && *r))
        }
        (Value::Boolean(l), BinaryOperator::Or, Value::Boolean(r)) => {
            Some(Value::Boolean(*l || *r))
        }
        (Value::Boolean(l), BinaryOperator::Eq, Value::Boolean(r)) => {
            Some(Value::Boolean(l == r))
        }

        // String comparisons
        (Value::String(l), BinaryOperator::Eq, Value::String(r)) => Some(Value::Boolean(l == r)),
        (Value::String(l), BinaryOperator::NotEq, Value::String(r)) => {
            Some(Value::Boolean(l != r))
        }

        // Null comparisons - NULL = anything is NULL (but we return false for simplicity)
        (Value::Null, _, _) | (_, _, Value::Null) => None,

        _ => None,
    }
}

/// Simplify expressions (remove redundant operations)
pub struct SimplifyExpressionsRule;

impl OptimizationRule for SimplifyExpressionsRule {
    fn name(&self) -> &str {
        "SimplifyExpressions"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        transform_plan(plan, &ExpressionSimplifier)
    }
}

struct ExpressionSimplifier;

impl PlanTransformer for ExpressionSimplifier {
    fn transform(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let simplified = simplify_expr(predicate);
                Ok(LogicalPlan::Filter {
                    input,
                    predicate: simplified,
                })
            }
            other => Ok(other),
        }
    }
}

fn simplify_expr(expr: Expr) -> Expr {
    match expr {
        // x = x => true (for non-null columns)
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } if exprs_equal(&left, &right) => Expr::Literal(Value::Boolean(true)),

        // x != x => false (for non-null columns)
        Expr::BinaryOp {
            left,
            op: BinaryOperator::NotEq,
            right,
        } if exprs_equal(&left, &right) => Expr::Literal(Value::Boolean(false)),

        // x >= x => true, x <= x => true
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } if exprs_equal(&left, &right) => Expr::Literal(Value::Boolean(true)),

        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } if exprs_equal(&left, &right) => Expr::Literal(Value::Boolean(true)),

        other => other,
    }
}

fn exprs_equal(a: &Expr, b: &Expr) -> bool {
    match (a, b) {
        (
            Expr::Column { name: n1, .. },
            Expr::Column { name: n2, .. },
        ) => n1 == n2,
        (Expr::Literal(v1), Expr::Literal(v2)) => v1 == v2,
        _ => false,
    }
}

/// Predicate pushdown: push filters closer to data sources
pub struct PredicatePushdownRule;

impl OptimizationRule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        push_predicates_down(plan)
    }
}

fn push_predicates_down(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        // Filter over Project: try to push through
        LogicalPlan::Filter {
            input,
            predicate,
        } => {
            match *input {
                // Push filter through project if predicate doesn't use project aliases
                LogicalPlan::Project {
                    input: proj_input,
                    exprs,
                    schema,
                } => {
                    // Check if we can push the predicate down
                    let pred_cols = ColumnCollector::collect_from_expr(&predicate);
                    let proj_cols: HashSet<_> = exprs
                        .iter()
                        .filter_map(|e| {
                            if let Expr::Column { name, .. } = e {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    // If predicate only uses columns from the input, push it down
                    if pred_cols.iter().all(|c| proj_cols.contains(c)) {
                        let pushed_input = push_predicates_down(LogicalPlan::Filter {
                            input: proj_input,
                            predicate,
                        })?;
                        Ok(LogicalPlan::Project {
                            input: Box::new(pushed_input),
                            exprs,
                            schema,
                        })
                    } else {
                        // Can't push, keep filter where it is
                        Ok(LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Project {
                                input: proj_input,
                                exprs,
                                schema,
                            }),
                            predicate,
                        })
                    }
                }

                // Push filter into scan
                LogicalPlan::Scan {
                    table,
                    schema,
                    filter: existing_filter,
                    projection,
                } => {
                    let combined_filter = match existing_filter {
                        Some(existing) => Some(Box::new(Expr::BinaryOp {
                            left: existing,
                            op: BinaryOperator::And,
                            right: Box::new(predicate),
                        })),
                        None => Some(Box::new(predicate)),
                    };

                    Ok(LogicalPlan::Scan {
                        table,
                        schema,
                        filter: combined_filter,
                        projection,
                    })
                }

                // Push through join (only for predicates on one side)
                LogicalPlan::Join {
                    left,
                    right,
                    condition,
                    join_type,
                } => {
                    // For simplicity, just keep filter above join
                    // A full implementation would analyze which side the predicate belongs to
                    let optimized_join = push_predicates_down(LogicalPlan::Join {
                        left,
                        right,
                        condition,
                        join_type,
                    })?;

                    Ok(LogicalPlan::Filter {
                        input: Box::new(optimized_join),
                        predicate,
                    })
                }

                other => {
                    let optimized = push_predicates_down(other)?;
                    Ok(LogicalPlan::Filter {
                        input: Box::new(optimized),
                        predicate,
                    })
                }
            }
        }

        // Recursively process other nodes
        LogicalPlan::Project { input, exprs, schema } => {
            let optimized = push_predicates_down(*input)?;
            Ok(LogicalPlan::Project {
                input: Box::new(optimized),
                exprs,
                schema,
            })
        }

        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            let opt_left = push_predicates_down(*left)?;
            let opt_right = push_predicates_down(*right)?;
            Ok(LogicalPlan::Join {
                left: Box::new(opt_left),
                right: Box::new(opt_right),
                condition,
                join_type,
            })
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            let optimized = push_predicates_down(*input)?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(optimized),
                group_by,
                aggregates,
                schema,
            })
        }

        LogicalPlan::Sort { input, order_by } => {
            let optimized = push_predicates_down(*input)?;
            Ok(LogicalPlan::Sort {
                input: Box::new(optimized),
                order_by,
            })
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let optimized = push_predicates_down(*input)?;
            Ok(LogicalPlan::Limit {
                input: Box::new(optimized),
                limit,
                offset,
            })
        }

        other => Ok(other),
    }
}

/// Projection pushdown: push projections closer to data sources
pub struct ProjectionPushdownRule;

impl OptimizationRule for ProjectionPushdownRule {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // For simplicity, just collect required columns at the top level
        // A full implementation would track required columns through the tree
        Ok(plan)
    }
}

/// Eliminate empty plans
pub struct EliminateEmptyRule;

impl OptimizationRule for EliminateEmptyRule {
    fn name(&self) -> &str {
        "EliminateEmpty"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        transform_plan(plan, &EmptyEliminator)
    }
}

struct EmptyEliminator;

impl PlanTransformer for EmptyEliminator {
    fn transform(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Project on empty => empty
            LogicalPlan::Project { input, .. } if is_empty(&input) => Ok(LogicalPlan::Empty),

            // Filter on empty => empty
            LogicalPlan::Filter { input, .. } if is_empty(&input) => Ok(LogicalPlan::Empty),

            // Sort on empty => empty
            LogicalPlan::Sort { input, .. } if is_empty(&input) => Ok(LogicalPlan::Empty),

            // Limit on empty => empty
            LogicalPlan::Limit { input, .. } if is_empty(&input) => Ok(LogicalPlan::Empty),

            // Join with empty => depends on join type
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                use crate::JoinType::*;
                match (is_empty(&left), is_empty(&right), join_type) {
                    // Inner join with empty side => empty
                    (true, _, Inner) | (_, true, Inner) => Ok(LogicalPlan::Empty),
                    // Cross join with empty => empty
                    (true, _, Cross) | (_, true, Cross) => Ok(LogicalPlan::Empty),
                    _ => Ok(LogicalPlan::Join {
                        left,
                        right,
                        join_type,
                        condition: Expr::Literal(Value::Boolean(true)),
                    }),
                }
            }

            other => Ok(other),
        }
    }
}

fn is_empty(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Empty)
}

/// Merge consecutive limits
pub struct MergeLimitsRule;

impl OptimizationRule for MergeLimitsRule {
    fn name(&self) -> &str {
        "MergeLimits"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        transform_plan(plan, &LimitMerger)
    }
}

struct LimitMerger;

impl PlanTransformer for LimitMerger {
    fn transform(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Limit(Limit(x)) => single Limit
            LogicalPlan::Limit {
                input,
                limit: outer_limit,
                offset: outer_offset,
            } => {
                if let LogicalPlan::Limit {
                    input: inner_input,
                    limit: inner_limit,
                    offset: inner_offset,
                } = *input
                {
                    // Take the minimum of the two limits
                    // Combine offsets
                    let new_limit = outer_limit.min(inner_limit.saturating_sub(outer_offset));
                    let new_offset = inner_offset + outer_offset;

                    Ok(LogicalPlan::Limit {
                        input: inner_input,
                        limit: new_limit,
                        offset: new_offset,
                    })
                } else {
                    Ok(LogicalPlan::Limit {
                        input,
                        limit: outer_limit,
                        offset: outer_offset,
                    })
                }
            }

            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_folding() {
        let optimizer = RuleBasedOptimizer::new();

        // 1 + 2 should fold to 3
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Value::Int64(1))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Value::Int64(2))),
        };

        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Literal(Value::Int64(3))));
    }

    #[test]
    fn test_boolean_simplification() {
        // x AND true => x
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "x".to_string(),
                index: None,
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Value::Boolean(true))),
        };

        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Column { name, .. } if name == "x"));
    }

    #[test]
    fn test_filter_elimination() {
        let optimizer = RuleBasedOptimizer::new();

        // Filter with predicate=true should be eliminated
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema: Schema::empty(),
                filter: None,
                projection: None,
            }),
            predicate: Expr::Literal(Value::Boolean(true)),
        };

        let optimized = optimizer.optimize(plan).unwrap();
        assert!(matches!(optimized, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn test_filter_to_empty() {
        let optimizer = RuleBasedOptimizer::new();

        // Filter with predicate=false should become Empty
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema: Schema::empty(),
                filter: None,
                projection: None,
            }),
            predicate: Expr::Literal(Value::Boolean(false)),
        };

        let optimized = optimizer.optimize(plan).unwrap();
        assert!(matches!(optimized, LogicalPlan::Empty));
    }

    #[test]
    fn test_predicate_pushdown_to_scan() {
        let optimizer = RuleBasedOptimizer::new();

        // Filter over Scan should push predicate into Scan
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                schema: Schema::empty(),
                filter: None,
                projection: None,
            }),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "id".to_string(),
                    index: None,
                }),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Value::Int64(10))),
            },
        };

        let optimized = optimizer.optimize(plan).unwrap();

        if let LogicalPlan::Scan { filter, .. } = optimized {
            assert!(filter.is_some());
        } else {
            panic!("Expected Scan with pushed filter");
        }
    }

    #[test]
    fn test_merge_limits() {
        let optimizer = RuleBasedOptimizer::new();

        // LIMIT 5 (LIMIT 10) => LIMIT 5
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    schema: Schema::empty(),
                    filter: None,
                    projection: None,
                }),
                limit: 10,
                offset: 0,
            }),
            limit: 5,
            offset: 0,
        };

        let optimized = optimizer.optimize(plan).unwrap();

        if let LogicalPlan::Limit { limit, .. } = optimized {
            assert_eq!(limit, 5);
        } else {
            panic!("Expected single Limit");
        }
    }

    #[test]
    fn test_double_negation() {
        // NOT NOT x => x
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Column {
                    name: "active".to_string(),
                    index: None,
                }),
            }),
        };

        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Column { name, .. } if name == "active"));
    }

    #[test]
    fn test_is_null_constant() {
        // IS NULL on a constant
        let expr = Expr::IsNull(Box::new(Expr::Literal(Value::Null)));
        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Literal(Value::Boolean(true))));

        let expr = Expr::IsNull(Box::new(Expr::Literal(Value::Int64(5))));
        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Literal(Value::Boolean(false))));
    }

    #[test]
    fn test_arithmetic_identity() {
        // x + 0 => x
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "x".to_string(),
                index: None,
            }),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Value::Int64(0))),
        };

        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Column { name, .. } if name == "x"));

        // x * 1 => x
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "x".to_string(),
                index: None,
            }),
            op: BinaryOperator::Mul,
            right: Box::new(Expr::Literal(Value::Int64(1))),
        };

        let folded = fold_expr(expr);
        assert!(matches!(folded, Expr::Column { name, .. } if name == "x"));
    }
}
