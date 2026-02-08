//! Predicate Pushdown Analysis
//!
//! Provides analysis and optimization for pushing down predicates:
//! - Expression classification
//! - Pushdown safety analysis
//! - Cost-based pushdown decisions
//! - Aggregation pushdown support

use std::collections::{HashMap, HashSet};


use crate::{ColumnStatistics, FdwCapabilities, Qual, QualOperator};
use thunder_common::prelude::*;

// ============================================================================
// Pushdown Analyzer
// ============================================================================

/// Analyzes expressions for pushdown eligibility
pub struct PushdownAnalyzer {
    /// FDW capabilities
    capabilities: FdwCapabilities,
    /// Column statistics
    statistics: HashMap<String, ColumnStatistics>,
    /// Columns available in remote table
    remote_columns: HashSet<String>,
    /// Column type mapping
    column_types: HashMap<String, DataType>,
}

impl PushdownAnalyzer {
    /// Create a new pushdown analyzer
    pub fn new(capabilities: FdwCapabilities) -> Self {
        Self {
            capabilities,
            statistics: HashMap::new(),
            remote_columns: HashSet::new(),
            column_types: HashMap::new(),
        }
    }

    /// Set available remote columns
    pub fn with_remote_columns(mut self, columns: impl IntoIterator<Item = String>) -> Self {
        self.remote_columns = columns.into_iter().collect();
        self
    }

    /// Set column statistics
    pub fn with_statistics(mut self, stats: HashMap<String, ColumnStatistics>) -> Self {
        self.statistics = stats;
        self
    }

    /// Set column types
    pub fn with_column_types(mut self, types: HashMap<String, DataType>) -> Self {
        self.column_types = types;
        self
    }

    /// Analyze a qualifier for pushdown
    pub fn analyze_qual(&self, qual: &Qual) -> PushdownDecision {
        // Check if column exists in remote table
        if !self.remote_columns.is_empty() && !self.remote_columns.contains(&qual.column) {
            return PushdownDecision::CannotPush {
                reason: "Column not in remote table".to_string(),
            };
        }

        // Check operator support
        if !self.is_operator_supported(&qual.operator) {
            return PushdownDecision::CannotPush {
                reason: format!("Operator {:?} not supported", qual.operator),
            };
        }

        // Check value type compatibility
        if let Some(col_type) = self.column_types.get(&qual.column) {
            if !self.is_type_compatible(col_type, &qual.value) {
                return PushdownDecision::CannotPush {
                    reason: "Type mismatch between column and value".to_string(),
                };
            }
        }

        // Estimate selectivity
        let selectivity = self.estimate_selectivity(qual);

        // Decide based on selectivity
        if selectivity < 0.001 {
            // Very selective - definitely push
            PushdownDecision::ShouldPush {
                selectivity,
                cost_reduction: 0.99,
            }
        } else if selectivity < 0.1 {
            // Moderately selective - push
            PushdownDecision::ShouldPush {
                selectivity,
                cost_reduction: 1.0 - selectivity,
            }
        } else if selectivity < 0.5 {
            // Less selective - consider pushing
            PushdownDecision::MayPush {
                selectivity,
                cost_reduction: 0.5 - selectivity,
            }
        } else {
            // Not very selective - might not be worth pushing
            PushdownDecision::MayPush {
                selectivity,
                cost_reduction: 0.0,
            }
        }
    }

    /// Analyze multiple qualifiers for pushdown
    pub fn analyze_quals(&self, quals: &[Qual]) -> PushdownPlan {
        let mut pushable = Vec::new();
        let mut local = Vec::new();
        let mut combined_selectivity = 1.0;

        for qual in quals {
            match self.analyze_qual(qual) {
                PushdownDecision::ShouldPush { selectivity, .. } => {
                    pushable.push(qual.clone());
                    combined_selectivity *= selectivity;
                }
                PushdownDecision::MayPush { selectivity, cost_reduction } => {
                    // Push if cost reduction is positive
                    if cost_reduction > 0.0 {
                        pushable.push(qual.clone());
                        combined_selectivity *= selectivity;
                    } else {
                        local.push(qual.clone());
                    }
                }
                PushdownDecision::CannotPush { .. } => {
                    local.push(qual.clone());
                }
            }
        }

        PushdownPlan {
            pushed_quals: pushable,
            local_quals: local,
            estimated_selectivity: combined_selectivity,
        }
    }

    /// Check if an operator is supported for pushdown
    fn is_operator_supported(&self, op: &QualOperator) -> bool {
        if !self.capabilities.supports_predicate_pushdown {
            return false;
        }

        // All basic operators are typically supported
        matches!(
            op,
            QualOperator::Eq
                | QualOperator::NotEq
                | QualOperator::Lt
                | QualOperator::LtEq
                | QualOperator::Gt
                | QualOperator::GtEq
                | QualOperator::Like
                | QualOperator::IsNull
                | QualOperator::IsNotNull
                | QualOperator::In
        )
    }

    /// Check type compatibility
    fn is_type_compatible(&self, col_type: &DataType, value: &Value) -> bool {
        match (col_type, value) {
            (_, Value::Null) => true,
            (DataType::Boolean, Value::Boolean(_)) => true,
            (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64, Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_)) => true,
            (DataType::Float32 | DataType::Float64, Value::Float32(_) | Value::Float64(_)) => true,
            (DataType::String, Value::String(_)) => true,
            (DataType::Binary, Value::Binary(_)) => true,
            (DataType::Timestamp, Value::Timestamp(_)) => true,
            (DataType::Date, Value::Date(_)) => true,
            (DataType::Time, Value::Time(_)) => true,
            _ => false,
        }
    }

    /// Estimate selectivity for a qualifier
    fn estimate_selectivity(&self, qual: &Qual) -> f64 {
        // Use statistics if available
        if let Some(stats) = self.statistics.get(&qual.column) {
            let distinct = stats.distinct_count.max(1) as f64;

            match qual.operator {
                QualOperator::Eq => 1.0 / distinct,
                QualOperator::NotEq => 1.0 - (1.0 / distinct),
                QualOperator::Lt | QualOperator::Gt => 0.33,
                QualOperator::LtEq | QualOperator::GtEq => 0.33,
                QualOperator::Like => {
                    // LIKE selectivity depends on pattern
                    if let Value::String(s) = &qual.value {
                        if s.starts_with('%') {
                            0.5 // Leading wildcard - poor selectivity
                        } else {
                            0.1 // Prefix match - better selectivity
                        }
                    } else {
                        0.25
                    }
                }
                QualOperator::In => {
                    // IN selectivity depends on list size
                    if let Value::Array(arr) = &qual.value {
                        (arr.len() as f64) / distinct
                    } else {
                        0.1
                    }
                }
                QualOperator::IsNull => stats.null_count as f64 / distinct,
                QualOperator::IsNotNull => 1.0 - (stats.null_count as f64 / distinct),
            }
        } else {
            // Default selectivity estimates
            match qual.operator {
                QualOperator::Eq => 0.01,
                QualOperator::NotEq => 0.99,
                QualOperator::Lt | QualOperator::Gt => 0.33,
                QualOperator::LtEq | QualOperator::GtEq => 0.33,
                QualOperator::Like => 0.25,
                QualOperator::In => 0.1,
                QualOperator::IsNull => 0.01,
                QualOperator::IsNotNull => 0.99,
            }
        }
    }
}

/// Pushdown decision for a single qualifier
#[derive(Debug, Clone)]
pub enum PushdownDecision {
    /// Should definitely push this qualifier
    ShouldPush {
        selectivity: f64,
        cost_reduction: f64,
    },
    /// May push this qualifier (cost-based decision)
    MayPush {
        selectivity: f64,
        cost_reduction: f64,
    },
    /// Cannot push this qualifier
    CannotPush { reason: String },
}

/// Pushdown plan for multiple qualifiers
#[derive(Debug, Clone)]
pub struct PushdownPlan {
    /// Qualifiers to push to remote
    pub pushed_quals: Vec<Qual>,
    /// Qualifiers to evaluate locally
    pub local_quals: Vec<Qual>,
    /// Estimated combined selectivity of pushed quals
    pub estimated_selectivity: f64,
}

// ============================================================================
// Limit Pushdown
// ============================================================================

/// Analyzes limit/offset for pushdown
pub struct LimitPushdownAnalyzer {
    capabilities: FdwCapabilities,
}

impl LimitPushdownAnalyzer {
    pub fn new(capabilities: FdwCapabilities) -> Self {
        Self { capabilities }
    }

    /// Analyze limit for pushdown
    pub fn analyze(&self, limit: Option<usize>, offset: Option<usize>, has_order: bool) -> LimitPushdownPlan {
        if !self.capabilities.supports_limit_pushdown {
            return LimitPushdownPlan {
                push_limit: None,
                push_offset: None,
                local_limit: limit,
                local_offset: offset,
            };
        }

        // If there's an order by, we might need to be careful
        if has_order {
            // Still safe to push limit+offset if ordering is also pushed
            LimitPushdownPlan {
                push_limit: limit,
                push_offset: offset,
                local_limit: None,
                local_offset: None,
            }
        } else if offset.is_some() {
            // Without ordering, offset is meaningless to push
            // But we can push a limit that includes offset
            let total = limit.unwrap_or(0) + offset.unwrap_or(0);
            LimitPushdownPlan {
                push_limit: if total > 0 { Some(total) } else { limit },
                push_offset: None,
                local_limit: limit,
                local_offset: offset,
            }
        } else {
            // Simple limit push
            LimitPushdownPlan {
                push_limit: limit,
                push_offset: None,
                local_limit: None,
                local_offset: None,
            }
        }
    }
}

/// Limit pushdown plan
#[derive(Debug, Clone)]
pub struct LimitPushdownPlan {
    /// Limit to push to remote
    pub push_limit: Option<usize>,
    /// Offset to push to remote
    pub push_offset: Option<usize>,
    /// Limit to apply locally
    pub local_limit: Option<usize>,
    /// Offset to apply locally
    pub local_offset: Option<usize>,
}

// ============================================================================
// Aggregation Pushdown
// ============================================================================

/// Aggregation function types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
}

/// Aggregation pushdown analysis
pub struct AggregationPushdownAnalyzer {
    capabilities: FdwCapabilities,
}

impl AggregationPushdownAnalyzer {
    pub fn new(capabilities: FdwCapabilities) -> Self {
        Self { capabilities }
    }

    /// Analyze aggregation for pushdown
    pub fn analyze(&self, aggregates: &[AggregateExpr], group_by: &[String]) -> AggregationPushdownPlan {
        if !self.capabilities.supports_aggregation_pushdown {
            return AggregationPushdownPlan {
                pushed_aggregates: vec![],
                pushed_group_by: vec![],
                local_aggregates: aggregates.to_vec(),
                local_group_by: group_by.to_vec(),
                partial_aggregation: false,
            };
        }

        let mut pushable = Vec::new();
        let mut local = Vec::new();

        for agg in aggregates {
            if self.is_aggregate_pushable(&agg.function) {
                pushable.push(agg.clone());
            } else {
                local.push(agg.clone());
            }
        }

        // If any aggregates are local, we need partial aggregation
        let partial = !local.is_empty() && !pushable.is_empty();

        AggregationPushdownPlan {
            pushed_aggregates: pushable,
            pushed_group_by: if local.is_empty() { group_by.to_vec() } else { vec![] },
            local_aggregates: local,
            local_group_by: group_by.to_vec(),
            partial_aggregation: partial,
        }
    }

    fn is_aggregate_pushable(&self, func: &AggregateFunction) -> bool {
        // Most simple aggregates can be pushed
        matches!(
            func,
            AggregateFunction::Count
                | AggregateFunction::Sum
                | AggregateFunction::Min
                | AggregateFunction::Max
                | AggregateFunction::Avg
        )
    }
}

/// Aggregate expression
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub column: Option<String>,
    pub distinct: bool,
    pub alias: Option<String>,
}

/// Aggregation pushdown plan
#[derive(Debug, Clone)]
pub struct AggregationPushdownPlan {
    pub pushed_aggregates: Vec<AggregateExpr>,
    pub pushed_group_by: Vec<String>,
    pub local_aggregates: Vec<AggregateExpr>,
    pub local_group_by: Vec<String>,
    pub partial_aggregation: bool,
}

// ============================================================================
// Join Pushdown
// ============================================================================

/// Join type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Join condition
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_column: String,
    pub right_column: String,
    pub operator: QualOperator,
}

/// Analyzes join pushdown opportunities
pub struct JoinPushdownAnalyzer;

impl JoinPushdownAnalyzer {
    /// Analyze if a join can be pushed to a single foreign server
    pub fn analyze(
        &self,
        left_server: &str,
        right_server: &str,
        join_type: &JoinType,
        conditions: &[JoinCondition],
    ) -> JoinPushdownPlan {
        // Can only push if both tables are on the same server
        if left_server != right_server {
            return JoinPushdownPlan {
                can_push: false,
                reason: Some("Tables on different servers".to_string()),
                pushed_conditions: vec![],
                local_conditions: conditions.to_vec(),
            };
        }

        // Check join type support
        let supported_join = matches!(join_type, JoinType::Inner | JoinType::Left | JoinType::Right);
        if !supported_join {
            return JoinPushdownPlan {
                can_push: false,
                reason: Some(format!("Join type {:?} not supported for pushdown", join_type)),
                pushed_conditions: vec![],
                local_conditions: conditions.to_vec(),
            };
        }

        // All conditions can be pushed for same-server join
        JoinPushdownPlan {
            can_push: true,
            reason: None,
            pushed_conditions: conditions.to_vec(),
            local_conditions: vec![],
        }
    }
}

/// Join pushdown plan
#[derive(Debug, Clone)]
pub struct JoinPushdownPlan {
    pub can_push: bool,
    pub reason: Option<String>,
    pub pushed_conditions: Vec<JoinCondition>,
    pub local_conditions: Vec<JoinCondition>,
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Cost model for pushdown decisions
pub struct PushdownCostModel {
    /// Network latency per request (ms)
    pub network_latency_ms: f64,
    /// Bandwidth (MB/s)
    pub bandwidth_mbs: f64,
    /// Remote processing cost factor
    pub remote_cost_factor: f64,
    /// Local processing cost factor
    pub local_cost_factor: f64,
}

impl Default for PushdownCostModel {
    fn default() -> Self {
        Self {
            network_latency_ms: 1.0,
            bandwidth_mbs: 100.0,
            remote_cost_factor: 1.0,
            local_cost_factor: 0.1,
        }
    }
}

impl PushdownCostModel {
    /// Estimate cost of pushing a filter
    pub fn estimate_pushdown_cost(
        &self,
        total_rows: usize,
        selectivity: f64,
        avg_row_size: usize,
    ) -> PushdownCostEstimate {
        let filtered_rows = (total_rows as f64 * selectivity).max(1.0) as usize;

        // Cost without pushdown (fetch all, filter locally)
        let without_pushdown = self.estimate_transfer_cost(total_rows, avg_row_size)
            + self.estimate_local_filter_cost(total_rows);

        // Cost with pushdown (remote filter, fetch filtered)
        let with_pushdown = self.estimate_remote_filter_cost(total_rows)
            + self.estimate_transfer_cost(filtered_rows, avg_row_size);

        PushdownCostEstimate {
            cost_with_pushdown: with_pushdown,
            cost_without_pushdown: without_pushdown,
            should_push: with_pushdown < without_pushdown,
            savings: without_pushdown - with_pushdown,
        }
    }

    fn estimate_transfer_cost(&self, rows: usize, avg_row_size: usize) -> f64 {
        let data_mb = (rows * avg_row_size) as f64 / (1024.0 * 1024.0);
        self.network_latency_ms + (data_mb / self.bandwidth_mbs * 1000.0)
    }

    fn estimate_remote_filter_cost(&self, rows: usize) -> f64 {
        rows as f64 * 0.001 * self.remote_cost_factor
    }

    fn estimate_local_filter_cost(&self, rows: usize) -> f64 {
        rows as f64 * 0.0001 * self.local_cost_factor
    }
}

/// Cost estimate for pushdown decision
#[derive(Debug, Clone)]
pub struct PushdownCostEstimate {
    pub cost_with_pushdown: f64,
    pub cost_without_pushdown: f64,
    pub should_push: bool,
    pub savings: f64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_capabilities() -> FdwCapabilities {
        FdwCapabilities {
            supports_predicate_pushdown: true,
            supports_limit_pushdown: true,
            supports_aggregation_pushdown: true,
            supports_modification: true,
            supports_transactions: true,
            max_connections: 10,
        }
    }

    #[test]
    fn test_pushdown_analyzer_eq() {
        let analyzer = PushdownAnalyzer::new(test_capabilities())
            .with_remote_columns(vec!["id".to_string(), "name".to_string()]);

        let qual = Qual {
            column: "id".to_string(),
            operator: QualOperator::Eq,
            value: Value::Int64(1),
        };

        let decision = analyzer.analyze_qual(&qual);
        assert!(matches!(decision, PushdownDecision::ShouldPush { .. }));
    }

    #[test]
    fn test_pushdown_analyzer_missing_column() {
        let analyzer = PushdownAnalyzer::new(test_capabilities())
            .with_remote_columns(vec!["id".to_string()]);

        let qual = Qual {
            column: "unknown".to_string(),
            operator: QualOperator::Eq,
            value: Value::Int64(1),
        };

        let decision = analyzer.analyze_qual(&qual);
        assert!(matches!(decision, PushdownDecision::CannotPush { .. }));
    }

    #[test]
    fn test_limit_pushdown() {
        let analyzer = LimitPushdownAnalyzer::new(test_capabilities());

        let plan = analyzer.analyze(Some(10), None, false);
        assert_eq!(plan.push_limit, Some(10));
        assert!(plan.local_limit.is_none());
    }

    #[test]
    fn test_limit_pushdown_with_offset() {
        let analyzer = LimitPushdownAnalyzer::new(test_capabilities());

        let plan = analyzer.analyze(Some(10), Some(5), false);
        assert_eq!(plan.push_limit, Some(15)); // limit + offset
        assert_eq!(plan.local_limit, Some(10));
        assert_eq!(plan.local_offset, Some(5));
    }

    #[test]
    fn test_aggregation_pushdown() {
        let analyzer = AggregationPushdownAnalyzer::new(test_capabilities());

        let aggregates = vec![
            AggregateExpr {
                function: AggregateFunction::Count,
                column: None,
                distinct: false,
                alias: Some("cnt".to_string()),
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                column: Some("amount".to_string()),
                distinct: false,
                alias: Some("total".to_string()),
            },
        ];

        let plan = analyzer.analyze(&aggregates, &["region".to_string()]);
        assert_eq!(plan.pushed_aggregates.len(), 2);
        assert!(plan.local_aggregates.is_empty());
    }

    #[test]
    fn test_join_pushdown_same_server() {
        let analyzer = JoinPushdownAnalyzer;

        let conditions = vec![JoinCondition {
            left_column: "id".to_string(),
            right_column: "user_id".to_string(),
            operator: QualOperator::Eq,
        }];

        let plan = analyzer.analyze("server1", "server1", &JoinType::Inner, &conditions);
        assert!(plan.can_push);
        assert_eq!(plan.pushed_conditions.len(), 1);
    }

    #[test]
    fn test_join_pushdown_different_servers() {
        let analyzer = JoinPushdownAnalyzer;

        let conditions = vec![JoinCondition {
            left_column: "id".to_string(),
            right_column: "user_id".to_string(),
            operator: QualOperator::Eq,
        }];

        let plan = analyzer.analyze("server1", "server2", &JoinType::Inner, &conditions);
        assert!(!plan.can_push);
    }

    #[test]
    fn test_cost_model() {
        // Use a model with lower remote cost to demonstrate pushdown benefit
        let model = PushdownCostModel {
            network_latency_ms: 10.0,   // 10ms latency
            bandwidth_mbs: 10.0,        // 10 MB/s (slower network)
            remote_cost_factor: 0.1,    // Remote filtering is efficient
            local_cost_factor: 1.0,     // Local processing is normal cost
        };

        let estimate = model.estimate_pushdown_cost(
            1000000, // 1M rows
            0.001,   // 0.1% selectivity (very selective)
            1000,    // 1KB per row
        );

        // With high selectivity and slow network, pushdown should help
        assert!(estimate.should_push);
        assert!(estimate.savings > 0.0);
    }

    #[test]
    fn test_cost_model_low_selectivity() {
        let model = PushdownCostModel::default();

        let estimate = model.estimate_pushdown_cost(
            100000, // 100k rows
            0.9,    // 90% selectivity - not very selective
            100,    // 100 bytes per row
        );

        // With 90% selectivity, pushdown might not help much
        // but it could still be beneficial due to reduced local processing
    }
}
