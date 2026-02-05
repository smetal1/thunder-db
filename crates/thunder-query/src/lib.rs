//! # Thunder Query
//!
//! Query execution engine for ThunderDB providing:
//! - Volcano-style iterator model
//! - Vectorized execution
//! - Parallel query processing
//! - Physical plan execution

pub mod executor;
pub mod parallel;
pub mod physical_plan;
pub mod vectorized;

use async_trait::async_trait;
use thunder_common::prelude::*;
use thunder_sql::LogicalPlan;

// Re-exports for executor module
pub use executor::{
    Aggregator, AsyncExecutor, ExecutionContext, ExprEvaluator, MemoryExecutor,
};

// Re-exports for physical_plan module
pub use physical_plan::{
    format_physical_plan, walk_physical_plan, DefaultPhysicalPlanner, PhysicalPlanVisitor,
    PlannerConfig,
};

// Re-exports for vectorized module
pub use vectorized::{
    ColumnVector, VectorBatch, VectorizedEvaluator, VectorizedExecutor, VectorizedFilter,
    VectorizedHashAggregate, VectorizedHashJoin, VectorizedProject, VectorizedSort,
    DEFAULT_BATCH_SIZE,
};

// Re-exports for parallel module
pub use parallel::{
    Exchange, ParallelContext, ParallelExecutor, ParallelHashAggregate, ParallelHashJoin,
    ParallelScan, ParallelSort, Partition, Partitioner, DEFAULT_NUM_WORKERS,
};

/// Physical plan types
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential scan
    SeqScan {
        table_id: TableId,
        schema: Schema,
        filter: Option<thunder_sql::Expr>,
    },
    /// Index scan
    IndexScan {
        table_id: TableId,
        index_id: IndexId,
        schema: Schema,
        range: Option<ScanRange>,
    },
    /// Hash join
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: thunder_sql::JoinType,
    },
    /// Merge join
    MergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: thunder_sql::JoinType,
    },
    /// Nested loop join
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: thunder_sql::Expr,
        join_type: thunder_sql::JoinType,
    },
    /// Hash aggregate
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<usize>,
        aggregates: Vec<thunder_sql::AggregateExpr>,
        schema: Schema,
    },
    /// Sort
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<thunder_sql::SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: usize,
    },
    /// Project
    Project {
        input: Box<PhysicalPlan>,
        exprs: Vec<thunder_sql::Expr>,
        schema: Schema,
    },
    /// Filter
    Filter {
        input: Box<PhysicalPlan>,
        predicate: thunder_sql::Expr,
    },
    /// Exchange (for distributed execution)
    Exchange {
        input: Box<PhysicalPlan>,
        partitioning: Partitioning,
    },
    /// Insert
    Insert {
        table_id: TableId,
        input: Box<PhysicalPlan>,
    },
    /// Update
    Update {
        table_id: TableId,
        input: Box<PhysicalPlan>,
        assignments: Vec<(usize, thunder_sql::Expr)>,
    },
    /// Delete
    Delete {
        table_id: TableId,
        input: Box<PhysicalPlan>,
    },
    /// Values (inline data)
    Values {
        values: Vec<Vec<Value>>,
        schema: Schema,
    },
    /// Empty
    Empty { schema: Schema },
}

/// Scan range for index scans
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end_inclusive: bool,
}

/// Partitioning strategy for distributed execution
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Round-robin distribution
    RoundRobin { partitions: usize },
    /// Hash partitioning
    Hash { columns: Vec<usize>, partitions: usize },
    /// Range partitioning
    Range { column: usize, boundaries: Vec<Value> },
    /// Single partition (no distribution)
    Single,
}

/// Query executor trait
#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute a physical plan and return results
    async fn execute(&self, plan: PhysicalPlan, txn_id: TxnId) -> Result<ExecutionResult>;
}

/// Execution result
pub struct ExecutionResult {
    /// Result schema
    pub schema: Schema,
    /// Result batches
    pub batches: Vec<RecordBatch>,
    /// Rows affected (for DML)
    pub rows_affected: Option<u64>,
    /// Execution statistics
    pub stats: ExecutionStats,
}

/// A batch of records (simplified, wraps Arrow RecordBatch)
pub struct RecordBatch {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl RecordBatch {
    pub fn new(schema: Schema, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub execution_time_ms: u64,
}

/// Physical planner trait
pub trait PhysicalPlanner: Send + Sync {
    /// Convert a logical plan to a physical plan
    fn plan(&self, logical: LogicalPlan) -> Result<PhysicalPlan>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_batch() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
        ]);

        let rows = vec![
            Row::new(vec![Value::Int64(1), Value::String("Alice".into())]),
            Row::new(vec![Value::Int64(2), Value::String("Bob".into())]),
        ];

        let batch = RecordBatch::new(schema, rows);
        assert_eq!(batch.num_rows(), 2);
    }
}
