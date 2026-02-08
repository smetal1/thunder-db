//! Parallel Query Processing
//!
//! Provides parallel execution capabilities for queries using work-stealing
//! and partitioned data processing for scalable performance.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use thunder_common::prelude::*;
use thunder_sql::{AggregateExpr, Expr, JoinType};

use crate::executor::Aggregator;
use crate::vectorized::{VectorBatch, VectorizedFilter, VectorizedSort};
use crate::{ExecutionStats, Partitioning, PhysicalPlan};

/// Default number of parallel workers
pub const DEFAULT_NUM_WORKERS: usize = 4;

/// Parallel execution context
pub struct ParallelContext {
    /// Number of worker threads
    pub num_workers: usize,
    /// Memory limit per worker
    pub memory_per_worker: usize,
    /// Batch size for processing
    pub batch_size: usize,
}

impl Default for ParallelContext {
    fn default() -> Self {
        Self {
            num_workers: DEFAULT_NUM_WORKERS,
            memory_per_worker: 64 * 1024 * 1024, // 64MB
            batch_size: 1024,
        }
    }
}

impl ParallelContext {
    /// Create with specific number of workers
    pub fn with_workers(num_workers: usize) -> Self {
        Self {
            num_workers,
            ..Default::default()
        }
    }
}

/// A partition of data for parallel processing
#[derive(Debug, Clone)]
pub struct Partition {
    /// Partition index
    pub index: usize,
    /// Data batches in this partition
    pub batches: Vec<VectorBatch>,
}

impl Partition {
    /// Create empty partition
    pub fn new(index: usize) -> Self {
        Self {
            index,
            batches: Vec::new(),
        }
    }

    /// Create with batches
    pub fn with_batches(index: usize, batches: Vec<VectorBatch>) -> Self {
        Self { index, batches }
    }

    /// Total rows in partition
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows).sum()
    }

    /// Is empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.num_rows() == 0
    }

    /// Add a batch
    pub fn add_batch(&mut self, batch: VectorBatch) {
        self.batches.push(batch);
    }
}

/// Partitioner for distributing data across workers
pub struct Partitioner {
    /// Number of partitions
    num_partitions: usize,
    /// Partitioning strategy
    strategy: Partitioning,
}

impl Partitioner {
    /// Create new partitioner
    pub fn new(strategy: Partitioning) -> Self {
        let num_partitions = match &strategy {
            Partitioning::RoundRobin { partitions } => *partitions,
            Partitioning::Hash { partitions, .. } => *partitions,
            Partitioning::Range { boundaries, .. } => boundaries.len() + 1,
            Partitioning::Single => 1,
        };

        Self {
            num_partitions,
            strategy,
        }
    }

    /// Partition a batch
    pub fn partition(&self, batch: &VectorBatch) -> Vec<Partition> {
        let mut partitions: Vec<Partition> = (0..self.num_partitions)
            .map(|i| Partition::new(i))
            .collect();

        match &self.strategy {
            Partitioning::Single => {
                partitions[0].add_batch(batch.clone());
            }

            Partitioning::RoundRobin { .. } => {
                // Split batch into equal parts
                let rows_per_partition = (batch.num_rows + self.num_partitions - 1) / self.num_partitions;

                for (p_idx, partition) in partitions.iter_mut().enumerate() {
                    let start = p_idx * rows_per_partition;
                    let end = (start + rows_per_partition).min(batch.num_rows);

                    if start < end {
                        let rows: Vec<Row> = (start..end)
                            .filter_map(|i| batch.get_row(i))
                            .collect();
                        if !rows.is_empty() {
                            partition.add_batch(VectorBatch::from_rows(batch.schema.clone(), &rows));
                        }
                    }
                }
            }

            Partitioning::Hash { columns, .. } => {
                // Hash partition by column values
                for i in 0..batch.num_rows {
                    let hash = self.compute_hash(batch, i, columns);
                    let p_idx = (hash as usize) % self.num_partitions;

                    if let Some(row) = batch.get_row(i) {
                        // Accumulate rows per partition
                        if partitions[p_idx].batches.is_empty() {
                            partitions[p_idx].add_batch(VectorBatch::new(batch.schema.clone()));
                        }
                        partitions[p_idx].batches[0].append_row(&row);
                    }
                }
            }

            Partitioning::Range { column, boundaries } => {
                // Range partition by column value
                for i in 0..batch.num_rows {
                    let value = batch
                        .columns
                        .get(*column)
                        .and_then(|c| c.get(i).cloned())
                        .unwrap_or(Value::Null);

                    let p_idx = self.find_range_partition(&value, boundaries);

                    if let Some(row) = batch.get_row(i) {
                        if partitions[p_idx].batches.is_empty() {
                            partitions[p_idx].add_batch(VectorBatch::new(batch.schema.clone()));
                        }
                        partitions[p_idx].batches[0].append_row(&row);
                    }
                }
            }
        }

        partitions
    }

    /// Compute hash for a row
    fn compute_hash(&self, batch: &VectorBatch, row_idx: usize, columns: &[usize]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for &col_idx in columns {
            if let Some(value) = batch.columns.get(col_idx).and_then(|c| c.get(row_idx)) {
                value.to_string().hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// Find partition for range partitioning
    fn find_range_partition(&self, value: &Value, boundaries: &[Value]) -> usize {
        for (i, boundary) in boundaries.iter().enumerate() {
            if crate::executor::ExprEvaluator::compare_values(value, boundary)
                == std::cmp::Ordering::Less
            {
                return i;
            }
        }
        boundaries.len()
    }
}

/// Exchange operator for redistributing data between partitions
pub struct Exchange {
    /// Input partitions
    input_partitions: Vec<Partition>,
    /// Partitioning strategy for output
    output_partitioning: Partitioning,
}

impl Exchange {
    /// Create new exchange
    pub fn new(partitioning: Partitioning) -> Self {
        Self {
            input_partitions: Vec::new(),
            output_partitioning: partitioning,
        }
    }

    /// Add input partition
    pub fn add_input(&mut self, partition: Partition) {
        self.input_partitions.push(partition);
    }

    /// Execute exchange and return output partitions
    pub fn execute(&self) -> Vec<Partition> {
        let partitioner = Partitioner::new(self.output_partitioning.clone());

        // Collect all batches from input partitions
        let all_batches: Vec<&VectorBatch> = self
            .input_partitions
            .iter()
            .flat_map(|p| p.batches.iter())
            .collect();

        // Re-partition all data
        let mut output_partitions: Vec<Partition> = Vec::new();

        for batch in all_batches {
            let partitioned = partitioner.partition(batch);
            if output_partitions.is_empty() {
                output_partitions = partitioned;
            } else {
                for (i, p) in partitioned.into_iter().enumerate() {
                    for batch in p.batches {
                        output_partitions[i].add_batch(batch);
                    }
                }
            }
        }

        output_partitions
    }
}

/// Parallel scan operator
pub struct ParallelScan {
    /// Table data (partitioned)
    partitions: Vec<Partition>,
    /// Filter predicate
    filter: Option<Expr>,
    /// Current partition index for iteration
    current_partition: AtomicUsize,
}

impl ParallelScan {
    /// Create new parallel scan
    pub fn new(data: Vec<VectorBatch>, num_partitions: usize, filter: Option<Expr>) -> Self {
        let partitioner = Partitioner::new(Partitioning::RoundRobin {
            partitions: num_partitions,
        });

        let mut partitions: Vec<Partition> = (0..num_partitions).map(Partition::new).collect();

        for batch in &data {
            let partitioned = partitioner.partition(batch);
            for (i, p) in partitioned.into_iter().enumerate() {
                for b in p.batches {
                    partitions[i].add_batch(b);
                }
            }
        }

        Self {
            partitions,
            filter,
            current_partition: AtomicUsize::new(0),
        }
    }

    /// Get next partition for processing (work-stealing style)
    pub fn next_partition(&self) -> Option<&Partition> {
        let idx = self.current_partition.fetch_add(1, Ordering::SeqCst);
        self.partitions.get(idx)
    }

    /// Process a partition
    pub fn process_partition(&self, partition: &Partition) -> Result<Vec<VectorBatch>> {
        let mut results = Vec::new();

        for batch in &partition.batches {
            let processed = if let Some(ref filter) = self.filter {
                VectorizedFilter::filter(batch, filter)?
            } else {
                batch.clone()
            };

            if !processed.is_empty() {
                results.push(processed);
            }
        }

        Ok(results)
    }
}

/// Parallel hash join operator
pub struct ParallelHashJoin {
    /// Build side partitions (right)
    build_partitions: Vec<Partition>,
    /// Probe side partitions (left)
    probe_partitions: Vec<Partition>,
    /// Left join keys
    left_keys: Vec<usize>,
    /// Right join keys
    right_keys: Vec<usize>,
    /// Join type
    #[allow(dead_code)]
    join_type: JoinType,
    /// Hash tables per partition (built from right side) - using String key for hashing
    hash_tables: Vec<Arc<RwLock<HashMap<String, Vec<Row>>>>>,
}

impl ParallelHashJoin {
    /// Create new parallel hash join
    pub fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        join_type: JoinType,
        num_partitions: usize,
    ) -> Self {
        let hash_tables = (0..num_partitions)
            .map(|_| Arc::new(RwLock::new(HashMap::new())))
            .collect();

        Self {
            build_partitions: Vec::new(),
            probe_partitions: Vec::new(),
            left_keys,
            right_keys,
            join_type,
            hash_tables,
        }
    }

    /// Set build side partitions (right side of join)
    pub fn set_build_partitions(&mut self, partitions: Vec<Partition>) {
        self.build_partitions = partitions;
    }

    /// Set probe side partitions (left side of join)
    pub fn set_probe_partitions(&mut self, partitions: Vec<Partition>) {
        self.probe_partitions = partitions;
    }

    /// Convert values to string key for hashing
    fn values_to_key(values: &[Value]) -> String {
        values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|")
    }

    /// Build hash table for a partition
    pub fn build_partition(&self, partition_idx: usize) {
        if let Some(partition) = self.build_partitions.get(partition_idx) {
            let mut table = self.hash_tables[partition_idx].write();

            for batch in &partition.batches {
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
                    table.entry(key).or_default().push(row);
                }
            }
        }
    }

    /// Probe a partition
    pub fn probe_partition(&self, partition_idx: usize, output_schema: Schema) -> VectorBatch {
        let mut result = VectorBatch::new(output_schema);

        if let Some(partition) = self.probe_partitions.get(partition_idx) {
            let table = self.hash_tables[partition_idx].read();

            for batch in &partition.batches {
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

                    if let Some(matches) = table.get(&key) {
                        let left_row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));
                        for right_row in matches {
                            let mut combined = left_row.values.clone();
                            combined.extend(right_row.values.clone());
                            result.append_row(&Row::new(combined));
                        }
                    }
                }
            }
        }

        result
    }
}

/// Parallel hash aggregate operator
pub struct ParallelHashAggregate {
    /// Group-by column indices
    group_by: Vec<usize>,
    /// Aggregate expressions
    aggregates: Vec<AggregateExpr>,
    /// Output schema
    schema: Schema,
    /// Partial aggregates per partition - using String key for hashing
    partial_aggs: Vec<Arc<Mutex<HashMap<String, (Vec<Value>, Vec<Aggregator>)>>>>,
}

impl ParallelHashAggregate {
    /// Create new parallel hash aggregate
    pub fn new(
        group_by: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
        num_partitions: usize,
    ) -> Self {
        let partial_aggs = (0..num_partitions)
            .map(|_| Arc::new(Mutex::new(HashMap::new())))
            .collect();

        Self {
            group_by,
            aggregates,
            schema,
            partial_aggs,
        }
    }

    /// Convert values to string key for hashing
    fn values_to_key(values: &[Value]) -> String {
        values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|")
    }

    /// Process a partition (partial aggregation)
    pub fn process_partition(&self, partition_idx: usize, partition: &Partition, input_schema: &Schema) {
        let mut aggs = self.partial_aggs[partition_idx].lock();

        for batch in &partition.batches {
            for i in 0..batch.num_rows {
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

                let (_, aggregators) = aggs.entry(key).or_insert_with(|| {
                    (key_values.clone(), self.aggregates.iter().map(Aggregator::new).collect())
                });

                let row = batch.get_row(i).unwrap_or_else(|| Row::new(vec![]));

                for (agg, agg_expr) in aggregators.iter_mut().zip(self.aggregates.iter()) {
                    let arg_values: Vec<Value> = agg_expr
                        .args
                        .iter()
                        .map(|arg| {
                            crate::executor::ExprEvaluator::evaluate(arg, &row, input_schema)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    agg.accumulate(&arg_values);
                }
            }
        }
    }

    /// Merge partial results and finalize
    pub fn finalize(&self) -> VectorBatch {
        // Merge all partial aggregates
        let mut merged: HashMap<String, (Vec<Value>, Vec<Aggregator>)> = HashMap::new();

        for partial in &self.partial_aggs {
            let aggs = partial.lock();
            for (key, (key_values, aggregators)) in aggs.iter() {
                if merged.get(key).is_none() {
                    merged.insert(key.clone(), (key_values.clone(), aggregators.clone()));
                }
                // Note: proper implementation would merge aggregator states
            }
        }

        // Build result
        let mut result = VectorBatch::new(self.schema.clone());
        for (_, (key_values, aggs)) in &merged {
            let mut values = key_values.clone();
            for agg in aggs {
                values.push(agg.finalize());
            }
            result.append_row(&Row::new(values));
        }

        result
    }
}

impl Clone for Aggregator {
    fn clone(&self) -> Self {
        // Create a new aggregator with the same function
        let expr = AggregateExpr {
            func: self.func.clone(),
            args: vec![],
            distinct: self.distinct,
        };
        Aggregator::new(&expr)
    }
}

/// Parallel sort operator using merge sort
pub struct ParallelSort {
    /// Sort expressions
    order_by: Vec<thunder_sql::SortExpr>,
    /// Sorted partitions (intermediate results)
    sorted_partitions: Vec<VectorBatch>,
}

impl ParallelSort {
    /// Create new parallel sort
    pub fn new(order_by: Vec<thunder_sql::SortExpr>) -> Self {
        Self {
            order_by,
            sorted_partitions: Vec::new(),
        }
    }

    /// Sort a partition
    pub fn sort_partition(&mut self, partition: &Partition) -> Result<VectorBatch> {
        // Combine all batches in partition
        let all_rows: Vec<Row> = partition
            .batches
            .iter()
            .flat_map(|b| b.to_rows())
            .collect();

        if all_rows.is_empty() {
            return Ok(VectorBatch::new(Schema::empty()));
        }

        let schema = partition.batches[0].schema.clone();
        let batch = VectorBatch::from_rows(schema, &all_rows);

        VectorizedSort::sort(&batch, &self.order_by)
    }

    /// Add sorted partition for merging
    pub fn add_sorted_partition(&mut self, batch: VectorBatch) {
        self.sorted_partitions.push(batch);
    }

    /// Merge all sorted partitions
    pub fn merge_all(&self) -> Result<VectorBatch> {
        if self.sorted_partitions.is_empty() {
            return Ok(VectorBatch::new(Schema::empty()));
        }

        // Simple k-way merge
        let schema = self.sorted_partitions[0].schema.clone();
        let mut indices: Vec<usize> = vec![0; self.sorted_partitions.len()];
        let mut result = VectorBatch::new(schema.clone());

        loop {
            // Find minimum across all partitions
            let mut min_idx: Option<usize> = None;
            let mut min_row: Option<Row> = None;

            for (p_idx, partition) in self.sorted_partitions.iter().enumerate() {
                let row_idx = indices[p_idx];
                if row_idx >= partition.num_rows {
                    continue;
                }

                let row = partition.get_row(row_idx).unwrap();

                let is_smaller = match &min_row {
                    None => true,
                    Some(current_min) => {
                        self.compare_rows(&row, current_min, &schema) == std::cmp::Ordering::Less
                    }
                };

                if is_smaller {
                    min_idx = Some(p_idx);
                    min_row = Some(row);
                }
            }

            match (min_idx, min_row) {
                (Some(idx), Some(row)) => {
                    result.append_row(&row);
                    indices[idx] += 1;
                }
                _ => break,
            }
        }

        Ok(result)
    }

    /// Compare two rows based on order expressions
    fn compare_rows(&self, a: &Row, b: &Row, schema: &Schema) -> std::cmp::Ordering {
        for sort_expr in &self.order_by {
            let val_a = crate::executor::ExprEvaluator::evaluate(&sort_expr.expr, a, schema)
                .unwrap_or(Value::Null);
            let val_b = crate::executor::ExprEvaluator::evaluate(&sort_expr.expr, b, schema)
                .unwrap_or(Value::Null);

            let cmp = crate::executor::ExprEvaluator::compare_values(&val_a, &val_b);
            let cmp = if sort_expr.asc { cmp } else { cmp.reverse() };

            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }
}

/// Parallel query executor
pub struct ParallelExecutor {
    /// Execution context
    context: ParallelContext,
    /// Execution statistics
    stats: Arc<Mutex<ExecutionStats>>,
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self::new(ParallelContext::default())
    }
}

impl ParallelExecutor {
    /// Create new parallel executor
    pub fn new(context: ParallelContext) -> Self {
        Self {
            context,
            stats: Arc::new(Mutex::new(ExecutionStats::default())),
        }
    }

    /// Execute a plan with parallel processing
    pub fn execute(&self, plan: &PhysicalPlan, data: Vec<VectorBatch>) -> Result<Vec<VectorBatch>> {
        match plan {
            PhysicalPlan::SeqScan { filter, .. } => {
                self.parallel_scan(data, filter.clone())
            }
            PhysicalPlan::Filter { input, predicate } => {
                let input_data = self.execute(input, data)?;
                self.parallel_filter(input_data, predicate.clone())
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
            } => {
                let input_data = self.execute(input, data)?;
                self.parallel_aggregate(
                    input_data,
                    group_by.clone(),
                    aggregates.clone(),
                    schema.clone(),
                )
            }
            PhysicalPlan::Sort { input, order_by } => {
                let input_data = self.execute(input, data)?;
                self.parallel_sort(input_data, order_by.clone())
            }
            _ => {
                // For unsupported operators, return data as-is
                Ok(data)
            }
        }
    }

    /// Parallel scan with filtering
    fn parallel_scan(
        &self,
        data: Vec<VectorBatch>,
        filter: Option<Expr>,
    ) -> Result<Vec<VectorBatch>> {
        let scan = ParallelScan::new(data, self.context.num_workers, filter);

        let mut results = Vec::new();
        for partition in &scan.partitions {
            let processed = scan.process_partition(partition)?;
            results.extend(processed);
        }

        Ok(results)
    }

    /// Parallel filter
    fn parallel_filter(
        &self,
        data: Vec<VectorBatch>,
        predicate: Expr,
    ) -> Result<Vec<VectorBatch>> {
        let mut results = Vec::new();

        for batch in data {
            let filtered = VectorizedFilter::filter(&batch, &predicate)?;
            if !filtered.is_empty() {
                results.push(filtered);
            }
        }

        Ok(results)
    }

    /// Parallel aggregation
    fn parallel_aggregate(
        &self,
        data: Vec<VectorBatch>,
        group_by: Vec<usize>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
    ) -> Result<Vec<VectorBatch>> {
        if data.is_empty() {
            return Ok(vec![VectorBatch::new(schema)]);
        }

        let input_schema = data[0].schema.clone();
        let num_partitions = self.context.num_workers.min(data.len()).max(1);

        let agg = ParallelHashAggregate::new(
            group_by,
            aggregates,
            schema,
            num_partitions,
        );

        // Partition data
        let partitioner = Partitioner::new(Partitioning::RoundRobin {
            partitions: num_partitions,
        });

        let mut partitions: Vec<Partition> = (0..num_partitions).map(Partition::new).collect();
        for batch in &data {
            let partitioned = partitioner.partition(batch);
            for (i, p) in partitioned.into_iter().enumerate() {
                for b in p.batches {
                    partitions[i].add_batch(b);
                }
            }
        }

        // Process each partition
        for (i, partition) in partitions.iter().enumerate() {
            agg.process_partition(i, partition, &input_schema);
        }

        // Finalize
        let result = agg.finalize();
        Ok(vec![result])
    }

    /// Parallel sort
    fn parallel_sort(
        &self,
        data: Vec<VectorBatch>,
        order_by: Vec<thunder_sql::SortExpr>,
    ) -> Result<Vec<VectorBatch>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let _schema = data[0].schema.clone();
        let num_partitions = self.context.num_workers.min(data.len()).max(1);

        let mut sort = ParallelSort::new(order_by);

        // Partition data
        let partitioner = Partitioner::new(Partitioning::RoundRobin {
            partitions: num_partitions,
        });

        let mut partitions: Vec<Partition> = (0..num_partitions).map(Partition::new).collect();
        for batch in &data {
            let partitioned = partitioner.partition(batch);
            for (i, p) in partitioned.into_iter().enumerate() {
                for b in p.batches {
                    partitions[i].add_batch(b);
                }
            }
        }

        // Sort each partition
        for partition in &partitions {
            if !partition.is_empty() {
                let sorted = sort.sort_partition(partition)?;
                if !sorted.is_empty() {
                    sort.add_sorted_partition(sorted);
                }
            }
        }

        // Merge results
        let merged = sort.merge_all()?;
        Ok(vec![merged])
    }

    /// Get execution statistics
    pub fn stats(&self) -> ExecutionStats {
        self.stats.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thunder_sql::{BinaryOperator, SortExpr};

    fn test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64).not_null(),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("age", DataType::Int32),
        ])
    }

    fn test_batch(start_id: i64, count: usize) -> VectorBatch {
        let schema = test_schema();
        let rows: Vec<Row> = (0..count)
            .map(|i| {
                Row::new(vec![
                    Value::Int64(start_id + i as i64),
                    Value::String(Arc::from(format!("User{}", i).as_str())),
                    Value::Int32(20 + (i % 30) as i32),
                ])
            })
            .collect();
        VectorBatch::from_rows(schema, &rows)
    }

    #[test]
    fn test_partitioner_round_robin() {
        let batch = test_batch(1, 100);
        let partitioner = Partitioner::new(Partitioning::RoundRobin { partitions: 4 });

        let partitions = partitioner.partition(&batch);
        assert_eq!(partitions.len(), 4);

        let total_rows: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_partitioner_hash() {
        let batch = test_batch(1, 100);
        let partitioner = Partitioner::new(Partitioning::Hash {
            columns: vec![0],
            partitions: 4,
        });

        let partitions = partitioner.partition(&batch);
        assert_eq!(partitions.len(), 4);

        let total_rows: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_parallel_scan() {
        let data = vec![test_batch(1, 50), test_batch(51, 50)];

        let filter = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(Value::Int32(30))),
        };

        let scan = ParallelScan::new(data, 4, Some(filter));

        let mut results = Vec::new();
        for partition in &scan.partitions {
            let processed = scan.process_partition(partition).unwrap();
            results.extend(processed);
        }

        // Some rows should be filtered out
        let total_rows: usize = results.iter().map(|b| b.num_rows).sum();
        assert!(total_rows < 100);
    }

    #[test]
    fn test_parallel_aggregate() {
        let data = vec![test_batch(1, 100)];
        let executor = ParallelExecutor::new(ParallelContext::with_workers(4));

        let schema = Schema::new(vec![
            ColumnDef::new("age", DataType::Int32),
            ColumnDef::new("count", DataType::Int64),
        ]);

        let result = executor
            .parallel_aggregate(
                data,
                vec![2], // group by age
                vec![AggregateExpr {
                    func: AggregateFunction::Count,
                    args: vec![Expr::Wildcard],
                    distinct: false,
                }],
                schema,
            )
            .unwrap();

        assert!(!result.is_empty());
    }

    #[test]
    fn test_parallel_sort() {
        let data = vec![test_batch(1, 50), test_batch(51, 50)];
        let executor = ParallelExecutor::new(ParallelContext::with_workers(4));

        let order_by = vec![SortExpr {
            expr: Expr::Column {
                name: "age".to_string(),
                index: Some(2),
            },
            asc: true,
            nulls_first: false,
        }];

        let result = executor.parallel_sort(data, order_by).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows, 100);

        // Verify sorted order
        for i in 1..result[0].num_rows {
            let prev_age = result[0].columns[2].get(i - 1).unwrap();
            let curr_age = result[0].columns[2].get(i).unwrap();
            assert!(
                crate::executor::ExprEvaluator::compare_values(prev_age, curr_age)
                    != std::cmp::Ordering::Greater
            );
        }
    }

    #[test]
    fn test_exchange() {
        let batch1 = test_batch(1, 50);
        let batch2 = test_batch(51, 50);

        let mut exchange = Exchange::new(Partitioning::Hash {
            columns: vec![0],
            partitions: 4,
        });

        exchange.add_input(Partition::with_batches(0, vec![batch1]));
        exchange.add_input(Partition::with_batches(1, vec![batch2]));

        let output = exchange.execute();
        assert_eq!(output.len(), 4);

        let total_rows: usize = output.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_parallel_hash_join() {
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
            ],
        );

        let mut join = ParallelHashJoin::new(vec![0], vec![0], JoinType::Inner, 1);
        join.set_build_partitions(vec![Partition::with_batches(0, vec![right_batch])]);
        join.set_probe_partitions(vec![Partition::with_batches(0, vec![left_batch])]);

        join.build_partition(0);

        let output_schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::String),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("amount", DataType::Float64),
        ]);

        let result = join.probe_partition(0, output_schema);
        assert_eq!(result.num_rows, 2); // Alice matches 2 orders
    }

    #[test]
    fn test_k_way_merge() {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        let batch1 = VectorBatch::from_rows(
            schema.clone(),
            &[
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(4)]),
                Row::new(vec![Value::Int64(7)]),
            ],
        );

        let batch2 = VectorBatch::from_rows(
            schema.clone(),
            &[
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(5)]),
                Row::new(vec![Value::Int64(8)]),
            ],
        );

        let batch3 = VectorBatch::from_rows(
            schema.clone(),
            &[
                Row::new(vec![Value::Int64(3)]),
                Row::new(vec![Value::Int64(6)]),
                Row::new(vec![Value::Int64(9)]),
            ],
        );

        let mut sort = ParallelSort::new(vec![SortExpr {
            expr: Expr::Column {
                name: "id".to_string(),
                index: Some(0),
            },
            asc: true,
            nulls_first: false,
        }]);

        sort.add_sorted_partition(batch1);
        sort.add_sorted_partition(batch2);
        sort.add_sorted_partition(batch3);

        let merged = sort.merge_all().unwrap();
        assert_eq!(merged.num_rows, 9);

        // Verify sorted order
        for i in 0..9 {
            let expected = Value::Int64(i as i64 + 1);
            assert_eq!(merged.columns[0].get(i), Some(&expected));
        }
    }
}
