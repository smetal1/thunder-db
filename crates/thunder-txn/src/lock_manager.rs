//! Lock manager implementation for ThunderDB.
//!
//! Provides hierarchical locking with:
//! - Row-level and table-level locks
//! - Multiple lock modes (S, X, IS, IX, SIX)
//! - Lock compatibility matrix
//! - Wait queues with timeout support

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thunder_common::prelude::*;

/// Lock modes following standard database locking protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Shared lock - allows concurrent reads
    Shared,
    /// Exclusive lock - no concurrent access
    Exclusive,
    /// Intention Shared - intent to acquire S lock on descendant
    IntentionShared,
    /// Intention Exclusive - intent to acquire X lock on descendant
    IntentionExclusive,
    /// Shared + Intention Exclusive - read table, write some rows
    ShareIntentionExclusive,
}

impl LockMode {
    /// Check if this lock mode is compatible with another.
    /// Two transactions can hold locks simultaneously if their modes are compatible.
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        use LockMode::*;
        matches!(
            (self, other),
            // IS is compatible with IS, IX, S, SIX
            (IntentionShared, IntentionShared)
                | (IntentionShared, IntentionExclusive)
                | (IntentionShared, Shared)
                | (IntentionShared, ShareIntentionExclusive)
                // IX is compatible with IS, IX
                | (IntentionExclusive, IntentionShared)
                | (IntentionExclusive, IntentionExclusive)
                // S is compatible with IS, S
                | (Shared, IntentionShared)
                | (Shared, Shared)
                // SIX is compatible with IS only
                | (ShareIntentionExclusive, IntentionShared)
            // X is compatible with nothing
        )
    }

    /// Check if this mode can be upgraded to the target mode.
    pub fn can_upgrade_to(&self, target: &LockMode) -> bool {
        use LockMode::*;
        match (self, target) {
            (Shared, Exclusive) => true,
            (IntentionShared, Shared) => true,
            (IntentionShared, IntentionExclusive) => true,
            (IntentionShared, ShareIntentionExclusive) => true,
            (IntentionExclusive, Exclusive) => true,
            (IntentionExclusive, ShareIntentionExclusive) => true,
            (Shared, ShareIntentionExclusive) => true,
            _ => false,
        }
    }
}

/// Resource identifier for locking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockResource {
    /// Database-level lock
    Database,
    /// Table-level lock
    Table(TableId),
    /// Row-level lock
    Row(TableId, RowId),
    /// Page-level lock (for B+Tree operations)
    Page(PageId),
}

/// Lock request from a transaction.
#[derive(Debug, Clone)]
pub struct LockRequest {
    pub txn_id: TxnId,
    pub resource: LockResource,
    pub mode: LockMode,
    pub timestamp: Instant,
}

impl LockRequest {
    pub fn new(txn_id: TxnId, resource: LockResource, mode: LockMode) -> Self {
        Self {
            txn_id,
            resource,
            mode,
            timestamp: Instant::now(),
        }
    }
}

/// Result of a lock acquisition attempt.
#[derive(Debug, Clone)]
pub enum LockResult {
    /// Lock was granted immediately
    Granted,
    /// Lock request is waiting in queue
    Waiting,
    /// Lock request was denied (e.g., deadlock prevention)
    Denied(String),
    /// Lock acquisition timed out
    Timeout,
    /// Transaction already holds the lock (possibly in different mode)
    AlreadyHeld(LockMode),
}

/// State of a lock held by transactions.
#[derive(Debug)]
struct LockState {
    /// Current holders of the lock: txn_id -> mode
    holders: HashMap<TxnId, LockMode>,
    /// Queue of waiting requests
    wait_queue: VecDeque<LockRequest>,
    /// Condition variable for waiters
    condvar: Arc<Condvar>,
}

impl LockState {
    fn new() -> Self {
        Self {
            holders: HashMap::new(),
            wait_queue: VecDeque::new(),
            condvar: Arc::new(Condvar::new()),
        }
    }

    /// Check if a new lock request is compatible with current holders.
    fn is_compatible(&self, txn_id: TxnId, mode: LockMode) -> bool {
        for (&holder_txn, &holder_mode) in &self.holders {
            if holder_txn != txn_id && !mode.is_compatible(&holder_mode) {
                return false;
            }
        }
        true
    }

    /// Check if transaction already holds a lock.
    fn get_held_mode(&self, txn_id: TxnId) -> Option<LockMode> {
        self.holders.get(&txn_id).copied()
    }

    /// Grant lock to transaction.
    fn grant(&mut self, txn_id: TxnId, mode: LockMode) {
        self.holders.insert(txn_id, mode);
    }

    /// Release lock held by transaction.
    fn release(&mut self, txn_id: TxnId) -> bool {
        self.holders.remove(&txn_id).is_some()
    }

    /// Add request to wait queue.
    fn enqueue(&mut self, request: LockRequest) {
        self.wait_queue.push_back(request);
    }

    /// Remove request from wait queue.
    fn dequeue(&mut self, txn_id: TxnId) {
        self.wait_queue.retain(|r| r.txn_id != txn_id);
    }

    /// Try to grant locks to waiting requests.
    fn process_wait_queue(&mut self) -> Vec<TxnId> {
        let mut granted = Vec::new();
        let mut remaining = VecDeque::new();

        while let Some(request) = self.wait_queue.pop_front() {
            if self.is_compatible(request.txn_id, request.mode) {
                self.grant(request.txn_id, request.mode);
                granted.push(request.txn_id);
            } else {
                remaining.push_back(request);
            }
        }

        self.wait_queue = remaining;
        granted
    }
}

/// Lock manager statistics.
#[derive(Debug, Clone, Default)]
pub struct LockManagerStats {
    pub locks_granted: u64,
    pub locks_denied: u64,
    pub locks_waited: u64,
    pub locks_released: u64,
    pub deadlocks_detected: u64,
    pub timeouts: u64,
}

/// Lock manager implementation.
pub struct LockManager {
    /// Lock states keyed by resource
    locks: DashMap<LockResource, Arc<Mutex<LockState>>>,
    /// Transactions and their held locks
    txn_locks: DashMap<TxnId, Vec<LockResource>>,
    /// Wait-for graph: waiter -> set of holders it's waiting for
    wait_for: DashMap<TxnId, Vec<TxnId>>,
    /// Lock acquisition timeout
    timeout: Duration,
    /// Statistics
    stats: Mutex<LockManagerStats>,
    /// Lock count for debugging
    lock_count: AtomicU64,
}

impl LockManager {
    /// Create a new lock manager with default timeout.
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(30))
    }

    /// Create a new lock manager with custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            locks: DashMap::new(),
            txn_locks: DashMap::new(),
            wait_for: DashMap::new(),
            timeout,
            stats: Mutex::new(LockManagerStats::default()),
            lock_count: AtomicU64::new(0),
        }
    }

    /// Acquire a lock on a resource.
    pub fn acquire(
        &self,
        txn_id: TxnId,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<LockResult> {
        let lock_state = self
            .locks
            .entry(resource.clone())
            .or_insert_with(|| Arc::new(Mutex::new(LockState::new())))
            .clone();

        let mut state = lock_state.lock();

        // Check if transaction already holds a lock on this resource
        if let Some(held_mode) = state.get_held_mode(txn_id) {
            if held_mode == mode {
                return Ok(LockResult::AlreadyHeld(held_mode));
            }
            // Lock upgrade case
            if held_mode.can_upgrade_to(&mode) {
                if state.is_compatible(txn_id, mode) {
                    state.grant(txn_id, mode);
                    self.stats.lock().locks_granted += 1;
                    return Ok(LockResult::Granted);
                }
                // Need to wait for upgrade
            } else {
                return Ok(LockResult::AlreadyHeld(held_mode));
            }
        }

        // Try to acquire immediately
        if state.is_compatible(txn_id, mode) && state.wait_queue.is_empty() {
            state.grant(txn_id, mode);
            self.record_lock(txn_id, resource);
            self.stats.lock().locks_granted += 1;
            self.lock_count.fetch_add(1, Ordering::SeqCst);
            return Ok(LockResult::Granted);
        }

        // Need to wait - add to queue and record wait-for edges
        let request = LockRequest::new(txn_id, resource.clone(), mode);
        state.enqueue(request);

        // Record wait-for relationships for deadlock detection
        let holders: Vec<TxnId> = state.holders.keys().copied().collect();
        self.wait_for.insert(txn_id, holders.clone());

        // Check for deadlock before waiting
        if self.has_cycle(txn_id) {
            state.dequeue(txn_id);
            self.wait_for.remove(&txn_id);
            self.stats.lock().deadlocks_detected += 1;
            return Ok(LockResult::Denied("Deadlock detected".into()));
        }

        self.stats.lock().locks_waited += 1;

        // Wait with timeout
        let condvar = state.condvar.clone();
        let deadline = Instant::now() + self.timeout;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.dequeue(txn_id);
                self.wait_for.remove(&txn_id);
                self.stats.lock().timeouts += 1;
                return Ok(LockResult::Timeout);
            }

            // Wait for notification
            let result = condvar.wait_for(&mut state, remaining);

            // Check if we got the lock
            if state.get_held_mode(txn_id).is_some() {
                self.wait_for.remove(&txn_id);
                self.record_lock(txn_id, resource);
                self.lock_count.fetch_add(1, Ordering::SeqCst);
                return Ok(LockResult::Granted);
            }

            // Check for spurious wakeup or if we're still waiting
            if result.timed_out() {
                state.dequeue(txn_id);
                self.wait_for.remove(&txn_id);
                self.stats.lock().timeouts += 1;
                return Ok(LockResult::Timeout);
            }
        }
    }

    /// Try to acquire a lock without waiting.
    pub fn try_acquire(
        &self,
        txn_id: TxnId,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<LockResult> {
        let lock_state = self
            .locks
            .entry(resource.clone())
            .or_insert_with(|| Arc::new(Mutex::new(LockState::new())))
            .clone();

        let mut state = lock_state.lock();

        // Check if already held
        if let Some(held_mode) = state.get_held_mode(txn_id) {
            return Ok(LockResult::AlreadyHeld(held_mode));
        }

        // Try to acquire immediately
        if state.is_compatible(txn_id, mode) && state.wait_queue.is_empty() {
            state.grant(txn_id, mode);
            self.record_lock(txn_id, resource);
            self.stats.lock().locks_granted += 1;
            self.lock_count.fetch_add(1, Ordering::SeqCst);
            Ok(LockResult::Granted)
        } else {
            self.stats.lock().locks_denied += 1;
            Ok(LockResult::Waiting)
        }
    }

    /// Release a lock on a resource.
    pub fn release(&self, txn_id: TxnId, resource: &LockResource) -> Result<bool> {
        if let Some(lock_state) = self.locks.get(resource) {
            let lock_state = lock_state.clone();
            let mut state = lock_state.lock();

            if state.release(txn_id) {
                self.remove_lock(txn_id, resource);
                self.stats.lock().locks_released += 1;
                self.lock_count.fetch_sub(1, Ordering::SeqCst);

                // Process waiting requests
                let granted = state.process_wait_queue();

                // Notify waiters
                if !granted.is_empty() {
                    state.condvar.notify_all();
                }

                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Release all locks held by a transaction.
    pub fn release_all(&self, txn_id: TxnId) -> Result<usize> {
        let resources: Vec<LockResource> = self
            .txn_locks
            .remove(&txn_id)
            .map(|(_, v)| v)
            .unwrap_or_default();

        let count = resources.len();

        for resource in resources {
            if let Some(lock_state) = self.locks.get(&resource) {
                let lock_state = lock_state.clone();
                let mut state = lock_state.lock();

                if state.release(txn_id) {
                    self.stats.lock().locks_released += 1;
                    self.lock_count.fetch_sub(1, Ordering::SeqCst);

                    // Process waiting requests
                    let granted = state.process_wait_queue();

                    // Notify waiters
                    if !granted.is_empty() {
                        state.condvar.notify_all();
                    }
                }
            }
        }

        // Clean up wait-for edges
        self.wait_for.remove(&txn_id);

        Ok(count)
    }

    /// Check if a transaction holds a lock on a resource.
    pub fn is_locked(&self, txn_id: TxnId, resource: &LockResource) -> bool {
        if let Some(lock_state) = self.locks.get(resource) {
            let state = lock_state.lock();
            return state.get_held_mode(txn_id).is_some();
        }
        false
    }

    /// Get the lock mode held by a transaction on a resource.
    pub fn get_lock_mode(&self, txn_id: TxnId, resource: &LockResource) -> Option<LockMode> {
        self.locks.get(resource).and_then(|lock_state| {
            let state = lock_state.lock();
            state.get_held_mode(txn_id)
        })
    }

    /// Get statistics.
    pub fn stats(&self) -> LockManagerStats {
        self.stats.lock().clone()
    }

    /// Get current lock count.
    pub fn lock_count(&self) -> u64 {
        self.lock_count.load(Ordering::SeqCst)
    }

    /// Record that a transaction holds a lock on a resource.
    fn record_lock(&self, txn_id: TxnId, resource: LockResource) {
        self.txn_locks
            .entry(txn_id)
            .or_insert_with(Vec::new)
            .push(resource);
    }

    /// Remove lock record for a transaction.
    fn remove_lock(&self, txn_id: TxnId, resource: &LockResource) {
        if let Some(mut locks) = self.txn_locks.get_mut(&txn_id) {
            locks.retain(|r| r != resource);
        }
    }

    /// Check for cycles in wait-for graph (deadlock detection).
    fn has_cycle(&self, start: TxnId) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![start];

        while let Some(txn) = stack.pop() {
            if !visited.insert(txn) {
                if txn == start {
                    return true;
                }
                continue;
            }

            if let Some(waiters) = self.wait_for.get(&txn) {
                for &holder in waiters.value() {
                    if holder == start {
                        return true;
                    }
                    if !visited.contains(&holder) {
                        stack.push(holder);
                    }
                }
            }
        }

        false
    }

    /// Get all locks held by a transaction.
    pub fn get_locks(&self, txn_id: TxnId) -> Vec<(LockResource, LockMode)> {
        let resources = self
            .txn_locks
            .get(&txn_id)
            .map(|r| r.clone())
            .unwrap_or_default();

        resources
            .into_iter()
            .filter_map(|resource| {
                self.get_lock_mode(txn_id, &resource)
                    .map(|mode| (resource, mode))
            })
            .collect()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_mode_compatibility() {
        use LockMode::*;

        // Shared locks are compatible with each other
        assert!(Shared.is_compatible(&Shared));
        assert!(Shared.is_compatible(&IntentionShared));
        assert!(!Shared.is_compatible(&Exclusive));
        assert!(!Shared.is_compatible(&IntentionExclusive));

        // Exclusive locks are compatible with nothing
        assert!(!Exclusive.is_compatible(&Shared));
        assert!(!Exclusive.is_compatible(&Exclusive));
        assert!(!Exclusive.is_compatible(&IntentionShared));
        assert!(!Exclusive.is_compatible(&IntentionExclusive));

        // Intention locks
        assert!(IntentionShared.is_compatible(&IntentionShared));
        assert!(IntentionShared.is_compatible(&IntentionExclusive));
        assert!(IntentionExclusive.is_compatible(&IntentionShared));
        assert!(IntentionExclusive.is_compatible(&IntentionExclusive));
    }

    #[test]
    fn test_lock_acquire_release() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource = LockResource::Row(TableId(1), RowId(1));

        // Acquire exclusive lock
        let result = lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();
        assert!(matches!(result, LockResult::Granted));

        // Verify lock is held
        assert!(lm.is_locked(txn1, &resource));
        assert_eq!(lm.get_lock_mode(txn1, &resource), Some(LockMode::Exclusive));

        // Release lock
        assert!(lm.release(txn1, &resource).unwrap());
        assert!(!lm.is_locked(txn1, &resource));
    }

    #[test]
    fn test_shared_lock_concurrency() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        let resource = LockResource::Row(TableId(1), RowId(1));

        // Both transactions can acquire shared locks
        let result1 = lm.acquire(txn1, resource.clone(), LockMode::Shared).unwrap();
        let result2 = lm.acquire(txn2, resource.clone(), LockMode::Shared).unwrap();

        assert!(matches!(result1, LockResult::Granted));
        assert!(matches!(result2, LockResult::Granted));

        assert!(lm.is_locked(txn1, &resource));
        assert!(lm.is_locked(txn2, &resource));
    }

    #[test]
    fn test_exclusive_lock_blocks() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let txn2 = TxnId(2);
        let resource = LockResource::Row(TableId(1), RowId(1));

        // Txn1 acquires exclusive lock
        let result1 = lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();
        assert!(matches!(result1, LockResult::Granted));

        // Txn2 cannot acquire shared lock immediately
        let result2 = lm.try_acquire(txn2, resource.clone(), LockMode::Shared).unwrap();
        assert!(matches!(result2, LockResult::Waiting));
    }

    #[test]
    fn test_release_all() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource1 = LockResource::Row(TableId(1), RowId(1));
        let resource2 = LockResource::Row(TableId(1), RowId(2));

        lm.acquire(txn1, resource1.clone(), LockMode::Exclusive).unwrap();
        lm.acquire(txn1, resource2.clone(), LockMode::Exclusive).unwrap();

        assert_eq!(lm.lock_count(), 2);

        let released = lm.release_all(txn1).unwrap();
        assert_eq!(released, 2);
        assert_eq!(lm.lock_count(), 0);
    }

    #[test]
    fn test_lock_upgrade() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource = LockResource::Row(TableId(1), RowId(1));

        // Acquire shared lock
        let result1 = lm.acquire(txn1, resource.clone(), LockMode::Shared).unwrap();
        assert!(matches!(result1, LockResult::Granted));

        // Upgrade to exclusive
        let result2 = lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();
        assert!(matches!(result2, LockResult::Granted));

        assert_eq!(lm.get_lock_mode(txn1, &resource), Some(LockMode::Exclusive));
    }

    #[test]
    fn test_already_held() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource = LockResource::Row(TableId(1), RowId(1));

        lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();

        // Try to acquire same lock again
        let result = lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();
        assert!(matches!(result, LockResult::AlreadyHeld(LockMode::Exclusive)));
    }

    #[test]
    fn test_hierarchical_locking() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let table = LockResource::Table(TableId(1));
        let row = LockResource::Row(TableId(1), RowId(1));

        // Acquire intention exclusive on table
        let result1 = lm.acquire(txn1, table.clone(), LockMode::IntentionExclusive).unwrap();
        assert!(matches!(result1, LockResult::Granted));

        // Acquire exclusive on row
        let result2 = lm.acquire(txn1, row.clone(), LockMode::Exclusive).unwrap();
        assert!(matches!(result2, LockResult::Granted));

        // Another transaction can still acquire IS on table
        let txn2 = TxnId(2);
        let result3 = lm.acquire(txn2, table.clone(), LockMode::IntentionShared).unwrap();
        assert!(matches!(result3, LockResult::Granted));
    }

    #[test]
    fn test_get_locks() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource1 = LockResource::Row(TableId(1), RowId(1));
        let resource2 = LockResource::Row(TableId(1), RowId(2));

        lm.acquire(txn1, resource1.clone(), LockMode::Shared).unwrap();
        lm.acquire(txn1, resource2.clone(), LockMode::Exclusive).unwrap();

        let locks = lm.get_locks(txn1);
        assert_eq!(locks.len(), 2);
    }

    #[test]
    fn test_stats() {
        let lm = LockManager::new();
        let txn1 = TxnId(1);
        let resource = LockResource::Row(TableId(1), RowId(1));

        lm.acquire(txn1, resource.clone(), LockMode::Exclusive).unwrap();
        lm.release(txn1, &resource).unwrap();

        let stats = lm.stats();
        assert_eq!(stats.locks_granted, 1);
        assert_eq!(stats.locks_released, 1);
    }
}
