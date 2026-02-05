//! Two-Phase Commit (2PC) coordinator for distributed transactions.
//!
//! Implements the classic 2PC protocol:
//! - Prepare phase: Ask all participants to prepare
//! - Commit phase: If all prepared, commit; otherwise abort
//!
//! Also supports presumed abort optimization and recovery.

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thunder_common::prelude::*;
use tokio::sync::mpsc;

/// Participant ID (node ID in the cluster).
pub type ParticipantId = u64;

/// Distributed transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistributedTxnState {
    /// Transaction is active
    Active,
    /// Prepare phase in progress
    Preparing,
    /// All participants prepared
    Prepared,
    /// Committing
    Committing,
    /// Committed
    Committed,
    /// Aborting
    Aborting,
    /// Aborted
    Aborted,
}

/// Participant state in a distributed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantState {
    /// Not yet contacted
    Unknown,
    /// Prepare request sent
    PrepareSent,
    /// Participant voted yes
    Prepared,
    /// Participant voted no
    Aborted,
    /// Commit request sent
    CommitSent,
    /// Commit acknowledged
    Committed,
    /// Abort request sent
    AbortSent,
    /// Abort acknowledged
    AbortAcked,
}

/// Message types for 2PC protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TwoPhaseMessage {
    /// Prepare request from coordinator
    PrepareRequest { txn_id: TxnId },
    /// Prepare response from participant
    PrepareResponse {
        txn_id: TxnId,
        participant_id: ParticipantId,
        vote: PrepareVote,
    },
    /// Commit request from coordinator
    CommitRequest { txn_id: TxnId },
    /// Commit acknowledgment from participant
    CommitAck {
        txn_id: TxnId,
        participant_id: ParticipantId,
    },
    /// Abort request from coordinator
    AbortRequest { txn_id: TxnId },
    /// Abort acknowledgment from participant
    AbortAck {
        txn_id: TxnId,
        participant_id: ParticipantId,
    },
    /// Query transaction status (for recovery)
    StatusQuery { txn_id: TxnId },
    /// Status response
    StatusResponse {
        txn_id: TxnId,
        state: DistributedTxnState,
    },
}

/// Participant's vote in prepare phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrepareVote {
    /// Yes, ready to commit
    Yes,
    /// No, cannot commit (abort)
    No,
}

/// Distributed transaction info.
#[derive(Debug)]
struct DistributedTxn {
    /// Transaction ID
    txn_id: TxnId,
    /// Current state
    state: DistributedTxnState,
    /// Participants in this transaction
    participants: HashMap<ParticipantId, ParticipantState>,
    /// Start time
    start_time: Instant,
    /// Timeout duration
    timeout: Duration,
}

impl DistributedTxn {
    fn new(txn_id: TxnId, participants: Vec<ParticipantId>, timeout: Duration) -> Self {
        let mut part_map = HashMap::new();
        for p in participants {
            part_map.insert(p, ParticipantState::Unknown);
        }
        Self {
            txn_id,
            state: DistributedTxnState::Active,
            participants: part_map,
            start_time: Instant::now(),
            timeout,
        }
    }

    fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    fn all_prepared(&self) -> bool {
        self.participants
            .values()
            .all(|s| *s == ParticipantState::Prepared)
    }

    fn any_aborted(&self) -> bool {
        self.participants
            .values()
            .any(|s| *s == ParticipantState::Aborted)
    }

    fn all_committed(&self) -> bool {
        self.participants
            .values()
            .all(|s| *s == ParticipantState::Committed)
    }

    fn all_abort_acked(&self) -> bool {
        self.participants
            .values()
            .all(|s| *s == ParticipantState::AbortAcked)
    }
}

/// Coordinator configuration.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Timeout for prepare phase
    pub prepare_timeout: Duration,
    /// Timeout for commit phase
    pub commit_timeout: Duration,
    /// Maximum concurrent distributed transactions
    pub max_concurrent_txns: usize,
    /// Enable presumed abort optimization
    pub presumed_abort: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            prepare_timeout: Duration::from_secs(30),
            commit_timeout: Duration::from_secs(60),
            max_concurrent_txns: 1000,
            presumed_abort: true,
        }
    }
}

/// Coordinator statistics.
#[derive(Debug, Clone, Default)]
pub struct CoordinatorStats {
    pub txns_started: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub prepare_timeouts: u64,
    pub commit_timeouts: u64,
    pub recovered_txns: u64,
}

/// Participant interface for the coordinator.
#[async_trait]
pub trait Participant: Send + Sync {
    /// Get participant ID.
    fn id(&self) -> ParticipantId;

    /// Send prepare request.
    async fn prepare(&self, txn_id: TxnId) -> Result<PrepareVote>;

    /// Send commit request.
    async fn commit(&self, txn_id: TxnId) -> Result<()>;

    /// Send abort request.
    async fn abort(&self, txn_id: TxnId) -> Result<()>;
}

/// Two-Phase Commit coordinator.
pub struct TwoPhaseCoordinator {
    /// Configuration
    config: CoordinatorConfig,
    /// Active distributed transactions
    transactions: DashMap<TxnId, RwLock<DistributedTxn>>,
    /// Committed transactions (for recovery)
    committed_txns: DashMap<TxnId, Instant>,
    /// Participants
    participants: DashMap<ParticipantId, Arc<dyn Participant>>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Statistics
    stats: Mutex<CoordinatorStats>,
    /// WAL for durability (would write to actual WAL in production)
    wal_log: Mutex<Vec<WalRecord>>,
}

/// WAL record for coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalRecord {
    Begin {
        txn_id: TxnId,
        participants: Vec<ParticipantId>,
    },
    Prepared {
        txn_id: TxnId,
    },
    Commit {
        txn_id: TxnId,
    },
    Abort {
        txn_id: TxnId,
    },
    End {
        txn_id: TxnId,
    },
}

impl TwoPhaseCoordinator {
    /// Create a new coordinator.
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            transactions: DashMap::new(),
            committed_txns: DashMap::new(),
            participants: DashMap::new(),
            next_txn_id: AtomicU64::new(1),
            stats: Mutex::new(CoordinatorStats::default()),
            wal_log: Mutex::new(Vec::new()),
        }
    }

    /// Register a participant.
    pub fn register_participant(&self, participant: Arc<dyn Participant>) {
        let id = participant.id();
        self.participants.insert(id, participant);
    }

    /// Begin a distributed transaction.
    pub fn begin(&self, participant_ids: Vec<ParticipantId>) -> Result<TxnId> {
        if self.transactions.len() >= self.config.max_concurrent_txns {
            return Err(Error::ResourceExhausted(
                "Too many concurrent distributed transactions".into(),
            ));
        }

        // Verify all participants exist
        for pid in &participant_ids {
            if !self.participants.contains_key(pid) {
                return Err(Error::NotFound("Participant".into(), pid.to_string()));
            }
        }

        let txn_id = TxnId(self.next_txn_id.fetch_add(1, Ordering::SeqCst));
        let txn = DistributedTxn::new(txn_id, participant_ids.clone(), self.config.prepare_timeout);

        // WAL: Begin record
        self.wal_log.lock().push(WalRecord::Begin {
            txn_id,
            participants: participant_ids,
        });

        self.transactions.insert(txn_id, RwLock::new(txn));
        self.stats.lock().txns_started += 1;

        Ok(txn_id)
    }

    /// Execute 2PC commit protocol.
    pub async fn commit(&self, txn_id: TxnId) -> Result<bool> {
        // Phase 1: Prepare
        let prepare_result = self.prepare_phase(txn_id).await?;

        if !prepare_result {
            // Abort if any participant voted no
            self.abort_phase(txn_id).await?;
            return Ok(false);
        }

        // WAL: Prepared record (commit decision is now durable)
        self.wal_log.lock().push(WalRecord::Prepared { txn_id });

        // Phase 2: Commit
        self.commit_phase(txn_id).await?;

        Ok(true)
    }

    /// Prepare phase: Ask all participants to prepare.
    async fn prepare_phase(&self, txn_id: TxnId) -> Result<bool> {
        // Update state to Preparing
        {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let mut txn = txn_ref.write();
            txn.state = DistributedTxnState::Preparing;
        }

        // Get participants for this transaction
        let participant_ids: Vec<ParticipantId> = {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let txn = txn_ref.read();
            txn.participants.keys().copied().collect()
        };

        // Send prepare requests to all participants concurrently
        let mut handles = Vec::new();

        for pid in participant_ids {
            let participant = self.participants.get(&pid).map(|p| p.clone());
            if let Some(participant) = participant {
                let txn_id = txn_id;
                handles.push(tokio::spawn(async move {
                    let vote = participant.prepare(txn_id).await;
                    (pid, vote)
                }));
            }
        }

        // Collect votes
        let timeout = self.config.prepare_timeout;
        let deadline = Instant::now() + timeout;

        for handle in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok((pid, Ok(vote)))) => {
                    let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                        Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
                    })?;
                    let mut txn = txn_ref.write();

                    match vote {
                        PrepareVote::Yes => {
                            txn.participants.insert(pid, ParticipantState::Prepared);
                        }
                        PrepareVote::No => {
                            txn.participants.insert(pid, ParticipantState::Aborted);
                        }
                    }
                }
                Ok(Ok((pid, Err(_)))) => {
                    // Participant error - treat as abort
                    let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                        Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
                    })?;
                    let mut txn = txn_ref.write();
                    txn.participants.insert(pid, ParticipantState::Aborted);
                    self.stats.lock().prepare_timeouts += 1;
                }
                Ok(Err(_)) | Err(_) => {
                    // Task error or timeout - ignore for now
                    self.stats.lock().prepare_timeouts += 1;
                }
            }
        }

        // Check if all prepared
        let all_prepared = {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let mut txn = txn_ref.write();

            if txn.all_prepared() {
                txn.state = DistributedTxnState::Prepared;
                true
            } else {
                txn.state = DistributedTxnState::Aborting;
                false
            }
        };

        Ok(all_prepared)
    }

    /// Commit phase: Tell all participants to commit.
    async fn commit_phase(&self, txn_id: TxnId) -> Result<()> {
        // Update state
        {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let mut txn = txn_ref.write();
            txn.state = DistributedTxnState::Committing;
        }

        // WAL: Commit record
        self.wal_log.lock().push(WalRecord::Commit { txn_id });

        // Get participants
        let participant_ids: Vec<ParticipantId> = {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let txn = txn_ref.read();
            txn.participants.keys().copied().collect()
        };

        // Send commit to all participants
        let mut handles = Vec::new();

        for pid in participant_ids {
            let participant = self.participants.get(&pid).map(|p| p.clone());
            if let Some(participant) = participant {
                let txn_id = txn_id;
                handles.push(tokio::spawn(async move {
                    let result = participant.commit(txn_id).await;
                    (pid, result)
                }));
            }
        }

        // Wait for acknowledgments (with retries if needed)
        let timeout = self.config.commit_timeout;
        let deadline = Instant::now() + timeout;

        for handle in handles {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok((pid, Ok(())))) => {
                    if let Some(txn_ref) = self.transactions.get(&txn_id) {
                        let mut txn = txn_ref.write();
                        txn.participants.insert(pid, ParticipantState::Committed);
                    }
                }
                Ok(Ok((_, Err(_)))) => {
                    // Participant error, would retry in production
                    self.stats.lock().commit_timeouts += 1;
                }
                Ok(Err(_)) | Err(_) => {
                    // Task error or timeout
                    self.stats.lock().commit_timeouts += 1;
                }
            }
        }

        // Mark as committed
        {
            if let Some(txn_ref) = self.transactions.get(&txn_id) {
                let mut txn = txn_ref.write();
                txn.state = DistributedTxnState::Committed;
            }
        }

        // Record commit
        self.committed_txns.insert(txn_id, Instant::now());
        self.stats.lock().txns_committed += 1;

        // WAL: End record
        self.wal_log.lock().push(WalRecord::End { txn_id });

        // Clean up
        self.transactions.remove(&txn_id);

        Ok(())
    }

    /// Abort phase: Tell all participants to abort.
    async fn abort_phase(&self, txn_id: TxnId) -> Result<()> {
        // Update state
        {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let mut txn = txn_ref.write();
            txn.state = DistributedTxnState::Aborting;
        }

        // WAL: Abort record
        self.wal_log.lock().push(WalRecord::Abort { txn_id });

        // Get participants
        let participant_ids: Vec<ParticipantId> = {
            let txn_ref = self.transactions.get(&txn_id).ok_or_else(|| {
                Error::NotFound("Distributed transaction".into(), txn_id.0.to_string())
            })?;
            let txn = txn_ref.read();
            txn.participants.keys().copied().collect()
        };

        // Send abort to all participants
        let mut handles = Vec::new();

        for pid in participant_ids {
            let participant = self.participants.get(&pid).map(|p| p.clone());
            if let Some(participant) = participant {
                let txn_id = txn_id;
                handles.push(tokio::spawn(async move {
                    let result = participant.abort(txn_id).await;
                    (pid, result)
                }));
            }
        }

        // Wait for acknowledgments
        for handle in handles {
            match handle.await {
                Ok((pid, Ok(()))) => {
                    if let Some(txn_ref) = self.transactions.get(&txn_id) {
                        let mut txn = txn_ref.write();
                        txn.participants.insert(pid, ParticipantState::AbortAcked);
                    }
                }
                _ => {
                    // Ignore errors during abort
                }
            }
        }

        // Mark as aborted
        {
            if let Some(txn_ref) = self.transactions.get(&txn_id) {
                let mut txn = txn_ref.write();
                txn.state = DistributedTxnState::Aborted;
            }
        }

        self.stats.lock().txns_aborted += 1;

        // WAL: End record
        self.wal_log.lock().push(WalRecord::End { txn_id });

        // Clean up
        self.transactions.remove(&txn_id);

        Ok(())
    }

    /// Abort a transaction explicitly.
    pub async fn abort(&self, txn_id: TxnId) -> Result<()> {
        self.abort_phase(txn_id).await
    }

    /// Get transaction state.
    pub fn get_state(&self, txn_id: TxnId) -> Option<DistributedTxnState> {
        if let Some(txn_ref) = self.transactions.get(&txn_id) {
            return Some(txn_ref.read().state);
        }
        if self.committed_txns.contains_key(&txn_id) {
            return Some(DistributedTxnState::Committed);
        }
        if self.config.presumed_abort {
            // Presumed abort: unknown transactions are assumed aborted
            return Some(DistributedTxnState::Aborted);
        }
        None
    }

    /// Get statistics.
    pub fn stats(&self) -> CoordinatorStats {
        self.stats.lock().clone()
    }

    /// Get active transaction count.
    pub fn active_txn_count(&self) -> usize {
        self.transactions.len()
    }

    /// Recover in-doubt transactions after restart.
    pub async fn recover(&self) -> Result<usize> {
        let mut recovered = 0;

        // In a real implementation, this would read from persistent WAL
        // and re-drive any in-doubt transactions

        // For transactions in Prepared state, complete the commit
        // For transactions in Preparing state, abort them

        let txn_ids: Vec<TxnId> = self.transactions.iter().map(|e| *e.key()).collect();

        for txn_id in txn_ids {
            let state = {
                self.transactions
                    .get(&txn_id)
                    .map(|t| t.read().state)
            };

            match state {
                Some(DistributedTxnState::Prepared) => {
                    // Complete the commit
                    self.commit_phase(txn_id).await?;
                    recovered += 1;
                }
                Some(DistributedTxnState::Preparing | DistributedTxnState::Active) => {
                    // Abort
                    self.abort_phase(txn_id).await?;
                    recovered += 1;
                }
                _ => {}
            }
        }

        self.stats.lock().recovered_txns += recovered as u64;

        Ok(recovered)
    }

    /// Clean up committed transaction records older than the given duration.
    pub fn gc(&self, older_than: Duration) {
        let now = Instant::now();
        self.committed_txns
            .retain(|_, time| now.duration_since(*time) < older_than);
    }
}

/// Mock participant for testing.
#[cfg(test)]
pub struct MockParticipant {
    id: ParticipantId,
    vote: PrepareVote,
    fail_commit: bool,
}

#[cfg(test)]
impl MockParticipant {
    pub fn new(id: ParticipantId, vote: PrepareVote) -> Self {
        Self {
            id,
            vote,
            fail_commit: false,
        }
    }

    pub fn with_fail_commit(mut self, fail: bool) -> Self {
        self.fail_commit = fail;
        self
    }
}

#[cfg(test)]
#[async_trait]
impl Participant for MockParticipant {
    fn id(&self) -> ParticipantId {
        self.id
    }

    async fn prepare(&self, _txn_id: TxnId) -> Result<PrepareVote> {
        Ok(self.vote)
    }

    async fn commit(&self, _txn_id: TxnId) -> Result<()> {
        if self.fail_commit {
            Err(Error::Internal("Simulated commit failure".into()))
        } else {
            Ok(())
        }
    }

    async fn abort(&self, _txn_id: TxnId) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_2pc_commit_success() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        // Register participants that vote yes
        let p1 = Arc::new(MockParticipant::new(1, PrepareVote::Yes));
        let p2 = Arc::new(MockParticipant::new(2, PrepareVote::Yes));

        coordinator.register_participant(p1);
        coordinator.register_participant(p2);

        // Begin transaction
        let txn_id = coordinator.begin(vec![1, 2]).unwrap();

        // Commit should succeed
        let result = coordinator.commit(txn_id).await.unwrap();
        assert!(result);

        // Transaction should be committed
        assert_eq!(
            coordinator.get_state(txn_id),
            Some(DistributedTxnState::Committed)
        );
    }

    #[tokio::test]
    async fn test_2pc_abort_on_no_vote() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        // One participant votes no
        let p1 = Arc::new(MockParticipant::new(1, PrepareVote::Yes));
        let p2 = Arc::new(MockParticipant::new(2, PrepareVote::No));

        coordinator.register_participant(p1);
        coordinator.register_participant(p2);

        let txn_id = coordinator.begin(vec![1, 2]).unwrap();

        // Commit should fail
        let result = coordinator.commit(txn_id).await.unwrap();
        assert!(!result);

        // With presumed abort, unknown transactions are considered aborted
        assert_eq!(
            coordinator.get_state(txn_id),
            Some(DistributedTxnState::Aborted)
        );
    }

    #[tokio::test]
    async fn test_begin_abort() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        let p1 = Arc::new(MockParticipant::new(1, PrepareVote::Yes));
        coordinator.register_participant(p1);

        let txn_id = coordinator.begin(vec![1]).unwrap();

        // Explicitly abort
        coordinator.abort(txn_id).await.unwrap();

        assert_eq!(
            coordinator.get_state(txn_id),
            Some(DistributedTxnState::Aborted)
        );
    }

    #[tokio::test]
    async fn test_participant_not_found() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        // Try to begin with non-existent participant
        let result = coordinator.begin(vec![999]);
        assert!(result.is_err());
    }

    #[test]
    fn test_stats() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        let p1 = Arc::new(MockParticipant::new(1, PrepareVote::Yes));
        coordinator.register_participant(p1);

        let _txn_id = coordinator.begin(vec![1]).unwrap();

        let stats = coordinator.stats();
        assert_eq!(stats.txns_started, 1);
    }

    #[tokio::test]
    async fn test_multiple_transactions() {
        let coordinator = TwoPhaseCoordinator::new(CoordinatorConfig::default());

        let p1 = Arc::new(MockParticipant::new(1, PrepareVote::Yes));
        let p2 = Arc::new(MockParticipant::new(2, PrepareVote::Yes));

        coordinator.register_participant(p1);
        coordinator.register_participant(p2);

        // Start multiple transactions
        let txn1 = coordinator.begin(vec![1]).unwrap();
        let txn2 = coordinator.begin(vec![2]).unwrap();
        let txn3 = coordinator.begin(vec![1, 2]).unwrap();

        assert_eq!(coordinator.active_txn_count(), 3);

        // Commit all
        coordinator.commit(txn1).await.unwrap();
        coordinator.commit(txn2).await.unwrap();
        coordinator.commit(txn3).await.unwrap();

        assert_eq!(coordinator.active_txn_count(), 0);

        let stats = coordinator.stats();
        assert_eq!(stats.txns_committed, 3);
    }

    #[test]
    fn test_presumed_abort() {
        let config = CoordinatorConfig {
            presumed_abort: true,
            ..Default::default()
        };
        let coordinator = TwoPhaseCoordinator::new(config);

        // Unknown transaction is assumed aborted
        let state = coordinator.get_state(TxnId(999));
        assert_eq!(state, Some(DistributedTxnState::Aborted));
    }

    #[test]
    fn test_config_defaults() {
        let config = CoordinatorConfig::default();
        assert_eq!(config.prepare_timeout, Duration::from_secs(30));
        assert_eq!(config.commit_timeout, Duration::from_secs(60));
        assert!(config.presumed_abort);
    }
}
