//! Raft Node
//!
//! Wraps the raft crate's RawNode to provide a higher-level interface
//! for managing Raft consensus within ThunderDB.

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use raft::eraftpb::{ConfChange, Entry, EntryType, Message as RaftEraftMessage, Snapshot};
use raft::prelude::*;
use raft::{Config as RaftConfig, RawNode, StateRole, Storage};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thunder_common::prelude::*;

use crate::{Command, RaftMessage, RaftMessageType};

/// Raft node configuration
#[derive(Debug, Clone)]
pub struct RaftNodeConfig {
    /// Node ID (must be unique in cluster)
    pub id: u64,
    /// Election timeout ticks
    pub election_tick: usize,
    /// Heartbeat timeout ticks
    pub heartbeat_tick: usize,
    /// Maximum size of entries in a single append message
    pub max_size_per_msg: u64,
    /// Maximum number of inflight append messages
    pub max_inflight_msgs: usize,
    /// Tick interval
    pub tick_interval: Duration,
    /// Whether to use pre-vote
    pub pre_vote: bool,
    /// Snapshot count threshold for compaction
    pub snapshot_count: u64,
}

impl Default for RaftNodeConfig {
    fn default() -> Self {
        Self {
            id: 0,
            election_tick: 10,
            heartbeat_tick: 3,
            max_size_per_msg: 1024 * 1024, // 1MB
            max_inflight_msgs: 256,
            tick_interval: Duration::from_millis(100),
            pre_vote: true,
            snapshot_count: 10000,
        }
    }
}

/// Log entry for the Raft log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Entry index
    pub index: u64,
    /// Term when entry was received
    pub term: u64,
    /// Entry type
    pub entry_type: LogEntryType,
    /// Entry data
    pub data: Vec<u8>,
}

/// Type of log entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEntryType {
    /// Normal command entry
    Normal,
    /// Configuration change
    ConfigChange,
}

/// In-memory storage for Raft log
pub struct MemoryStorage {
    /// Hard state (term, vote, commit)
    hard_state: RwLock<raft::prelude::HardState>,
    /// Conf state (voters, learners)
    conf_state: RwLock<raft::prelude::ConfState>,
    /// Snapshot
    snapshot: RwLock<Snapshot>,
    /// Log entries
    entries: RwLock<Vec<Entry>>,
    /// First index in log
    first_index: AtomicU64,
}

impl MemoryStorage {
    pub fn new() -> Self {
        // Create dummy entry at index 0 (raft convention)
        let mut dummy = Entry::default();
        dummy.index = 0;
        dummy.term = 0;

        Self {
            hard_state: RwLock::new(raft::prelude::HardState::default()),
            conf_state: RwLock::new(raft::prelude::ConfState::default()),
            snapshot: RwLock::new(Snapshot::default()),
            entries: RwLock::new(vec![dummy]), // Dummy entry at index 0
            first_index: AtomicU64::new(1),
        }
    }

    pub fn new_with_conf_state(voters: Vec<u64>) -> Self {
        let storage = Self::new();
        {
            let mut conf_state = storage.conf_state.write();
            conf_state.voters = voters;
        }
        storage
    }

    /// Set hard state
    pub fn set_hard_state(&self, hs: raft::prelude::HardState) {
        *self.hard_state.write() = hs;
    }

    /// Append entries to the log
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut log = self.entries.write();
        let first_index = self.first_index.load(Ordering::SeqCst);
        let last_index = log.len() as u64 + first_index - 1;

        let first_new = entries[0].index;

        if first_new < first_index {
            // Truncate compacted entries
            return Err(Error::Internal("Entry compacted".to_string()));
        }

        if first_new > last_index + 1 {
            return Err(Error::Internal(format!(
                "Missing entries [{}, {})",
                last_index + 1,
                first_new
            )));
        }

        // Find offset to append
        let offset = (first_new - first_index) as usize;
        if offset < log.len() {
            log.truncate(offset);
        }

        log.extend_from_slice(entries);
        Ok(())
    }

    /// Create a snapshot
    pub fn create_snapshot(&self, index: u64, conf_state: raft::prelude::ConfState, data: Vec<u8>) -> Result<()> {
        let mut snapshot = self.snapshot.write();

        if index <= snapshot.get_metadata().index {
            return Err(Error::Internal("Snapshot index too old".to_string()));
        }

        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = {
            let log = self.entries.read();
            let first_index = self.first_index.load(Ordering::SeqCst);
            let offset = (index - first_index) as usize;
            if offset < log.len() {
                log[offset].term
            } else {
                0
            }
        };
        snapshot.mut_metadata().mut_conf_state().clone_from(&conf_state);
        snapshot.data = data.into();

        Ok(())
    }

    /// Compact log up to index
    pub fn compact(&self, index: u64) -> Result<()> {
        let mut log = self.entries.write();
        let first_index = self.first_index.load(Ordering::SeqCst);

        if index < first_index {
            return Err(Error::Internal("Index already compacted".to_string()));
        }

        let offset = (index - first_index) as usize;
        if offset >= log.len() {
            return Err(Error::Internal("Index out of range".to_string()));
        }

        // Keep entries from index onwards
        log.drain(0..offset);
        self.first_index.store(index, Ordering::SeqCst);

        Ok(())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.read().clone(),
            conf_state: self.conf_state.read().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let max_size = max_size.into().unwrap_or(u64::MAX);
        let log = self.entries.read();
        let first_index = self.first_index.load(Ordering::SeqCst);

        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let last_index = first_index + log.len() as u64 - 1;
        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let offset = (low - first_index) as usize;
        let end = (high - first_index) as usize;

        let mut entries = Vec::new();
        let mut size = 0u64;

        for entry in &log[offset..end] {
            // Estimate entry size: index(8) + term(8) + type(4) + data_len + context_len
            let entry_size = 20 + entry.data.len() as u64 + entry.context.len() as u64;
            if size + entry_size > max_size && !entries.is_empty() {
                break;
            }
            entries.push(entry.clone());
            size += entry_size;
        }

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let log = self.entries.read();
        let first_index = self.first_index.load(Ordering::SeqCst);

        if idx < first_index {
            let snapshot = self.snapshot.read();
            if idx == snapshot.get_metadata().index {
                return Ok(snapshot.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        let offset = (idx - first_index) as usize;
        if offset >= log.len() {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        Ok(log[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index.load(Ordering::SeqCst))
    }

    fn last_index(&self) -> raft::Result<u64> {
        let log = self.entries.read();
        // Return the actual index of the last entry
        Ok(log.last().map(|e| e.index).unwrap_or(0))
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        Ok(self.snapshot.read().clone())
    }
}

/// Proposal waiting for response
struct PendingProposal {
    /// Proposal ID
    id: u64,
    /// Command data
    data: Vec<u8>,
    /// Time when proposal was made
    proposed_at: Instant,
    /// Oneshot sender for result
    tx: Option<tokio::sync::oneshot::Sender<Result<()>>>,
}

/// Raft node wrapper
pub struct RaftNode<S: Storage> {
    /// Region ID this node belongs to
    region_id: RegionId,
    /// Node configuration
    config: RaftNodeConfig,
    /// The underlying raft node
    raw_node: Mutex<RawNode<S>>,
    /// Pending proposals
    pending_proposals: DashMap<u64, PendingProposal>,
    /// Next proposal ID
    next_proposal_id: AtomicU64,
    /// Applied index
    applied_index: AtomicU64,
    /// Last tick time
    last_tick: RwLock<Instant>,
    /// Leader ID
    leader_id: AtomicU64,
    /// Callbacks for applied entries
    apply_callbacks: RwLock<Vec<Box<dyn Fn(&Entry) + Send + Sync>>>,
}

impl<S: Storage> RaftNode<S> {
    /// Create a new Raft node
    pub fn new(region_id: RegionId, config: RaftNodeConfig, storage: S) -> Result<Self> {
        let raft_config = RaftConfig {
            id: config.id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            max_size_per_msg: config.max_size_per_msg,
            max_inflight_msgs: config.max_inflight_msgs,
            pre_vote: config.pre_vote,
            ..Default::default()
        };

        let raw_node = RawNode::new(&raft_config, storage, &raft::default_logger())
            .map_err(|e| Error::Internal(format!("Failed to create RawNode: {}", e)))?;

        Ok(Self {
            region_id,
            config,
            raw_node: Mutex::new(raw_node),
            pending_proposals: DashMap::new(),
            next_proposal_id: AtomicU64::new(1),
            applied_index: AtomicU64::new(0),
            last_tick: RwLock::new(Instant::now()),
            leader_id: AtomicU64::new(0),
            apply_callbacks: RwLock::new(Vec::new()),
        })
    }

    /// Get the region ID
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get the node ID
    pub fn node_id(&self) -> u64 {
        self.config.id
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        let raw_node = self.raw_node.lock();
        raw_node.raft.state == StateRole::Leader
    }

    /// Get the current leader ID
    pub fn leader_id(&self) -> Option<u64> {
        let id = self.leader_id.load(Ordering::SeqCst);
        if id == 0 {
            None
        } else {
            Some(id)
        }
    }

    /// Get the current term
    pub fn term(&self) -> u64 {
        let raw_node = self.raw_node.lock();
        raw_node.raft.term
    }

    /// Get the applied index
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    /// Tick the Raft node
    pub fn tick(&self) -> bool {
        let mut raw_node = self.raw_node.lock();
        raw_node.tick();
        *self.last_tick.write() = Instant::now();
        raw_node.has_ready()
    }

    /// Propose a command
    pub fn propose(&self, cmd: Command) -> Result<u64> {
        let data = bincode::serialize(&cmd)
            .map_err(|e| Error::Internal(format!("Serialize error: {}", e)))?;

        let proposal_id = self.next_proposal_id.fetch_add(1, Ordering::SeqCst);

        // Add context with proposal ID
        let mut context = Vec::with_capacity(8);
        context.extend_from_slice(&proposal_id.to_le_bytes());

        let mut raw_node = self.raw_node.lock();
        raw_node
            .propose(context, data.clone())
            .map_err(|e| Error::Internal(format!("Propose error: {}", e)))?;

        // Store pending proposal
        self.pending_proposals.insert(
            proposal_id,
            PendingProposal {
                id: proposal_id,
                data,
                proposed_at: Instant::now(),
                tx: None,
            },
        );

        Ok(proposal_id)
    }

    /// Propose a command and wait for result
    pub async fn propose_and_wait(&self, cmd: Command, timeout: Duration) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let data = bincode::serialize(&cmd)
            .map_err(|e| Error::Internal(format!("Serialize error: {}", e)))?;

        let proposal_id = self.next_proposal_id.fetch_add(1, Ordering::SeqCst);

        let mut context = Vec::with_capacity(8);
        context.extend_from_slice(&proposal_id.to_le_bytes());

        {
            let mut raw_node = self.raw_node.lock();
            raw_node
                .propose(context, data.clone())
                .map_err(|e| Error::Internal(format!("Propose error: {}", e)))?;
        }

        self.pending_proposals.insert(
            proposal_id,
            PendingProposal {
                id: proposal_id,
                data,
                proposed_at: Instant::now(),
                tx: Some(tx),
            },
        );

        // Wait for result with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Internal("Proposal cancelled".to_string())),
            Err(_) => {
                self.pending_proposals.remove(&proposal_id);
                Err(Error::Internal("Proposal timeout".to_string()))
            }
        }
    }

    /// Propose a configuration change
    pub fn propose_conf_change(&self, change: ConfChange) -> Result<()> {
        let mut raw_node = self.raw_node.lock();
        raw_node
            .propose_conf_change(vec![], change)
            .map_err(|e| Error::Internal(format!("Propose conf change error: {}", e)))?;
        Ok(())
    }

    /// Step the Raft state machine with a message
    pub fn step(&self, msg: RaftEraftMessage) -> Result<()> {
        let mut raw_node = self.raw_node.lock();
        raw_node
            .step(msg)
            .map_err(|e| Error::Internal(format!("Step error: {}", e)))?;
        Ok(())
    }

    /// Check if the node has ready state
    pub fn has_ready(&self) -> bool {
        let raw_node = self.raw_node.lock();
        raw_node.has_ready()
    }

    /// Get ready state and process it
    pub fn ready(&self) -> Ready {
        let mut raw_node = self.raw_node.lock();
        raw_node.ready()
    }

    /// Advance the Raft state after processing ready
    pub fn advance(&self, ready: Ready) {
        let mut raw_node = self.raw_node.lock();

        // Update leader ID
        if let Some(ss) = ready.ss() {
            self.leader_id.store(ss.leader_id, Ordering::SeqCst);
        }

        let mut light_ready = raw_node.advance(ready);

        // Commit entries
        if let Some(commit) = light_ready.commit_index() {
            // Update applied index
            self.applied_index.store(commit, Ordering::SeqCst);
        }

        raw_node.advance_apply();
    }

    /// Apply committed entries
    pub fn apply_entries(&self, entries: &[Entry]) -> Result<Vec<(u64, Command)>> {
        let mut applied = Vec::new();

        for entry in entries {
            if entry.data.is_empty() {
                // Empty entry, likely a leader election
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let cmd: Command = bincode::deserialize(&entry.data)
                        .map_err(|e| Error::Internal(format!("Deserialize error: {}", e)))?;

                    applied.push((entry.index, cmd));

                    // Complete pending proposal if exists
                    if entry.context.len() >= 8 {
                        let proposal_id = u64::from_le_bytes(
                            entry.context[0..8].try_into().unwrap()
                        );
                        if let Some((_, mut proposal)) = self.pending_proposals.remove(&proposal_id) {
                            if let Some(tx) = proposal.tx.take() {
                                let _ = tx.send(Ok(()));
                            }
                        }
                    }
                }
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    // Handle configuration change
                    tracing::info!("Applying config change at index {}", entry.index);
                }
            }

            // Update applied index
            self.applied_index.store(entry.index, Ordering::SeqCst);
        }

        Ok(applied)
    }

    /// Register an apply callback
    pub fn on_apply<F>(&self, callback: F)
    where
        F: Fn(&Entry) + Send + Sync + 'static,
    {
        self.apply_callbacks.write().push(Box::new(callback));
    }

    /// Get Raft status
    pub fn status(&self) -> RaftStatus {
        let raw_node = self.raw_node.lock();
        let raft = &raw_node.raft;

        RaftStatus {
            id: self.config.id,
            term: raft.term,
            vote: raft.vote,
            commit: raft.raft_log.committed,
            applied: self.applied_index.load(Ordering::SeqCst),
            state: match raft.state {
                StateRole::Follower => NodeState::Follower,
                StateRole::Candidate => NodeState::Candidate,
                StateRole::Leader => NodeState::Leader,
                StateRole::PreCandidate => NodeState::PreCandidate,
            },
            leader_id: if raft.leader_id == 0 {
                None
            } else {
                Some(raft.leader_id)
            },
        }
    }

    /// Campaign for leadership (used for initial bootstrap)
    pub fn campaign(&self) -> Result<()> {
        let mut raw_node = self.raw_node.lock();
        raw_node
            .campaign()
            .map_err(|e| Error::Internal(format!("Campaign error: {}", e)))?;
        Ok(())
    }

    /// Transfer leadership to another node
    pub fn transfer_leader(&self, target: u64) {
        let mut raw_node = self.raw_node.lock();
        raw_node.transfer_leader(target);
    }

    /// Clean up expired proposals
    pub fn cleanup_proposals(&self, timeout: Duration) {
        let now = Instant::now();
        let expired: Vec<u64> = self
            .pending_proposals
            .iter()
            .filter(|r| now.duration_since(r.proposed_at) > timeout)
            .map(|r| *r.key())
            .collect();

        for id in expired {
            if let Some((_, mut proposal)) = self.pending_proposals.remove(&id) {
                if let Some(tx) = proposal.tx.take() {
                    let _ = tx.send(Err(Error::Internal("Proposal expired".to_string())));
                }
            }
        }
    }
}

/// Raft node state (renamed to avoid conflict with raft::RaftState)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

/// Raft status information
#[derive(Debug, Clone)]
pub struct RaftStatus {
    pub id: u64,
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
    pub applied: u64,
    pub state: NodeState,
    pub leader_id: Option<u64>,
}

/// Convert our RaftMessage to raft crate's Message
pub fn to_raft_message(msg: &RaftMessage) -> RaftEraftMessage {
    let mut m = RaftEraftMessage::default();
    m.from = msg.from.0;
    m.to = msg.to.0;
    m.term = msg.term;

    m.set_msg_type(match msg.msg_type {
        RaftMessageType::RequestVote => raft::eraftpb::MessageType::MsgRequestVote,
        RaftMessageType::RequestVoteResponse => raft::eraftpb::MessageType::MsgRequestVoteResponse,
        RaftMessageType::AppendEntries => raft::eraftpb::MessageType::MsgAppend,
        RaftMessageType::AppendEntriesResponse => raft::eraftpb::MessageType::MsgAppendResponse,
        RaftMessageType::Heartbeat => raft::eraftpb::MessageType::MsgHeartbeat,
        RaftMessageType::HeartbeatResponse => raft::eraftpb::MessageType::MsgHeartbeatResponse,
        RaftMessageType::Snapshot => raft::eraftpb::MessageType::MsgSnapshot,
    });

    m
}

/// Convert raft crate's Message to our RaftMessage
pub fn from_raft_message(region_id: RegionId, m: &RaftEraftMessage) -> RaftMessage {
    use raft::eraftpb::MessageType;

    RaftMessage {
        region_id,
        from: NodeId(m.from),
        to: NodeId(m.to),
        msg_type: match m.get_msg_type() {
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => RaftMessageType::RequestVote,
            MessageType::MsgRequestVoteResponse | MessageType::MsgRequestPreVoteResponse => {
                RaftMessageType::RequestVoteResponse
            }
            MessageType::MsgAppend => RaftMessageType::AppendEntries,
            MessageType::MsgAppendResponse => RaftMessageType::AppendEntriesResponse,
            MessageType::MsgHeartbeat => RaftMessageType::Heartbeat,
            MessageType::MsgHeartbeatResponse => RaftMessageType::HeartbeatResponse,
            MessageType::MsgSnapshot => RaftMessageType::Snapshot,
            _ => RaftMessageType::Heartbeat, // Default
        },
        term: m.term,
        // Store key message fields as payload (index, log_term, commit, entries count)
        payload: {
            let mut payload = Vec::with_capacity(32);
            payload.extend_from_slice(&m.index.to_le_bytes());
            payload.extend_from_slice(&m.log_term.to_le_bytes());
            payload.extend_from_slice(&m.commit.to_le_bytes());
            payload.extend_from_slice(&(m.entries.len() as u64).to_le_bytes());
            payload
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node(id: u64, peers: Vec<u64>) -> RaftNode<MemoryStorage> {
        let config = RaftNodeConfig {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };

        let storage = MemoryStorage::new_with_conf_state(peers);
        RaftNode::new(RegionId(1), config, storage).unwrap()
    }

    #[test]
    fn test_memory_storage() {
        let storage = MemoryStorage::new_with_conf_state(vec![1, 2, 3]);

        // Check initial state
        let state = storage.initial_state().unwrap();
        assert_eq!(state.conf_state.voters, vec![1, 2, 3]);

        // Check indices (note: we have a dummy entry at index 0)
        assert_eq!(storage.first_index().unwrap(), 1);
        // Initially last_index is 0 because we just have the dummy entry at index 0
        assert_eq!(storage.last_index().unwrap(), 0);

        // Append entries
        let mut entry = Entry::default();
        entry.index = 1;
        entry.term = 1;
        entry.data = vec![1, 2, 3].into();
        storage.append(&[entry]).unwrap();

        // After appending, last_index should be 1
        assert_eq!(storage.last_index().unwrap(), 1);
        assert_eq!(storage.term(1).unwrap(), 1);
    }

    #[test]
    fn test_raft_node_creation() {
        let node = create_test_node(1, vec![1, 2, 3]);

        assert_eq!(node.node_id(), 1);
        assert_eq!(node.region_id(), RegionId(1));
        assert!(!node.is_leader());
    }

    #[test]
    fn test_raft_node_tick() {
        let node = create_test_node(1, vec![1]);

        // Tick the node
        node.tick();

        let status = node.status();
        assert_eq!(status.id, 1);
    }

    #[test]
    fn test_raft_status() {
        let node = create_test_node(1, vec![1, 2, 3]);

        let status = node.status();
        assert_eq!(status.id, 1);
        assert_eq!(status.state, NodeState::Follower);
        assert_eq!(status.leader_id, None);
    }

    #[test]
    fn test_message_conversion() {
        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(2),
            msg_type: RaftMessageType::Heartbeat,
            term: 5,
            payload: vec![],
        };

        let raft_msg = to_raft_message(&msg);
        assert_eq!(raft_msg.from, 1);
        assert_eq!(raft_msg.to, 2);
        assert_eq!(raft_msg.term, 5);

        let back = from_raft_message(RegionId(1), &raft_msg);
        assert_eq!(back.from, NodeId(1));
        assert_eq!(back.to, NodeId(2));
        assert_eq!(back.term, 5);
    }

    #[tokio::test]
    async fn test_propose_command() {
        let node = create_test_node(1, vec![1]);

        // Bootstrap as single-node cluster
        node.campaign().unwrap();

        // Tick multiple times to become leader
        for _ in 0..20 {
            node.tick();
            if node.has_ready() {
                let ready = node.ready();
                node.advance(ready);
            }
        }

        // Now propose a command
        let cmd = Command::Put {
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
            txn_id: None,
        };

        let proposal_id = node.propose(cmd).unwrap();
        assert!(proposal_id > 0);
    }

    #[test]
    fn test_storage_compact() {
        let storage = MemoryStorage::new();

        // Append some entries
        for i in 1..=10 {
            let mut entry = Entry::default();
            entry.index = i;
            entry.term = 1;
            storage.append(&[entry]).unwrap();
        }

        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 10);

        // Compact up to index 5
        storage.compact(5).unwrap();

        assert_eq!(storage.first_index().unwrap(), 5);
        assert_eq!(storage.last_index().unwrap(), 10);

        // Old entries should be compacted
        assert!(storage.term(4).is_err());
        assert_eq!(storage.term(5).unwrap(), 1);
    }

    #[test]
    fn test_log_entry_serialization() {
        let entry = LogEntry {
            index: 100,
            term: 5,
            entry_type: LogEntryType::Normal,
            data: vec![1, 2, 3, 4],
        };

        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: LogEntry = bincode::deserialize(&serialized).unwrap();

        assert_eq!(entry.index, deserialized.index);
        assert_eq!(entry.term, deserialized.term);
        assert_eq!(entry.data, deserialized.data);
    }
}
