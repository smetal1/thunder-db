//! # Thunder Cluster
//!
//! Distributed cluster management for ThunderDB providing:
//! - Raft consensus for replication
//! - Region-based sharding
//! - Automatic rebalancing
//! - Distributed transaction coordination
//! - Gossip-based peer discovery (Cassandra-style)

pub mod catalog_sync;
pub mod coordinator;
pub mod deadlock;
pub mod generated;
pub mod gossip;
pub mod membership;
pub mod raft_node;
pub mod region;
pub mod replication;
pub mod scatter_gather;
pub mod topology;
pub mod transport;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thunder_common::prelude::*;

// Re-exports from coordinator
pub use coordinator::{ClusterCoordinator, CoordinatorConfig, ClusterState, Scheduler, RebalanceMove};

// Re-exports from membership
pub use membership::{
    HealthChecker, HealthStatus, Membership, MembershipConfig, MembershipDigest, MembershipEvent,
    NodeDigest, NodeMeta, NodeSelector,
};

// Re-exports from raft_node
pub use raft_node::{
    from_raft_message, to_raft_message, LogEntry, LogEntryType, MemoryStorage, NodeState,
    RaftNode, RaftNodeConfig, RaftStatus,
};

// Re-exports from region
pub use region::{
    Peer, Region, RegionAction, RegionConfig, RegionEpoch, RegionManager, RegionMeta, RegionState,
    RegionStats, SplitFinder,
};

// Re-exports from transport
pub use transport::{
    ClusterServer, ClusterTransport, ConnectionState, GrpcTransport, InMemoryTransport,
    MessageBatch, MessageRouter, NetworkTransport, PeerConnection, TransportConfig,
};

// Re-exports from gossip
pub use gossip::{
    AppStateKey, EndpointState, EndpointStateUpdate, GossipConfig, GossipDigest,
    GossipDigestEntry, GossipEvent, GossipMessage, Gossiper, HeartbeatState,
    PhiAccrualDetector, VersionedValue,
};

// Re-exports from replication
pub use replication::{
    ReplicationMessage, ReplicationReceiver, ReplicationStatus, ReplicatorConfig,
    WalReplicator, WalShipmentEntry,
};

// Re-exports from topology
pub use topology::{GridTopology, HashRing, ShardConfig, TableShard, TableShardMap};

// Re-exports from catalog_sync
pub use catalog_sync::{
    DistributedCatalog, DistributedTableEntry, Distribution, ForwardedQueryResult, QueryExecutor,
};

// Re-exports from scatter_gather
pub use scatter_gather::{DistributionPlan, MergedResult, PartialResult, ScatterGatherCoordinator};

// Re-exports from deadlock
pub use deadlock::{DeadlockCycle, DistributedDeadlockDetector, WaitForEdge};

/// Cluster manager trait
#[async_trait]
pub trait ClusterManager: Send + Sync {
    /// Get the current node ID
    fn node_id(&self) -> NodeId;

    /// Check if this node is the leader for a region
    fn is_leader(&self, region_id: RegionId) -> bool;

    /// Get the leader for a region
    async fn get_leader(&self, region_id: RegionId) -> Result<Option<NodeId>>;

    /// Get all nodes in the cluster
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Get regions managed by this node
    async fn get_regions(&self) -> Result<Vec<RegionInfo>>;

    /// Locate the region containing a key
    fn locate_region(&self, key: &[u8]) -> Option<RegionId>;

    /// Propose a command to a region
    async fn propose(&self, region_id: RegionId, cmd: Command) -> Result<()>;
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
    pub status: NodeStatus,
    pub region_count: usize,
}

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Online,
    Offline,
    Joining,
    Leaving,
}

/// Region information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    pub id: RegionId,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub leader: Option<NodeId>,
    pub peers: Vec<NodeId>,
    pub size_bytes: u64,
}

/// Commands that can be proposed to the Raft group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Write a key-value pair
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: Option<TxnId>,
    },
    /// Delete a key
    Delete {
        key: Vec<u8>,
        txn_id: Option<TxnId>,
    },
    /// Prepare a transaction (2PC phase 1)
    Prepare { txn_id: TxnId },
    /// Commit a transaction (2PC phase 2)
    Commit { txn_id: TxnId },
    /// Abort a transaction
    Abort { txn_id: TxnId },
    /// Split a region
    Split { region_id: RegionId, split_key: Vec<u8> },
    /// Merge two regions
    Merge { region_a: RegionId, region_b: RegionId },
    /// Add a peer to a region
    AddPeer { region_id: RegionId, peer: NodeId },
    /// Remove a peer from a region
    RemovePeer { region_id: RegionId, peer: NodeId },
}

/// Raft message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMessage {
    pub region_id: RegionId,
    pub from: NodeId,
    pub to: NodeId,
    pub msg_type: RaftMessageType,
    pub term: u64,
    pub payload: Vec<u8>,
}

/// Raft message type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessageType {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    Heartbeat,
    HeartbeatResponse,
    Snapshot,
}

/// Transport trait for cluster communication
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a Raft message to a node
    async fn send(&self, msg: RaftMessage) -> Result<()>;

    /// Receive Raft messages
    async fn recv(&self) -> Result<RaftMessage>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_info() {
        let region = RegionInfo {
            id: RegionId(1),
            start_key: vec![0],
            end_key: vec![255],
            leader: Some(NodeId(1)),
            peers: vec![NodeId(1), NodeId(2), NodeId(3)],
            size_bytes: 1024 * 1024,
        };

        assert_eq!(region.peers.len(), 3);
    }

    #[test]
    fn test_command_serialization() {
        let cmd = Command::Put {
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
            txn_id: Some(TxnId(100)),
        };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: Command = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            Command::Put { key, value, txn_id } => {
                assert_eq!(key, vec![1, 2, 3]);
                assert_eq!(value, vec![4, 5, 6]);
                assert_eq!(txn_id, Some(TxnId(100)));
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_raft_message_serialization() {
        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(2),
            msg_type: RaftMessageType::Heartbeat,
            term: 5,
            payload: vec![1, 2, 3],
        };

        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: RaftMessage = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.region_id, RegionId(1));
        assert_eq!(deserialized.from, NodeId(1));
        assert_eq!(deserialized.to, NodeId(2));
        assert_eq!(deserialized.msg_type, RaftMessageType::Heartbeat);
        assert_eq!(deserialized.term, 5);
    }

    #[test]
    fn test_node_status() {
        assert_ne!(NodeStatus::Online, NodeStatus::Offline);
        assert_eq!(NodeStatus::Joining, NodeStatus::Joining);
    }
}
