//! WAL Shipping Replication
//!
//! Provides data replication via WAL (Write-Ahead Log) shipping from
//! a leader node to follower nodes. The leader streams WAL entries to
//! followers, which apply them to their own storage.
//!
//! Architecture:
//! - `WalReplicator`: runs on the leader, reads WAL entries and ships to followers
//! - `ReplicationTarget`: tracks per-follower replication state
//! - `ReplicationReceiver`: runs on followers, applies received WAL entries
//! - `ReplicationMessage`: transport layer message types

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use thunder_common::prelude::*;
use thunder_storage::wal::WalReader;
use thunder_storage::WalWriter;

use crate::{NetworkTransport, RaftMessage, RaftMessageType};

// ── Replication message types ──────────────────────────────────────

/// Replication-specific message types sent between leader and followers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// WAL entries shipped from leader to follower
    WalShipment {
        /// Serialized WAL entries (header + payload pairs)
        entries: Vec<WalShipmentEntry>,
        /// LSN of the last entry in this batch
        sender_lsn: u64,
    },

    /// Acknowledgment from follower to leader
    WalAck {
        /// The highest LSN the follower has durably applied
        confirmed_lsn: u64,
    },

    /// Request from a follower to start replication from a given LSN
    ReplicationRequest {
        /// The LSN to start streaming from
        start_lsn: u64,
    },

    /// Leader's response indicating replication session is established
    ReplicationResponse {
        /// Whether the request was accepted
        accepted: bool,
        /// Current leader LSN
        leader_lsn: u64,
    },
}

/// A single WAL entry in a shipment batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalShipmentEntry {
    pub lsn: u64,
    pub prev_lsn: u64,
    pub txn_id: u64,
    pub table_id: u64,
    pub record_type: u8,
    pub data: Vec<u8>,
}

impl ReplicationMessage {
    /// Encode to bytes for transport.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

// ── Replication target (per-follower state on leader) ──────────────

/// Tracks the replication state for a single follower node.
pub struct ReplicationTarget {
    /// Follower node ID
    pub node_id: NodeId,
    /// Network address of the follower
    pub addr: String,
    /// The highest LSN confirmed by this follower
    pub confirmed_lsn: AtomicU64,
    /// The last LSN we shipped to this follower
    pub last_shipped_lsn: AtomicU64,
    /// Whether this target is actively receiving
    pub active: std::sync::atomic::AtomicBool,
}

impl ReplicationTarget {
    pub fn new(node_id: NodeId, addr: String) -> Self {
        Self {
            node_id,
            addr,
            confirmed_lsn: AtomicU64::new(0),
            last_shipped_lsn: AtomicU64::new(0),
            active: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::SeqCst)
    }

    pub fn last_shipped_lsn(&self) -> u64 {
        self.last_shipped_lsn.load(Ordering::SeqCst)
    }
}

// ── WAL Replicator (leader-side) ───────────────────────────────────

/// Configuration for the WAL replicator.
#[derive(Debug, Clone)]
pub struct ReplicatorConfig {
    /// WAL directory to read from
    pub wal_dir: PathBuf,
    /// Maximum number of entries per shipment batch
    pub batch_size: usize,
    /// How often to check for new WAL entries (polling interval)
    pub poll_interval: Duration,
    /// Timeout for shipping a batch to a follower
    pub ship_timeout: Duration,
}

impl Default for ReplicatorConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("data/wal"),
            batch_size: 1000,
            poll_interval: Duration::from_millis(100),
            ship_timeout: Duration::from_secs(5),
        }
    }
}

/// The WAL Replicator runs on the leader node and ships WAL entries
/// to follower nodes for replication.
pub struct WalReplicator {
    config: ReplicatorConfig,
    /// Transport layer for sending messages to followers
    transport: Arc<dyn NetworkTransport>,
    /// Per-follower replication targets
    followers: Arc<RwLock<HashMap<NodeId, Arc<ReplicationTarget>>>>,
    /// The LSN up to which we've shipped to all followers
    last_shipped_lsn: AtomicU64,
    /// Reference to the WAL writer for reading current LSN
    wal_writer: Arc<dyn WalWriter>,
    /// Node ID of this (leader) node
    node_id: NodeId,
}

impl WalReplicator {
    pub fn new(
        config: ReplicatorConfig,
        transport: Arc<dyn NetworkTransport>,
        wal_writer: Arc<dyn WalWriter>,
        node_id: NodeId,
    ) -> Self {
        Self {
            config,
            transport,
            followers: Arc::new(RwLock::new(HashMap::new())),
            last_shipped_lsn: AtomicU64::new(0),
            wal_writer,
            node_id,
        }
    }

    /// Add a follower to the replication set.
    pub async fn add_follower(&self, node_id: NodeId, addr: String) {
        let target = Arc::new(ReplicationTarget::new(node_id, addr));
        self.followers.write().await.insert(node_id, target);
        info!("Added replication follower: {:?}", node_id);
    }

    /// Remove a follower from the replication set.
    pub async fn remove_follower(&self, node_id: NodeId) {
        if let Some(target) = self.followers.write().await.remove(&node_id) {
            target.active.store(false, Ordering::SeqCst);
            info!("Removed replication follower: {:?}", node_id);
        }
    }

    /// Handle a WAL acknowledgment from a follower.
    pub async fn handle_ack(&self, from: NodeId, confirmed_lsn: u64) {
        let followers = self.followers.read().await;
        if let Some(target) = followers.get(&from) {
            target.confirmed_lsn.store(confirmed_lsn, Ordering::SeqCst);
            debug!(
                "Follower {:?} confirmed LSN {}",
                from, confirmed_lsn
            );
        }
    }

    /// Start the streaming replication loop. Runs until cancelled.
    pub async fn start_streaming(&self, cancel: CancellationToken) {
        info!("WAL replicator started for node {:?}", self.node_id);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("WAL replicator shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    if let Err(e) = self.ship_pending_entries().await {
                        warn!("WAL shipping error: {}", e);
                    }
                }
            }
        }
    }

    /// Ship any pending WAL entries to all followers.
    async fn ship_pending_entries(&self) -> Result<()> {
        let current_lsn = self.wal_writer.current_lsn().0;
        let last_shipped = self.last_shipped_lsn.load(Ordering::SeqCst);

        if current_lsn <= last_shipped {
            return Ok(()); // Nothing new to ship
        }

        // Read new WAL entries from disk
        let entries = self.read_wal_entries(Lsn(last_shipped), Lsn(current_lsn))?;
        if entries.is_empty() {
            return Ok(());
        }

        let batch_lsn = entries.last().map(|e| e.lsn).unwrap_or(last_shipped);

        // Create the shipment message
        let msg = ReplicationMessage::WalShipment {
            entries: entries.clone(),
            sender_lsn: batch_lsn,
        };
        let payload = msg.encode();

        // Ship to all active followers
        let followers = self.followers.read().await;
        for (node_id, target) in followers.iter() {
            if !target.active.load(Ordering::SeqCst) {
                continue;
            }

            let raft_msg = RaftMessage {
                region_id: RegionId(0), // Replication uses region 0 as a sentinel
                from: self.node_id,
                to: *node_id,
                msg_type: RaftMessageType::AppendEntries, // Re-use for WAL shipping
                term: 0,
                payload: payload.clone(),
            };

            match self.transport.send(*node_id, raft_msg).await {
                Ok(()) => {
                    target
                        .last_shipped_lsn
                        .store(batch_lsn, Ordering::SeqCst);
                    debug!("Shipped WAL entries up to LSN {} to {:?}", batch_lsn, node_id);
                }
                Err(e) => {
                    warn!(
                        "Failed to ship WAL entries to follower {:?}: {}",
                        node_id, e
                    );
                }
            }
        }

        self.last_shipped_lsn.store(batch_lsn, Ordering::SeqCst);
        Ok(())
    }

    /// Read WAL entries between two LSNs from the WAL segments on disk.
    fn read_wal_entries(&self, from_lsn: Lsn, to_lsn: Lsn) -> Result<Vec<WalShipmentEntry>> {
        let mut reader = WalReader::new(&self.config.wal_dir);
        reader.seek_to_lsn(from_lsn)?;

        let mut entries = Vec::new();
        let mut count = 0;

        while count < self.config.batch_size {
            match reader.read_next()? {
                Some((header, data)) => {
                    if header.lsn > to_lsn.0 {
                        break;
                    }
                    if header.lsn <= from_lsn.0 {
                        continue; // Skip already-shipped entries
                    }

                    entries.push(WalShipmentEntry {
                        lsn: header.lsn,
                        prev_lsn: header.prev_lsn,
                        txn_id: header.txn_id,
                        table_id: header.table_id,
                        record_type: header.record_type,
                        data: data.to_vec(),
                    });
                    count += 1;
                }
                None => break, // No more entries
            }
        }

        Ok(entries)
    }

    /// Get replication lag (in LSN) for a specific follower.
    pub async fn get_follower_lag(&self, node_id: NodeId) -> Option<u64> {
        let followers = self.followers.read().await;
        let target = followers.get(&node_id)?;
        let current = self.wal_writer.current_lsn().0;
        let confirmed = target.confirmed_lsn();
        Some(current.saturating_sub(confirmed))
    }

    /// Get replication status for all followers.
    pub async fn get_status(&self) -> Vec<ReplicationStatus> {
        let current_lsn = self.wal_writer.current_lsn().0;
        let followers = self.followers.read().await;

        followers
            .iter()
            .map(|(node_id, target)| {
                let confirmed = target.confirmed_lsn();
                ReplicationStatus {
                    node_id: *node_id,
                    confirmed_lsn: confirmed,
                    last_shipped_lsn: target.last_shipped_lsn(),
                    lag: current_lsn.saturating_sub(confirmed),
                    active: target.active.load(Ordering::SeqCst),
                }
            })
            .collect()
    }
}

/// Replication status for a single follower.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub node_id: NodeId,
    pub confirmed_lsn: u64,
    pub last_shipped_lsn: u64,
    pub lag: u64,
    pub active: bool,
}

// ── Replication Receiver (follower-side) ───────────────────────────

/// The ReplicationReceiver runs on follower nodes and applies received
/// WAL entries to local storage.
pub struct ReplicationReceiver {
    /// The highest LSN we've applied locally
    applied_lsn: AtomicU64,
    /// Transport for sending ACKs back to the leader
    transport: Arc<dyn NetworkTransport>,
    /// This follower's node ID
    node_id: NodeId,
    /// Leader's node ID (set when replication starts)
    leader_id: RwLock<Option<NodeId>>,
    /// Channel for incoming WAL shipments
    shipment_tx: mpsc::Sender<ReplicationMessage>,
    shipment_rx: tokio::sync::Mutex<mpsc::Receiver<ReplicationMessage>>,
    /// WAL writer for durably writing received entries
    wal_writer: Arc<dyn WalWriter>,
}

impl ReplicationReceiver {
    pub fn new(
        transport: Arc<dyn NetworkTransport>,
        node_id: NodeId,
        wal_writer: Arc<dyn WalWriter>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(10_000);
        Self {
            applied_lsn: AtomicU64::new(0),
            transport,
            node_id,
            leader_id: RwLock::new(None),
            shipment_tx: tx,
            shipment_rx: tokio::sync::Mutex::new(rx),
            wal_writer,
        }
    }

    /// Set the leader node ID.
    pub async fn set_leader(&self, leader_id: NodeId) {
        *self.leader_id.write().await = Some(leader_id);
        info!(
            "Replication receiver {:?}: leader set to {:?}",
            self.node_id, leader_id
        );
    }

    /// Handle an incoming replication message. Call this from the transport layer.
    pub async fn handle_message(&self, msg: ReplicationMessage) -> Result<()> {
        self.shipment_tx.send(msg).await.map_err(|e| {
            Error::Internal(format!("Failed to queue replication message: {}", e))
        })
    }

    /// Start the replication apply loop. Runs until cancelled.
    pub async fn start_applying(&self, cancel: CancellationToken) {
        info!("Replication receiver started for node {:?}", self.node_id);

        let mut rx = self.shipment_rx.lock().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Replication receiver shutting down");
                    break;
                }
                msg = rx.recv() => {
                    match msg {
                        Some(ReplicationMessage::WalShipment { entries, sender_lsn }) => {
                            if let Err(e) = self.apply_entries(&entries).await {
                                error!("Failed to apply WAL entries: {}", e);
                                continue;
                            }

                            // Send ACK back to leader
                            if let Err(e) = self.send_ack(sender_lsn).await {
                                warn!("Failed to send WAL ACK: {}", e);
                            }
                        }
                        Some(_) => {
                            debug!("Received non-shipment replication message");
                        }
                        None => {
                            info!("Replication channel closed");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Apply a batch of WAL entries to local storage.
    async fn apply_entries(&self, entries: &[WalShipmentEntry]) -> Result<()> {
        for entry in entries {
            let current_applied = self.applied_lsn.load(Ordering::SeqCst);
            if entry.lsn <= current_applied {
                continue; // Already applied
            }

            // Write the entry to local WAL for durability
            let wal_entry = thunder_storage::WalEntry {
                lsn: Lsn(entry.lsn),
                txn_id: TxnId(entry.txn_id),
                entry_type: thunder_storage::WalEntryType::Commit, // Simplified: treat as commit
                table_id: TableId(entry.table_id),
                data: entry.data.clone(),
            };

            self.wal_writer.append(wal_entry).await?;
            self.applied_lsn.store(entry.lsn, Ordering::SeqCst);
        }

        // Sync WAL to ensure durability
        self.wal_writer.sync().await?;

        debug!(
            "Applied {} WAL entries, applied_lsn={}",
            entries.len(),
            self.applied_lsn.load(Ordering::SeqCst)
        );

        Ok(())
    }

    /// Send acknowledgment to the leader.
    async fn send_ack(&self, confirmed_lsn: u64) -> Result<()> {
        let leader = self.leader_id.read().await;
        let leader_id = leader.ok_or_else(|| {
            Error::Internal("No leader set for replication receiver".into())
        })?;

        let ack = ReplicationMessage::WalAck { confirmed_lsn };
        let msg = RaftMessage {
            region_id: RegionId(0),
            from: self.node_id,
            to: leader_id,
            msg_type: RaftMessageType::AppendEntriesResponse,
            term: 0,
            payload: ack.encode(),
        };

        self.transport.send(leader_id, msg).await
    }

    /// Get the currently applied LSN.
    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_message_roundtrip() {
        let msg = ReplicationMessage::WalShipment {
            entries: vec![WalShipmentEntry {
                lsn: 42,
                prev_lsn: 41,
                txn_id: 100,
                table_id: 1,
                record_type: 0,
                data: vec![1, 2, 3, 4],
            }],
            sender_lsn: 42,
        };

        let encoded = msg.encode();
        let decoded = ReplicationMessage::decode(&encoded).expect("decode failed");

        match decoded {
            ReplicationMessage::WalShipment {
                entries,
                sender_lsn,
            } => {
                assert_eq!(sender_lsn, 42);
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].lsn, 42);
                assert_eq!(entries[0].data, vec![1, 2, 3, 4]);
            }
            _ => panic!("Expected WalShipment"),
        }
    }

    #[test]
    fn test_ack_message_roundtrip() {
        let msg = ReplicationMessage::WalAck {
            confirmed_lsn: 999,
        };
        let encoded = msg.encode();
        let decoded = ReplicationMessage::decode(&encoded).expect("decode failed");

        match decoded {
            ReplicationMessage::WalAck { confirmed_lsn } => {
                assert_eq!(confirmed_lsn, 999);
            }
            _ => panic!("Expected WalAck"),
        }
    }

    #[test]
    fn test_replication_target() {
        let target = ReplicationTarget::new(NodeId(1), "127.0.0.1:5433".to_string());
        assert_eq!(target.confirmed_lsn(), 0);
        assert_eq!(target.last_shipped_lsn(), 0);
        assert!(target.active.load(Ordering::SeqCst));

        target.confirmed_lsn.store(100, Ordering::SeqCst);
        assert_eq!(target.confirmed_lsn(), 100);
    }
}
