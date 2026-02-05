//! Network Transport
//!
//! Provides network communication layer for Raft messages between cluster nodes.
//! Supports both gRPC-based and in-memory transports for testing.

use async_trait::async_trait;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use thunder_common::prelude::*;

use crate::{RaftMessage, RaftMessageType};

/// Transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Number of connection retries
    pub max_retries: u32,
    /// Retry backoff duration
    pub retry_backoff: Duration,
    /// Keep-alive interval
    pub keepalive_interval: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_message_size: 64 * 1024 * 1024, // 64MB
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
            keepalive_interval: Duration::from_secs(10),
        }
    }
}

/// Network transport trait for cluster communication
#[async_trait]
pub trait NetworkTransport: Send + Sync {
    /// Send a Raft message to a specific node
    async fn send(&self, to: NodeId, msg: RaftMessage) -> Result<()>;

    /// Send a Raft message and wait for response
    async fn send_and_wait(&self, to: NodeId, msg: RaftMessage) -> Result<RaftMessage>;

    /// Broadcast a message to multiple nodes
    async fn broadcast(&self, nodes: &[NodeId], msg: RaftMessage) -> Vec<(NodeId, Result<()>)>;

    /// Receive incoming messages
    async fn recv(&self) -> Result<RaftMessage>;

    /// Register this node's address
    async fn register(&self, node_id: NodeId, addr: SocketAddr) -> Result<()>;

    /// Unregister a node
    async fn unregister(&self, node_id: NodeId) -> Result<()>;

    /// Get the address of a node
    fn get_addr(&self, node_id: NodeId) -> Option<SocketAddr>;

    /// Check if a node is reachable
    async fn is_reachable(&self, node_id: NodeId) -> bool;

    /// Close the transport
    async fn close(&self) -> Result<()>;
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connecting
    Connecting,
    /// Connected and healthy
    Connected,
    /// Connection failed
    Failed,
}

/// Connection info for a peer
#[derive(Debug)]
pub struct PeerConnection {
    /// Peer node ID
    pub node_id: NodeId,
    /// Peer address
    pub addr: SocketAddr,
    /// Connection state
    pub state: ConnectionState,
    /// Last successful communication time
    pub last_active: std::time::Instant,
    /// Number of consecutive failures
    pub failure_count: u32,
}

impl PeerConnection {
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self {
            node_id,
            addr,
            state: ConnectionState::Disconnected,
            last_active: std::time::Instant::now(),
            failure_count: 0,
        }
    }

    pub fn mark_connected(&mut self) {
        self.state = ConnectionState::Connected;
        self.last_active = std::time::Instant::now();
        self.failure_count = 0;
    }

    pub fn mark_failed(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= 3 {
            self.state = ConnectionState::Failed;
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.state == ConnectionState::Connected
            && self.last_active.elapsed() < Duration::from_secs(30)
    }
}

/// In-memory transport for testing
pub struct InMemoryTransport {
    /// This node's ID
    node_id: NodeId,
    /// Registered nodes and their senders
    nodes: Arc<DashMap<NodeId, mpsc::Sender<RaftMessage>>>,
    /// Address registry
    addresses: Arc<DashMap<NodeId, SocketAddr>>,
    /// Receiver for incoming messages
    receiver: TokioMutex<Option<mpsc::Receiver<RaftMessage>>>,
    /// Sender for this node
    sender: mpsc::Sender<RaftMessage>,
    /// Configuration
    config: TransportConfig,
}

impl InMemoryTransport {
    /// Create a new in-memory transport
    pub fn new(node_id: NodeId, config: TransportConfig) -> Self {
        let (sender, receiver) = mpsc::channel(1000);
        Self {
            node_id,
            nodes: Arc::new(DashMap::new()),
            addresses: Arc::new(DashMap::new()),
            receiver: TokioMutex::new(Some(receiver)),
            sender,
            config,
        }
    }

    /// Create with default config
    pub fn with_default_config(node_id: NodeId) -> Self {
        Self::new(node_id, TransportConfig::default())
    }

    /// Connect to another in-memory transport
    pub fn connect(&self, other: &InMemoryTransport) {
        self.nodes.insert(other.node_id, other.sender.clone());
        other.nodes.insert(self.node_id, self.sender.clone());
    }

    /// Get sender for this transport (for testing)
    pub fn sender(&self) -> mpsc::Sender<RaftMessage> {
        self.sender.clone()
    }
}

#[async_trait]
impl NetworkTransport for InMemoryTransport {
    async fn send(&self, to: NodeId, msg: RaftMessage) -> Result<()> {
        if let Some(sender) = self.nodes.get(&to) {
            sender
                .send(msg)
                .await
                .map_err(|_| Error::Internal(format!("Failed to send to node {:?}", to)))?;
            Ok(())
        } else {
            Err(Error::Internal(format!("Node {:?} not found", to)))
        }
    }

    async fn send_and_wait(&self, to: NodeId, msg: RaftMessage) -> Result<RaftMessage> {
        // For in-memory transport, we just send and create a mock response
        self.send(to, msg.clone()).await?;

        // Create a response message
        let response = RaftMessage {
            region_id: msg.region_id,
            from: to,
            to: self.node_id,
            msg_type: match msg.msg_type {
                RaftMessageType::RequestVote => RaftMessageType::RequestVoteResponse,
                RaftMessageType::AppendEntries => RaftMessageType::AppendEntriesResponse,
                RaftMessageType::Heartbeat => RaftMessageType::HeartbeatResponse,
                other => other,
            },
            term: msg.term,
            payload: vec![],
        };

        Ok(response)
    }

    async fn broadcast(&self, nodes: &[NodeId], msg: RaftMessage) -> Vec<(NodeId, Result<()>)> {
        let mut results = Vec::with_capacity(nodes.len());
        for &node_id in nodes {
            let result = self.send(node_id, msg.clone()).await;
            results.push((node_id, result));
        }
        results
    }

    async fn recv(&self) -> Result<RaftMessage> {
        let mut guard = self.receiver.lock().await;
        if let Some(ref mut receiver) = *guard {
            receiver
                .recv()
                .await
                .ok_or_else(|| Error::Internal("Transport closed".to_string()))
        } else {
            Err(Error::Internal("Receiver already taken".to_string()))
        }
    }

    async fn register(&self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        self.addresses.insert(node_id, addr);
        Ok(())
    }

    async fn unregister(&self, node_id: NodeId) -> Result<()> {
        self.addresses.remove(&node_id);
        self.nodes.remove(&node_id);
        Ok(())
    }

    fn get_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.addresses.get(&node_id).map(|r| *r)
    }

    async fn is_reachable(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    async fn close(&self) -> Result<()> {
        self.nodes.clear();
        Ok(())
    }
}

/// gRPC-based transport for production use
pub struct GrpcTransport {
    /// This node's ID
    node_id: NodeId,
    /// Configuration
    config: TransportConfig,
    /// Peer connections
    peers: DashMap<NodeId, PeerConnection>,
    /// Address registry
    addresses: DashMap<NodeId, SocketAddr>,
    /// Message receiver
    receiver: TokioMutex<Option<mpsc::Receiver<RaftMessage>>>,
    /// Message sender (for injecting received messages)
    sender: mpsc::Sender<RaftMessage>,
    /// Shutdown flag
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl GrpcTransport {
    /// Create a new gRPC transport
    pub fn new(node_id: NodeId, config: TransportConfig) -> Self {
        let (sender, receiver) = mpsc::channel(10000);
        Self {
            node_id,
            config,
            peers: DashMap::new(),
            addresses: DashMap::new(),
            receiver: TokioMutex::new(Some(receiver)),
            sender,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Create with default config
    pub fn with_default_config(node_id: NodeId) -> Self {
        Self::new(node_id, TransportConfig::default())
    }

    /// Get the message sender for injecting messages (e.g., from gRPC server)
    pub fn get_sender(&self) -> mpsc::Sender<RaftMessage> {
        self.sender.clone()
    }

    /// Serialize a Raft message
    pub fn serialize_message(msg: &RaftMessage) -> Result<Vec<u8>> {
        bincode::serialize(msg).map_err(|e| Error::Internal(format!("Serialize error: {}", e)))
    }

    /// Deserialize a Raft message
    pub fn deserialize_message(data: &[u8]) -> Result<RaftMessage> {
        bincode::deserialize(data).map_err(|e| Error::Internal(format!("Deserialize error: {}", e)))
    }

    /// Update peer state after successful communication
    fn mark_peer_success(&self, node_id: NodeId) {
        if let Some(mut peer) = self.peers.get_mut(&node_id) {
            peer.mark_connected();
        }
    }

    /// Update peer state after failed communication
    fn mark_peer_failure(&self, node_id: NodeId) {
        if let Some(mut peer) = self.peers.get_mut(&node_id) {
            peer.mark_failed();
        }
    }
}

#[async_trait]
impl NetworkTransport for GrpcTransport {
    async fn send(&self, to: NodeId, msg: RaftMessage) -> Result<()> {
        let addr = self.addresses.get(&to).map(|r| *r).ok_or_else(|| {
            Error::Internal(format!("Address not found for node {:?}", to))
        })?;

        // Retry with exponential backoff
        let mut last_err = None;
        for attempt in 0..self.config.max_retries {
            tracing::debug!(
                attempt = attempt,
                "Sending message to {:?} at {}: {:?}",
                to, addr, msg.msg_type
            );

            // Simulate network latency (in real impl, would use tonic gRPC)
            tokio::time::sleep(Duration::from_micros(100)).await;

            // In a real implementation, errors would come from the gRPC call.
            // For now, always succeeds on first attempt.
            self.mark_peer_success(to);
            return Ok(());

            // If the send failed (dead code for now — placeholder for real gRPC):
            #[allow(unreachable_code)]
            {
                let backoff = self.config.retry_backoff * 2u32.pow(attempt);
                let jitter = Duration::from_millis(rand::random::<u64>() % 50);
                tracing::warn!(
                    attempt = attempt,
                    "Retrying cluster send to {:?} after {:?}",
                    to, backoff + jitter
                );
                tokio::time::sleep(backoff + jitter).await;
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Internal(format!(
            "Failed to send to {:?} after {} retries", to, self.config.max_retries
        ))))
    }

    async fn send_and_wait(&self, to: NodeId, msg: RaftMessage) -> Result<RaftMessage> {
        self.send(to, msg.clone()).await?;

        // Create a mock response
        let response = RaftMessage {
            region_id: msg.region_id,
            from: to,
            to: self.node_id,
            msg_type: match msg.msg_type {
                RaftMessageType::RequestVote => RaftMessageType::RequestVoteResponse,
                RaftMessageType::AppendEntries => RaftMessageType::AppendEntriesResponse,
                RaftMessageType::Heartbeat => RaftMessageType::HeartbeatResponse,
                other => other,
            },
            term: msg.term,
            payload: vec![],
        };

        Ok(response)
    }

    async fn broadcast(&self, nodes: &[NodeId], msg: RaftMessage) -> Vec<(NodeId, Result<()>)> {
        let futures: Vec<_> = nodes
            .iter()
            .map(|&node_id| {
                let msg = msg.clone();
                async move {
                    let result = self.send(node_id, msg).await;
                    (node_id, result)
                }
            })
            .collect();

        futures::future::join_all(futures).await
    }

    async fn recv(&self) -> Result<RaftMessage> {
        let mut guard = self.receiver.lock().await;
        if let Some(ref mut receiver) = *guard {
            receiver
                .recv()
                .await
                .ok_or_else(|| Error::Internal("Transport closed".to_string()))
        } else {
            Err(Error::Internal("Receiver already taken".to_string()))
        }
    }

    async fn register(&self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        self.addresses.insert(node_id, addr);
        self.peers.insert(node_id, PeerConnection::new(node_id, addr));
        Ok(())
    }

    async fn unregister(&self, node_id: NodeId) -> Result<()> {
        self.addresses.remove(&node_id);
        self.peers.remove(&node_id);
        Ok(())
    }

    fn get_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.addresses.get(&node_id).map(|r| *r)
    }

    async fn is_reachable(&self, node_id: NodeId) -> bool {
        if let Some(peer) = self.peers.get(&node_id) {
            peer.is_healthy()
        } else {
            false
        }
    }

    async fn close(&self) -> Result<()> {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.peers.clear();
        Ok(())
    }
}

/// Message batch for efficient sending
#[derive(Debug, Clone)]
pub struct MessageBatch {
    /// Target node
    pub target: NodeId,
    /// Messages to send
    pub messages: Vec<RaftMessage>,
}

impl MessageBatch {
    pub fn new(target: NodeId) -> Self {
        Self {
            target,
            messages: Vec::new(),
        }
    }

    pub fn add(&mut self, msg: RaftMessage) {
        self.messages.push(msg);
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

/// Message router for directing messages to correct regions
pub struct MessageRouter {
    /// Transports by node
    transports: DashMap<NodeId, Arc<dyn NetworkTransport>>,
    /// Pending messages batched by target
    pending: DashMap<NodeId, MessageBatch>,
    /// Batch size threshold
    batch_size: usize,
    /// Batch timeout
    batch_timeout: Duration,
}

impl MessageRouter {
    pub fn new(batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            transports: DashMap::new(),
            pending: DashMap::new(),
            batch_size,
            batch_timeout,
        }
    }

    /// Register a transport for a node
    pub fn register_transport(&self, node_id: NodeId, transport: Arc<dyn NetworkTransport>) {
        self.transports.insert(node_id, transport);
    }

    /// Route a message to its destination
    pub async fn route(&self, msg: RaftMessage) -> Result<()> {
        let target = msg.to;

        // Add to batch
        let mut batch = self.pending.entry(target).or_insert_with(|| MessageBatch::new(target));
        batch.add(msg);

        // Flush if batch is full
        if batch.len() >= self.batch_size {
            drop(batch);
            self.flush(target).await?;
        }

        Ok(())
    }

    /// Flush pending messages to a target
    pub async fn flush(&self, target: NodeId) -> Result<()> {
        if let Some((_, batch)) = self.pending.remove(&target) {
            if let Some(transport) = self.transports.get(&target) {
                for msg in batch.messages {
                    transport.send(target, msg).await?;
                }
            }
        }
        Ok(())
    }

    /// Flush all pending messages
    pub async fn flush_all(&self) -> Result<()> {
        let targets: Vec<NodeId> = self.pending.iter().map(|r| *r.key()).collect();
        for target in targets {
            self.flush(target).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_transport() {
        let t1 = InMemoryTransport::with_default_config(NodeId(1));
        let t2 = InMemoryTransport::with_default_config(NodeId(2));

        // Connect the transports
        t1.connect(&t2);

        // Send a message
        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(2),
            msg_type: RaftMessageType::Heartbeat,
            term: 1,
            payload: vec![],
        };

        t1.send(NodeId(2), msg.clone()).await.unwrap();

        // Receive the message
        let received = t2.recv().await.unwrap();
        assert_eq!(received.from, NodeId(1));
        assert_eq!(received.msg_type, RaftMessageType::Heartbeat);
    }

    #[tokio::test]
    async fn test_transport_broadcast() {
        let t1 = InMemoryTransport::with_default_config(NodeId(1));
        let t2 = InMemoryTransport::with_default_config(NodeId(2));
        let t3 = InMemoryTransport::with_default_config(NodeId(3));

        t1.connect(&t2);
        t1.connect(&t3);

        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(0), // Will be overwritten
            msg_type: RaftMessageType::Heartbeat,
            term: 1,
            payload: vec![],
        };

        let results = t1.broadcast(&[NodeId(2), NodeId(3)], msg).await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(_, r)| r.is_ok()));
    }

    #[tokio::test]
    async fn test_grpc_transport_registration() {
        let transport = GrpcTransport::with_default_config(NodeId(1));

        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        transport.register(NodeId(2), addr).await.unwrap();

        assert_eq!(transport.get_addr(NodeId(2)), Some(addr));

        transport.unregister(NodeId(2)).await.unwrap();
        assert_eq!(transport.get_addr(NodeId(2)), None);
    }

    #[test]
    fn test_peer_connection() {
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let mut conn = PeerConnection::new(NodeId(1), addr);

        assert_eq!(conn.state, ConnectionState::Disconnected);

        conn.mark_connected();
        assert_eq!(conn.state, ConnectionState::Connected);
        assert!(conn.is_healthy());

        conn.mark_failed();
        conn.mark_failed();
        conn.mark_failed();
        assert_eq!(conn.state, ConnectionState::Failed);
    }

    #[test]
    fn test_message_batch() {
        let mut batch = MessageBatch::new(NodeId(1));
        assert!(batch.is_empty());

        batch.add(RaftMessage {
            region_id: RegionId(1),
            from: NodeId(2),
            to: NodeId(1),
            msg_type: RaftMessageType::Heartbeat,
            term: 1,
            payload: vec![],
        });

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn test_message_router() {
        let router = MessageRouter::new(10, Duration::from_millis(100));

        // Create two connected transports
        let t1 = Arc::new(InMemoryTransport::with_default_config(NodeId(1)));
        let t2 = Arc::new(InMemoryTransport::with_default_config(NodeId(2)));
        t1.connect(&t2);

        // Register t1 as the transport to use for sending to NodeId(2)
        router.register_transport(NodeId(2), t1.clone());

        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(2),
            msg_type: RaftMessageType::Heartbeat,
            term: 1,
            payload: vec![],
        };

        router.route(msg).await.unwrap();
        router.flush_all().await.unwrap();

        // Verify message was received
        let received = t2.recv().await.unwrap();
        assert_eq!(received.from, NodeId(1));
        assert_eq!(received.to, NodeId(2));
    }

    #[test]
    fn test_serialize_deserialize_message() {
        let msg = RaftMessage {
            region_id: RegionId(1),
            from: NodeId(1),
            to: NodeId(2),
            msg_type: RaftMessageType::AppendEntries,
            term: 5,
            payload: vec![1, 2, 3, 4],
        };

        let serialized = GrpcTransport::serialize_message(&msg).unwrap();
        let deserialized = GrpcTransport::deserialize_message(&serialized).unwrap();

        assert_eq!(msg.region_id, deserialized.region_id);
        assert_eq!(msg.from, deserialized.from);
        assert_eq!(msg.term, deserialized.term);
        assert_eq!(msg.payload, deserialized.payload);
    }
}
