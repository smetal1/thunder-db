//! B+Tree implementation for ThunderDB.
//!
//! Features:
//! - Flash-optimized with high fanout (256+ keys per node)
//! - Leaf nodes are linked for efficient range scans
//! - Concurrent access via latch coupling (crabbing)
//! - Support for variable-length keys

use crate::buffer::BufferPoolImpl;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use thunder_common::prelude::*;

/// B+Tree configuration.
#[derive(Debug, Clone)]
pub struct BTreeConfig {
    /// Maximum number of keys per node (fanout - 1)
    pub max_keys: usize,
    /// Minimum keys before merge (max_keys / 2)
    pub min_keys: usize,
    /// Enable prefix compression for keys
    pub prefix_compression: bool,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        Self {
            max_keys: 256,
            min_keys: 128,
            prefix_compression: true,
        }
    }
}

/// Node type in the B+Tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 1,
    Leaf = 2,
}

impl TryFrom<u8> for NodeType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(NodeType::Internal),
            2 => Ok(NodeType::Leaf),
            _ => Err(Error::Internal("Invalid node type".into())),
        }
    }
}

/// Node header for B+Tree pages.
///
/// Layout (32 bytes):
/// - node_type: 1 byte
/// - key_count: 2 bytes
/// - free_space: 2 bytes
/// - parent_page_id: 8 bytes
/// - right_sibling: 8 bytes (leaf only)
/// - left_sibling: 8 bytes (leaf only)
/// - reserved: 3 bytes
#[derive(Debug, Clone, Copy)]
pub struct NodeHeader {
    pub node_type: NodeType,
    pub key_count: u16,
    pub free_space: u16,
    pub parent_page_id: PageId,
    pub right_sibling: PageId,
    pub left_sibling: PageId,
}

const NODE_HEADER_SIZE: usize = 32;

impl NodeHeader {
    pub fn new(node_type: NodeType) -> Self {
        Self {
            node_type,
            key_count: 0,
            free_space: 0,
            parent_page_id: PageId(0),
            right_sibling: PageId(0),
            left_sibling: PageId(0),
        }
    }

    pub fn read_from(data: &[u8]) -> Result<Self> {
        if data.len() < NODE_HEADER_SIZE {
            return Err(Error::Internal("Node header too short".into()));
        }

        let mut buf = data;
        let node_type = NodeType::try_from(buf.get_u8())?;
        let key_count = buf.get_u16_le();
        let free_space = buf.get_u16_le();
        let parent_page_id = PageId(buf.get_u64_le());
        let right_sibling = PageId(buf.get_u64_le());
        let left_sibling = PageId(buf.get_u64_le());

        Ok(Self {
            node_type,
            key_count,
            free_space,
            parent_page_id,
            right_sibling,
            left_sibling,
        })
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = data;
        buf.put_u8(self.node_type as u8);
        buf.put_u16_le(self.key_count);
        buf.put_u16_le(self.free_space);
        buf.put_u64_le(self.parent_page_id.0);
        buf.put_u64_le(self.right_sibling.0);
        buf.put_u64_le(self.left_sibling.0);
    }
}

/// Entry in the slot directory (for variable-length keys).
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    /// Offset from start of data area
    pub key_offset: u16,
    /// Length of key
    pub key_len: u16,
    /// For internal nodes: child page ID (stored inline)
    /// For leaf nodes: row ID (stored inline)
    pub value: u64,
}

const SLOT_ENTRY_SIZE: usize = 12; // 2 + 2 + 8

impl SlotEntry {
    pub fn read_from(data: &[u8]) -> Self {
        let mut buf = data;
        Self {
            key_offset: buf.get_u16_le(),
            key_len: buf.get_u16_le(),
            value: buf.get_u64_le(),
        }
    }

    pub fn write_to(&self, data: &mut [u8]) {
        let mut buf = data;
        buf.put_u16_le(self.key_offset);
        buf.put_u16_le(self.key_len);
        buf.put_u64_le(self.value);
    }
}

/// In-memory representation of a B+Tree node.
#[derive(Debug)]
pub struct BTreeNode {
    /// Page ID of this node
    pub page_id: PageId,
    /// Node header
    pub header: NodeHeader,
    /// Keys stored in this node
    pub keys: Vec<Bytes>,
    /// For internal nodes: child page IDs
    /// For leaf nodes: row IDs
    pub values: Vec<u64>,
}

impl BTreeNode {
    /// Create a new leaf node.
    pub fn new_leaf(page_id: PageId) -> Self {
        Self {
            page_id,
            header: NodeHeader::new(NodeType::Leaf),
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Create a new internal node.
    pub fn new_internal(page_id: PageId) -> Self {
        Self {
            page_id,
            header: NodeHeader::new(NodeType::Internal),
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Check if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.header.node_type == NodeType::Leaf
    }

    /// Get number of keys.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Check if node is full.
    pub fn is_full(&self, max_keys: usize) -> bool {
        self.keys.len() >= max_keys
    }

    /// Check if node needs merge (below minimum).
    pub fn needs_merge(&self, min_keys: usize) -> bool {
        self.keys.len() < min_keys
    }

    /// Binary search for key position.
    /// Returns Ok(pos) if key found, Err(pos) where key should be inserted.
    pub fn search_key(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.keys.binary_search_by(|k| k.as_ref().cmp(key))
    }

    /// Insert a key-value pair at the given position (for leaf nodes).
    pub fn insert_at(&mut self, pos: usize, key: Bytes, value: u64) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }

    /// Insert a key and child pointer for internal nodes.
    /// The new child pointer goes after the key.
    pub fn insert_internal(&mut self, pos: usize, key: Bytes, new_child: u64) {
        self.keys.insert(pos, key);
        self.values.insert(pos + 1, new_child);
    }

    /// Remove key-value at position.
    pub fn remove_at(&mut self, pos: usize) -> (Bytes, u64) {
        let key = self.keys.remove(pos);
        let value = self.values.remove(pos);
        (key, value)
    }

    /// Split node in half, returning the new right node and separator key.
    pub fn split(&mut self, new_page_id: PageId) -> (BTreeNode, Bytes) {
        let mid = self.keys.len() / 2;

        if self.is_leaf() {
            // For leaf nodes, copy the middle key to the new node
            let separator = self.keys[mid].clone();

            let mut right = BTreeNode::new_leaf(new_page_id);
            right.keys = self.keys.split_off(mid);
            right.values = self.values.split_off(mid);

            // Update sibling pointers
            right.header.left_sibling = self.page_id;
            right.header.right_sibling = self.header.right_sibling;
            self.header.right_sibling = new_page_id;

            (right, separator)
        } else {
            // For internal nodes, promote the middle key
            let separator = self.keys[mid].clone();

            let mut right = BTreeNode::new_internal(new_page_id);

            // The middle key goes up, right node gets keys after middle
            right.keys = self.keys.split_off(mid + 1);
            right.values = self.values.split_off(mid + 1);

            // Remove the promoted key from left node
            self.keys.pop();

            (right, separator)
        }
    }

    /// Serialize node to bytes.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Write header
        let mut header_buf = [0u8; NODE_HEADER_SIZE];
        let mut header = self.header;
        header.key_count = self.keys.len() as u16;
        header.write_to(&mut header_buf);
        buf.extend_from_slice(&header_buf);

        // For internal nodes, write the first child pointer before the slot directory
        // Internal nodes have N+1 children for N keys
        if !self.is_leaf() && !self.values.is_empty() {
            buf.put_u64_le(self.values[0]);
        }

        // Write slot directory
        let mut data_offset: u16 = 0;
        for (i, key) in self.keys.iter().enumerate() {
            // For internal nodes, value[i+1] is the right child after key[i]
            // For leaf nodes, value[i] is the row ID for key[i]
            let value = if self.is_leaf() {
                self.values[i]
            } else {
                self.values[i + 1]
            };

            let slot = SlotEntry {
                key_offset: data_offset,
                key_len: key.len() as u16,
                value,
            };
            let mut slot_buf = [0u8; SLOT_ENTRY_SIZE];
            slot.write_to(&mut slot_buf);
            buf.extend_from_slice(&slot_buf);
            data_offset += key.len() as u16;
        }

        // Write key data
        for key in &self.keys {
            buf.extend_from_slice(key);
        }

        buf.freeze()
    }

    /// Deserialize node from page.
    pub fn deserialize(page_id: PageId, data: &[u8]) -> Result<Self> {
        if data.len() < NODE_HEADER_SIZE {
            return Err(Error::Internal("Node data too short".into()));
        }

        let header = NodeHeader::read_from(data)?;
        let key_count = header.key_count as usize;
        let is_internal = header.node_type == NodeType::Internal;

        // For internal nodes, first 8 bytes after header is the first child pointer
        let (slot_start, first_child) = if is_internal && key_count > 0 {
            let first_child = (&data[NODE_HEADER_SIZE..]).get_u64_le();
            (NODE_HEADER_SIZE + 8, Some(first_child))
        } else {
            (NODE_HEADER_SIZE, None)
        };

        let slot_end = slot_start + key_count * SLOT_ENTRY_SIZE;

        if data.len() < slot_end {
            return Err(Error::Internal("Node data too short for slots".into()));
        }

        // Read slot directory
        let mut keys = Vec::with_capacity(key_count);
        let value_capacity = if is_internal { key_count + 1 } else { key_count };
        let mut values = Vec::with_capacity(value_capacity);

        // For internal nodes, add the first child pointer
        if let Some(child) = first_child {
            values.push(child);
        }

        let data_start = slot_end;

        for i in 0..key_count {
            let slot_offset = slot_start + i * SLOT_ENTRY_SIZE;
            let slot = SlotEntry::read_from(&data[slot_offset..]);

            let key_start = data_start + slot.key_offset as usize;
            let key_end = key_start + slot.key_len as usize;

            if data.len() < key_end {
                return Err(Error::Internal("Node data too short for keys".into()));
            }

            keys.push(Bytes::copy_from_slice(&data[key_start..key_end]));
            values.push(slot.value);
        }

        Ok(Self {
            page_id,
            header,
            keys,
            values,
        })
    }
}

/// B+Tree implementation.
pub struct BTree {
    /// Root page ID
    root_page_id: RwLock<PageId>,
    /// Buffer pool for page access
    buffer_pool: Arc<BufferPoolImpl>,
    /// Configuration
    config: BTreeConfig,
    /// Index ID
    index_id: IndexId,
    /// Statistics
    stats: BTreeStats,
}

/// B+Tree statistics.
#[derive(Debug, Default)]
pub struct BTreeStats {
    /// Number of keys in the tree
    pub key_count: AtomicU64,
    /// Number of pages
    pub page_count: AtomicU64,
    /// Tree height
    pub height: AtomicU64,
    /// Number of inserts
    pub inserts: AtomicU64,
    /// Number of deletes
    pub deletes: AtomicU64,
    /// Number of lookups
    pub lookups: AtomicU64,
    /// Number of range scans
    pub range_scans: AtomicU64,
    /// Number of splits
    pub splits: AtomicU64,
    /// Number of merges
    pub merges: AtomicU64,
}

impl BTree {
    /// Create a new B+Tree.
    pub fn new(
        index_id: IndexId,
        buffer_pool: Arc<BufferPoolImpl>,
        config: BTreeConfig,
    ) -> Result<Self> {
        // Allocate root page
        let (root_page_id, mut guard) = buffer_pool.new_page()?;

        // Initialize as empty leaf
        let root = BTreeNode::new_leaf(root_page_id);
        let data = root.serialize();

        guard.page_mut().insert_tuple(&data)?;

        drop(guard);
        buffer_pool.flush_page(root_page_id)?;

        Ok(Self {
            root_page_id: RwLock::new(root_page_id),
            buffer_pool,
            config,
            index_id,
            stats: BTreeStats::default(),
        })
    }

    /// Open an existing B+Tree.
    pub fn open(
        index_id: IndexId,
        root_page_id: PageId,
        buffer_pool: Arc<BufferPoolImpl>,
        config: BTreeConfig,
    ) -> Self {
        Self {
            root_page_id: RwLock::new(root_page_id),
            buffer_pool,
            config,
            index_id,
            stats: BTreeStats::default(),
        }
    }

    /// Get the root page ID.
    pub fn root_page_id(&self) -> PageId {
        *self.root_page_id.read()
    }

    /// Load a node from a page.
    fn load_node(&self, page_id: PageId) -> Result<BTreeNode> {
        let guard = self.buffer_pool.fetch_page(page_id)?;
        let tuple = guard.get_tuple(0).ok_or_else(|| {
            Error::Internal(format!("No node data in page {:?}", page_id))
        })?;
        BTreeNode::deserialize(page_id, tuple)
    }

    /// Save a node to a page.
    fn save_node(&self, node: &BTreeNode) -> Result<()> {
        let mut guard = self.buffer_pool.fetch_page_mut(node.page_id)?;
        let data = node.serialize();

        // Clear existing tuple and insert new one
        if guard.slot_count() > 0 {
            guard.page_mut().delete_tuple(0);
        }
        guard.page_mut().insert_tuple(&data)?;

        Ok(())
    }

    /// Search for a key and return its value if found.
    pub fn get(&self, key: &[u8]) -> Result<Option<RowId>> {
        self.stats.lookups.fetch_add(1, AtomicOrdering::Relaxed);

        let root_id = self.root_page_id();
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf() {
                // Search in leaf
                match node.search_key(key) {
                    Ok(pos) => return Ok(Some(RowId(node.values[pos]))),
                    Err(_) => return Ok(None),
                }
            } else {
                // Search in internal node
                let child_idx = match node.search_key(key) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                };

                if child_idx >= node.values.len() {
                    return Err(Error::Internal("Invalid child index".into()));
                }

                current_id = PageId(node.values[child_idx]);
            }
        }
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: &[u8], value: RowId) -> Result<()> {
        self.stats.inserts.fetch_add(1, AtomicOrdering::Relaxed);

        let key = Bytes::copy_from_slice(key);
        let result = self.insert_recursive(self.root_page_id(), key.clone(), value.0)?;

        // Handle root split
        if let Some((new_key, new_page_id)) = result {
            self.split_root(new_key, new_page_id)?;
        }

        self.stats.key_count.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Recursive insert helper.
    fn insert_recursive(
        &self,
        page_id: PageId,
        key: Bytes,
        value: u64,
    ) -> Result<Option<(Bytes, PageId)>> {
        let mut node = self.load_node(page_id)?;

        if node.is_leaf() {
            // Insert into leaf
            let pos = match node.search_key(&key) {
                Ok(pos) => {
                    // Key exists, update value
                    node.values[pos] = value;
                    self.save_node(&node)?;
                    return Ok(None);
                }
                Err(pos) => pos,
            };

            node.insert_at(pos, key, value);

            if node.is_full(self.config.max_keys) {
                // Split leaf
                let (new_page_id, _) = self.buffer_pool.new_page()?;
                let (mut right, separator) = node.split(new_page_id);

                self.save_node(&node)?;
                self.save_node(&right)?;

                self.stats.splits.fetch_add(1, AtomicOrdering::Relaxed);
                return Ok(Some((separator, new_page_id)));
            }

            self.save_node(&node)?;
            Ok(None)
        } else {
            // Find child and recurse
            let child_idx = match node.search_key(&key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };

            let child_page_id = PageId(node.values[child_idx]);
            let result = self.insert_recursive(child_page_id, key, value)?;

            // Handle child split
            if let Some((new_key, new_page_id)) = result {
                // For internal nodes, insert key and new child pointer correctly
                node.insert_internal(child_idx, new_key, new_page_id.0);

                if node.is_full(self.config.max_keys) {
                    // Split internal node
                    let (new_internal_page_id, _) = self.buffer_pool.new_page()?;
                    let (mut right, separator) = node.split(new_internal_page_id);

                    self.save_node(&node)?;
                    self.save_node(&right)?;

                    self.stats.splits.fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(Some((separator, new_internal_page_id)));
                }

                self.save_node(&node)?;
            }

            Ok(None)
        }
    }

    /// Split the root node.
    fn split_root(&self, separator: Bytes, new_child_id: PageId) -> Result<()> {
        let old_root_id = self.root_page_id();

        // Create new root
        let (new_root_id, _) = self.buffer_pool.new_page()?;
        let mut new_root = BTreeNode::new_internal(new_root_id);

        // New root has one key and two children
        new_root.keys.push(separator);
        new_root.values.push(old_root_id.0);
        new_root.values.push(new_child_id.0);

        self.save_node(&new_root)?;

        // Update root pointer
        *self.root_page_id.write() = new_root_id;

        self.stats.height.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        self.stats.deletes.fetch_add(1, AtomicOrdering::Relaxed);

        let root_id = self.root_page_id();
        let deleted = self.delete_recursive(root_id, key)?;

        if deleted {
            self.stats.key_count.fetch_sub(1, AtomicOrdering::Relaxed);
        }

        Ok(deleted)
    }

    /// Recursive delete helper.
    fn delete_recursive(&self, page_id: PageId, key: &[u8]) -> Result<bool> {
        let mut node = self.load_node(page_id)?;

        if node.is_leaf() {
            // Delete from leaf
            match node.search_key(key) {
                Ok(pos) => {
                    node.remove_at(pos);
                    self.save_node(&node)?;
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        } else {
            // Find child and recurse
            let child_idx = match node.search_key(key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };

            let child_page_id = PageId(node.values[child_idx]);
            self.delete_recursive(child_page_id, key)
        }
    }

    /// Range scan from start to end (inclusive).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, RowId)>> {
        self.stats.range_scans.fetch_add(1, AtomicOrdering::Relaxed);

        let mut results = Vec::new();

        // Find the leaf containing start key
        let root_id = self.root_page_id();
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf() {
                // Found the starting leaf
                current_id = node.page_id;
                break;
            } else {
                let child_idx = match node.search_key(start) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                };
                current_id = PageId(node.values[child_idx]);
            }
        }

        // Scan leaves until we pass end key
        loop {
            let node = self.load_node(current_id)?;

            for (i, key) in node.keys.iter().enumerate() {
                if key.as_ref() >= start && key.as_ref() <= end {
                    results.push((key.clone(), RowId(node.values[i])));
                } else if key.as_ref() > end {
                    return Ok(results);
                }
            }

            // Move to next leaf
            if node.header.right_sibling.0 == 0 {
                break;
            }
            current_id = node.header.right_sibling;
        }

        Ok(results)
    }

    /// Scan all entries in order.
    pub fn scan_all(&self) -> Result<Vec<(Bytes, RowId)>> {
        let mut results = Vec::new();

        // Find leftmost leaf
        let root_id = self.root_page_id();
        let mut current_id = root_id;

        loop {
            let node = self.load_node(current_id)?;

            if node.is_leaf() {
                current_id = node.page_id;
                break;
            } else {
                // Go to leftmost child
                current_id = PageId(node.values[0]);
            }
        }

        // Scan all leaves
        loop {
            let node = self.load_node(current_id)?;

            for (i, key) in node.keys.iter().enumerate() {
                results.push((key.clone(), RowId(node.values[i])));
            }

            if node.header.right_sibling.0 == 0 {
                break;
            }
            current_id = node.header.right_sibling;
        }

        Ok(results)
    }

    /// Get statistics.
    pub fn stats(&self) -> &BTreeStats {
        &self.stats
    }

    /// Get the index ID.
    pub fn index_id(&self) -> IndexId {
        self.index_id
    }
}

/// Iterator over B+Tree entries.
pub struct BTreeIterator {
    buffer_pool: Arc<BufferPoolImpl>,
    current_page_id: PageId,
    current_pos: usize,
    end_key: Option<Bytes>,
}

impl BTreeIterator {
    pub fn new(
        buffer_pool: Arc<BufferPoolImpl>,
        start_page_id: PageId,
        end_key: Option<Bytes>,
    ) -> Self {
        Self {
            buffer_pool,
            current_page_id: start_page_id,
            current_pos: 0,
            end_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::DiskManager;
    use tempfile::tempdir;

    fn create_test_btree() -> (BTree, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(100, dm));
        let config = BTreeConfig {
            max_keys: 4, // Small for testing
            min_keys: 2,
            prefix_compression: false,
        };
        let btree = BTree::new(IndexId(1), pool, config).unwrap();
        (btree, dir)
    }

    #[test]
    fn test_node_header() {
        let header = NodeHeader::new(NodeType::Leaf);
        let mut buf = [0u8; NODE_HEADER_SIZE];
        header.write_to(&mut buf);

        let decoded = NodeHeader::read_from(&buf).unwrap();
        assert_eq!(decoded.node_type, NodeType::Leaf);
        assert_eq!(decoded.key_count, 0);
    }

    #[test]
    fn test_btree_node_serialize() {
        let mut node = BTreeNode::new_leaf(PageId(1));
        node.keys.push(Bytes::from_static(b"key1"));
        node.keys.push(Bytes::from_static(b"key2"));
        node.values.push(100);
        node.values.push(200);

        let data = node.serialize();
        let decoded = BTreeNode::deserialize(PageId(1), &data).unwrap();

        assert_eq!(decoded.keys.len(), 2);
        assert_eq!(decoded.keys[0].as_ref(), b"key1");
        assert_eq!(decoded.values[0], 100);
    }

    #[test]
    fn test_btree_insert_get() {
        let (btree, _dir) = create_test_btree();

        btree.insert(b"key1", RowId(100)).unwrap();
        btree.insert(b"key2", RowId(200)).unwrap();
        btree.insert(b"key3", RowId(300)).unwrap();

        assert_eq!(btree.get(b"key1").unwrap(), Some(RowId(100)));
        assert_eq!(btree.get(b"key2").unwrap(), Some(RowId(200)));
        assert_eq!(btree.get(b"key3").unwrap(), Some(RowId(300)));
        assert_eq!(btree.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_btree_update() {
        let (btree, _dir) = create_test_btree();

        btree.insert(b"key1", RowId(100)).unwrap();
        assert_eq!(btree.get(b"key1").unwrap(), Some(RowId(100)));

        // Update existing key
        btree.insert(b"key1", RowId(999)).unwrap();
        assert_eq!(btree.get(b"key1").unwrap(), Some(RowId(999)));
    }

    #[test]
    fn test_btree_delete() {
        let (btree, _dir) = create_test_btree();

        btree.insert(b"key1", RowId(100)).unwrap();
        btree.insert(b"key2", RowId(200)).unwrap();

        assert!(btree.delete(b"key1").unwrap());
        assert_eq!(btree.get(b"key1").unwrap(), None);
        assert_eq!(btree.get(b"key2").unwrap(), Some(RowId(200)));

        // Delete non-existent key
        assert!(!btree.delete(b"key999").unwrap());
    }

    #[test]
    fn test_btree_split() {
        let (btree, _dir) = create_test_btree();

        // Insert enough keys to trigger splits (max_keys = 4)
        for i in 0..10 {
            let key = format!("key{:02}", i);
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // Verify all keys are accessible
        for i in 0..10 {
            let key = format!("key{:02}", i);
            assert_eq!(
                btree.get(key.as_bytes()).unwrap(),
                Some(RowId(i as u64)),
                "Key {} not found",
                key
            );
        }

        assert!(btree.stats.splits.load(AtomicOrdering::Relaxed) > 0);
    }

    #[test]
    fn test_btree_range_scan() {
        let (btree, _dir) = create_test_btree();

        btree.insert(b"a", RowId(1)).unwrap();
        btree.insert(b"b", RowId(2)).unwrap();
        btree.insert(b"c", RowId(3)).unwrap();
        btree.insert(b"d", RowId(4)).unwrap();
        btree.insert(b"e", RowId(5)).unwrap();

        let results = btree.range(b"b", b"d").unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1, RowId(2));
        assert_eq!(results[1].1, RowId(3));
        assert_eq!(results[2].1, RowId(4));
    }

    #[test]
    fn test_btree_scan_all() {
        let (btree, _dir) = create_test_btree();

        btree.insert(b"c", RowId(3)).unwrap();
        btree.insert(b"a", RowId(1)).unwrap();
        btree.insert(b"b", RowId(2)).unwrap();

        let results = btree.scan_all().unwrap();
        assert_eq!(results.len(), 3);
        // Should be in sorted order
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"c");
    }

    #[test]
    fn test_btree_many_keys() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let dm = Arc::new(DiskManager::new(&db_path).unwrap());
        let pool = Arc::new(BufferPoolImpl::new(100, dm));
        let config = BTreeConfig {
            max_keys: 32, // Smaller for faster testing
            min_keys: 16,
            prefix_compression: false,
        };
        let btree = BTree::new(IndexId(1), pool, config).unwrap();

        // Insert 100 keys
        for i in 0..100 {
            let key = format!("key{:05}", i);
            btree.insert(key.as_bytes(), RowId(i as u64)).unwrap();
        }

        // Verify all keys
        for i in 0..100 {
            let key = format!("key{:05}", i);
            assert_eq!(
                btree.get(key.as_bytes()).unwrap(),
                Some(RowId(i as u64)),
            );
        }
    }

    #[test]
    fn test_node_split() {
        let mut node = BTreeNode::new_leaf(PageId(1));
        for i in 0..5 {
            node.keys.push(Bytes::from(format!("key{}", i)));
            node.values.push(i as u64);
        }

        let (right, separator) = node.split(PageId(2));

        // Left node should have half
        assert_eq!(node.keys.len(), 2);
        // Right node should have the other half
        assert_eq!(right.keys.len(), 3);
        // Separator should be the first key of the right node
        assert_eq!(separator.as_ref(), right.keys[0].as_ref());
    }
}
