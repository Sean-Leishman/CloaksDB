use crate::error::BTreeError;
use crate::page_manager::PageManager;
use crate::slotted_page::SlottedPage;
use crate::types::NodeType;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Page<K, V> {
    pub page_id: u64,
    pub node_type: NodeType,
    pub keys: Vec<K>,
    pub values: Vec<V>,
    pub pointers: Vec<u64>,
}

impl<K, V> Page<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(node_type: NodeType, page_manager: &mut PageManager) -> Page<K, V> {
        let page_id = page_manager.allocate_page().unwrap();
        Page {
            page_id: page_id,
            node_type: node_type,
            keys: vec![],
            values: vec![],
            pointers: vec![],
        }
    }

    pub fn find_key_position(&self, key: &K) -> usize {
        self.keys
            .iter()
            .position(|k| key <= k)
            .unwrap_or(self.keys.len())
    }

    pub fn insert_key_value(&mut self, key: K, value: V) {
        let pos = self.find_key_position(&key);
        if pos < self.keys.len() && self.keys[pos] == key {
            self.values[pos] = value;
            return;
        }

        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }

    pub fn insert_key_value_node(&mut self, pos: usize, key: K, value: V, new_node_id: u64) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        self.pointers.insert(pos + 1, new_node_id);
    }

    pub fn can_insert_variable(&self, key: &K, value: &V, page_size: usize) -> bool {
        let key_bytes = bincode::serialize(key).unwrap();
        let value_bytes = bincode::serialize(value).unwrap();

        let slotted = SlottedPage::from_page(self, page_size);
        slotted.can_insert(key_bytes.len(), value_bytes.len(), page_size)
    }

    pub fn serialize(&self, page_size: usize) -> Result<Vec<u8>, BTreeError> {
        let slotted = SlottedPage::from_page(self, page_size);
        Ok(slotted.serialize(page_size)?)
    }

    pub fn deserialize(buffer: &[u8]) -> Self {
        let slotted = SlottedPage::deserialize(buffer);
        SlottedPage::to_page(&slotted)
    }
}
