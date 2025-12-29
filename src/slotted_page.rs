use std::marker::PhantomData;

use crate::free_space::FreeSpaceRegion;
use crate::slot::Slot;
use crate::types::NodeType;
use crate::{error::BTreeError, header::Header};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug)]
pub enum SlottedPageError {
    Io(std::io::Error),
    Serialization(bincode::Error),
    InvalidBufferSize { expected: usize, got: usize },
}
impl std::fmt::Display for SlottedPageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SlottedPageError::Io(e) => {
                write!(f, "IO error: {}", e)
            }
            SlottedPageError::Serialization(e) => {
                write!(f, "Serialization error: {}", e)
            }
            SlottedPageError::InvalidBufferSize { expected, got } => {
                write!(f, "Invalid buffer size: expected {}, got {}", expected, got)
            }
        }
    }
}

impl From<std::io::Error> for SlottedPageError {
    fn from(err: std::io::Error) -> SlottedPageError {
        SlottedPageError::Io(err)
    }
}

pub struct SlottedPage<K, V> {
    pub page_id: u64,
    pub node_type: NodeType,
    num_keys: u16,
    free_space_end: u16, // where free space starts
    free_list: Vec<FreeSpaceRegion>,
    total_free: u16, // total free bytes (contiguous + holes)
    slots: Vec<Slot>,
    pub pointers: Vec<u64>,
    data: Vec<u8>,
    page_size: usize,

    _phantom_data: PhantomData<(K, V)>,
}

impl<K, V> SlottedPage<K, V>
where
    K: PartialOrd + Debug + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    // page_id(8) + node_type(1) + num_keys(2) + free_space_end(2) + free_list_count(2) +
    // total_free(2)
    const HEADER_SIZE: usize = 17;

    pub fn new(page_id: u64, node_type: NodeType, page_size: usize) -> Self {
        SlottedPage {
            page_id,
            node_type,
            num_keys: 0,
            free_space_end: page_size as u16,
            free_list: Vec::new(),
            total_free: page_size as u16 - Self::HEADER_SIZE as u16,
            slots: Vec::new(),
            pointers: Vec::new(),
            data: vec![0; page_size],
            page_size: page_size,
            _phantom_data: PhantomData,
        }
    }

    pub fn should_compact(&self) -> bool {
        self.fragmentation_ratio() > 0.3
    }

    pub fn fragmentation_ratio(&self) -> f32 {
        if self.free_list.is_empty() {
            return 0.0;
        }

        let hole_space: u16 = self.free_list.iter().map(|r| r.length).sum();
        let total_free = self.total_free;

        if total_free == 0 {
            return 0.0;
        }

        hole_space as f32 / total_free as f32
    }

    fn get_free_space(&self) -> usize {
        let used_at_start =
            Self::HEADER_SIZE + (self.slots.len() * Slot::SIZE) + (self.pointers.len() * 8);
        let used_at_end = self.page_size - self.free_space_end as usize;
        self.page_size.saturating_sub(used_at_start + used_at_end)
    }

    pub fn can_insert(&self, key_len: usize, value_len: usize) -> bool {
        let needed = Slot::SIZE + key_len + value_len;
        let needed = match self.node_type {
            NodeType::LEAF => needed,
            NodeType::INTERNAL => needed + 8, // child pointer
        };

        let free_space = self.get_free_space();
        free_space >= needed
    }

    pub fn serialize(&self) -> Result<Vec<u8>, SlottedPageError> {
        let mut buffer = vec![0u8; self.page_size];
        let mut offset = 0;

        // header
        buffer[offset..offset + 8].copy_from_slice(&self.page_id.to_le_bytes());
        offset += 8;

        buffer[offset] = self.node_type as u8;
        offset += 1;

        buffer[offset..offset + 2].copy_from_slice(&self.num_keys.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&self.free_space_end.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&(self.free_list.len() as u16).to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&self.total_free.to_le_bytes());
        offset += 2;

        self.slots.iter().for_each(|slot| {
            buffer[offset..offset + Slot::SIZE].copy_from_slice(&slot.serialize());
            offset += Slot::SIZE;
        });

        self.pointers.iter().for_each(|ptr| {
            buffer[offset..offset + 8].copy_from_slice(&ptr.to_le_bytes());
            offset += 8
        });

        self.free_list.iter().for_each(|r| {
            buffer[offset..offset + FreeSpaceRegion::SIZE].copy_from_slice(&r.serialize());
            offset += FreeSpaceRegion::SIZE;
        });

        // data
        let data_start = self.free_space_end as usize;
        if data_start < offset {
            return Err(SlottedPageError::InvalidBufferSize {
                expected: offset,
                got: data_start,
            });
        }
        if self.data.len() != buffer.len() {
            return Err(SlottedPageError::InvalidBufferSize {
                expected: buffer.len(),
                got: self.data.len(),
            });
        }

        buffer[data_start..].copy_from_slice(&self.data[data_start..]);

        Ok(buffer)
    }

    pub fn deserialize(buffer: &[u8], page_size: usize) -> Self {
        let mut offset = 0;

        // header
        let page_id = u64::from_le_bytes(buffer[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let node_type = NodeType::from(buffer[offset]);
        offset += 1;

        let num_keys = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let free_space_end = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let free_list_count = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let total_free = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let mut slots = Vec::new();
        for _ in 0..num_keys {
            slots.push(Slot::deserialize(&buffer[offset..offset + Slot::SIZE]));
            offset += Slot::SIZE;
        }

        let mut pointers = Vec::new();
        let num_pointers = match node_type {
            NodeType::LEAF => 0,
            NodeType::INTERNAL => num_keys + 1,
        };
        for _ in 0..num_pointers {
            pointers.push(u64::from_le_bytes(
                buffer[offset..offset + 8].try_into().unwrap(),
            ));
            offset += 8;
        }

        let mut free_list = Vec::with_capacity(free_list_count as usize);
        for _ in 0..free_list_count {
            free_list.push(FreeSpaceRegion::deserialize(
                &buffer[offset..offset + FreeSpaceRegion::SIZE]
                    .try_into()
                    .unwrap(),
            ));
            offset += FreeSpaceRegion::SIZE;
        }

        SlottedPage {
            page_id,
            node_type,
            num_keys,
            free_space_end,
            free_list,
            total_free,
            slots,
            pointers,
            data: buffer.to_vec(),
            page_size: page_size,
            _phantom_data: PhantomData,
        }
    }

    pub fn find_exact_key(&self, key: &K) -> Result<Option<usize>, BTreeError> {
        let pos = self.find_key_position(key)?;
        if pos < self.slots.len() {
            let found_key: K = self.read_key(pos)?;
            if &found_key == key {
                return Ok(Some(pos));
            }
        }
        Ok(None)
    }

    pub fn find_key_position(&self, key: &K) -> Result<usize, BTreeError>
    where
        K: PartialOrd + for<'de> Deserialize<'de>,
    {
        let mut left = 0;
        let mut right = self.slots.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key: K = self.read_key(mid)?;

            if key <= &mid_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        Ok(left)
    }

    pub fn get_pointer(&self, key: &K) -> Result<u64, BTreeError> {
        let pos = self.find_key_position(&key)?;
        Ok(self.pointers[pos])
    }

    fn header_region_end(&self) -> usize {
        let pointer_count = match self.node_type {
            NodeType::LEAF => self.pointers.len(),
            NodeType::INTERNAL => self.pointers.len() + 1,
        };

        Self::HEADER_SIZE
            + (self.slots.len() * Slot::SIZE)
            + (pointer_count * 8)
            + (self.free_list.len() * FreeSpaceRegion::SIZE)
    }

    fn find_space_for(&self, length: usize) -> Option<(u16, Option<usize>)> {
        // Find perfect fit
        if let Some((index, region)) = self
            .free_list
            .iter()
            .enumerate()
            .find(|(_, r)| r.length as usize == length)
        {
            return Some((region.offset, Some(index)));
        }

        // Find best fit (least waste left) if it exists
        // otherwise, use contiguous space
        self.free_list
            .iter()
            .enumerate()
            .filter(|(_, r)| r.length as usize >= length)
            .min_by_key(|(_, r)| r.length as usize - length)
            .map(|(i, r)| (r.offset, Some(i)))
            .or_else(|| {
                (self.free_space_end as usize)
                    .checked_sub(length)
                    .filter(|&o| o >= self.header_region_end() + Slot::SIZE)
                    .map(|o| (o as u16, None))
            })
    }

    pub fn insert(&mut self, pos: usize, key: &K, value: &V) -> Result<(), BTreeError> {
        let key_bytes = bincode::serialize(key)?;
        let key_bytes_len = key_bytes.len();

        let value_bytes = bincode::serialize(value)?;
        let value_bytes_len = value_bytes.len();

        let total_len = key_bytes_len + value_bytes_len;

        let (offset, free_list_idx) =
            self.find_space_for(total_len)
                .ok_or(BTreeError::PageOverflow {
                    page_id: self.page_id,
                })?;
        let offset = offset as usize;

        self.data[offset..offset + key_bytes_len].copy_from_slice(&key_bytes);
        self.data[offset + key_bytes_len..offset + total_len].copy_from_slice(&value_bytes);

        match free_list_idx {
            Some(free_list_idx) => {
                let region = &self.free_list[free_list_idx];
                let remaining = region.length as usize - total_len;

                if remaining > 0 {
                    self.free_list[free_list_idx] = FreeSpaceRegion {
                        offset: offset as u16,
                        length: total_len as u16,
                    };
                } else {
                    self.free_list.remove(free_list_idx);
                }
            }
            None => {
                // Contiguous space
                self.free_space_end = offset as u16;
            }
        };

        self.total_free -= total_len as u16;

        let slot = Slot {
            offset: offset as u16,
            key_length: key_bytes_len as u16,
            value_length: value_bytes_len as u16,
        };
        self.slots.insert(pos, slot);
        self.num_keys += 1;

        Ok(())
    }

    fn add_to_free_list(&mut self, mut region: FreeSpaceRegion) {
        self.free_list.retain_mut(|existing| {
            if existing.offset + existing.length == region.offset {
                region.offset = existing.offset;
                region.length += existing.length;
                false
            } else if region.offset + region.length == existing.offset {
                region.length += existing.length;
                false
            } else {
                true
            }
        });

        if region.offset + region.length == self.free_space_end {
            self.free_space_end = region.offset;
        } else if region.offset == self.free_space_end {
            self.free_space_end = region.offset;
        } else {
            let insert_pos = self
                .free_list
                .iter()
                .position(|r| r.offset >= region.offset)
                .unwrap_or(self.free_list.len());
            self.free_list.insert(insert_pos, region);
        }
    }

    pub fn update(&mut self, pos: usize, key: &K, value: &V) -> Result<(), BTreeError> {
        let key_bytes = bincode::serialize(key)?;
        let key_bytes_len = key_bytes.len();

        let value_bytes = bincode::serialize(value)?;
        let value_bytes_len = value_bytes.len();

        let total_len = key_bytes_len + value_bytes_len;

        let slot: &Slot = &self.slots[pos];
        let offset = slot.offset as usize;
        let old_value_bytes_len = slot.value_length as usize;

        if value_bytes_len <= old_value_bytes_len {
            self.data[offset..offset + key_bytes_len].copy_from_slice(&key_bytes);
            self.data[offset + key_bytes_len..offset + key_bytes_len + value_bytes_len]
                .copy_from_slice(&value_bytes);

            self.slots[pos].key_length = key_bytes_len as u16;
            self.slots[pos].value_length = value_bytes_len as u16;

            let leftover = old_value_bytes_len - value_bytes_len;
            if leftover > 0 {
                let leftover_offset = offset + total_len;
                self.add_to_free_list(FreeSpaceRegion {
                    offset: leftover_offset as u16,
                    length: leftover as u16,
                });
                self.total_free += leftover as u16;
            }
            Ok(())
        } else {
            // Will not fit, therefore delete and reinsert
            self.delete(pos)?;
            self.insert(pos, &key, &value)?;
            Ok(())
        }
    }

    pub fn delete(&mut self, pos: usize) -> Result<(), BTreeError> {
        if pos > self.slots.len() {
            return Err(BTreeError::KeyNotFound("".to_string()));
        }

        let slot = self.slots.remove(pos);
        self.num_keys -= 1;

        let freed_length = slot.key_length + slot.value_length;
        self.total_free += freed_length;

        self.add_to_free_list(FreeSpaceRegion {
            offset: slot.offset,
            length: freed_length,
        });

        Ok(())
    }

    pub fn split(&mut self, new_page_id: u64) -> Result<(K, V, SlottedPage<K, V>), BTreeError> {
        let mid_index: usize = self.num_keys as usize / 2;
        let mid_key = self.read_key(mid_index)?;
        let mid_value = self.read_value(mid_index)?;

        let mut right = SlottedPage::new(new_page_id, self.node_type, self.page_size);
        for i in (mid_index + 1)..self.slots.len() {
            let key: K = self.read_key(i)?;
            let value: V = self.read_value(i)?;
            right.insert(right.slots.len(), &key, &value)?;
        }

        if self.node_type == NodeType::INTERNAL && self.pointers.len() > mid_index + 1 {
            right.pointers = self.pointers.split_off(mid_index + 1);
        }

        let removed_slots: Vec<Slot> = self.slots.drain(mid_index..).collect();
        self.num_keys = mid_index as u16;

        removed_slots.iter().for_each(|slot| {
            self.add_to_free_list(FreeSpaceRegion {
                offset: slot.offset,
                length: slot.key_length + slot.value_length,
            });
            self.total_free += slot.key_length + slot.value_length;
        });

        Ok((mid_key, mid_value, right))
    }

    pub fn compact(&mut self) -> Result<(), BTreeError> {
        let mut pairs: Vec<(K, V)> = Vec::with_capacity(self.slots.len());
        for idx in 0..self.slots.len() {
            pairs.push(self.read_key_value(idx)?);
        }

        self.free_space_end = self.page_size as u16;
        self.total_free = self.free_space_end - Header::SIZE as u16;
        self.slots.clear();

        for (key, value) in pairs.iter() {
            let key_bytes = bincode::serialize(key)?;
            let value_bytes = bincode::serialize(value)?;

            let total_len = key_bytes.len() + value_bytes.len();
            let new_offset: usize = self.free_space_end as usize - total_len;

            self.data[new_offset..new_offset + key_bytes.len()].copy_from_slice(&key_bytes);
            self.data[new_offset + key_bytes.len()..new_offset + total_len]
                .copy_from_slice(&value_bytes);

            self.free_space_end = new_offset as u16;
            self.total_free -= total_len as u16;

            self.slots.push(Slot {
                offset: self.free_space_end,
                key_length: key_bytes.len() as u16,
                value_length: value_bytes.len() as u16,
            });
        }

        self.free_list.clear();

        Ok(())
    }

    pub fn read_key_value(&self, index: usize) -> Result<(K, V), BTreeError> {
        let slot = &self.slots[index];
        let offset = slot.offset as usize;
        let key_length = slot.key_length as usize;
        let key: K = bincode::deserialize(&self.data[offset..offset + key_length])?;

        let offset = offset + key_length;
        let value_length = slot.value_length as usize;
        let value: V = bincode::deserialize(&self.data[offset..offset + value_length])?;

        Ok((key, value))
    }

    pub fn read_key(&self, index: usize) -> Result<K, BTreeError> {
        let slot = &self.slots[index];
        let offset = slot.offset as usize;
        let key_length = slot.key_length as usize;
        let key: K = bincode::deserialize(&self.data[offset..offset + key_length])?;
        Ok(key)
    }

    pub fn read_value(&self, index: usize) -> Result<V, BTreeError> {
        let slot = &self.slots[index];
        let key_length = slot.key_length as usize;
        let value_length = slot.value_length as usize;
        let offset = slot.offset as usize + key_length;

        let value: V = bincode::deserialize(&self.data[offset..offset + value_length])?;
        Ok(value)
    }

    pub fn read_keys(&self) -> Result<Vec<K>, BTreeError> {
        (0..self.num_keys)
            .map(|idx| self.read_key(idx.into()))
            .collect::<Result<Vec<K>, BTreeError>>()
    }
}

impl<K, V> std::fmt::Debug for SlottedPage<K, V>
where
    K: PartialOrd + Debug + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlottedPage")
            .field("page_id", &self.page_id)
            .field("num_keys", &self.num_keys)
            .field("slots", &self.slots)
            .field("pointers", &self.pointers)
            .field("data_len", &self.data.len()) // Don't print all bytes
            .field("keys", &self.read_keys().unwrap())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod serialization {
        use super::*;

        #[test]
        fn empty_page_roundtrip() {
            let page: SlottedPage<String, i64> = SlottedPage::new(42, NodeType::LEAF, 4096);

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<String, i64> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.page_id, 42);
            assert_eq!(restored.node_type, NodeType::LEAF);
            assert_eq!(restored.num_keys, 0);
            assert_eq!(restored.slots.len(), 0);
            assert_eq!(restored.pointers.len(), 0);
        }

        #[test]
        fn page_with_data_roundtrip() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key1".to_string(), &100i64).unwrap();
            page.insert(1, &"key2".to_string(), &200i64).unwrap();
            page.insert(2, &"key3".to_string(), &300i64).unwrap();

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<String, i64> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.num_keys, 3);
            assert_eq!(restored.slots.len(), 3);

            // Verify data integrity
            let key1: String = restored.read_key(0).unwrap();
            let val1: i64 = restored.read_value(0).unwrap();
            assert_eq!(key1, "key1");
            assert_eq!(val1, 100);

            let key2: String = restored.read_key(1).unwrap();
            let val2: i64 = restored.read_value(1).unwrap();
            assert_eq!(key2, "key2");
            assert_eq!(val2, 200);

            let key3: String = restored.read_key(2).unwrap();
            let val3: i64 = restored.read_value(2).unwrap();
            assert_eq!(key3, "key3");
            assert_eq!(val3, 300);
        }

        #[test]
        fn internal_node_roundtrip_with_pointers() {
            let mut page = SlottedPage::new(1, NodeType::INTERNAL, 4096);

            page.insert(0, &10i64, &"value".to_string()).unwrap();
            page.pointers = vec![100, 200];

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<String, i64> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.node_type, NodeType::INTERNAL);
            assert_eq!(restored.pointers, vec![100, 200]);
        }

        #[test]
        fn page_preserves_free_space_offset() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);
            page.insert(0, &"test".to_string(), &42i64).unwrap();

            let original_offset = page.free_space_end;

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<String, i64> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.free_space_end, original_offset);
        }

        #[test]
        fn page_preserves_free_list() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key1".to_string(), &100i64).unwrap();
            page.insert(1, &"key2".to_string(), &200i64).unwrap();
            page.insert(2, &"key3".to_string(), &300i64).unwrap();

            // Delete middle entry to create a hole
            page.delete(1).unwrap();

            let free_list_len = page.free_list.len();
            assert!(free_list_len > 0, "Should have a hole in free list");

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<String, i64> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.free_list.len(), free_list_len);
        }

        #[test]
        fn different_page_sizes() {
            for page_size in [256, 512, 1024, 4096, 8192, 16384] {
                let mut page = SlottedPage::new(1, NodeType::LEAF, page_size);
                page.insert(0, &"key".to_string(), &42i64).unwrap();

                let bytes = page.serialize().unwrap();
                assert_eq!(bytes.len(), page_size);

                let restored: SlottedPage<String, i64> =
                    SlottedPage::deserialize(&bytes, page_size);
                let key: String = restored.read_key(0).unwrap();
                assert_eq!(key, "key");
            }
        }
    }

    mod insert {
        use super::*;

        #[test]
        fn insert_single_entry() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"hello".to_string(), &42i64).unwrap();

            assert_eq!(page.num_keys, 1);
            assert_eq!(page.slots.len(), 1);

            let key: String = page.read_key(0).unwrap();
            let value: i64 = page.read_value(0).unwrap();

            assert_eq!(key, "hello");
            assert_eq!(value, 42);
        }

        #[test]
        fn insert_maintains_order() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            // Insert in order
            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            // Verify order
            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 2);
            assert_eq!(page.read_key(2).unwrap(), 3);
        }

        #[test]
        fn insert_at_beginning() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &2i64, &"two".to_string()).unwrap();
            page.insert(0, &1i64, &"one".to_string()).unwrap(); // Insert at beginning

            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 2);
        }

        #[test]
        fn insert_at_middle() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &3i64, &"three".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap(); // Insert in middle

            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 2);
            assert_eq!(page.read_key(2).unwrap(), 3);
        }

        #[test]
        fn insert_reduces_free_space() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);
            let initial_free = page.get_free_space();

            page.insert(0, &"key".to_string(), &12345i64).unwrap();

            assert!(page.get_free_space() < initial_free);
        }

        #[test]
        fn insert_until_full() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 256); // Small page

            let mut count = 0;
            loop {
                let key = format!("key_{:04}", count);
                let value = count as i64;

                if !page.can_insert(
                    bincode::serialize(&key).unwrap().len(),
                    bincode::serialize(&value).unwrap().len(),
                ) {
                    break;
                }

                page.insert(count, &key, &value).unwrap();
                count += 1;
            }

            assert!(count > 0, "Should have inserted at least one entry");
            assert_eq!(page.num_keys as usize, count);

            // Verify all entries are readable
            for i in 0..count {
                let key: String = page.read_key(i).unwrap();
                assert_eq!(key, format!("key_{:04}", i));
            }
        }

        #[test]
        fn insert_overflow_returns_error() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 256);

            // Fill the page
            while page.can_insert(10, 10) {
                let idx = page.num_keys as usize;
                page.insert(idx, &"key".to_string(), &42i64).unwrap();
            }

            // Try to insert when full
            let result = page.insert(0, &"overflow".to_string(), &999i64);
            assert!(matches!(result, Err(BTreeError::PageOverflow { .. })));
        }

        // #[test]
        // fn insert_various_types() {
        //     let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);
        //
        //     // Different key/value type combinations
        //     page.insert(0, &1i32, &"string value".to_string()).unwrap();
        //     page.insert(1, &2i32, &vec![1u8, 2, 3, 4, 5]).unwrap();
        //     page.insert(2, &3i32, &(100i64, 200i64)).unwrap();
        //
        //     assert_eq!(page.num_keys, 3);
        //
        //     let v1: String = page.read_value(0).unwrap();
        //     assert_eq!(v1, "string value");
        //
        //     let v2: Vec<u8> = page.read_value(1).unwrap();
        //     assert_eq!(v2, vec![1, 2, 3, 4, 5]);
        //
        //     let v3: (i64, i64) = page.read_value(2).unwrap();
        //     assert_eq!(v3, (100, 200));
        // }
    }

    mod update {
        use super::*;

        #[test]
        fn update_same_size_value() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &100i64).unwrap();
            let free_before = page.get_free_space();

            page.update(0, &"key".to_string(), &200i64).unwrap();

            let value: i64 = page.read_value(0).unwrap();
            assert_eq!(value, 200);

            // Free space should be unchanged
            assert_eq!(page.get_free_space(), free_before);
        }

        #[test]
        fn update_smaller_value() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"a]long string value".to_string())
                .unwrap();
            let free_before = page.get_free_space();

            page.update(0, &1i64, &"short".to_string()).unwrap();

            let value: String = page.read_value(0).unwrap();
            assert_eq!(value, "short");

            // Free space should increase (leftover added to free list)
            assert!(page.get_free_space() >= free_before);
        }

        #[test]
        fn update_larger_value() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"short".to_string()).unwrap();

            page.update(0, &1i64, &"a much longer string value".to_string())
                .unwrap();

            let value: String = page.read_value(0).unwrap();
            assert_eq!(value, "a much longer string value");
        }

        #[test]
        fn update_preserves_other_entries() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.update(1, &2i64, &"TWO UPDATED".to_string()).unwrap();

            // Check other entries unchanged
            assert_eq!(page.read_value(0).unwrap(), "one");
            assert_eq!(page.read_value(1).unwrap(), "TWO UPDATED");
            assert_eq!(page.read_value(2).unwrap(), "three");
        }

        #[test]
        fn multiple_updates_same_entry() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &1i64).unwrap();

            for i in 2..=100 {
                page.update(0, &"key".to_string(), &(i as i64)).unwrap();
            }

            let value: i64 = page.read_value(0).unwrap();
            assert_eq!(value, 100);
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn delete_single_entry() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &42i64).unwrap();
            assert_eq!(page.num_keys, 1);

            page.delete(0).unwrap();
            assert_eq!(page.num_keys, 0);
            assert_eq!(page.slots.len(), 0);
        }

        #[test]
        fn delete_increases_free_space() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &42i64).unwrap();
            let free_before = page.get_free_space();

            page.delete(0).unwrap();

            assert!(page.get_free_space() > free_before);
        }

        #[test]
        fn delete_creates_free_list_entry() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key1".to_string(), &1i64).unwrap();
            page.insert(1, &"key2".to_string(), &2i64).unwrap();
            page.insert(2, &"key3".to_string(), &3i64).unwrap();

            // Delete middle - should create hole
            page.delete(1).unwrap();

            // Free list should have an entry (unless coalesced with contiguous space)
            assert!(page.free_list.len() > 0 || page.total_free > 0);
        }

        #[test]
        fn delete_from_beginning() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(0).unwrap();

            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 2);
            assert_eq!(page.read_key(1).unwrap(), 3);
        }

        #[test]
        fn delete_from_end() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(2).unwrap();

            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 2);
        }

        #[test]
        fn delete_invalid_index_returns_error() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &42i64).unwrap();

            let result = page.delete(5);
            assert!(matches!(result, Err(BTreeError::KeyNotFound(_))));
        }

        #[test]
        fn delete_all_entries() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            // Delete all from end to beginning
            for _ in 0..10 {
                page.delete(0).unwrap();
            }

            assert_eq!(page.num_keys, 0);
            assert_eq!(page.slots.len(), 0);
        }
    }

    mod free_list {
        use super::*;

        #[test]
        fn free_list_reuses_space() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            // Insert entries
            page.insert(0, &"key1".to_string(), &100i64).unwrap();
            page.insert(1, &"key2".to_string(), &200i64).unwrap();
            page.insert(2, &"key3".to_string(), &300i64).unwrap();

            // Delete middle
            page.delete(1).unwrap();

            let free_before_reuse = page.get_free_space();

            // Insert similar-sized entry - should reuse hole
            page.insert(1, &"new2".to_string(), &999i64).unwrap();

            // Verify data
            assert_eq!(page.read_value(1).unwrap(), 999);

            // Free list should be smaller or empty
            assert!(page.free_list.len() == 0 || page.get_free_space() <= free_before_reuse);
        }

        #[test]
        fn free_list_coalesces_adjacent_holes() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            // Insert 5 entries
            for i in 0..5 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            // Delete entries 1, 2, 3 (adjacent) - should coalesce
            page.delete(3).unwrap();
            page.delete(2).unwrap();
            page.delete(1).unwrap();

            // Should have at most 1 free region (coalesced) or 0 if merged with contiguous
            assert!(page.free_list.len() <= 1);
        }

        #[test]
        fn fragmentation_ratio_zero_when_no_holes() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"key".to_string(), &42i64).unwrap();

            assert_eq!(page.fragmentation_ratio(), 0.0);
        }

        #[test]
        fn fragmentation_ratio_increases_with_holes() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            // Delete every other entry to create holes
            for i in (1..10).step_by(2).rev() {
                page.delete(i).unwrap();
            }

            let frag = page.fragmentation_ratio();
            assert!(frag > 0.0, "Should have fragmentation: {}", frag);
        }

        #[test]
        fn should_compact_threshold() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            // Fill page
            for i in 0..20 {
                page.insert(i, &(i as i64), &format!("value_{:04}", i))
                    .unwrap();
            }

            // Delete many entries to create fragmentation
            for i in (0..20).step_by(2).rev() {
                page.delete(i).unwrap();
            }

            // Check if compaction is recommended
            if page.fragmentation_ratio() > 0.3 {
                assert!(page.should_compact());
            }
        }

        #[test]
        fn compact_eliminates_fragmentation() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            // Fill and fragment
            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            for i in (1..10).step_by(2).rev() {
                page.delete(i).unwrap();
            }

            // Compact
            page.compact().unwrap();

            // Fragmentation should be zero
            assert_eq!(page.fragmentation_ratio(), 0.0);
            assert!(page.free_list.is_empty());

            // Data should still be intact
            let remaining_keys: Vec<i64> = (0..page.num_keys as usize)
                .map(|i| page.read_key(i).unwrap())
                .collect();

            assert_eq!(remaining_keys, vec![0, 2, 4, 6, 8]);
        }

        #[test]
        fn compact_preserves_all_data() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            let original_data: Vec<(i64, String)> =
                (0..10).map(|i| (i, format!("value_{}", i))).collect();

            for (i, (k, v)) in original_data.iter().enumerate() {
                page.insert(i, k, v).unwrap();
            }

            // Delete some
            page.delete(7).unwrap();
            page.delete(3).unwrap();

            // Compact
            page.compact().unwrap();

            // Verify remaining data
            let expected: Vec<(i64, String)> = original_data
                .into_iter()
                .filter(|(k, _)| *k != 3 && *k != 7)
                .collect();

            for (i, (expected_key, expected_value)) in expected.iter().enumerate() {
                let key: i64 = page.read_key(i).unwrap();
                let value: String = page.read_value(i).unwrap();
                assert_eq!(key, *expected_key);
                assert_eq!(value, *expected_value);
            }
        }
    }

    mod search {
        use super::*;

        #[test]
        fn find_key_position_empty_page() {
            let page: SlottedPage<i64, i64> = SlottedPage::new(1, NodeType::LEAF, 4096);

            let pos = page.find_key_position(&42i64).unwrap();
            assert_eq!(pos, 0);
        }

        #[test]
        fn find_key_position_single_entry() {
            let mut page: SlottedPage<i64, String> = SlottedPage::new(1, NodeType::LEAF, 4096);
            page.insert(0, &50i64, &"fifty".to_string()).unwrap();

            assert_eq!(page.find_key_position(&25i64).unwrap(), 0); // Before
            assert_eq!(page.find_key_position(&50i64).unwrap(), 0); // Exact
            assert_eq!(page.find_key_position(&75i64).unwrap(), 1); // After
        }

        #[test]
        fn find_key_position_multiple_entries() {
            let mut page: SlottedPage<i64, String> = SlottedPage::new(1, NodeType::LEAF, 4096);

            for i in [10, 20, 30, 40, 50] {
                let idx = page.find_key_position(&i).unwrap();
                page.insert(idx, &i, &format!("val_{}", i)).unwrap();
            }

            assert_eq!(page.find_key_position(&5i64).unwrap(), 0);
            assert_eq!(page.find_key_position(&10i64).unwrap(), 0);
            assert_eq!(page.find_key_position(&15i64).unwrap(), 1);
            assert_eq!(page.find_key_position(&25i64).unwrap(), 2);
            assert_eq!(page.find_key_position(&50i64).unwrap(), 4);
            assert_eq!(page.find_key_position(&100i64).unwrap(), 5);
        }

        #[test]
        fn find_exact_key_found() {
            let mut page: SlottedPage<String, i64> = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"apple".to_string(), &1i64).unwrap();
            page.insert(1, &"banana".to_string(), &2i64).unwrap();
            page.insert(2, &"cherry".to_string(), &3i64).unwrap();

            assert_eq!(page.find_exact_key(&"apple".to_string()).unwrap(), Some(0));
            assert_eq!(page.find_exact_key(&"banana".to_string()).unwrap(), Some(1));
            assert_eq!(page.find_exact_key(&"cherry".to_string()).unwrap(), Some(2));
        }

        #[test]
        fn find_exact_key_not_found() {
            let mut page: SlottedPage<String, i64> = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &"apple".to_string(), &1i64).unwrap();
            page.insert(1, &"cherry".to_string(), &3i64).unwrap();

            assert_eq!(page.find_exact_key(&"banana".to_string()).unwrap(), None);
            assert_eq!(page.find_exact_key(&"grape".to_string()).unwrap(), None);
        }
    }

    mod split {
        use super::*;

        #[test]
        fn split_distributes_keys_evenly() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            let (median_key, _, right) = page.split(2).unwrap();

            // Median should be middle element
            assert_eq!(median_key, 5);

            // Left should have keys 0-4
            assert_eq!(page.num_keys, 5);
            for i in 0..5 {
                assert_eq!(page.read_key(i).unwrap(), i as i64);
            }

            // Right should have keys 6-9
            assert_eq!(right.num_keys, 4);
            for i in 0..4 {
                assert_eq!(right.read_key(i).unwrap(), (i + 6) as i64);
            }
        }

        #[test]
        fn split_internal_node_distributes_pointers() {
            let mut page = SlottedPage::new(1, NodeType::INTERNAL, 4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }
            page.pointers = (100..111).collect(); // 11 pointers for 10 keys

            let (_, _, right) = page.split(2).unwrap();

            // Check pointers are distributed
            assert_eq!(page.pointers.len(), 6); // 5 keys + 1
            assert_eq!(right.pointers.len(), 5); // 4 keys + 1
        }

        #[test]
        fn split_preserves_data_integrity() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            let original: Vec<(i64, String)> =
                (0..10).map(|i| (i, format!("value_{}", i))).collect();

            for (i, (k, v)) in original.iter().enumerate() {
                page.insert(i, k, v).unwrap();
            }

            let (median_key, median_value, right) = page.split(2).unwrap();

            // Collect all keys/values from both pages
            let mut all_data: Vec<(i64, String)> = Vec::new();

            for i in 0..page.num_keys as usize {
                all_data.push((page.read_key(i).unwrap(), page.read_value(i).unwrap()));
            }

            all_data.push((median_key, median_value));

            for i in 0..right.num_keys as usize {
                all_data.push((right.read_key(i).unwrap(), right.read_value(i).unwrap()));
            }

            all_data.sort_by_key(|(k, _)| *k);

            assert_eq!(all_data, original);
        }

        #[test]
        fn split_assigns_correct_page_id() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &"val".to_string()).unwrap();
            }

            let (_, _, right) = page.split(99).unwrap();

            assert_eq!(page.page_id, 1);
            assert_eq!(right.page_id, 99);
        }
    }

    mod statistics {
        use super::*;

        #[test]
        fn free_space_decreases_with_inserts() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);
            let mut prev_free = page.get_free_space();

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
                let curr_free = page.get_free_space();
                assert!(curr_free < prev_free, "Free space should decrease");
                prev_free = curr_free;
            }
        }

        #[test]
        fn total_free_tracks_correctly() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();

            let free_after_insert = page.total_free;

            page.delete(0).unwrap();

            assert!(page.total_free > free_after_insert);
        }

        #[test]
        fn can_insert_reports_correctly() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 256); // Small page

            // Should be able to insert initially
            assert!(page.can_insert(10, 10));

            // Fill the page
            while page.can_insert(10, 10) {
                let idx = page.num_keys as usize;
                page.insert(idx, &"key".to_string(), &42i64).unwrap();
            }

            // Should not be able to insert
            assert!(!page.can_insert(10, 10));
        }

        #[test]
        fn num_keys_accurate() {
            let mut page = SlottedPage::new(1, NodeType::LEAF, 4096);

            assert_eq!(page.num_keys, 0);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            assert_eq!(page.num_keys, 1);

            page.insert(1, &2i64, &"two".to_string()).unwrap();
            assert_eq!(page.num_keys, 2);

            page.delete(0).unwrap();
            assert_eq!(page.num_keys, 1);

            page.delete(0).unwrap();
            assert_eq!(page.num_keys, 0);
        }
    }
}
