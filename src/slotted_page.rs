use std::{io::Write, marker::PhantomData};

use crate::error::BTreeError;
use crate::slot::Slot;
use crate::types::NodeType;
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
    free_space_offset: u16, // where free space starts
    slots: Vec<Slot>,
    pub pointers: Vec<u64>,
    data: Vec<u8>,

    _phantom_data: PhantomData<(K, V)>,
}

impl<K, V> SlottedPage<K, V>
where
    K: PartialOrd + Debug + PartialEq + Serialize + for<'de> Deserialize<'de>,
    V: Debug + Serialize + for<'de> Deserialize<'de>,
{
    const HEADER_SIZE: usize = 13;

    pub fn new(page_id: u64, node_type: NodeType, page_size: usize) -> Self {
        println!("SlottedPage::new: {}", page_size);
        SlottedPage {
            page_id,
            node_type,
            num_keys: 0,
            free_space_offset: page_size as u16,
            slots: Vec::new(),
            pointers: Vec::new(),
            data: vec![0; page_size],
            _phantom_data: PhantomData,
        }
    }

    fn get_free_space(&self, page_size: usize) -> usize {
        let used_at_start =
            Self::HEADER_SIZE + (self.slots.len() * Slot::SIZE) + (self.pointers.len() * 8);
        let used_at_end = page_size - self.free_space_offset as usize;
        println!(
            "get_free_space ({}): {} - ({} + {})",
            self.page_id, page_size, used_at_start, used_at_end
        );
        page_size.saturating_sub(used_at_start + used_at_end)
    }

    pub fn can_insert(&self, key: &K, value: &V, page_size: usize) -> bool {
        let key_len = bincode::serialize(key).unwrap().len();
        let value_len = bincode::serialize(value).unwrap().len();

        let needed = Slot::SIZE + key_len + value_len;
        let needed = match self.node_type {
            NodeType::LEAF => needed,
            NodeType::INTERNAL => needed + 8, // child pointer
        };

        let free_space = self.get_free_space(page_size);
        println!(
            "can_insert ({}): {} >= {}",
            self.page_id, free_space, needed
        );
        free_space >= needed
    }

    pub fn serialize(&self, page_size: usize) -> Result<Vec<u8>, SlottedPageError> {
        let mut buffer = vec![0u8; page_size];
        let mut offset = 0;

        // header
        buffer[offset..offset + 8].copy_from_slice(&self.page_id.to_le_bytes());
        offset += 8;

        buffer[offset] = self.node_type as u8;
        offset += 1;

        buffer[offset..offset + 2].copy_from_slice(&self.num_keys.to_le_bytes());
        offset += 2;

        buffer[offset..offset + 2].copy_from_slice(&self.free_space_offset.to_le_bytes());
        offset += 2;

        self.slots.iter().for_each(|slot| {
            buffer[offset..offset + Slot::SIZE].copy_from_slice(&slot.serialize());
            offset += Slot::SIZE;
        });

        self.pointers.iter().for_each(|ptr| {
            buffer[offset..offset + 8].copy_from_slice(&ptr.to_le_bytes());
            offset += 8
        });

        // data
        let data_start = self.free_space_offset as usize;
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

    pub fn deserialize(buffer: &[u8]) -> Self {
        let mut offset = 0;

        // header
        let page_id = u64::from_le_bytes(buffer[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let node_type = NodeType::from(buffer[offset]);
        offset += 1;

        let num_keys = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let free_space_offset = u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
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

        SlottedPage {
            page_id,
            node_type,
            num_keys,
            free_space_offset,
            slots,
            pointers,
            data: buffer.to_vec(),
            _phantom_data: PhantomData,
        }
    }

    pub fn find_exact_key(&self, key: &K) -> Result<usize, BTreeError> {
        let pos = self.find_key_position(key)?;
        if pos < self.slots.len() {
            let found_key: K = self.read_key(pos)?;
            if &found_key == key {
                return Ok(pos);
            }
        }
        Err(BTreeError::KeyNotFound("".to_string()))
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

    pub fn insert(&mut self, pos: usize, key: &K, value: &V) -> Result<(), BTreeError> {
        let key_bytes = bincode::serialize(key)?;
        let key_bytes_len = key_bytes.len();

        let value_bytes = bincode::serialize(value)?;
        let value_bytes_len = value_bytes.len();

        if !self.can_insert(key, value, 256) {
            println!("insert failed ({}): {:?}", self.page_id, key);
            return Err(BTreeError::PageOverflow {
                page_id: self.page_id,
            });
        }

        let total_len = key_bytes_len + value_bytes_len;
        let new_offset = self.free_space_offset as usize - total_len;

        self.data[new_offset..new_offset + key_bytes_len].copy_from_slice(&key_bytes);
        self.data[new_offset + key_bytes_len..new_offset + total_len].copy_from_slice(&value_bytes);
        self.free_space_offset = new_offset as u16;

        let slot = Slot {
            offset: new_offset as u16,
            key_length: key_bytes_len as u16,
            value_length: value_bytes_len as u16,
        };
        self.slots.insert(pos, slot);
        self.num_keys += 1;

        Ok(())
    }

    pub fn update(&mut self, pos: usize, key: &K, value: &V) -> Result<(), BTreeError> {
        let key_bytes = bincode::serialize(key)?;
        let key_bytes_len = key_bytes.len();

        let value_bytes = bincode::serialize(value)?;
        let value_bytes_len = value_bytes.len();

        let slot: &Slot = &self.slots[pos];
        let offset = slot.offset as usize;
        let old_value_bytes_len = slot.value_length as usize;

        if value_bytes_len <= old_value_bytes_len {
            self.data[offset..offset + key_bytes_len].copy_from_slice(&key_bytes);
            self.data[offset + key_bytes_len..offset + key_bytes_len + value_bytes_len]
                .copy_from_slice(&value_bytes);

            self.slots[pos].key_length = key_bytes_len as u16;
            self.slots[pos].value_length = value_bytes_len as u16;
            Ok(())
        } else {
            let total_len = key_bytes_len + value_bytes_len;
            if self.get_free_space(256) < total_len {
                println!("her1");
                return Err(BTreeError::PageOverflow {
                    page_id: self.page_id,
                });
            }

            let new_offset = self.free_space_offset as usize - total_len;
            self.data[new_offset..new_offset + key_bytes_len].copy_from_slice(&key_bytes);
            self.data[new_offset + key_bytes_len..new_offset + total_len]
                .copy_from_slice(&value_bytes);

            self.free_space_offset = new_offset as u16;

            self.slots[pos] = Slot {
                offset: new_offset as u16,
                key_length: key_bytes_len as u16,
                value_length: value_bytes_len as u16,
            };

            Ok(())
        }
    }

    pub fn split(&mut self, new_page_id: u64) -> Result<(K, V, SlottedPage<K, V>), BTreeError> {
        let mid_index: usize = self.num_keys as usize / 2;
        let mid_key = self.read_key(mid_index)?;
        let mid_value = self.read_value(mid_index)?;

        let mut right = SlottedPage::new(new_page_id, self.node_type, 256);
        for i in (mid_index + 1)..self.slots.len() {
            let key: K = self.read_key(i)?;
            let value: V = self.read_value(i)?;
            right.insert(right.slots.len(), &key, &value)?;
        }

        if self.node_type == NodeType::INTERNAL && self.pointers.len() > mid_index + 1 {
            right.pointers = self.pointers.split_off(mid_index + 1);
        }

        self.slots.truncate(mid_index);
        self.compact()?;
        self.num_keys = mid_index as u16;

        Ok((mid_key, mid_value, right))
    }

    pub fn compact(&mut self) -> Result<(), BTreeError> {
        let mut pairs: Vec<(K, V)> = Vec::with_capacity(self.slots.len());
        for idx in 0..self.slots.len() {
            pairs.push(self.read_key_value(idx)?);
        }

        self.free_space_offset = 256;
        self.slots.clear();

        for (key, value) in pairs.iter() {
            let key_bytes = bincode::serialize(key)?;
            let value_bytes = bincode::serialize(value)?;

            let total_len = key_bytes.len() + value_bytes.len();
            let new_offset: usize = self.free_space_offset as usize - total_len;

            self.data[new_offset..new_offset + key_bytes.len()].copy_from_slice(&key_bytes);
            self.data[new_offset + key_bytes.len()..new_offset + total_len]
                .copy_from_slice(&value_bytes);

            self.free_space_offset = new_offset as u16;

            self.slots.push(Slot {
                offset: self.free_space_offset,
                key_length: key_bytes.len() as u16,
                value_length: value_bytes.len() as u16,
            });
        }

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
