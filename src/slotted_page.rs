use crate::page::Page;
use crate::slot::Slot;
use crate::types::NodeType;
use serde::{Deserialize, Serialize};

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

pub struct SlottedPage {
    page_id: u64,
    node_type: NodeType,
    num_keys: u16,
    free_space_offset: u16, // where free space starts
    slots: Vec<Slot>,
    pointers: Vec<u64>,
    data: Vec<u8>,
}

impl SlottedPage {
    const HEADER_SIZE: usize = 13;

    fn new(page_id: u64, node_type: NodeType, page_size: usize) -> SlottedPage {
        SlottedPage {
            page_id,
            node_type,
            num_keys: 0,
            free_space_offset: page_size as u16,
            slots: Vec::new(),
            pointers: Vec::new(),
            data: vec![0; page_size],
        }
    }

    fn get_free_space(&self, page_size: usize) -> usize {
        let used_at_start =
            Self::HEADER_SIZE + (self.slots.len() * Slot::SIZE) + (self.pointers.len() * 8);
        let used_at_end = page_size - self.free_space_offset as usize;
        page_size.saturating_sub(used_at_start + used_at_end)
    }

    pub fn can_insert(&self, key_len: usize, value_len: usize, page_size: usize) -> bool {
        let needed = Slot::SIZE + key_len + value_len;
        let needed = match self.node_type {
            NodeType::LEAF => needed,
            NodeType::INTERNAL => needed + 8, // child pointer
        };

        let free_space = self.get_free_space(page_size);
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
        }
    }

    pub fn from_page<K, V>(page: &Page<K, V>, page_size: usize) -> SlottedPage
    where
        K: Serialize,
        V: Serialize,
    {
        println!("from_page: {:?}", page.page_id);
        let mut slotted = SlottedPage::new(page.page_id, page.node_type, page_size);
        slotted.num_keys = page.keys.len() as u16;
        slotted.pointers = page.pointers.clone();

        page.keys
            .iter()
            .zip(page.values.iter())
            .for_each(|(key, value)| {
                let key_bytes = bincode::serialize(key).unwrap();
                let value_bytes = bincode::serialize(value).unwrap();

                let total_len = key_bytes.len() + value_bytes.len();
                slotted.free_space_offset =
                    slotted.free_space_offset.saturating_sub(total_len as u16);
                let data_offset = slotted.free_space_offset;

                let mut offset = data_offset as usize;
                slotted.data[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
                offset += key_bytes.len();
                slotted.data[offset..offset + value_bytes.len()].copy_from_slice(&value_bytes);

                slotted.slots.push(Slot {
                    offset: data_offset as u16,
                    key_length: key_bytes.len() as u16,
                    value_length: value_bytes.len() as u16,
                })
            });

        slotted
    }

    pub fn to_page<K, V>(&self) -> Page<K, V>
    where
        K: for<'de> Deserialize<'de>,
        V: for<'de> Deserialize<'de>,
    {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        self.slots.iter().for_each(|slot| {
            let offset = slot.offset as usize;
            let key_length = slot.key_length as usize;
            let value_length = slot.value_length as usize;

            let key: K = bincode::deserialize(&self.data[offset..offset + key_length]).unwrap();
            let value: V = bincode::deserialize(
                &self.data[offset + key_length..offset + key_length + value_length],
            )
            .unwrap();

            keys.push(key);
            values.push(value);
        });

        Page {
            page_id: self.page_id,
            node_type: self.node_type,
            keys,
            values,
            pointers: self.pointers.clone(),
        }
    }
}
