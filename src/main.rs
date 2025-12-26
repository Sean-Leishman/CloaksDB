use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::PartialOrd;
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::marker::PhantomData;

const VERSION: u16 = 0;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum NodeType {
    INTERNAL = 0,
    LEAF = 1,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> NodeType {
        match value {
            0 => NodeType::INTERNAL,
            1 => NodeType::LEAF,
            _ => panic!("Invalid node type"),
        }
    }
}

#[derive(Debug)]
struct Header {
    magic_number: u16,
    version: u16,
    page_size: u64,
    root_page_id: u64,
    page_count: u64,
}

#[derive(Debug)]
enum HeaderError {
    InvalidMagicNumber(u16),
    InvalidBufferSize { expected: usize, got: usize },
    CorruptedData(String),
}

#[derive(Debug)]
enum PageManagerError {
    Io(std::io::Error),
    HeaderNotWritten,
}

#[derive(Debug)]
enum SlottedPageError {
    Io(std::io::Error),
    Serialization(bincode::Error),
    InvalidBufferSize { expected: usize, got: usize },
}

#[derive(Debug)]
enum BTreeError {
    Io(std::io::Error),
    Serialization(bincode::Error),
    Header(HeaderError),
    PageManager(PageManagerError),
    SlottedPage(SlottedPageError),
    KeyNotFound(String),
    InvalidNodeType(u8),
    PageOverflow { page_id: u64 },
}

impl std::fmt::Display for BTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BTreeError::Io(e) => {
                write!(f, "IO error: {}", e)
            }
            BTreeError::Serialization(e) => {
                write!(f, "Serialization error: {}", e)
            }
            BTreeError::Header(e) => {
                write!(f, "Header error: {}", e)
            }
            BTreeError::PageManager(e) => {
                write!(f, "PageManager error: {}", e)
            }
            BTreeError::SlottedPage(e) => {
                write!(f, "SlottedPage error: {}", e)
            }
            BTreeError::KeyNotFound(key) => {
                write!(f, "KeyNotFound: {}", key)
            }
            BTreeError::InvalidNodeType(node_type) => {
                write!(f, "InvalidNodeType: {}", node_type)
            }
            BTreeError::PageOverflow { page_id } => {
                write!(f, "PageOverflow: page_id={}", page_id)
            }
        }
    }
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HeaderError::InvalidMagicNumber(num) => {
                write!(f, "Invalid magic number: {} (must be > 0)", num)
            }
            HeaderError::InvalidBufferSize { expected, got } => {
                write!(f, "Invalid buffer size: expected {}, got {}", expected, got)
            }
            HeaderError::CorruptedData(msg) => {
                write!(f, "Corrupted header data: {}", msg)
            }
        }
    }
}

impl std::fmt::Display for PageManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PageManagerError::Io(e) => {
                write!(f, "IO error: {}", e)
            }
            PageManagerError::HeaderNotWritten {} => {
                write!(f, "Header has not been written")
            }
        }
    }
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

impl Error for HeaderError {}

impl From<std::io::Error> for BTreeError {
    fn from(err: std::io::Error) -> BTreeError {
        BTreeError::Io(err)
    }
}

impl From<std::io::Error> for PageManagerError {
    fn from(err: std::io::Error) -> PageManagerError {
        PageManagerError::Io(err)
    }
}

impl From<std::io::Error> for SlottedPageError {
    fn from(err: std::io::Error) -> SlottedPageError {
        SlottedPageError::Io(err)
    }
}

impl From<SlottedPageError> for BTreeError {
    fn from(err: SlottedPageError) -> BTreeError {
        BTreeError::SlottedPage(err)
    }
}

impl Header {
    const SIZE: usize = 28;
    fn serialize(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..2].copy_from_slice(&self.magic_number.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.version.to_le_bytes());
        buffer[4..12].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[12..20].copy_from_slice(&self.root_page_id.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.page_count.to_le_bytes());

        buffer
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, HeaderError> {
        let magic_number = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        if magic_number == 0 {
            return Err(HeaderError::InvalidMagicNumber(magic_number));
        }

        let version = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
        let page_size = u64::from_le_bytes(buffer[4..12].try_into().unwrap());
        let root_page_id = u64::from_le_bytes(buffer[12..20].try_into().unwrap());
        let page_count = u64::from_le_bytes(buffer[20..28].try_into().unwrap());

        Ok(Header {
            magic_number,
            version,
            page_size,
            root_page_id,
            page_count,
        })
    }
}

struct PageManager {
    file: File,
    page_size: u64,
    header_size: u64,
}

struct Slot {
    offset: u16,
    key_length: u16,
    value_length: u16,
}

impl Slot {
    const SIZE: usize = 6;

    fn serialize(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buffer[2..4].copy_from_slice(&self.key_length.to_le_bytes());
        buffer[4..6].copy_from_slice(&self.value_length.to_le_bytes());

        buffer
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let offset = u16::from_le_bytes(buffer[0..2].try_into().unwrap());
        let key_length = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
        let value_length = u16::from_le_bytes(buffer[4..6].try_into().unwrap());

        Slot {
            offset: offset,
            key_length: key_length,
            value_length: value_length,
        }
    }
}

impl PageManager {
    fn new(file: File, page_size: u64, header_size: u64) -> Self {
        let mut file_clone = file.try_clone().unwrap();
        let file_length = file_clone.seek(std::io::SeekFrom::End(0)).unwrap();
        if file_length < header_size {
            let header_buffer = vec![0u8; header_size as usize];
            file_clone.write_all(&header_buffer).unwrap();
        }

        PageManager {
            file,
            page_size,
            header_size,
        }
    }

    fn from_pageid(&self, page_id: u64) -> u64 {
        (page_id * self.page_size) + self.header_size
    }

    fn to_pageid(&self, byte_offset: u64) -> u64 {
        (byte_offset - self.header_size) / self.page_size
    }

    fn allocate_page(&mut self) -> Result<u64, PageManagerError> {
        self.file.seek(std::io::SeekFrom::End(0))?;

        let byte_offset = self.file.seek(std::io::SeekFrom::Current(0))?;
        if byte_offset < Header::SIZE as u64 {
            return Err(PageManagerError::HeaderNotWritten);
        }

        let page_id = self.to_pageid(byte_offset);

        self.file
            .write(&vec![0u8; self.page_size.try_into().unwrap()])?;

        return Ok(page_id);
    }

    fn write_header(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        if data.len() > self.header_size as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Buffer too large: expected {} got {}",
                    self.header_size,
                    data.len()
                ),
            ));
        }

        let byte_offset = self.file.seek(std::io::SeekFrom::Start(0))?;
        println!("write_header: {:?} {}", data, byte_offset);
        self.file.write_all(data)?;
        Ok(())
    }

    fn read_header(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut buffer = vec![0u8; self.header_size as usize];
        let byte_offset = self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.read(&mut buffer)?;
        println!("read_header: {:?} {}", buffer, byte_offset);
        Ok(buffer)
    }

    fn write_page(&mut self, page_id: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(self.from_pageid(page_id)))?;

        self.file.write_all(data)?;
        Ok(())
    }

    fn read_page(&mut self, page_id: u64) -> Result<(Box<Vec<u8>>, usize), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(self.from_pageid(page_id)))?;

        let buffer_size: usize = self.page_size.try_into().unwrap();
        let mut buffer = vec![0u8; buffer_size];
        let bytes_read = self.file.read(&mut buffer)?;
        Ok((Box::new(buffer), bytes_read))
    }
}

struct SlottedPage {
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

    fn can_insert(&self, key_len: usize, value_len: usize, page_size: usize) -> bool {
        let needed = Slot::SIZE + key_len + value_len;
        let needed = match self.node_type {
            NodeType::LEAF => needed,
            NodeType::INTERNAL => needed + 8, // child pointer
        };

        let free_space = self.get_free_space(page_size);
        free_space >= needed
    }

    fn serialize(&self, page_size: usize) -> Result<Vec<u8>, SlottedPageError> {
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

    fn deserialize(buffer: &[u8]) -> Self {
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

    fn from_page<K, V>(page: &Page<K, V>, page_size: usize) -> SlottedPage
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

    fn to_page<K, V>(&self) -> Page<K, V>
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Page<K, V> {
    page_id: u64,
    node_type: NodeType,
    keys: Vec<K>,
    values: Vec<V>,
    pointers: Vec<u64>,
}

impl<K, V> Page<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(node_type: NodeType, page_manager: &mut PageManager) -> Page<K, V> {
        let page_id = page_manager.allocate_page().unwrap();
        Page {
            page_id: page_id,
            node_type: node_type,
            keys: vec![],
            values: vec![],
            pointers: vec![],
        }
    }

    fn find_key_position(&self, key: &K) -> usize {
        self.keys
            .iter()
            .position(|k| key <= k)
            .unwrap_or(self.keys.len())
    }

    fn insert_key_value(&mut self, key: K, value: V) {
        let pos = self.find_key_position(&key);
        if pos < self.keys.len() && self.keys[pos] == key {
            self.values[pos] = value;
            return;
        }

        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }

    fn insert_key_value_node(&mut self, pos: usize, key: K, value: V, new_node_id: u64) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        self.pointers.insert(pos + 1, new_node_id);
    }

    fn can_insert_variable(&self, key: &K, value: &V, page_size: usize) -> bool {
        let key_bytes = bincode::serialize(key).unwrap();
        let value_bytes = bincode::serialize(value).unwrap();

        let slotted = SlottedPage::from_page(self, page_size);
        slotted.can_insert(key_bytes.len(), value_bytes.len(), page_size)
    }

    fn serialize(&self, page_size: usize) -> Result<Vec<u8>, BTreeError> {
        let slotted = SlottedPage::from_page(self, page_size);
        Ok(slotted.serialize(page_size)?)
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let slotted = SlottedPage::deserialize(buffer);
        SlottedPage::to_page(&slotted)
    }
}

struct BTree<K, V> {
    header: Header,
    page_manager: PageManager,

    _phantom: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(file: File, page_size: u64) -> BTree<K, V> {
        let mut page_manager = PageManager::new(file, page_size, Header::SIZE as u64);
        let mut header = match Self::read_header(&mut page_manager) {
            Ok(header) => header,
            Err(e) => {
                eprintln!("error from read_header: {:?}", e);
                Header {
                    magic_number: 1,
                    version: VERSION,
                    page_size: page_size,
                    root_page_id: 0,
                    page_count: 0,
                }
            }
        };
        println!("Header: {:?}", header);

        if header.page_count == 0 {
            // Called when header is initialised above or if, for some reason, the header is
            // created without a root page

            println!("Header created");
            let root_node = Self::create_page(&mut header, NodeType::LEAF, &mut page_manager);
            header.page_count += 1;
            header.root_page_id = root_node.page_id;

            let mut btree = BTree::<K, V> {
                header: header,
                page_manager: page_manager,
                _phantom: PhantomData,
            };

            BTree::<K, V>::write_header(&mut btree.header, &mut btree.page_manager).unwrap();
            BTree::<K, V>::write_node(&root_node, &mut btree.page_manager).unwrap();

            Self::read_header(&mut btree.page_manager).unwrap();

            return btree;
        }

        let btree = BTree::<K, V> {
            header: header,
            page_manager: page_manager,
            _phantom: PhantomData,
        };
        btree
    }

    fn read_header(page_manager: &mut PageManager) -> Result<Header, Box<dyn std::error::Error>> {
        let buffer = page_manager.read_header()?;
        Ok(Header::deserialize(&buffer)?)
    }

    fn create_page(
        header: &mut Header,
        node_type: NodeType,
        page_manager: &mut PageManager,
    ) -> Page<K, V> {
        header.page_count += 1;
        Self::write_header(header, page_manager).unwrap();

        Page::new(node_type, page_manager)
    }

    fn search(&mut self, key: K) -> Result<V, BTreeError> {
        self.search_node(key, self.header.root_page_id)
    }

    fn search_node(&mut self, key: K, page_id: u64) -> Result<V, BTreeError> {
        let node = self.read_node(page_id)?;
        let pos = node.find_key_position(&key);
        if pos < node.keys.len() && node.keys[pos] == key {
            return Ok(node.values[pos].clone());
        }

        match node.node_type {
            NodeType::INTERNAL => {
                let child_node_id = node.pointers[pos];
                self.search_node(key, child_node_id)
            }
            NodeType::LEAF => Err(BTreeError::KeyNotFound(format!("{:?}", key))),
        }
    }

    fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        let mut root = self.read_node(self.header.root_page_id)?;

        if let Some((promoted_key, promoted_value, right_node)) =
            self.insert_non_full(&mut root, key.clone(), value.clone())?
        {
            let mut new_root =
                Self::create_page(&mut self.header, NodeType::INTERNAL, &mut self.page_manager);
            new_root.pointers.push(self.header.root_page_id);

            new_root.keys.push(promoted_key);
            new_root.values.push(promoted_value);
            new_root.pointers.push(right_node.page_id);

            BTree::<K, V>::write_node(&new_root, &mut self.page_manager)?;
            self.header.root_page_id = new_root.page_id;

            return Ok(());
        }

        Ok(())
    }

    fn insert_non_full(
        &mut self,
        node: &mut Page<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, V, Page<K, V>)>, BTreeError> {
        let result: Result<Option<(K, V, Page<K, V>)>, BTreeError> = match node.node_type {
            NodeType::LEAF => {
                // If leaf is overflowing, it should be split
                // Parent should point to current node AND a new node
                node.insert_key_value(key.clone(), value.clone());
                if node.can_insert_variable(&key, &value, self.header.page_size as usize) {
                    BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                    Ok(None)
                } else {
                    Ok(Some(self.split_child(node)))
                }
            }
            NodeType::INTERNAL => {
                let child_pos = node.find_key_position(&key);
                let mut child = self.read_node(node.pointers[child_pos])?;

                // In internal node, insert key into child
                // The child can be split and therefore, the extra key is promoted and has to be
                // inserted into the parent
                // The parent can then be split in turn
                if let Some((promoted_key, promoted_value, new_right)) =
                    self.insert_non_full(&mut child, key, value)?
                {
                    let pos = node.find_key_position(&promoted_key);
                    node.insert_key_value_node(
                        pos,
                        promoted_key.clone(),
                        promoted_value.clone(),
                        new_right.page_id,
                    );

                    if node.can_insert_variable(
                        &promoted_key,
                        &promoted_value,
                        self.header.page_size as usize,
                    ) {
                        BTree::<K, V>::write_node(node, &mut self.page_manager).unwrap();
                        Ok(None)
                    } else {
                        Ok(Some(self.split_child(node)))
                    }
                } else {
                    Ok(None)
                }
            }
        };

        match result? {
            Some((key, value, node)) => Ok(Some((key, value, node))),
            None => Ok(None),
        }
    }

    fn split_child(&mut self, node: &mut Page<K, V>) -> (K, V, Page<K, V>) {
        let mid_index = node.keys.len() / 2;
        let mid_key = node.keys[mid_index].clone();
        let mid_value = node.values[mid_index].clone();

        let mut right_node =
            Self::create_page(&mut self.header, node.node_type, &mut self.page_manager);
        right_node.keys = node.keys.split_off(mid_index + 1);
        right_node.values = node.values.split_off(mid_index + 1);
        right_node.pointers = match node.node_type {
            NodeType::INTERNAL => node.pointers.split_off(mid_index + 1),
            NodeType::LEAF => vec![],
        };
        node.keys.pop();
        node.values.pop();

        BTree::<K, V>::write_node(&node, &mut self.page_manager).unwrap();
        BTree::<K, V>::write_node(&right_node, &mut self.page_manager).unwrap();

        (mid_key, mid_value, right_node)
    }

    fn write_header(header: &mut Header, page_manager: &mut PageManager) -> Result<(), BTreeError> {
        let buffer = header.serialize();
        page_manager.write_header(&buffer)?;
        Ok(())
    }

    fn write_node(page: &Page<K, V>, page_manager: &mut PageManager) -> Result<(), BTreeError> {
        let data = page.serialize(page_manager.page_size.try_into().unwrap())?;
        page_manager.write_page(page.page_id, &data)?;
        Ok(())
    }

    fn read_node(&mut self, page_id: u64) -> Result<Page<K, V>, BTreeError> {
        let (buffer, _) = self.page_manager.read_page(page_id)?;
        let node: Page<K, V> = Page::deserialize(&buffer);

        Ok(node)
    }

    fn print(&mut self, page_id: u64, level: usize, chars_prior: usize) {
        let node = self.read_node(page_id).unwrap();
        let prior_char = if level == 0 {
            ""
        } else {
            match node.pointers.is_empty() {
                false => "└",
                true => "├",
            }
        };
        let post_char = match node.pointers.is_empty() {
            false => "┐",
            true => "",
        };

        let stringified_keys = format!("{:?}", node.keys);
        println!(
            "{}{}{}{}",
            " ".repeat(chars_prior),
            prior_char,
            stringified_keys,
            post_char,
        );
        node.pointers
            .iter()
            .for_each(|&ptr| self.print(ptr, level + 1, 1 + chars_prior + stringified_keys.len()));
    }

    fn print_tree(&mut self) {
        println!("BTREE: {}", self.header.root_page_id);
        self.print(self.header.root_page_id, 0, 0);
        println!("\n")
    }
}

fn main() {
    let index_dir = format!("out/database/index");
    std::fs::create_dir_all(&index_dir).expect("Failed to create_dir");

    let index_filename = format!("{}/index_0", index_dir);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&index_filename)
        .expect("Failed to open file");

    let mut btree = BTree::<i64, i64>::new(file, 256);
    let mut rng = rand::rng();

    for i in 400..500 {
        let _: i64 = rng.random_range(0..101); // Generate a number in the range [0, 100]
        btree.insert(i, 100).unwrap();
        btree.print_tree();
    }
    btree.print_tree();
    println!("Finished run");

    btree.print_tree();
    println!("Search (key={}): {}", 500, btree.search(500).unwrap());
}
