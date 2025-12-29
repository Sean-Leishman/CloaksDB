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
    pub num_keys: u16,
    pub free_space_end: u16, // where free space starts
    pub free_list: Vec<FreeSpaceRegion>,
    pub total_free: u16, // total free bytes (contiguous + holes)
    pub slots: Vec<Slot>,
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
                println!("Assign from freelist: {:?} {:?}", region, remaining);

                if remaining > 0 {
                    println!("Init from freelist: {} {}", offset, total_len);
                    self.free_list[free_list_idx] = FreeSpaceRegion {
                        offset: offset as u16 + total_len as u16,
                        length: remaining as u16,
                    };
                } else {
                    println!("Remove from freelist: {}", free_list_idx);
                    self.free_list.remove(free_list_idx);
                }
            }
            None => {
                // Contiguous space
                println!("Assign from contiguous space: {}", offset);
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
        } else {
            let insert_pos = self
                .free_list
                .iter()
                .position(|r| r.offset > region.offset)
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

    // ─────────────────────────────────────────────────────────
    // Test Helpers
    // ─────────────────────────────────────────────────────────

    fn create_page(page_size: usize) -> SlottedPage<i64, String> {
        SlottedPage::new(0, NodeType::LEAF, page_size)
    }

    fn create_page_typed<K, V>(page_size: usize) -> SlottedPage<K, V>
    where
        K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
        V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
    {
        SlottedPage::new(0, NodeType::LEAF, page_size)
    }

    /// Helper to verify page integrity - checks for overlapping regions
    fn verify_page_integrity<K, V>(page: &SlottedPage<K, V>) -> Result<(), String>
    where
        K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
        V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
    {
        // Collect all used regions (offset, length)
        let mut used_regions: Vec<(u16, u16, &str)> = Vec::new();

        // Add slot data regions
        for (i, slot) in page.slots.iter().enumerate() {
            let len = slot.key_length + slot.value_length;
            used_regions.push((slot.offset, len, "slot"));
        }

        // Add free list regions
        for (i, region) in page.free_list.iter().enumerate() {
            used_regions.push((region.offset, region.length, "free"));
        }

        // Sort by offset
        used_regions.sort_by_key(|(offset, _, _)| *offset);

        // Check for overlaps
        for i in 0..used_regions.len() {
            let (offset1, len1, type1) = used_regions[i];
            let end1 = offset1 + len1;

            for j in (i + 1)..used_regions.len() {
                let (offset2, len2, type2) = used_regions[j];
                let end2 = offset2 + len2;

                // Check if regions overlap
                if offset1 < end2 && offset2 < end1 {
                    println!("Slots: {:?}", page.slots);
                    println!("Freelist: {:?}", page.free_list);
                    println!("Used Regions: {:?}", used_regions);
                    return Err(format!(
                        "Overlap detected: {} region at {}..{} overlaps with {} region at {}..{}",
                        type1, offset1, end1, type2, offset2, end2
                    ));
                }
            }
        }

        // Check that no slot region overlaps with free_space_end boundary
        for (i, slot) in page.slots.iter().enumerate() {
            if slot.offset < page.free_space_end {
                return Err(format!(
                    "Slot {} at offset {} is below free_space_end {}",
                    i, slot.offset, page.free_space_end
                ));
            }
        }

        // Check that free list regions don't extend into contiguous free space
        for region in &page.free_list {
            if region.offset < page.free_space_end {
                return Err(format!(
                    "Free region at offset {} extends below free_space_end {}",
                    region.offset, page.free_space_end
                ));
            }
        }

        Ok(())
    }

    /// Helper to dump page state for debugging
    fn dump_page_state<K, V>(page: &SlottedPage<K, V>, label: &str)
    where
        K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
        V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
    {
        println!("\n=== {} ===", label);
        println!("page_size: {}", page.page_size);
        println!("free_space_end: {}", page.free_space_end);
        println!("num_keys: {}", page.num_keys);
        println!("total_free: {}", page.total_free);
        println!("free_space(): {}", page.get_free_space());

        println!("\nSlots:");
        for (i, slot) in page.slots.iter().enumerate() {
            println!(
                "  [{}] offset={}, key_len={}, value_len={}, end={}",
                i,
                slot.offset,
                slot.key_length,
                slot.value_length,
                slot.offset + slot.key_length + slot.value_length
            );
        }

        println!("\nFree list:");
        for (i, region) in page.free_list.iter().enumerate() {
            println!(
                "  [{}] offset={}, length={}, end={}",
                i,
                region.offset,
                region.length,
                region.offset + region.length
            );
        }
        println!("===\n");
    }

    // ─────────────────────────────────────────────────────────
    // Free List Basic Tests
    // ─────────────────────────────────────────────────────────

    mod free_list_basic {
        use super::*;

        #[test]
        fn new_page_has_empty_free_list() {
            let page = create_page(4096);

            assert!(page.free_list.is_empty());
            assert_eq!(page.free_space_end, 4096);
        }

        #[test]
        fn delete_creates_free_region() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            let slot_before_delete = page.slots[1].clone();
            let expected_free_len = slot_before_delete.key_length + slot_before_delete.value_length;

            page.delete(1).unwrap();

            // Should have a free region OR coalesced with contiguous space
            let total_in_free_list: u16 = page.free_list.iter().map(|r| r.length).sum();

            // The freed space should be accounted for somewhere
            assert!(
                total_in_free_list >= expected_free_len || page.total_free >= expected_free_len,
                "Freed space not accounted for"
            );

            verify_page_integrity(&page).unwrap();
        }

        #[test]
        fn delete_middle_entry_creates_hole() {
            let mut page = create_page(4096);

            // Insert 3 entries
            page.insert(0, &1i64, &"aaa".to_string()).unwrap();
            page.insert(1, &2i64, &"bbb".to_string()).unwrap();
            page.insert(2, &3i64, &"ccc".to_string()).unwrap();

            dump_page_state(&page, "After 3 inserts");
            verify_page_integrity(&page).unwrap();

            // Delete middle - this creates a hole
            page.delete(1).unwrap();

            dump_page_state(&page, "After delete middle");
            verify_page_integrity(&page).unwrap();

            // Verify remaining data is correct
            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 3);
        }

        #[test]
        fn multiple_deletes_create_multiple_holes() {
            let mut page = create_page(4096);

            for i in 0..5 {
                page.insert(i, &(i as i64), &format!("value_{}", i))
                    .unwrap();
            }

            verify_page_integrity(&page).unwrap();

            // Delete entries 1 and 3 (non-adjacent)
            page.delete(3).unwrap();
            page.delete(1).unwrap();

            dump_page_state(&page, "After deleting 1 and 3");
            verify_page_integrity(&page).unwrap();

            // Verify remaining entries
            assert_eq!(page.num_keys, 3);
            assert_eq!(page.read_key(0).unwrap(), 0);
            assert_eq!(page.read_key(1).unwrap(), 2);
            assert_eq!(page.read_key(2).unwrap(), 4);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Free List Coalescing Tests
    // ─────────────────────────────────────────────────────────

    mod free_list_coalescing {
        use super::*;

        #[test]
        fn adjacent_deletes_coalesce() {
            let mut page = create_page(4096);

            // Insert entries with similar sizes
            for i in 0..5 {
                page.insert(i, &(i as i64), &"xxxx".to_string()).unwrap();
            }

            dump_page_state(&page, "After 5 inserts");

            // Delete adjacent entries
            page.delete(2).unwrap();
            dump_page_state(&page, "After delete index 2");

            page.delete(2).unwrap(); // Was index 3, now index 2
            dump_page_state(&page, "After delete index 2 again (was 3)");

            verify_page_integrity(&page).unwrap();

            // Should have coalesced into fewer regions
            assert!(
                page.free_list.len() <= 2,
                "Adjacent holes should coalesce, got {} regions",
                page.free_list.len()
            );
        }

        #[test]
        fn delete_at_boundary_extends_contiguous_space() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();

            let free_end_before = page.free_space_end;

            // Delete the only entry - should extend contiguous space
            page.delete(0).unwrap();

            dump_page_state(&page, "After deleting only entry");

            // free_space_end should have moved up (more contiguous space)
            // OR the free list should contain the region
            println!(
                "{:?} {} {:?}",
                page.free_space_end, free_end_before, page.free_list
            );
            assert!(
                page.free_space_end > free_end_before || !page.free_list.is_empty(),
                "Deleted space not recovered"
            );

            verify_page_integrity(&page).unwrap();
        }
    }

    // ─────────────────────────────────────────────────────────
    // Free List Reuse Tests - THIS IS WHERE THE BUG LIKELY IS
    // ─────────────────────────────────────────────────────────

    mod free_list_reuse {
        use super::*;

        #[test]
        fn insert_reuses_exact_fit_hole() {
            let mut page = create_page(4096);

            // Insert entries
            page.insert(0, &1i64, &"aaaa".to_string()).unwrap();
            page.insert(1, &2i64, &"bbbb".to_string()).unwrap();
            page.insert(2, &3i64, &"cccc".to_string()).unwrap();

            dump_page_state(&page, "After 3 inserts");
            verify_page_integrity(&page).unwrap();

            // Delete middle
            page.delete(1).unwrap();

            dump_page_state(&page, "After delete middle");
            verify_page_integrity(&page).unwrap();

            // Insert similar sized entry - should reuse hole
            let pos = page.find_key_position(&2i64).unwrap();
            page.insert(pos, &2i64, &"dddd".to_string()).unwrap();

            dump_page_state(&page, "After reinsert");
            verify_page_integrity(&page).unwrap();

            // Verify all data is intact
            assert_eq!(page.read_value(0).unwrap(), "aaaa");
            assert_eq!(page.read_value(1).unwrap(), "dddd");
            assert_eq!(page.read_value(2).unwrap(), "cccc");
        }

        #[test]
        fn insert_reuses_larger_hole() {
            let mut page = create_page(4096);

            // Insert with a large value
            page.insert(0, &1i64, &"small".to_string()).unwrap();
            page.insert(1, &2i64, &"this_is_a_much_larger_value".to_string())
                .unwrap();
            page.insert(2, &3i64, &"small".to_string()).unwrap();

            dump_page_state(&page, "After inserts with large middle");
            verify_page_integrity(&page).unwrap();

            // Delete the large entry
            page.delete(1).unwrap();

            dump_page_state(&page, "After delete large entry");
            verify_page_integrity(&page).unwrap();

            // Insert smaller entry - should fit in the hole with leftover
            let pos = page.find_key_position(&2i64).unwrap();
            page.insert(pos, &2i64, &"tiny".to_string()).unwrap();

            dump_page_state(&page, "After insert smaller into large hole");
            verify_page_integrity(&page).unwrap();

            // Verify data integrity
            assert_eq!(page.read_value(0).unwrap(), "small");
            assert_eq!(page.read_value(1).unwrap(), "tiny");
            assert_eq!(page.read_value(2).unwrap(), "small");
        }

        #[test]
        fn insert_into_middle_does_not_corrupt_neighbors() {
            let mut page = create_page(4096);

            // Insert 5 entries
            page.insert(0, &10i64, &"value_10".to_string()).unwrap();
            page.insert(1, &20i64, &"value_20".to_string()).unwrap();
            page.insert(2, &30i64, &"value_30".to_string()).unwrap();
            page.insert(3, &40i64, &"value_40".to_string()).unwrap();
            page.insert(4, &50i64, &"value_50".to_string()).unwrap();

            dump_page_state(&page, "After 5 inserts");
            verify_page_integrity(&page).unwrap();

            // Delete entry at index 2 (key=30)
            page.delete(2).unwrap();

            dump_page_state(&page, "After delete index 2");
            verify_page_integrity(&page).unwrap();

            // Now insert a new key that goes in the middle (key=25)
            let pos = page.find_key_position(&25i64).unwrap();
            println!("Inserting key 25 at position {}", pos);

            page.insert(pos, &25i64, &"value_25".to_string()).unwrap();

            dump_page_state(&page, "After insert key 25");
            verify_page_integrity(&page).unwrap();

            // CRITICAL: Verify ALL entries are correct
            let keys: Vec<i64> = (0..page.num_keys as usize)
                .map(|i| page.read_key(i).unwrap())
                .collect();
            let values: Vec<String> = (0..page.num_keys as usize)
                .map(|i| page.read_value(i).unwrap())
                .collect();

            println!("Keys: {:?}", keys);
            println!("Values: {:?}", values);

            assert_eq!(keys, vec![10, 20, 25, 40, 50]);
            assert_eq!(
                values,
                vec!["value_10", "value_20", "value_25", "value_40", "value_50"]
            );
        }

        #[test]
        fn multiple_insert_delete_cycles() {
            let mut page = create_page(4096);

            // Cycle 1: Insert
            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("v{}", i)).unwrap();
            }
            verify_page_integrity(&page).unwrap();

            // Cycle 2: Delete every other
            for i in (0..10).step_by(2).rev() {
                page.delete(i).unwrap();
            }
            verify_page_integrity(&page).unwrap();

            dump_page_state(&page, "After deleting evens");

            // Cycle 3: Insert new entries
            for i in 0..5 {
                let key = i * 2; // 0, 2, 4, 6, 8
                let pos = page.find_key_position(&key).unwrap();
                page.insert(pos, &key, &format!("new_{}", key)).unwrap();
                verify_page_integrity(&page).unwrap();
            }

            dump_page_state(&page, "After reinserting evens");

            // Verify all entries
            for i in 0..10 {
                let key: i64 = page.read_key(i).unwrap();
                assert_eq!(key, i as i64, "Key at index {} is wrong", i);
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    // Update Tests
    // ─────────────────────────────────────────────────────────

    mod update {
        use super::*;

        #[test]
        fn update_same_size_value() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"aaaa".to_string()).unwrap();

            let offset_before = page.slots[0].offset;

            page.update(0, &1i64, &"bbbb".to_string()).unwrap();

            // Should update in place (same offset)
            assert_eq!(page.slots[0].offset, offset_before);
            assert_eq!(page.read_value(0).unwrap(), "bbbb");

            verify_page_integrity(&page).unwrap();
        }

        #[test]
        fn update_smaller_value() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"this_is_a_long_value".to_string())
                .unwrap();
            page.insert(1, &2i64, &"other".to_string()).unwrap();

            dump_page_state(&page, "Before update to smaller");

            let offset_before = page.slots[0].offset;
            let len_before = page.slots[0].value_length;

            page.update(0, &1i64, &"short".to_string()).unwrap();

            dump_page_state(&page, "After update to smaller");
            verify_page_integrity(&page).unwrap();

            // Value should be updated
            assert_eq!(page.read_value(0).unwrap(), "short");
            // Other entry should be unaffected
            assert_eq!(page.read_value(1).unwrap(), "other");

            // Leftover space should be in free list or total_free increased
            assert!(
                page.slots[0].value_length < len_before,
                "Value length should have decreased"
            );
        }

        #[test]
        fn update_larger_value_relocates() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"short".to_string()).unwrap();
            page.insert(1, &2i64, &"other".to_string()).unwrap();

            dump_page_state(&page, "Before update to larger");

            let offset_before = page.slots[0].offset;

            page.update(0, &1i64, &"this_is_a_much_longer_value".to_string())
                .unwrap();

            dump_page_state(&page, "After update to larger");
            verify_page_integrity(&page).unwrap();

            // Value should be updated
            assert_eq!(page.read_value(0).unwrap(), "this_is_a_much_longer_value");
            // Other entry should be unaffected
            assert_eq!(page.read_value(1).unwrap(), "other");

            // Offset should have changed (relocated)
            assert_ne!(
                page.slots[0].offset, offset_before,
                "Should have relocated to new position"
            );
        }

        #[test]
        fn update_does_not_corrupt_other_entries() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();
            page.insert(3, &4i64, &"four".to_string()).unwrap();
            page.insert(4, &5i64, &"five".to_string()).unwrap();

            verify_page_integrity(&page).unwrap();

            // Update middle entry with larger value
            page.update(2, &3i64, &"THREE_UPDATED_LONGER".to_string())
                .unwrap();

            verify_page_integrity(&page).unwrap();

            // Verify all entries
            assert_eq!(page.read_value(0).unwrap(), "one");
            assert_eq!(page.read_value(1).unwrap(), "two");
            assert_eq!(page.read_value(2).unwrap(), "THREE_UPDATED_LONGER");
            assert_eq!(page.read_value(3).unwrap(), "four");
            assert_eq!(page.read_value(4).unwrap(), "five");
        }

        #[test]
        fn multiple_updates_same_entry() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"initial".to_string()).unwrap();
            page.insert(1, &2i64, &"other".to_string()).unwrap();

            for i in 0..20 {
                let value = format!("update_{:02}", i);
                page.update(0, &1i64, &value).unwrap();
                verify_page_integrity(&page).unwrap();

                assert_eq!(page.read_value(0).unwrap(), value);
                assert_eq!(page.read_value(1).unwrap(), "other");
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    // Delete Tests
    // ─────────────────────────────────────────────────────────

    mod delete {
        use super::*;

        #[test]
        fn delete_first_entry() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(0).unwrap();

            verify_page_integrity(&page).unwrap();

            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 2);
            assert_eq!(page.read_key(1).unwrap(), 3);
        }

        #[test]
        fn delete_last_entry() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(2).unwrap();

            verify_page_integrity(&page).unwrap();

            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 2);
        }

        #[test]
        fn delete_middle_entry() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(1).unwrap();

            verify_page_integrity(&page).unwrap();

            assert_eq!(page.num_keys, 2);
            assert_eq!(page.read_key(0).unwrap(), 1);
            assert_eq!(page.read_key(1).unwrap(), 3);
            assert_eq!(page.read_value(0).unwrap(), "one");
            assert_eq!(page.read_value(1).unwrap(), "three");
        }

        #[test]
        fn delete_all_entries_one_by_one() {
            let mut page = create_page(4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("val_{}", i)).unwrap();
            }

            for _ in 0..10 {
                page.delete(0).unwrap();
                verify_page_integrity(&page).unwrap();
            }

            assert_eq!(page.num_keys, 0);
        }

        #[test]
        fn delete_in_reverse_order() {
            let mut page = create_page(4096);

            for i in 0..10 {
                page.insert(i, &(i as i64), &format!("val_{}", i)).unwrap();
            }

            for i in (0..10).rev() {
                page.delete(i).unwrap();
                verify_page_integrity(&page).unwrap();
            }

            assert_eq!(page.num_keys, 0);
        }

        #[test]
        fn delete_and_verify_remaining() {
            let mut page = create_page(4096);

            let entries: Vec<(i64, String)> =
                (0..10).map(|i| (i, format!("value_{}", i))).collect();

            for (i, (k, v)) in entries.iter().enumerate() {
                page.insert(i, k, v).unwrap();
            }

            // Delete indices 2, 5, 7
            page.delete(7).unwrap();
            page.delete(5).unwrap();
            page.delete(2).unwrap();

            verify_page_integrity(&page).unwrap();

            // Expected remaining: 0, 1, 3, 4, 6, 8, 9
            let expected_keys = vec![0i64, 1, 3, 4, 6, 8, 9];
            let actual_keys: Vec<i64> = (0..page.num_keys as usize)
                .map(|i| page.read_key(i).unwrap())
                .collect();

            assert_eq!(actual_keys, expected_keys);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Data Corruption Detection Tests
    // ─────────────────────────────────────────────────────────

    mod corruption_detection {
        use super::*;

        #[test]
        fn detect_overlapping_slot_regions() {
            let mut page = create_page(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();

            // Manually corrupt: make slot 0's length extend into slot 1
            // This simulates what might happen with the bug
            let original_len = page.slots[0].value_length;
            page.slots[0].value_length = original_len + 100; // Extend into next region

            let result = verify_page_integrity(&page);
            assert!(result.is_err(), "Should detect overlap");
        }

        #[test]
        fn insert_delete_insert_pattern() {
            let mut page = create_page(4096);

            // This pattern often exposes free list bugs
            page.insert(0, &100i64, &"aaaaaaaaaa".to_string()).unwrap();
            page.insert(1, &200i64, &"bbbbbbbbbb".to_string()).unwrap();
            page.insert(2, &300i64, &"cccccccccc".to_string()).unwrap();

            verify_page_integrity(&page).unwrap();

            // Delete middle
            page.delete(1).unwrap();
            verify_page_integrity(&page).unwrap();

            // Insert at beginning (should shift slots but reuse space)
            page.insert(0, &50i64, &"dddddddddd".to_string()).unwrap();
            verify_page_integrity(&page).unwrap();

            // Check all values
            assert_eq!(page.read_key(0).unwrap(), 50);
            assert_eq!(page.read_key(1).unwrap(), 100);
            assert_eq!(page.read_key(2).unwrap(), 300);

            assert_eq!(page.read_value(0).unwrap(), "dddddddddd");
            assert_eq!(page.read_value(1).unwrap(), "aaaaaaaaaa");
            assert_eq!(page.read_value(2).unwrap(), "cccccccccc");
        }

        #[test]
        fn stress_insert_delete_insert() {
            let mut page = create_page(4096);

            // Fill with entries
            for i in 0..20 {
                page.insert(i, &(i as i64), &format!("value_{:03}", i))
                    .unwrap();
            }

            // Delete random entries
            for i in [15, 10, 5, 0, 18, 7, 12].iter() {
                if (*i as u16) < page.num_keys {
                    page.delete(*i).unwrap();
                    verify_page_integrity(&page).unwrap();
                }
            }

            dump_page_state(&page, "After random deletes");

            // Insert new entries
            for i in 100..110 {
                let pos = page.find_key_position(&i).unwrap();
                page.insert(pos, &i, &format!("new_{}", i)).unwrap();
                dump_page_state(&page, "After random deletes");
                verify_page_integrity(&page).unwrap();
            }

            dump_page_state(&page, "After new inserts");

            // Verify all keys are readable and unique
            let mut keys: Vec<i64> = (0..page.num_keys as usize)
                .map(|i| page.read_key(i).unwrap())
                .collect();

            let len_before = keys.len();
            keys.sort();
            keys.dedup();
            assert_eq!(keys.len(), len_before, "Duplicate keys found!");
        }
    }

    // ─────────────────────────────────────────────────────────
    // Serialization Roundtrip with Free List
    // ─────────────────────────────────────────────────────────

    mod serialization_with_free_list {
        use super::*;

        #[test]
        fn roundtrip_preserves_free_list() {
            let mut page: SlottedPage<i64, String> = create_page_typed(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(1).unwrap();

            let free_list_len = page.free_list.len();
            let total_free = page.total_free;

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<i64, String> = SlottedPage::deserialize(&bytes, 4096);

            assert_eq!(restored.free_list.len(), free_list_len);
            assert_eq!(restored.total_free, total_free);

            verify_page_integrity(&restored).unwrap();
        }

        #[test]
        fn roundtrip_after_reuse() {
            let mut page: SlottedPage<i64, String> = create_page_typed(4096);

            page.insert(0, &1i64, &"one".to_string()).unwrap();
            page.insert(1, &2i64, &"two".to_string()).unwrap();
            page.insert(2, &3i64, &"three".to_string()).unwrap();

            page.delete(1).unwrap();
            page.insert(1, &2i64, &"TWO".to_string()).unwrap();

            let bytes = page.serialize().unwrap();
            let restored: SlottedPage<i64, String> = SlottedPage::deserialize(&bytes, 4096);

            verify_page_integrity(&restored).unwrap();

            assert_eq!(restored.read_value(0).unwrap(), "one");
            assert_eq!(restored.read_value(1).unwrap(), "TWO");
            assert_eq!(restored.read_value(2).unwrap(), "three");
        }
    }
}
