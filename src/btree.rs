use crate::constants::VERSION;
use crate::error::BTreeError;
use crate::header::Header;
use crate::page_manager::PageManager;
use crate::slotted_page::SlottedPage;
use crate::types::NodeType;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::File;
use std::marker::PhantomData;

use log::{debug, error, info, trace};

pub struct BTree<K, V> {
    header: Header,
    page_manager: PageManager,

    _phantom: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de> + ToString,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(file: File, page_size: u64) -> Result<BTree<K, V>, BTreeError> {
        debug!("Initialising BTree({:?}, {})", file, page_size);
        let mut page_manager = PageManager::new(file, page_size, Header::SIZE as u64);
        let mut header = match Self::read_header(&mut page_manager) {
            Ok(header) => header,
            Err(e) => {
                error!("After attempting to read header: {:?}", e);
                Header::new(1, VERSION, page_size, 0, 0)
            }
        };
        info!("Initialised header: {:?}", header);

        if header.pages_empty() {
            // Called when header is initialised above or if, for some reason, the header is
            // created without a root page

            let root_page = Self::create_page(&mut header, NodeType::LEAF, &mut page_manager);
            header.add_root_page(root_page.page_id);

            info!("Adding root page: {}", root_page.page_id);

            let mut btree = BTree::<K, V> {
                header: header,
                page_manager: page_manager,
                _phantom: PhantomData,
            };

            BTree::<K, V>::write_header(&mut btree.header, &mut btree.page_manager)?;
            BTree::<K, V>::write_page(&root_page, &mut btree.page_manager)?;

            Self::read_header(&mut btree.page_manager)?;

            return Ok(btree);
        }

        let btree = BTree::<K, V> {
            header: header,
            page_manager: page_manager,
            _phantom: PhantomData,
        };
        Ok(btree)
    }

    fn read_header(page_manager: &mut PageManager) -> Result<Header, BTreeError> {
        let buffer = page_manager.read_header()?;
        trace!("read_header: buffer {:?}", buffer);
        Ok(Header::deserialize(&buffer)?)
    }

    fn create_page(
        header: &mut Header,
        node_type: NodeType,
        page_manager: &mut PageManager,
    ) -> SlottedPage<K, V> {
        header.add_page();
        Self::write_header(header, page_manager).unwrap();

        let page_id = page_manager.allocate_page().unwrap();
        info!("Created new page id={}", page_id);

        SlottedPage::new(page_id, node_type, header.page_size as usize)
    }

    pub fn search(&mut self, key: K) -> Result<V, BTreeError> {
        self.search_node(&key, self.header.root_page_id)
    }

    fn search_node(&mut self, key: &K, page_id: u64) -> Result<V, BTreeError> {
        let node = self.read_page(page_id)?;
        match node.node_type {
            NodeType::INTERNAL => {
                let key_pos = node.find_exact_key(&key)?;
                match key_pos {
                    Some(key_pos) => node.read_value(key_pos),
                    None => {
                        let child_node_id = node.get_pointer(&key)?;
                        self.search_node(key, child_node_id)
                    }
                }
            }
            NodeType::LEAF => {
                let key_pos = node
                    .find_exact_key(&key)?
                    .ok_or(BTreeError::KeyNotFound(key.to_string()))?;
                node.read_value(key_pos)
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        info!("Insert key={:?} value={:?}", key, value);
        let mut root = self.read_page(self.header.root_page_id)?;

        if let Some((promoted_key, promoted_value, right)) =
            self.insert_into_page(&mut root, key.clone(), value.clone())?
        {
            let mut new_root =
                Self::create_page(&mut self.header, NodeType::INTERNAL, &mut self.page_manager);

            new_root.insert(0, &promoted_key, &promoted_value)?;
            new_root.pointers.push(self.header.root_page_id);
            new_root.pointers.push(right.page_id);

            info!(
                "Splitting root: promoted_key={:?} promoted_value={:?} new_root={:?}",
                promoted_key, promoted_value, new_root
            );

            BTree::<K, V>::write_page(&new_root, &mut self.page_manager)?;
            BTree::<K, V>::write_page(&root, &mut self.page_manager)?;
            self.header.root_page_id = new_root.page_id;

            return Ok(());
        }

        BTree::<K, V>::write_header(&self.header, &mut self.page_manager)?;
        Ok(())
    }

    fn insert_into_page(
        &mut self,
        page: &mut SlottedPage<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, V, SlottedPage<K, V>)>, BTreeError> {
        let result: Result<Option<(K, V, SlottedPage<K, V>)>, BTreeError> = match page.node_type {
            NodeType::LEAF => {
                // If leaf is overflowing, it should be split
                // Parent should point to current node AND a new node
                match page.find_exact_key(&key)? {
                    Some(pos) => {
                        page.update(pos, &key, &value)?;
                        debug!(
                            "Insert into leaf with exact key: pos={} page={:?}",
                            pos, page
                        );
                        BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                        Ok(None)
                    }
                    None => {
                        let key_len = bincode::serialize(&key)?.len();
                        let value_len = bincode::serialize(&value)?.len();
                        if page.can_insert(key_len, value_len) {
                            let pos = page.find_key_position(&key)?;
                            page.insert(pos, &key, &value)?;
                            BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                            debug!("Insert into leaf: pos={} page={:?}", pos, page);
                            Ok(None)
                        } else {
                            let new_page_id = self.page_manager.allocate_page()?;
                            debug!("Split leaf page: new_page_id={}", new_page_id);
                            let (promoted_key, promoted_value, mut right) =
                                page.split(new_page_id)?;

                            if key < promoted_key {
                                let pos = page.find_key_position(&key)?;
                                page.insert(pos, &key, &value)?;
                                debug!(
                                    "Insert into split left page: pos={} promoted_key={:?} key={:?}, page={:?}",
                                    pos, promoted_key, key, page
                                );
                            } else if promoted_key < key {
                                let pos = right.find_key_position(&key)?;
                                right.insert(pos, &key, &value)?;
                                debug!(
                                    "Insert into split right page: pos={} promoted_key={:?} key={:?} right={:?}",
                                    pos, promoted_key, key, right
                                );
                            } else {
                                panic!("Weird");
                            }

                            BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                            BTree::<K, V>::write_page(&right, &mut self.page_manager)?;

                            self.header.add_page();
                            Ok(Some((promoted_key, promoted_value, right)))
                        }
                    }
                }
            }
            NodeType::INTERNAL => {
                let mut child = self.read_page(page.get_pointer(&key)?)?;
                debug!("Inserting into internal node: child={:?}", child);

                // In internal node, insert key into child
                // The child can be split and therefore, the extra key is promoted and has to be
                // inserted into the parent
                // The parent can then be split in turn
                match self.insert_into_page(&mut child, key.clone(), value.clone())? {
                    Some((child_promoted_key, child_promoted_value, child_right)) => {
                        let insert_pos = page.find_key_position(&child_promoted_key)?;
                        debug!(
                            "Inserting into internal node: position={:?} child_promoted_key={:?}",
                            insert_pos, child_promoted_key
                        );
                        if page.can_insert(
                            bincode::serialize(&child_promoted_key)?.len(),
                            bincode::serialize(&child_promoted_value)?.len(),
                        ) {
                            page.insert(insert_pos, &child_promoted_key, &child_promoted_value)?;
                            page.pointers.insert(insert_pos + 1, child_right.page_id);
                            BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                            BTree::<K, V>::write_page(&child_right, &mut self.page_manager)?;
                            debug!(
                                "Inserted into internal node: position={:?} child_promoted_key={:?} page={:?}, child_right={:?}",
                                insert_pos, child_promoted_key, page, child_right
                            );
                            Ok(None)
                        } else {
                            let new_page_id = self.page_manager.allocate_page()?;
                            debug!("Splitting internal node: new_page_id={:?}", new_page_id);
                            let (to_promote_key, to_promote_value, mut right_of_current) =
                                page.split(new_page_id)?;
                            debug!(
                                "Split internal node: to_promote_key={:?} right_of_current={:?} page={:?}",
                                to_promote_key, right_of_current, page
                            );

                            if child_promoted_key < to_promote_key {
                                let insert_pos = page.find_key_position(&child_promoted_key)?;
                                page.insert(
                                    insert_pos,
                                    &child_promoted_key,
                                    &child_promoted_value,
                                )?;
                                page.pointers.insert(insert_pos + 1, child_right.page_id);
                                debug!(
                                    "Insert into left split internal node: child_promoted_key={:?}, child_right.page_id={} insert_pos={:?} page={:?}",
                                    child_promoted_key, child_right.page_id, insert_pos, page
                                );
                            } else if child_promoted_key > to_promote_key {
                                let insert_pos =
                                    right_of_current.find_key_position(&child_promoted_key)?;
                                right_of_current.insert(
                                    insert_pos,
                                    &child_promoted_key,
                                    &child_promoted_value,
                                )?;
                                right_of_current
                                    .pointers
                                    .insert(insert_pos + 1, child_right.page_id);
                                debug!(
                                    "Insert into right split internal node: child_promoted_key={:?}, child_right.page_id={} insert_pos={:?} right_of_current={:?}",
                                    child_promoted_key,
                                    child_right.page_id,
                                    insert_pos,
                                    right_of_current
                                );
                            } else {
                                panic!("Weird")
                            }

                            BTree::<K, V>::write_page(&page, &mut self.page_manager)?;
                            BTree::<K, V>::write_page(&child_right, &mut self.page_manager)?;
                            BTree::<K, V>::write_page(&right_of_current, &mut self.page_manager)?;
                            self.header.add_page();
                            Ok(Some((to_promote_key, to_promote_value, right_of_current)))
                        }
                    }
                    None => Ok(None),
                }
            }
        };

        match result? {
            Some((key, value, node)) => Ok(Some((key, value, node))),
            None => Ok(None),
        }
    }

    fn write_header(header: &Header, page_manager: &mut PageManager) -> Result<(), BTreeError> {
        let buffer = header.serialize();
        page_manager.write_header(&buffer)?;
        Ok(())
    }

    fn write_page(
        page: &SlottedPage<K, V>,
        page_manager: &mut PageManager,
    ) -> Result<(), BTreeError> {
        let data = page.serialize()?;
        page_manager.write_page(page.page_id, &data)?;
        Ok(())
    }

    fn read_page(&mut self, page_id: u64) -> Result<SlottedPage<K, V>, BTreeError> {
        let (buffer, _) = self.page_manager.read_page(page_id)?;
        let node: SlottedPage<K, V> =
            SlottedPage::deserialize(&buffer, self.header.page_size as usize);

        Ok(node)
    }

    fn print(&mut self, page_id: u64, level: usize, chars_prior: usize) {
        if level == 10 {
            panic!("Recursive limit");
        }
        let node = self.read_page(page_id).unwrap();
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

        let keys = node.read_keys().unwrap();
        let stringified_keys = match keys.len() <= 200 {
            true => format!("{:?}", keys),
            false => {
                let start = &keys[..2];
                let end = &keys[keys.len() - 2..];
                format!("{:?},...,{:?}", start, end)
            }
        };

        println!(
            "{}{}{}{}{:?} - {}",
            " ".repeat(chars_prior),
            prior_char,
            stringified_keys,
            post_char,
            node.node_type,
            node.page_id
        );
        node.pointers
            .iter()
            .for_each(|&ptr| self.print(ptr, level + 1, 1 + chars_prior + stringified_keys.len()));
    }

    pub fn print_tree(&mut self) {
        println!("BTREE: {}", self.header.root_page_id);
        self.print(self.header.root_page_id, 0, 0);
        println!("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    // ─────────────────────────────────────────────────────────
    // Test Helpers
    // ─────────────────────────────────────────────────────────

    fn create_temp_btree<K, V>(page_size: u64) -> BTree<K, V>
    where
        K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de> + ToString,
        V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let file = NamedTempFile::new().unwrap();
        BTree::new(file.reopen().unwrap(), page_size).unwrap()
    }

    fn create_btree_with_file<K, V>(
        page_size: u64,
    ) -> (BTree<K, V>, std::path::PathBuf, NamedTempFile)
    where
        K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de> + ToString,
        V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_owned();
        let btree = BTree::new(file.reopen().unwrap(), page_size).unwrap();
        (btree, path, file)
    }

    // ─────────────────────────────────────────────────────────
    // Initialization Tests
    // ─────────────────────────────────────────────────────────

    mod initialization {
        use super::*;

        #[test_log::test]
        fn new_btree_creates_root_page() {
            let btree = create_temp_btree::<i64, String>(4096);

            assert_eq!(btree.header.page_count, 1);
            assert_eq!(btree.header.root_page_id, 0);
        }

        #[test_log::test]
        fn new_btree_sets_page_size() {
            let btree = create_temp_btree::<i64, String>(8192);

            assert_eq!(btree.header.page_size, 8192);
        }

        #[test_log::test]
        fn new_btree_sets_version() {
            let btree = create_temp_btree::<i64, String>(4096);

            assert_eq!(btree.header.version, VERSION);
        }

        #[test_log::test]
        fn new_btree_root_is_leaf() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            let root = btree.read_page(btree.header.root_page_id).unwrap();
            assert_eq!(root.node_type, NodeType::LEAF);
        }

        #[test_log::test]
        fn different_page_sizes() {
            for page_size in [256, 512, 1024, 4096, 8192] {
                let btree = create_temp_btree::<i64, i64>(page_size);
                assert_eq!(btree.header.page_size, page_size);
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    // Header Management Tests
    // ─────────────────────────────────────────────────────────

    mod header_management {
        use super::*;

        #[test_log::test]
        fn read_header_after_creation() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            let header = BTree::<i64, String>::read_header(&mut btree.page_manager).unwrap();

            assert_eq!(header.page_count, 1);
            assert_eq!(header.page_size, 4096);
        }

        #[test_log::test]
        fn header_persists_after_write() {
            let (mut btree, path, _file) = create_btree_with_file::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();
            btree.insert(2, "two".to_string()).unwrap();

            let page_count = btree.header.page_count;

            // Reopen
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();

            let btree_copy = BTree::<i64, String>::new(file, 4096).unwrap();
            assert_eq!(btree_copy.header.page_count, page_count);
        }

        #[test_log::test]
        fn page_count_increments_on_create_page() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            let initial_count = btree.header.page_count;

            // Insert enough to cause splits
            for i in 0..100 {
                btree.insert(i, i).unwrap();
            }

            assert!(btree.header.page_count > initial_count);
        }

        #[test_log::test]
        fn root_page_id_changes_on_root_split() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            let initial_root = btree.header.root_page_id;

            // Insert enough to cause root split
            for i in 0..100 {
                btree.insert(i, i).unwrap();
            }

            // Root should change when tree grows taller
            assert_ne!(btree.header.root_page_id, initial_root);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Search Tests
    // ─────────────────────────────────────────────────────────

    mod search {
        use super::*;

        #[test_log::test]
        fn search_single_key() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(42, "answer".to_string()).unwrap();

            assert_eq!(btree.search(42).unwrap(), "answer");
        }

        #[test_log::test]
        fn search_nonexistent_key_returns_error() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();

            let result = btree.search(999);
            assert!(matches!(result, Err(BTreeError::KeyNotFound(_))));
        }

        #[test_log::test]
        fn search_empty_tree_returns_error() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            let result = btree.search(1);
            assert!(matches!(result, Err(BTreeError::KeyNotFound(_))));
        }

        #[test_log::test]
        fn search_first_key() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..100 {
                btree.insert(i, i * 10).unwrap();
            }

            assert_eq!(btree.search(0).unwrap(), 0);
        }

        #[test_log::test]
        fn search_last_key() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..100 {
                btree.insert(i, i * 10).unwrap();
            }

            assert_eq!(btree.search(99).unwrap(), 990);
        }

        #[test_log::test]
        fn search_middle_key() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..100 {
                btree.insert(i, i * 10).unwrap();
            }

            assert_eq!(btree.search(50).unwrap(), 500);
        }

        #[test_log::test]
        fn search_after_splits() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            // Insert enough to cause multiple splits
            for i in 0..200 {
                btree.insert(i, i).unwrap();
            }

            // All keys should still be findable
            for i in 0..200 {
                assert_eq!(btree.search(i).unwrap(), i);
            }
        }

        #[test_log::test]
        fn search_with_string_keys() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            btree.insert("apple".to_string(), 1).unwrap();
            btree.insert("banana".to_string(), 2).unwrap();
            btree.insert("cherry".to_string(), 3).unwrap();

            assert_eq!(btree.search("banana".to_string()).unwrap(), 2);
        }

        #[test_log::test]
        fn search_negative_keys() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(-100, "negative".to_string()).unwrap();
            btree.insert(0, "zero".to_string()).unwrap();
            btree.insert(100, "positive".to_string()).unwrap();

            assert_eq!(btree.search(-100).unwrap(), "negative");
            assert_eq!(btree.search(0).unwrap(), "zero");
            assert_eq!(btree.search(100).unwrap(), "positive");
        }
    }

    // ─────────────────────────────────────────────────────────
    // Insert Tests
    // ─────────────────────────────────────────────────────────

    mod insert {
        use super::*;

        #[test_log::test]
        fn insert_single_entry() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();

            assert_eq!(btree.search(1).unwrap(), "one");
        }

        #[test_log::test]
        fn insert_multiple_sequential() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..50 {
                btree.insert(i, i * 2).unwrap();
            }

            for i in 0..50 {
                assert_eq!(btree.search(i).unwrap(), i * 2);
            }
        }

        #[test_log::test]
        fn insert_multiple_reverse() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in (0..50).rev() {
                btree.insert(i, i * 2).unwrap();
            }

            for i in 0..50 {
                assert_eq!(btree.search(i).unwrap(), i * 2);
            }
        }

        #[test_log::test]
        fn insert_random_order() {
            use rand::rng;
            use rand::seq::SliceRandom;

            let mut btree = create_temp_btree::<i64, i64>(4096);
            let mut keys: Vec<i64> = (0..100).collect();
            keys.shuffle(&mut rng());

            for &k in &keys {
                btree.insert(k, k * 10).unwrap();
            }

            for k in 0..100 {
                assert_eq!(btree.search(k).unwrap(), k * 10);
            }
        }

        #[test_log::test]
        fn insert_updates_existing_key() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "original".to_string()).unwrap();
            btree.insert(1, "updated".to_string()).unwrap();

            assert_eq!(btree.search(1).unwrap(), "updated");
        }

        #[test_log::test]
        fn insert_update_preserves_other_keys() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();
            btree.insert(2, "two".to_string()).unwrap();
            btree.insert(3, "three".to_string()).unwrap();

            btree.insert(2, "TWO".to_string()).unwrap();

            assert_eq!(btree.search(1).unwrap(), "one");
            assert_eq!(btree.search(2).unwrap(), "TWO");
            assert_eq!(btree.search(3).unwrap(), "three");
        }

        #[test_log::test]
        fn insert_many_updates_same_key() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            btree.insert(1, 0).unwrap();

            for i in 1..=100 {
                btree.insert(1, i).unwrap();
            }

            assert_eq!(btree.search(1).unwrap(), 100);
        }

        #[test_log::test]
        fn insert_different_value_types() {
            // String values
            let mut btree1 = create_temp_btree::<i64, String>(4096);
            btree1.insert(1, "hello".to_string()).unwrap();
            assert_eq!(btree1.search(1).unwrap(), "hello");

            // Vector values
            let mut btree2 = create_temp_btree::<i64, Vec<u8>>(4096);
            btree2.insert(1, vec![1, 2, 3, 4, 5]).unwrap();
            assert_eq!(btree2.search(1).unwrap(), vec![1, 2, 3, 4, 5]);

            // Tuple values
            let mut btree3 = create_temp_btree::<i64, (i64, String)>(4096);
            btree3.insert(1, (42, "answer".to_string())).unwrap();
            assert_eq!(btree3.search(1).unwrap(), (42, "answer".to_string()));
        }
    }

    // ─────────────────────────────────────────────────────────
    // Split Tests
    // ─────────────────────────────────────────────────────────

    mod split {
        use super::*;

        #[test_log::test]
        fn insert_causes_leaf_split() {
            let mut btree = create_temp_btree::<i64, String>(512);

            let initial_page_count = btree.header.page_count;

            for i in 0..30 {
                btree.insert(i, format!("value_{}", i)).unwrap();
            }

            assert!(
                btree.header.page_count > initial_page_count,
                "Page count should increase after splits"
            );
        }

        #[test_log::test]
        fn split_maintains_all_data() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..100 {
                btree.insert(i, i * 10).unwrap();
            }

            // Verify all data is still accessible
            for i in 0..100 {
                assert_eq!(
                    btree.search(i).unwrap(),
                    i * 10,
                    "Key {} should have value {}",
                    i,
                    i * 10
                );
            }
        }

        #[test_log::test]
        fn multiple_splits_maintain_order() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            // Insert in random order
            let keys = vec![50, 25, 75, 10, 30, 60, 90, 5, 15, 27, 35, 55, 70, 80, 95];

            for &k in &keys {
                btree.insert(k, k).unwrap();
            }

            // All should be findable
            for &k in &keys {
                assert_eq!(btree.search(k).unwrap(), k);
            }
        }

        #[test_log::test]
        fn root_split_creates_new_root() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            let initial_root = btree.header.root_page_id;

            // Insert enough to cause root to split
            for i in 0..50 {
                btree.insert(i, i).unwrap();
            }

            // Verify root changed
            assert_ne!(btree.header.root_page_id, initial_root);

            // New root should be internal node
            let root = btree.read_page(btree.header.root_page_id).unwrap();
            assert_eq!(root.node_type, NodeType::INTERNAL);
        }

        #[test_log::test]
        fn split_with_sequential_inserts() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..200 {
                btree.insert(i, i).unwrap();
                // Verify all previous inserts still work
                for j in 0..=i {
                    assert_eq!(btree.search(j).unwrap(), j, "Failed after inserting {}", i);
                }
            }
        }

        #[test_log::test]
        fn split_with_reverse_inserts() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in (0..200).rev() {
                btree.insert(i, i).unwrap();
                assert_eq!(btree.search(i).unwrap(), i);
            }

            for i in 0..200 {
                assert_eq!(btree.search(i).unwrap(), i);
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    // Internal Node Tests
    // ─────────────────────────────────────────────────────────

    mod internal_nodes {
        use super::*;

        #[test_log::test]
        fn internal_node_pointers_correct() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            // Insert enough to create internal nodes
            for i in 0..100 {
                btree.insert(i, i).unwrap();
            }

            let root = btree.read_page(btree.header.root_page_id).unwrap();

            if root.node_type == NodeType::INTERNAL {
                // Internal node should have pointers
                assert!(!root.pointers.is_empty());
                // Should have one more pointer than keys
                assert_eq!(root.pointers.len(), root.num_keys as usize + 1);
            }
        }

        #[test_log::test]
        fn search_traverses_internal_nodes() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..100 {
                btree.insert(i, i * 10).unwrap();
            }

            // These searches should traverse internal nodes
            assert_eq!(btree.search(0).unwrap(), 0);
            assert_eq!(btree.search(50).unwrap(), 500);
            assert_eq!(btree.search(99).unwrap(), 990);
        }

        #[test_log::test]
        fn internal_node_split_preserves_data() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            // Insert many entries to cause internal node splits
            for i in 0..500 {
                btree.insert(i, i).unwrap();
            }

            // Verify all data
            for i in 0..500 {
                assert_eq!(btree.search(i).unwrap(), i, "Key {} not found", i);
            }
        }
    }

    // ─────────────────────────────────────────────────────────
    // Page I/O Tests
    // ─────────────────────────────────────────────────────────

    mod page_io {
        use super::*;

        #[test_log::test]
        fn read_page_returns_correct_data() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();
            btree.insert(2, "two".to_string()).unwrap();

            let page = btree.read_page(btree.header.root_page_id).unwrap();

            assert_eq!(page.num_keys, 2);
        }

        #[test_log::test]
        fn write_and_read_page_roundtrip() {
            let (mut btree, path, _original_file) = create_btree_with_file::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();
            btree.insert(2, "two".to_string()).unwrap();
            btree.insert(3, "three".to_string()).unwrap();

            let root_id = btree.header.root_page_id;
            drop(btree);

            // Reopen and read
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();

            let mut btree = BTree::<i64, String>::new(file, 4096).unwrap();
            let page = btree.read_page(root_id).unwrap();

            assert_eq!(page.num_keys, 3);
        }

        #[test_log::test]
        fn create_page_increments_count() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            let initial = btree.header.page_count;

            // Force page creation via splits
            for i in 0..50 {
                btree.insert(i, i).unwrap();
            }

            assert!(btree.header.page_count > initial);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Edge Cases
    // ─────────────────────────────────────────────────────────

    mod edge_cases {
        use super::*;

        #[test_log::test]
        fn empty_string_key() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            btree.insert("".to_string(), 42).unwrap();

            assert_eq!(btree.search("".to_string()).unwrap(), 42);
        }

        #[test_log::test]
        fn empty_string_value() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "".to_string()).unwrap();

            assert_eq!(btree.search(1).unwrap(), "");
        }

        #[test_log::test]
        fn large_key() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            let large_key = "x".repeat(500);
            btree.insert(large_key.clone(), 42).unwrap();

            assert_eq!(btree.search(large_key).unwrap(), 42);
        }

        #[test_log::test]
        fn large_value() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            let large_value = "y".repeat(1000);
            btree.insert(1, large_value.clone()).unwrap();

            assert_eq!(btree.search(1).unwrap(), large_value);
        }

        #[test_log::test]
        fn min_max_i64_keys() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(i64::MIN, "min".to_string()).unwrap();
            btree.insert(i64::MAX, "max".to_string()).unwrap();
            btree.insert(0, "zero".to_string()).unwrap();

            assert_eq!(btree.search(i64::MIN).unwrap(), "min");
            assert_eq!(btree.search(i64::MAX).unwrap(), "max");
            assert_eq!(btree.search(0).unwrap(), "zero");
        }

        #[test_log::test]
        fn unicode_keys() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            btree.insert("こんにちは".to_string(), 1).unwrap();
            btree.insert("مرحبا".to_string(), 2).unwrap();
            btree.insert("🎉🎊🎁".to_string(), 3).unwrap();

            assert_eq!(btree.search("こんにちは".to_string()).unwrap(), 1);
            assert_eq!(btree.search("مرحبا".to_string()).unwrap(), 2);
            assert_eq!(btree.search("🎉🎊🎁".to_string()).unwrap(), 3);
        }

        #[test_log::test]
        fn special_characters_in_keys() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            let special_keys = vec![
                "\n\t\r",
                "key with spaces",
                "key\0with\0nulls",
                "key'with\"quotes",
                "key\\with\\backslashes",
            ];

            for (i, key) in special_keys.iter().enumerate() {
                btree.insert(key.to_string(), i as i64).unwrap();
            }

            for (i, key) in special_keys.iter().enumerate() {
                assert_eq!(btree.search(key.to_string()).unwrap(), i as i64);
            }
        }

        #[test_log::test]
        fn duplicate_inserts_idempotent() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for _ in 0..100 {
                btree.insert(1, 42).unwrap();
            }

            assert_eq!(btree.search(1).unwrap(), 42);

            // Should only have one entry in root
            let root = btree.read_page(btree.header.root_page_id).unwrap();
            assert_eq!(root.num_keys, 1);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Stress Tests
    // ─────────────────────────────────────────────────────────

    mod stress {
        use super::*;

        #[test_log::test]
        fn insert_one_thousand_sequential() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..1000 {
                btree.insert(i, i * 2).unwrap();
            }

            for i in 0..1000 {
                assert_eq!(btree.search(i).unwrap(), i * 2);
            }
        }

        #[test_log::test]
        fn insert_one_hundred_random() {
            use rand::rng;
            use rand::seq::SliceRandom;

            let mut btree = create_temp_btree::<i64, i64>(256);
            let mut keys: Vec<i64> = (0..100).collect();
            keys.shuffle(&mut rng());

            for &k in &keys {
                btree.insert(k, k).unwrap();
                println!("Insert: {:?}", k);
                btree.print_tree();
            }

            btree.print_tree();

            for &k in &keys {
                assert_eq!(btree.search(k).unwrap(), k);
            }
        }

        #[test_log::test]
        fn insert_one_thousand_random() {
            use rand::rng;
            use rand::seq::SliceRandom;

            let mut btree = create_temp_btree::<i64, i64>(4096);
            let mut keys: Vec<i64> = (0..1000).collect();
            keys.shuffle(&mut rng());

            for &k in &keys {
                btree.insert(k, k).unwrap();
            }
            btree.print_tree();

            for &k in &keys {
                assert_eq!(btree.search(k).unwrap(), k);
            }
        }

        #[test_log::test]
        fn small_page_many_splits() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..500 {
                btree.insert(i, i).unwrap();
            }

            // Verify tree integrity
            for i in 0..500 {
                assert_eq!(btree.search(i).unwrap(), i);
            }

            // Should have created many pages
            assert!(btree.header.page_count > 10);
        }

        #[test_log::test]
        fn alternating_insert_and_search() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..500 {
                btree.insert(i, i * 10).unwrap();

                // Verify this and some previous inserts
                assert_eq!(btree.search(i).unwrap(), i * 10);

                if i > 0 {
                    assert_eq!(btree.search(i / 2).unwrap(), (i / 2) * 10);
                }
            }
        }

        #[test_log::test]
        fn many_updates() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            // Insert initial values
            for i in 0..100 {
                btree.insert(i, 0).unwrap();
            }

            // Update each key many times
            for round in 1..=50 {
                for i in 0..100 {
                    btree.insert(i, round).unwrap();
                }
            }

            // Verify final values
            for i in 0..100 {
                assert_eq!(btree.search(i).unwrap(), 50);
            }
        }

        #[test_log::test]
        #[ignore] // Run with: cargo test -- --ignored
        fn insert_ten_thousand() {
            let mut btree = create_temp_btree::<i64, i64>(4096);

            for i in 0..10_000 {
                btree.insert(i, i).unwrap();
            }

            // Spot check
            assert_eq!(btree.search(0).unwrap(), 0);
            assert_eq!(btree.search(5000).unwrap(), 5000);
            assert_eq!(btree.search(9999).unwrap(), 9999);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Tree Structure Tests
    // ─────────────────────────────────────────────────────────

    mod tree_structure {
        use super::*;

        #[test_log::test]
        fn single_entry_stays_in_root() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();
            assert_eq!(btree.header.page_count, 1);

            let root = btree.read_page(btree.header.root_page_id).unwrap();
            assert_eq!(root.node_type, NodeType::LEAF);
            assert_eq!(root.num_keys, 1);
        }

        #[test_log::test]
        fn tree_grows_in_height() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            // Track when root changes (indicates height increase)
            let mut root_changes = 0;
            let mut last_root = btree.header.root_page_id;

            for i in 0..200 {
                btree.insert(i, i).unwrap();

                if btree.header.root_page_id != last_root {
                    root_changes += 1;
                    last_root = btree.header.root_page_id;
                }
            }

            assert!(
                root_changes >= 2,
                "Tree should have grown in height multiple times"
            );
        }

        #[test_log::test]
        fn internal_node_has_correct_pointer_count() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..100 {
                btree.insert(i, i).unwrap();
            }

            fn check_pointers<K, V>(btree: &mut BTree<K, V>, page_id: u64)
            where
                K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de> + ToString,
                V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
            {
                let page = btree.read_page(page_id).unwrap();

                if page.node_type == NodeType::INTERNAL {
                    // Internal nodes should have num_keys + 1 pointers
                    assert_eq!(
                        page.pointers.len(),
                        page.num_keys as usize + 1,
                        "Page {} has {} keys but {} pointers",
                        page_id,
                        page.num_keys,
                        page.pointers.len()
                    );

                    // Recursively check children
                    for &ptr in &page.pointers {
                        check_pointers(btree, ptr);
                    }
                } else {
                    // Leaf nodes should have no pointers
                    assert!(
                        page.pointers.is_empty(),
                        "Leaf page {} should have no pointers",
                        page_id
                    );
                }
            }

            let root_page_id = btree.header.root_page_id;
            check_pointers(&mut btree, root_page_id);
        }

        #[test_log::test]
        fn all_leaves_at_same_depth() {
            let mut btree = create_temp_btree::<i64, i64>(256);

            for i in 0..100 {
                btree.insert(i, i).unwrap();
            }

            fn get_leaf_depths<K, V>(
                btree: &mut BTree<K, V>,
                page_id: u64,
                depth: usize,
            ) -> Vec<usize>
            where
                K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de> + ToString,
                V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
            {
                let page = btree.read_page(page_id).unwrap();

                if page.node_type == NodeType::LEAF {
                    vec![depth]
                } else {
                    page.pointers
                        .iter()
                        .flat_map(|&ptr| get_leaf_depths(btree, ptr, depth + 1))
                        .collect()
                }
            }

            let root_page_id = btree.header.root_page_id;
            let depths = get_leaf_depths(&mut btree, root_page_id, 0);

            // All leaves should be at the same depth
            let first_depth = depths[0];
            assert!(
                depths.iter().all(|&d| d == first_depth),
                "Leaf depths are not uniform: {:?}",
                depths
            );
        }
    }

    // ─────────────────────────────────────────────────────────
    // Error Handling Tests
    // ─────────────────────────────────────────────────────────

    mod error_handling {
        use super::*;

        #[test_log::test]
        fn search_returns_key_not_found_error() {
            let mut btree = create_temp_btree::<i64, String>(4096);

            btree.insert(1, "one".to_string()).unwrap();

            match btree.search(999) {
                Err(BTreeError::KeyNotFound(key)) => {
                    assert_eq!(key, "999");
                }
                other => panic!("Expected KeyNotFound, got {:?}", other),
            }
        }

        #[test_log::test]
        fn search_nonexistent_key_with_string() {
            let mut btree = create_temp_btree::<String, i64>(4096);

            btree.insert("exists".to_string(), 1).unwrap();

            match btree.search("missing".to_string()) {
                Err(BTreeError::KeyNotFound(key)) => {
                    assert_eq!(key, "missing");
                }
                other => panic!("Expected KeyNotFound, got {:?}", other),
            }
        }
    }
}
