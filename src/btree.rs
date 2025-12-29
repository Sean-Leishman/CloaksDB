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
        let mut page_manager = PageManager::new(file, page_size, Header::SIZE as u64);
        let mut header = match Self::read_header(&mut page_manager) {
            Ok(header) => header,
            Err(e) => {
                eprintln!("error from read_header: {:?}", e);
                Header::new(1, VERSION, page_size, 0, 0)
            }
        };
        println!("Header: {:?}", header);

        if header.pages_empty() {
            // Called when header is initialised above or if, for some reason, the header is
            // created without a root page

            println!("Header created");
            let root_page = Self::create_page(&mut header, NodeType::LEAF, &mut page_manager);
            header.add_root_page(root_page.page_id);

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
        let mut root = self.read_page(self.header.root_page_id)?;

        if let Some((promoted_key, promoted_value, right)) =
            self.insert_into_page(&mut root, key.clone(), value.clone())?
        {
            let mut new_root =
                Self::create_page(&mut self.header, NodeType::INTERNAL, &mut self.page_manager);

            new_root.insert(0, &promoted_key, &promoted_value)?;
            new_root.pointers.push(self.header.root_page_id);
            new_root.pointers.push(right.page_id);

            BTree::<K, V>::write_page(&new_root, &mut self.page_manager)?;
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
                        BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                        Ok(None)
                    }
                    None => {
                        let key_len = bincode::serialize(&key)?.len();
                        let value_len = bincode::serialize(&key)?.len();
                        if page.can_insert(key_len, value_len) {
                            let pos = page.find_key_position(&key)?;
                            page.insert(pos, &key, &value)?;
                            BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                            Ok(None)
                        } else {
                            let new_page_id = self.page_manager.allocate_page()?;
                            let (promoted_key, promoted_value, mut right) =
                                page.split(new_page_id)?;

                            if key < promoted_key {
                                let pos = page.find_key_position(&key)?;
                                page.insert(pos, &key, &value)?;
                            } else if promoted_key < key {
                                let pos = right.find_key_position(&key)?;
                                right.insert(pos, &key, &value)?;
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

                // In internal node, insert key into child
                // The child can be split and therefore, the extra key is promoted and has to be
                // inserted into the parent
                // The parent can then be split in turn
                match self.insert_into_page(&mut child, key.clone(), value.clone())? {
                    Some((child_promoted_key, child_promoted_value, child_right)) => {
                        let insert_pos = page.find_key_position(&child_promoted_key)?;
                        if page.can_insert(
                            bincode::serialize(&child_promoted_key)?.len(),
                            bincode::serialize(&child_promoted_value)?.len(),
                        ) {
                            page.insert(insert_pos, &child_promoted_key, &child_promoted_value)?;
                            page.pointers.insert(insert_pos + 1, child_right.page_id);
                            BTree::<K, V>::write_page(page, &mut self.page_manager)?;
                            BTree::<K, V>::write_page(&child_right, &mut self.page_manager)?;
                            Ok(None)
                        } else {
                            let new_page_id = self.page_manager.allocate_page()?;
                            let (to_promote_key, to_promote_value, mut right_of_current) =
                                page.split(new_page_id)?;

                            if child_promoted_key < to_promote_key {
                                let insert_pos = page.find_key_position(&child_promoted_key)?;
                                page.insert(
                                    insert_pos,
                                    &child_promoted_key,
                                    &child_promoted_value,
                                )?;
                                page.pointers
                                    .insert(insert_pos + 1, right_of_current.page_id);
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
                            } else {
                                panic!("Weird")
                            }

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
        let stringified_keys = match keys.len() <= 5 {
            true => format!("{:?}", keys),
            false => {
                let start = &keys[..2];
                let end = &keys[keys.len() - 2..];
                format!("{:?},...,{:?}", start, end)
            }
        };

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

    pub fn print_tree(&mut self) {
        println!("BTREE: {}", self.header.root_page_id);
        self.print(self.header.root_page_id, 0, 0);
        println!("\n")
    }
}
