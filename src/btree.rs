use crate::constants::VERSION;
use crate::error::BTreeError;
use crate::header::Header;
use crate::page::Page;
use crate::page_manager::PageManager;
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
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    pub fn new(file: File, page_size: u64) -> BTree<K, V> {
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
            let root_node = Self::create_page(&mut header, NodeType::LEAF, &mut page_manager);
            header.add_root_node(root_node.page_id);

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

    fn read_header(page_manager: &mut PageManager) -> Result<Header, BTreeError> {
        let buffer = page_manager.read_header()?;
        Ok(Header::deserialize(&buffer)?)
    }

    fn create_page(
        header: &mut Header,
        node_type: NodeType,
        page_manager: &mut PageManager,
    ) -> Page<K, V> {
        header.add_page();
        Self::write_header(header, page_manager).unwrap();

        Page::new(node_type, page_manager)
    }

    pub fn search(&mut self, key: K) -> Result<V, BTreeError> {
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

    pub fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
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

    pub fn print_tree(&mut self) {
        println!("BTREE: {}", self.header.root_page_id);
        self.print(self.header.root_page_id, 0, 0);
        println!("\n")
    }
}
