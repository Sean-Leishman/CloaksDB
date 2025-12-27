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
    ) -> SlottedPage<K, V> {
        header.add_page();
        Self::write_header(header, page_manager).unwrap();

        let page_id = page_manager.allocate_page().unwrap();
        SlottedPage::new(page_id, node_type, header.page_size as usize)
    }

    pub fn search(&mut self, key: K) -> Result<V, BTreeError> {
        self.search_node(key, self.header.root_page_id)
    }

    fn search_node(&mut self, key: K, page_id: u64) -> Result<V, BTreeError> {
        let node = self.read_node(page_id)?;
        match node.node_type {
            NodeType::INTERNAL => {
                let child_node_id = node.get_pointer(&key)?;
                self.search_node(key, child_node_id)
            }
            NodeType::LEAF => node.read_value(node.find_exact_key(&key)?),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        let mut root = self.read_node(self.header.root_page_id)?;

        if let Some((promoted_key, promoted_value, right_node)) =
            self.insert_non_full(&mut root, key.clone(), value.clone())?
        {
            println!(
                "full root: promoted_key={:?} \n\tnode={:?} \n\tright_node={:?}",
                promoted_key, root, right_node
            );
            let mut new_root =
                Self::create_page(&mut self.header, NodeType::INTERNAL, &mut self.page_manager);

            new_root.insert(0, &promoted_key, &promoted_value)?;
            new_root.pointers.push(self.header.root_page_id);
            new_root.pointers.push(right_node.page_id);
            // new_root.pointers.push(self.header.root_page_id);
            //
            // new_root.keys.push(promoted_key);
            // new_root.values.push(promoted_value);
            // new_root.pointers.push(right_node.page_id);

            BTree::<K, V>::write_node(&new_root, &mut self.page_manager)?;
            BTree::<K, V>::write_node(&right_node, &mut self.page_manager)?;
            BTree::<K, V>::write_node(&root, &mut self.page_manager)?;

            self.header.root_page_id = new_root.page_id;

            return Ok(());
        }

        Ok(())
    }

    fn insert_non_full(
        &mut self,
        node: &mut SlottedPage<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, V, SlottedPage<K, V>)>, BTreeError> {
        let result: Result<Option<(K, V, SlottedPage<K, V>)>, BTreeError> = match node.node_type {
            NodeType::LEAF => {
                // If leaf is overflowing, it should be split
                // Parent should point to current node AND a new node
                match node.find_exact_key(&key) {
                    Ok(pos) => {
                        node.update(pos, &key, &value)?;
                        BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                        Ok(None)
                    }
                    Err(_) => {
                        if node.can_insert(&key, &value, self.header.page_size as usize) {
                            println!("insert: LEAF and can_insert");
                            let pos = node.find_key_position(&key)?;
                            node.insert(pos, &key, &value)?;
                            BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                            Ok(None)
                        } else {
                            println!("insert: LEAF and not can_insert");
                            let new_page_id = self.page_manager.allocate_page()?;
                            let (promoted_key, promoted_value, mut right_node) =
                                node.split(new_page_id)?;

                            println!(
                                "insert: promoted_key={:?}\nright_node={:?}\nnode={:?}",
                                promoted_key, right_node, node
                            );

                            if key < promoted_key {
                                println!(
                                    "insert: into first node promoted_key={:?} key={:?}",
                                    promoted_key, key
                                );
                                let pos = node.find_key_position(&key)?;
                                node.insert(pos, &key, &value)?;
                            } else if promoted_key < key {
                                let pos = right_node.find_key_position(&key)?;
                                right_node.insert(pos, &key, &value)?;
                                println!(
                                    "insert: into second node promoted_key={:?} key={:?}",
                                    promoted_key, key
                                );
                            } else {
                                panic!("Weird");
                            }

                            println!("insert: write node={:?}", node);
                            println!("insert: write right_node={:?}", right_node);
                            BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                            BTree::<K, V>::write_node(&right_node, &mut self.page_manager)?;

                            self.header.add_page();
                            Ok(Some((promoted_key, promoted_value, right_node)))
                        }
                    }
                }
            }
            NodeType::INTERNAL => {
                // let child_pos = node.find_key_position(&k/*  */ey);
                let mut child = self.read_node(node.get_pointer(&key)?)?;

                // In internal node, insert key into child
                // The child can be split and therefore, the extra key is promoted and has to be
                // inserted into the parent
                // The parent can then be split in turn
                match self.insert_non_full(&mut child, key.clone(), value.clone())? {
                    Some((promoted_key, promoted_value, right_node)) => {
                        let insert_pos = node.find_key_position(&promoted_key)?;
                        println!("insert: INTERNAL and split");
                        if node.can_insert(
                            &promoted_key,
                            &promoted_value,
                            self.header.page_size as usize,
                        ) {
                            println!("insert: INTERNAL and split and can_insert");
                            node.insert(insert_pos, &promoted_key, &promoted_value)?;
                            node.pointers.insert(insert_pos + 1, right_node.page_id);
                            BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                            BTree::<K, V>::write_node(&right_node, &mut self.page_manager)?;
                            Ok(None)
                        } else {
                            println!("insert: INTERNAL and split and not can_insert");

                            let new_page_id = self.page_manager.allocate_page()?;
                            let (med_key, med_value, mut right) = node.split(new_page_id)?;

                            if promoted_key < med_key {
                                let insert_pos = node.find_key_position(&promoted_key)?;
                                node.insert(insert_pos, &promoted_key, &promoted_value)?;
                                node.pointers.insert(insert_pos + 1, right.page_id);
                            } else if promoted_key > med_key {
                                let insert_pos = right.find_key_position(&promoted_key)?;
                                right.insert(insert_pos, &promoted_key, &promoted_value)?;
                                right.pointers.insert(insert_pos + 1, right_node.page_id);
                            } else {
                                panic!("Weird")
                            }

                            BTree::<K, V>::write_node(node, &mut self.page_manager)?;
                            BTree::<K, V>::write_node(&right, &mut self.page_manager)?;
                            self.header.add_page();
                            Ok(Some((med_key, med_value, right)))
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

    fn write_header(header: &mut Header, page_manager: &mut PageManager) -> Result<(), BTreeError> {
        let buffer = header.serialize();
        page_manager.write_header(&buffer)?;
        Ok(())
    }

    fn write_node(
        page: &SlottedPage<K, V>,
        page_manager: &mut PageManager,
    ) -> Result<(), BTreeError> {
        let data = page.serialize(page_manager.page_size.try_into().unwrap())?;
        page_manager.write_page(page.page_id, &data)?;
        Ok(())
    }

    fn read_node(&mut self, page_id: u64) -> Result<SlottedPage<K, V>, BTreeError> {
        let (buffer, _) = self.page_manager.read_page(page_id)?;
        let node: SlottedPage<K, V> = SlottedPage::deserialize(&buffer);

        Ok(node)
    }

    fn print(&mut self, page_id: u64, level: usize, chars_prior: usize) {
        if level == 10 {
            panic!("Recursive limit");
        }
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

        let stringified_keys = format!("{:?}", node.read_keys().unwrap());
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
