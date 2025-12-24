use bincode::{deserialize, serialize};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Bytes, Read, Seek, Write};
use std::marker::PhantomData;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum NodeType {
    INTERNAL,
    LEAF,
}

struct Header {
    max_keys: usize,
}

struct PageManager {
    file: File,
    page_size: u64,
}

impl PageManager {
    fn allocate_page(&mut self) -> Result<u64, std::io::Error> {
        self.file.seek(std::io::SeekFrom::End(0))?;

        let byte_offset = self.file.seek(std::io::SeekFrom::Current(0))?;
        let page_id = byte_offset / self.page_size;

        self.file
            .write(&vec![0u8; self.page_size.try_into().unwrap()])?;

        return Ok(page_id);
    }

    fn write_page(&mut self, page_id: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(page_id * self.page_size))?;

        self.file.write_all(data)?;
        Ok(())
    }

    fn read_page(&mut self, page_id: u64) -> Result<(Box<Vec<u8>>, usize), std::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(page_id * self.page_size))?;

        let buffer_size: usize = (page_id * self.page_size).try_into().unwrap();
        let mut buffer = vec![0u8; buffer_size];
        let bytes_read = self.file.read(&mut buffer)?;
        Ok((Box::new(buffer), bytes_read))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Page<K, V> {
    page_id: u64,
    node_type: NodeType,
    keys: Vec<K>,
    values: Vec<V>,
    pointers: Vec<u64>,

    #[serde(skip_serializing)]
    max_keys: usize,
}

impl<K, V> Page<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(max_keys: usize, node_type: NodeType, page_manager: &mut PageManager) -> Page<K, V> {
        let page_id = page_manager.allocate_page().unwrap();
        Page {
            page_id: page_id,
            node_type: node_type,
            keys: vec![],
            values: vec![],
            pointers: vec![],
            max_keys: max_keys,
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

    fn is_full(&self) -> bool {
        return self.keys.len() == self.max_keys;
    }

    fn is_overfull(&self) -> bool {
        return self.keys.len() > self.max_keys;
    }
}

struct BTree<K, V> {
    header: Header,
    page_manager: PageManager,
    root_page_id: u64,

    _phantom: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Clone + PartialOrd + Debug + Serialize + for<'de> Deserialize<'de>,
    V: Clone + Debug + Serialize + for<'de> Deserialize<'de>,
{
    fn new(file: File, max_keys: usize) -> BTree<K, V> {
        let mut page_manager = PageManager {
            file: file,
            page_size: 4 * 1024,
        };

        let root_node: Page<K, V> = Page::new(max_keys, NodeType::LEAF, &mut page_manager);

        let mut btree = BTree::<K, V> {
            header: Header { max_keys: 4 },
            page_manager: page_manager,
            root_page_id: root_node.page_id,
            _phantom: PhantomData,
        };
        let _ = btree.write_node(&root_node);

        return btree;
    }

    fn insert(&mut self, key: K, value: V) -> Result<(), Box<dyn std::error::Error>> {
        let mut root = self.read_node(self.root_page_id)?;

        self.insert_non_full(&mut root, key, value)?;
        if root.is_full() {
            let mut new_root: Page<K, V> = Page::new(
                self.header.max_keys,
                NodeType::INTERNAL,
                &mut self.page_manager,
            );
            new_root.pointers.push(self.root_page_id);

            let (promoted_key, promoted_value, right_node) = self.split_child(&mut root);

            new_root.keys.push(promoted_key);
            new_root.values.push(promoted_value);
            new_root.pointers.push(right_node.page_id);

            self.write_node(&new_root)?;
            self.root_page_id = new_root.page_id;

            return Ok(());
        }

        Ok(())
    }

    fn insert_non_full(
        &mut self,
        node: &mut Page<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(K, V, Page<K, V>)>, Box<dyn std::error::Error>> {
        let result: Result<Option<(K, V, Page<K, V>)>, Box<dyn std::error::Error>> =
            match node.node_type {
                NodeType::LEAF => {
                    // If leaf is overflowing, it should be split
                    // Parent should point to current node AND a new node
                    node.insert_key_value(key, value);
                    if node.is_overfull() {
                        let split_child_result = self.split_child(node);
                        Ok(Some(split_child_result))
                    } else {
                        self.write_node(node)?;
                        Ok(None)
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
                        println!(
                            "Overflow with: key={:?} node={:?} child={:?}",
                            promoted_key, node, child
                        );
                        let pos = node.find_key_position(&promoted_key);
                        node.insert_key_value_node(
                            pos,
                            promoted_key,
                            promoted_value,
                            new_right.page_id,
                        );

                        if node.is_overfull() {
                            Ok(Some(self.split_child(node)))
                        } else {
                            self.write_node(node).unwrap();
                            Ok(None)
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

        let mut right_node = Page::new(node.max_keys, node.node_type, &mut self.page_manager);
        right_node.keys = node.keys.split_off(mid_index + 1);
        right_node.values = node.values.split_off(mid_index + 1);
        right_node.pointers = match node.node_type {
            NodeType::INTERNAL => node.pointers.split_off(mid_index + 1),
            NodeType::LEAF => vec![],
        };
        node.keys.pop();
        node.values.pop();

        self.write_node(&node).unwrap();
        self.write_node(&right_node).unwrap();

        (mid_key, mid_value, right_node)
    }

    fn write_node(&mut self, page: &Page<K, V>) -> Result<(), Box<dyn std::error::Error>> {
        println!("write_node page_id={} page={:?}", page.page_id, page);
        let data = serialize(page)?;
        self.page_manager.write_page(page.page_id, &data)?;
        Ok(())
    }

    fn read_node(&mut self, page_id: u64) -> Result<Page<K, V>, Box<dyn std::error::Error>> {
        let (buffer, _) = self.page_manager.read_page(page_id)?;
        let mut node: Page<K, V> = deserialize(&buffer)?;
        node.max_keys = self.header.max_keys;

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
        println!("BTREE: {}", self.root_page_id);
        self.print(self.root_page_id, 0, 0);
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
        .open(&index_filename)
        .expect("Failed to open file");

    let mut btree = BTree::<i64, i64>::new(file, 4);
    let mut rng = rand::rng();

    for i in 0..100 {
        let num: i64 = rng.random_range(0..101); // Generate a number in the range [0, 100]
        btree.insert(i, 100).unwrap();
        btree.print_tree();
    }
    btree.print_tree();
    println!("Finished run")
}
