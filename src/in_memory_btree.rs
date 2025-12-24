use rand::Rng;
use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::fs::File;

enum NodeType {
    Root,
    Internal,
    Leaf,
}

struct Page<K, V> {
    page_id: u64,
    node_type: NodeType,
    keys: Vec<K>,
    pointers: Vec<u64>,
    data: Option<Vec<V>>,
}

#[derive(Debug)]
struct BTree<K, V> {
    root: Node<K, V>,
    file: File,
}

#[derive(Debug, Clone, PartialEq)]
struct Node<K, V> {
    is_leaf: bool,
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<Box<Node<K, V>>>,
    max_keys: usize,
}

impl<K, V> Node<K, V>
where
    K: Clone + PartialEq + Debug + PartialOrd,
    V: Clone + PartialEq + Debug,
{
    fn new() -> Node<K, V> {
        Node {
            is_leaf: true,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            max_keys: 5,
        }
    }

    fn find_key_position(&self, key: &K) -> usize {
        self.keys
            .iter()
            .position(|k| key <= k)
            .unwrap_or(self.keys.len())
    }

    fn insert(&mut self, key: K, value: V) -> Option<(K, V, Box<Node<K, V>>)> {
        let pos = self.find_key_position(&key);
        if pos < self.keys.len() && self.keys[pos] == key {
            self.values[pos] = value;
            return None;
        }

        self.keys.insert(pos, key);
        self.values.insert(pos, value);

        if self.keys.len() > self.max_keys {
            Some(self.split())
        } else {
            None
        }
    }

    fn insert_non_leaf(&mut self, pos: usize, key: K, value: V, right_child: Box<Node<K, V>>) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        self.children.insert(pos + 1, right_child)
    }

    fn split(&mut self) -> (K, V, Box<Node<K, V>>) {
        let mid_index = self.keys.len() / 2;

        let mid_key = self.keys[mid_index].clone();
        let mid_value = self.values[mid_index].clone();

        let right_node = Box::new(Node {
            keys: self.keys.split_off(mid_index + 1),
            values: self.values.split_off(mid_index + 1),
            children: if !self.is_leaf {
                self.children.split_off(mid_index + 1)
            } else {
                vec![]
            },
            is_leaf: self.is_leaf,
            max_keys: self.max_keys,
        });

        self.keys.pop();
        self.values.pop();

        (mid_key, mid_value, right_node)
    }

    fn insert_internal(&mut self, key: K, value: V) -> Option<(K, V, Box<Node<K, V>>)> {
        if self.is_leaf {
            return self.insert(key, value);
        }

        let child_index = self.find_key_position(&key);
        if let Some(child) = self.children.get_mut(child_index) {
            if let Some((promoted_key, promoted_value, new_right)) =
                child.insert_internal(key, value)
            {
                let pos = self.find_key_position(&promoted_key);
                self.insert_non_leaf(pos, promoted_key, promoted_value, new_right);

                if self.keys.len() > self.max_keys {
                    return Some(self.split());
                }
            }
        }
        None
    }

    fn print_tree(&self, level: usize, chars_prior: usize) {
        let prior_char = if level == 0 {
            ""
        } else {
            match self.children.is_empty() {
                false => "└",
                true => "├",
            }
        };
        let post_char = match self.children.is_empty() {
            false => "┐",
            true => "",
        };

        let stringified_keys = format!("{:?}", self.keys);
        println!(
            "{}{}{}{}",
            " ".repeat(chars_prior),
            prior_char,
            stringified_keys,
            post_char,
        );
        self.children.iter().for_each(|child| {
            child.print_tree(level + 1, 1 + chars_prior + stringified_keys.len())
        });
    }
}

impl<K, V> BTree<K, V>
where
    K: Clone + PartialEq + Debug + PartialOrd,
    V: Clone + PartialEq + Debug,
{
    fn new(file: File) -> BTree<K, V> {
        BTree {
            root: Node::new(),
            file: file,
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if let Some((promoted_key, promoted_value, right_node)) =
            self.root.insert_internal(key, value)
        {
            // Requires an additional level of depth
            let new_root = Node {
                is_leaf: false,
                keys: vec![promoted_key],
                values: vec![promoted_value],
                children: vec![Box::new(self.root.clone()), right_node],
                max_keys: self.root.max_keys,
            };
            self.root = new_root
        }
    }

    fn print(&self) {
        self.root.print_tree(0, 0);
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

    let mut btree = BTree::<i64, i64>::new(file);
    let mut rng = rand::rng();

    for _ in 0..100 {
        let num: i64 = rng.random_range(0..101); // Generate a number in the range [0, 100]
        btree.insert(num, 100)
    }
    btree.print();
}
