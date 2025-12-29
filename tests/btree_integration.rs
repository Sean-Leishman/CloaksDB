use cloaksdb::BTree;
use tempfile::NamedTempFile; // Uses public API only

fn create_temp_btree() -> BTree<i64, String> {
    let file = NamedTempFile::new().unwrap();
    BTree::new(file.reopen().unwrap(), 4096).unwrap()
}

#[test]
fn insert_and_search() {
    let mut btree = create_temp_btree();

    btree.insert(1, "one".to_string()).unwrap();
    btree.insert(2, "two".to_string()).unwrap();

    assert_eq!(btree.search(1).unwrap(), "one");
    assert_eq!(btree.search(2).unwrap(), "two");
}

#[test]
fn persistence_across_reopen() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        let mut btree: BTree<i64, String> = BTree::<i64, String>::new(f, 4096).unwrap();
        btree.insert(1, "one".to_string()).unwrap();
    }

    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        let mut btree = BTree::<i64, String>::new(f, 4096).unwrap();
        assert_eq!(btree.search(1).unwrap(), "one");
    }
}
