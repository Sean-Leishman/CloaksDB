use cloaksdb::BTree;
use tempfile::NamedTempFile;

#[test]
fn insert_ten_thousand_entries() {
    let file = NamedTempFile::new().unwrap();
    let mut btree = BTree::<i64, i64>::new(file.reopen().unwrap(), 4096).unwrap();

    for i in 0..10_000 {
        btree.insert(i, i * 2).unwrap();
    }

    btree.print_tree();

    for i in 0..10_000 {
        assert_eq!(btree.search(i).unwrap(), i * 2);
    }
}

#[test]
#[ignore] // Run with: cargo test -- --ignored
fn insert_one_million_entries() {
    let file = NamedTempFile::new().unwrap();
    let mut btree = BTree::<i64, i64>::new(file.reopen().unwrap(), 4096).unwrap();

    for i in 0..1_000_000 {
        btree.insert(i, i).unwrap();
    }
}
