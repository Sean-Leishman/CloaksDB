use cloaksdb::BTree;
use rand::Rng;

fn main() {
    env_logger::init();

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

    let mut btree = BTree::<i64, i64>::new(file, 1024).unwrap();
    let mut rng = rand::rng();

    for i in (400..1000).rev() {
        let _: i64 = rng.random_range(0..101); // Generate a number in the range [0, 100]
        btree.insert(i, 100).unwrap()
    }
    btree.print_tree();
    println!("Finished run");

    // btree.print_tree();
    println!("Search (key={}): {}", 500, btree.search(500).unwrap());
}
