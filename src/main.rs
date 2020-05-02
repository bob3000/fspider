mod lib;
use async_std::path::Path;
use async_std::task;
use lib::{md5_file_map, HashFnameMap};
use std::collections::HashMap;

fn main() {
    let mut fhm: HashFnameMap<md5::Digest> = HashMap::new();
    let path = Path::new("./target");
    task::block_on(md5_file_map(&mut fhm, &path, &|e: &[u8]| md5::compute(e))).unwrap();
    println!("{:#?}", fhm);
}
