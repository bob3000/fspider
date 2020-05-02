use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::hash::Hash;
use std::path::Path;

use md5;

type HashFnameMap<T> = HashMap<T, Vec<String>>;
type HashFn<T> = dyn Fn(&[u8]) -> T;

pub fn md5_file_map<'a, T: Hash + Eq>(
    map: &'a mut HashFnameMap<T>,
    path: &Path,
    hash_fn: &HashFn<T>,
) -> Result<&'a mut HashFnameMap<T>, Box<dyn Error>> {
    for entry in fs::read_dir(path)? {
        let next_path = entry?.path();
        if next_path.is_dir() {
            md5_file_map(map, next_path.as_path(), hash_fn)?;
        } else {
            let fcontents = fs::read(&next_path)?;
            let fdigest = hash_fn(&fcontents[..]);
            let entry = map.entry(fdigest).or_insert(Vec::new());
            entry.push(format!("{}", next_path.as_path().to_str().unwrap()));
        }
    }
    Ok(map)
}

mod test {
    use super::*;

    #[test]
    fn test_md5_file_map() {
        let mut fhm: HashFnameMap<md5::Digest> = HashMap::new();
        let path = Path::new(".");
        md5_file_map(&mut fhm, &path, &|e: &[u8]| md5::compute(e)).unwrap();
        println!("{:#?}", fhm);
    }
}
