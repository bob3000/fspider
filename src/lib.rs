use async_recursion::async_recursion;
use async_std::fs;
use async_std::path::Path;
use async_std::prelude::*;
use async_std::task;
use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;

pub type HashFnameMap<T> = HashMap<T, Vec<String>>;
pub type HashFn<T> = dyn Fn(&[u8]) -> T + Send + Sync + 'static;

#[async_recursion]
pub async fn md5_file_map<'a, T>(
    map: &'a mut HashFnameMap<T>,
    path: &Path,
    hash_fn: &HashFn<T>,
) -> Result<&'a mut HashFnameMap<T>, Box<dyn Error>>
where
    T: Hash + Eq + Send + Sync,
{
    println!("{:?}", path.file_name());
    let mut entries = fs::read_dir(path).await?;
    while let Some(entry) = entries.next().await {
        let next_path = entry?.path();
        if next_path.is_dir().await {
            md5_file_map(map, next_path.as_path(), hash_fn).await?;
        } else {
            let fcontents = fs::read(&next_path).await?;
            let fdigest = hash_fn(&fcontents[..]);
            let entry = map.entry(fdigest).or_insert(Vec::new());
            entry.push(format!("{}", next_path.as_path().to_str().unwrap()));
        }
    }
    Ok(map)
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_md5_file_map() {
        let mut fhm: HashFnameMap<md5::Digest> = HashMap::new();
        let path = Path::new("./target");
        task::block_on(md5_file_map(&mut fhm, &path, &|e: &[u8]| md5::compute(e))).unwrap();
        println!("{:#?}", fhm);
    }
}
