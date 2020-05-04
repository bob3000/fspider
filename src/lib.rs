use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry};
use async_std::path::Path;
use async_std::prelude::*;
use async_std::task;
use futures::future::Future;
use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;

// pub type HashFnameMap<T> = HashMap<T, Vec<String>>;
// pub type HashFn<T> = dyn Fn(&[u8]) -> T + Send + Sync + 'static;

#[async_recursion(?Send)]
pub async fn hash_files<T, F>(
    path: impl AsRef<Path> + 'async_recursion,
    hash_fn: &'async_recursion dyn Fn(DirEntry) -> F,
) -> Result<(), Box<dyn Error + Send>>
where
    T: Hash + Eq + Send + Sync + Debug,
    F: Future<Output = T>,
{
    let mut entries = fs::read_dir(path).await.unwrap();
    while let Some(entry) = entries.next().await {
        let next_path = entry.as_ref().unwrap().path();
        if next_path.is_dir().await {
            hash_files(next_path.as_path(), &hash_fn).await.unwrap();
        } else {
            let fname = format!("{}", next_path.as_path().to_str().unwrap());
            let fdigest = hash_fn(entry.unwrap()).await;
            println!("{}: {:#?}", fname, fdigest);
        }
    }
    Ok(())
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_md5_file_map() {
        let path = Path::new("./target");
        task::block_on(hash_files(&path, &|e: DirEntry| async move {
            let contents = fs::read(e.path()).await.unwrap();
            md5::compute(contents)
        }))
        .unwrap();
    }
}
