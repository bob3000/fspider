use async_std::fs::{self, DirEntry};
use async_std::path::Path;
use async_std::prelude::*;
use futures::{
    executor,
    future::{Future, FutureExt, LocalBoxFuture},
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash; // 0.3.4
use std::sync::mpsc;

pub type HashFNameMap<T> = HashMap<T, Vec<String>>;

pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<String>);

pub fn hash_files<T, U>(
    path: impl AsRef<Path>,
    hash_fn: impl FnMut(DirEntry) -> T,
) -> HashFNameMap<U>
where
    T: Future<Output = U>,
    U: Hash + Eq + Send + Sync + Debug,
{
    async fn walk<T, U>(
        path: impl AsRef<Path>,
        mut hash_fn: impl FnMut(DirEntry) -> T,
        tx: mpsc::Sender<FileHash<U>>,
    ) where
        T: Future<Output = U>,
        U: Hash + Eq + Send + Sync + Debug,
    {
        fn recursive_walk<'a, T, U>(
            path: &'a Path,
            hash_fn: &'a mut dyn FnMut(DirEntry) -> T,
            tx: mpsc::Sender<FileHash<U>>,
        ) -> LocalBoxFuture<'a, ()>
        where
            T: Future<Output = U>,
            U: 'a + Hash + Eq + Send + Sync + Debug,
        {
            async move {
                let mut entries = fs::read_dir(path).await.unwrap();
                while let Some(path) = entries.next().await {
                    let entry = path.unwrap();
                    let path = entry.path();
                    if path.is_file().await {
                        let file_name = format!("{:?}", &entry.path());
                        let hash = hash_fn(entry).await;
                        let file_hash = FileHash(Some(hash), Some(file_name));
                        tx.send(file_hash).unwrap();
                    } else {
                        recursive_walk(&path, hash_fn, tx.clone()).await
                    }
                }
            }
            .boxed_local()
        }
        recursive_walk(path.as_ref(), &mut hash_fn, tx).await
    }

    let (tx, rx) = mpsc::channel();
    executor::block_on({ walk(path, hash_fn, tx) });
    let mut hash_fname_map: HashFNameMap<U> = HashMap::new();
    while let Some(ref mut file_hash) = rx.recv().iter_mut().next() {
        let key = file_hash.0.take().unwrap();
        let val = file_hash.1.take().unwrap();
        let entry = hash_fname_map.entry(key).or_insert(Vec::new());
        entry.push(val);
    }
    hash_fname_map
}

mod test {
    use super::*;

    #[test]
    fn test_md5_file_map() {
        let res = hash_files(Path::new("./target"), |entry| async move {
            let contents = fs::read(entry.path()).await;
            md5::compute(contents.unwrap())
        });
        println!("{:#?}", res);
    }
}
