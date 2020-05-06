use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry};
use async_std::path::Path;
use async_std::prelude::*;
use async_std::task;
use futures::future::Future;
use std::cmp::{Eq, Ord};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};
use futures::join;

pub type HashFnameMap<T> = HashMap<T, Vec<String>>;
pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<String>);

#[async_recursion(?Send)]
pub async fn hash_files<T, F>(
    sender: mpsc::Sender<FileHash<T>>,
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
            hash_files(sender.clone(), next_path.as_path(), &hash_fn)
                .await
                .unwrap();
        } else {
            let fname = format!("{}", next_path.as_path().to_str().unwrap());
            let fdigest = hash_fn(entry.unwrap()).await;
            sender.send(FileHash(Some(fdigest), Some(fname))).unwrap();
        }
    }
    Ok(())
}

async fn md5_hash_files<C>(path: impl AsRef<Path>, mut cb: C)
where C: FnMut(HashFnameMap<md5::Digest>) + Send + Sync + 'static
{
        let (sender, receiver): (Sender<FileHash<md5::Digest>>, Receiver<FileHash<md5::Digest>>)  = mpsc::channel();

        let mut hash_fname_map: HashFnameMap<md5::Digest> = HashMap::new();

        let reader_handle = task::spawn(async move {
            while let Ok(ref mut file_hash) = receiver.recv() {
                let key = file_hash.0.take().unwrap();
                let val = file_hash.1.take().unwrap();
                let entry = hash_fname_map.entry(key).or_insert(Vec::new());
                entry.push(val);
            }
            cb(hash_fname_map);
        });

        futures::join!(
            reader_handle,
            hash_files(sender, &path, &|e: DirEntry| async move {
                let contents = fs::read(e.path()).await.unwrap();
                md5::compute(contents)
            })
        ).1.unwrap();
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_md5_file_map() {
        let path = Path::new("./target");
        task::block_on(md5_hash_files(path, |hfm| {
            for (k, v) in hfm.iter() {
                println!("{:?}, {:#?}", k, v);
            }
        }));
        // println!("{:#?}", hash_fname_map);
    }
}
