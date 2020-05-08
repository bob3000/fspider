use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry};
use async_std::path::Path;
use async_std::prelude::*;
use async_std::task;
use futures::future::Future;
use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};
use futures::join;

pub type HashFNameMap<T> = HashMap<T, Vec<String>>;
pub type HashFNameResult<T> = Vec<(T, Vec<String>)>;
pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<String>);

#[async_recursion(?Send)]
pub async fn recursive_file_map<T, F>(
    sender: mpsc::Sender<T>,
    path: impl AsRef<Path> + 'async_recursion,
    map_fn: &'async_recursion dyn Fn(DirEntry) -> F,
) -> Result<(), Box<dyn Error + Send>>
where
    T: Send + Sync,
    F: Future<Output = T>,
{
    let mut entries = fs::read_dir(path).await.unwrap();
    while let Some(entry) = entries.next().await {
        let next_path = entry.as_ref().unwrap().path();
        if next_path.is_dir().await {
            recursive_file_map(sender.clone(), next_path.as_path(), &map_fn)
                .await
                .unwrap();
        } else {
            sender.send(map_fn(entry.unwrap()).await).unwrap();
        }
    }
    Ok(())
}

pub async fn count_files<C>(path: impl AsRef<Path>, mut cb: C)
where C: FnMut(Result<u64, Box<dyn Error + Send>>) + Send + Sync + 'static
{
    let (sender, receiver): (Sender<u64>, Receiver<u64>)  = mpsc::channel();
    let mut total_count = 0u64;

    let reader_handle = task::spawn(async move {
        while let Ok(n) = receiver.recv() {
            total_count += n;
        }
        cb(Ok(total_count));
    });

    let writer_handle = recursive_file_map(sender, &path, &|_: DirEntry| async move {
        1u64
    });

    futures::join!(
        reader_handle,
        writer_handle
    ).1.unwrap();
}

pub async fn md5_hash_files<C>(path: impl AsRef<Path>, mut cb: C)
where C: FnMut(Result<HashFNameResult<md5::Digest>, Box<dyn Error + Send>>) + Send + Sync + 'static
{
    let (sender, receiver): (Sender<FileHash<md5::Digest>>, Receiver<FileHash<md5::Digest>>)  = mpsc::channel();
    let mut hash_fname_map: HashFNameMap<md5::Digest> = HashMap::new();

    let reader_handle = task::spawn(async move {
        while let Ok(ref mut file_hash) = receiver.recv() {
            let key = file_hash.0.take().unwrap();
            let val = file_hash.1.take().unwrap();
            let entry = hash_fname_map.entry(key).or_insert(Vec::new());
            entry.push(val);
        }
        for v in hash_fname_map.values_mut() {
            v.sort();
        }
        let mut result: HashFNameResult<md5::Digest> = hash_fname_map.into_iter().collect();
        result.sort_by(|a, b| a.1[0].cmp(&b.1[0]));
        cb(Ok(result));
    });

    let writer_handle = recursive_file_map(sender, &path, &|e: DirEntry| async move {
        let contents = fs::read(e.path()).await.unwrap();
        let fdigest = md5::compute(contents);
        let fname = format!("{}", e.path().to_str().unwrap());
        FileHash(Some(fdigest), Some(fname))
    });

    futures::join!(
        reader_handle,
        writer_handle
    ).1.unwrap();
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_md5_file_map() {
        let path = Path::new("./test_fixtures");
        task::block_on(md5_hash_files(path, |got| {
            let want = r#"[
    (
        8c357cff93cddd4412996e178ba3f426,
        [
            "./test_fixtures/alpha/a",
        ],
    ),
    (
        40042c928f411964c8d542874c8c4fb8,
        [
            "./test_fixtures/alpha/b",
            "./test_fixtures/alpha/bravo/charlie/b",
        ],
    ),
    (
        059f99d9af988b464474f5a7815c7e22,
        [
            "./test_fixtures/alpha/bravo/charlie/c",
        ],
    ),
    (
        55179001e96aceaf7cf5cad3e2ff8873,
        [
            "./test_fixtures/alpha/bravo/charlie/d",
            "./test_fixtures/alpha/bravo/d",
        ],
    ),
]"#;
            assert_eq!(format!("{:#?}", got.unwrap()), want);
        }));
        // println!("{:#?}", hash_fname_map);
    }

    #[test]
    fn test_count_files() {
        let path = Path::new("./test_fixtures");
        task::block_on(count_files(path, |got| {
            assert_eq!(6u64, got.unwrap());
        }));
    }
}
