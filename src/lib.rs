use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry};
use async_std::path::{Path};
use async_std::prelude::*;
use async_std::task;
use futures::future::Future;
use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};
use futures::join;

pub type HashFNameMap<T> = HashMap<T, Vec<DirEntry>>;
pub type SortedFNames<T> = Vec<(T, Vec<DirEntry>)>;

#[derive(Debug)]
pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<DirEntry>);

pub enum SortOrder {
    Alphanumeric,
    Size,
}

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

pub async fn count_files(path: impl AsRef<Path>) -> Result<u64, Box<dyn Error>>
{
    let (sender, receiver): (Sender<u64>, Receiver<u64>)  = mpsc::channel();
    let mut total_count = 0u64;

    let reader_handle = task::spawn(async move {
        while let Ok(n) = receiver.recv() {
            total_count += n;
        }
        total_count
    });

    let writer_handle = recursive_file_map(sender, &path, &|_: DirEntry| async move {
        1u64
    });

    let results = futures::join!(
        reader_handle,
        writer_handle
    );
    println!("total count: {}", results.0);
    Ok(results.0)
}

pub async fn md5_hash_files<C>(path: impl AsRef<Path>, mut loop_cb: C
) -> Result<HashFNameMap<md5::Digest>, Box<dyn Error + Send>>
where
    C: FnMut() + Send + Sync + 'static
{
    let (sender, receiver): (Sender<FileHash<md5::Digest>>, Receiver<FileHash<md5::Digest>>)  = mpsc::channel();
    let mut hash_fname_map: HashFNameMap<md5::Digest> = HashMap::new();

    let reader_handle = task::spawn(async move {
        while let Ok(ref mut file_hash) = receiver.recv() {
            let key = file_hash.0.take().unwrap();
            let val = file_hash.1.take().unwrap();
            let entry = hash_fname_map.entry(key).or_insert(Vec::new());
            entry.push(val);
            loop_cb();
        }
        for v in hash_fname_map.values_mut() {
            v.sort_by(|a, b| {
                a.path().partial_cmp(&b.path()).unwrap()
            });
        }
        hash_fname_map
    });

    let writer_handle = recursive_file_map(sender, &path, &|e: DirEntry| async move {
        let contents = fs::read(e.path()).await.unwrap();
        let fdigest = md5::compute(contents);
        FileHash(Some(fdigest), Some(e))
    });

    let retval = futures::join!(
        reader_handle,
        writer_handle
    );
    Ok(retval.0)
}

pub fn sort_hash_fname<T>(hfm: HashFNameMap<T>, order: SortOrder) -> SortedFNames<T> {
    let mut tuples: SortedFNames<T> = hfm.into_iter().collect();

    match order {
        SortOrder::Size => tuples.sort_by(|a, b| {
                                let meta_a = task::block_on(a.1[0].metadata()).unwrap();
                                let meta_b = task::block_on(b.1[0].metadata()).unwrap();
                                meta_a.len().cmp(&meta_b.len())
        }),
        SortOrder::Alphanumeric => tuples.sort_by(|a, b| {
            a.1[0].path().cmp(&b.1[0].path())}),
    }
    tuples
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_md5_file_map() {
        let path = Path::new("./test_fixtures");
        let got = task::block_on(md5_hash_files(path, || {})).unwrap();
        let sorted_tuples = sort_hash_fname(got, SortOrder::Alphanumeric);
        let want = r#"[
    (
        8c357cff93cddd4412996e178ba3f426,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/a",
                },
            ),
        ],
    ),
    (
        40042c928f411964c8d542874c8c4fb8,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/b",
                },
            ),
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/bravo/charlie/b",
                },
            ),
        ],
    ),
    (
        059f99d9af988b464474f5a7815c7e22,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/bravo/charlie/c",
                },
            ),
        ],
    ),
    (
        55179001e96aceaf7cf5cad3e2ff8873,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/bravo/charlie/d",
                },
            ),
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/bravo/d",
                },
            ),
        ],
    ),
]"#;
        assert_eq!(format!("{:#?}", sorted_tuples), want);
        // println!("{:#?}", sorted_tuples);
    }

    #[test]
    fn test_count_files() {
        let path = Path::new("./test_fixtures");
        let got = task::block_on(count_files(path));
        assert_eq!(6u64, got.unwrap());
    }
}
