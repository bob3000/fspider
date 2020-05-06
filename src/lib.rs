use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry};
use async_std::path::Path;
use async_std::prelude::*;
use async_std::task;
use futures::future::Future;
use std::cmp::{Eq, Ord, Ordering};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};
use futures::join;

pub type HashFNameMap<T> = HashMap<T, Vec<String>>;
pub type HashFNameResult<T> = Vec<(T, Vec<String>)>;
pub struct FileHash<T: Hash + Eq + Ord + Send + Sync>(Option<T>, Option<String>);

#[async_recursion(?Send)]
pub async fn hash_files<T, F>(
    sender: mpsc::Sender<FileHash<T>>,
    path: impl AsRef<Path> + 'async_recursion,
    hash_fn: &'async_recursion dyn Fn(DirEntry) -> F,
) -> Result<(), Box<dyn Error + Send>>
where
    T: Hash + Eq + Ord + Send + Sync + Debug,
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

#[derive(Debug, Eq, PartialEq, Hash)]
struct MD5Digest {
    digest: md5::Digest,
}

impl PartialOrd for MD5Digest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(format!("{:?}", self.digest).cmp(&format!("{:?}", other.digest)))
    }
}

impl Ord for MD5Digest {
    fn cmp(&self, other: &Self) -> Ordering {
        format!("{:?}", self.digest).cmp(&format!("{:?}", other.digest))
    }
}

impl MD5Digest {
    fn new(digest: md5::Digest) -> Self {
        MD5Digest{
            digest
        }
    }
}

async fn md5_hash_files<C>(path: impl AsRef<Path>, mut cb: C)
where C: FnMut(HashFNameResult<MD5Digest>) + Send + Sync + 'static
{
        let (sender, receiver): (Sender<FileHash<MD5Digest>>, Receiver<FileHash<MD5Digest>>)  = mpsc::channel();

        let mut hash_fname_map: HashFNameMap<MD5Digest> = HashMap::new();

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
            let mut result: HashFNameResult<MD5Digest> = hash_fname_map.into_iter().collect();
            result.sort();
            cb(result);
        });


        futures::join!(
            reader_handle,
            hash_files(sender, &path, &|e: DirEntry| async move {
                let contents = fs::read(e.path()).await.unwrap();
                MD5Digest::new(md5::compute(contents))
            })
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
        MD5Digest {
            digest: 059f99d9af988b464474f5a7815c7e22,
        },
        [
            "./test_fixtures/alpha/bravo/charlie/c",
        ],
    ),
    (
        MD5Digest {
            digest: 40042c928f411964c8d542874c8c4fb8,
        },
        [
            "./test_fixtures/alpha/b",
            "./test_fixtures/alpha/bravo/charlie/b",
        ],
    ),
    (
        MD5Digest {
            digest: 55179001e96aceaf7cf5cad3e2ff8873,
        },
        [
            "./test_fixtures/alpha/bravo/charlie/d",
            "./test_fixtures/alpha/bravo/d",
        ],
    ),
    (
        MD5Digest {
            digest: 8c357cff93cddd4412996e178ba3f426,
        },
        [
            "./test_fixtures/alpha/a",
        ],
    ),
]"#;
            assert_eq!(format!("{:#?}", got), want);
        }));
        // println!("{:#?}", hash_fname_map);
    }
}
