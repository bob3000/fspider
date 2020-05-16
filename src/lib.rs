use async_recursion::async_recursion;
use async_std::fs::{self, DirEntry, File};
use async_std::path::{Path};
use async_std::prelude::*;
use async_std::task;
use async_std::io::{BufReader, SeekFrom};
use futures::future::Future;
use std::cmp::Eq;
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};
use futures::join;

pub type HashFNameMapInner<T> = HashMap<T, Vec<DirEntry>>;
pub type DupVecInner = Vec<Vec<DirEntry>>;

#[derive(Debug)]
pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<DirEntry>);

pub enum SortOrder {
    Alphanumeric,
    Size,
}

#[derive(Debug)]
pub struct DupVec {
    inner: Vec<Vec<DirEntry>>,
}

impl DupVec {
    pub fn sort(mut self, order: SortOrder) -> DupVec {
        match order {
            SortOrder::Size =>
                self.inner.sort_by(|a, b| {
                                    let meta_a = task::block_on(a[0].metadata()).unwrap();
                                    let meta_b = task::block_on(b[0].metadata()).unwrap();
                                    meta_a.len().cmp(&meta_b.len())
            }),
            SortOrder::Alphanumeric => self.inner.sort_by(|a, b| {
                a[0].path().cmp(&b[0].path())}),
        }
        self
    }

    pub fn into_inner(self) -> DupVecInner {
        self.inner
    }
}

pub struct HashFNameMap<T> {
    inner: HashFNameMapInner<T>,
 }

impl<T: Hash + Eq> HashFNameMap<T> {
    pub fn new() -> Self {
        Self{ inner: HashMap::new() }
    }

    pub fn duplicates(self) -> DupVec {
        let inner = self.inner.into_iter().filter(|(_, v)| v.len() > 1).map(|(_, v)| v).collect::<DupVecInner>(); //.collect::<Vec<(T, Vec<DirEntry>)>>().values()
        DupVec { inner }
    }
}

#[async_recursion(?Send)]
pub async fn recursive_file_map<T, F>(
    sender: mpsc::Sender<T>,
    path: impl AsRef<Path> + 'async_recursion,
    mut max_depth: i16,
    map_fn: &'async_recursion dyn Fn(DirEntry) -> F,
) -> Result<(), Box<dyn Error + Send>>
where
    T: Send + Sync,
    F: Future<Output = T>,
{
    let mut entries = fs::read_dir(path).await.unwrap();
    while let Some(entry) = entries.next().await {
        if max_depth == 0 { break; } else { max_depth -= 1 };
        let next_path = entry.as_ref().unwrap().path();

        if next_path.is_dir().await {
            recursive_file_map(sender.clone(), next_path.as_path(), max_depth, &map_fn)
                .await
                .unwrap();
        } else {
            sender.send(map_fn(entry.unwrap()).await).unwrap();
        }
    }
    Ok(())
}

pub async fn count_files(path: impl AsRef<Path>, max_depth: i16) -> Result<u64, Box<dyn Error>>
{
    let (sender, receiver): (Sender<u64>, Receiver<u64>)  = mpsc::channel();
    let mut total_count = 0u64;

    let reader_handle = task::spawn(async move {
        while let Ok(n) = receiver.recv() {
            total_count += n;
        }
        total_count
    });

    let writer_handle = recursive_file_map(sender, &path, max_depth, &|_: DirEntry| async move {
        1u64
    });

    let results = futures::join!(
        reader_handle,
        writer_handle
    );
    println!("total count: {}", results.0);
    Ok(results.0)
}

#[derive(Copy, Clone, Debug)]
pub struct MD5HashFileOpts {
    pub max_depth: i16,
    pub read_buf_size: usize,
    pub sample_rate: u64,
    pub sample_threshold: u64,
}

pub async fn md5_hash_files<C>(path: impl AsRef<Path>, opts: MD5HashFileOpts, mut loop_cb: C
) -> Result<HashFNameMap<md5::Digest>, Box<dyn Error + Send>>
where
    C: FnMut() + Send + Sync + 'static
{
    let (sender, receiver): (Sender<FileHash<md5::Digest>>, Receiver<FileHash<md5::Digest>>)  = mpsc::channel();
    let mut hash_fname_map: HashFNameMap<md5::Digest> = HashFNameMap::new();

    let reader_handle = task::spawn(async move {
        while let Ok(ref mut file_hash) = receiver.recv() {
            let key = file_hash.0.take().unwrap();
            let val = file_hash.1.take().unwrap();
            let entry = hash_fname_map.inner.entry(key).or_insert(Vec::new());
            entry.push(val);
            loop_cb();
        }
        for v in hash_fname_map.inner.values_mut() {
            v.sort_by(|a, b| {
                a.path().partial_cmp(&b.path()).unwrap()
            });
        }
        hash_fname_map
    });

    let hash_file_fn = &|e: DirEntry| async move {
        let f_handle = File::open(e.path()).await.unwrap();
        let f_size = f_handle.metadata().await.unwrap().len();
        let mut read_buf = vec![0; opts.read_buf_size];
        let mut buf_reader = BufReader::with_capacity(opts.read_buf_size, f_handle);
        let mut fdigest = md5::compute("");
        let do_sample = f_size > opts.sample_threshold;
        let mut skip_bytes = 0;
        let sample_rate = if opts.sample_rate < 1u64 { 1i64 } else { opts.sample_rate as i64 };
        if do_sample {
            skip_bytes = (f_size as i64 / sample_rate) as i64;
        }
        fn hash_it(mut read_buf: &mut Vec<u8>, last_digest: md5::Digest) -> md5::Digest {
            let mut to_hash: Vec<u8> = Vec::with_capacity(read_buf.len() + last_digest.0.len());
            to_hash.append(&mut last_digest.0.to_vec());
            to_hash.append(&mut read_buf);
            md5::compute(to_hash)
        }
        while 0 <  buf_reader.read(&mut read_buf[..]).await.unwrap() {
            fdigest = hash_it(&mut read_buf, fdigest);
            buf_reader.seek(SeekFrom::Current(skip_bytes)).await.unwrap();
        }
        buf_reader.read_to_end(&mut read_buf).await.unwrap();
        fdigest = hash_it(&mut read_buf, fdigest);

        FileHash(Some(fdigest), Some(e))
    };

    let writer_handle = recursive_file_map(sender, &path, opts.max_depth, hash_file_fn);

    let retval = futures::join!(
        reader_handle,
        writer_handle
    );
    Ok(retval.0)
}

mod test {
    use super::*;
    use md5;

    #[test]
    fn test_dup_vec() {
        let path = Path::new("./test_fixtures");
        let opts = MD5HashFileOpts{ max_depth: -1, read_buf_size: 256, sample_rate: 10, sample_threshold: 1024, };
        let got = task::block_on(md5_hash_files(path, opts, || {})).unwrap().duplicates().sort(SortOrder::Size);
        let want = r#"DupVec {
    inner: [
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
    ],
}"#;

        // println!("{:#?}", got);
        assert_eq!(format!("{:#?}", got), want);
    }

    #[test]
    fn test_md5_file_map() {
        let path = Path::new("./test_fixtures");
        let opts = MD5HashFileOpts{ max_depth: -1, read_buf_size: 256, sample_rate: 10, sample_threshold: 1024, };
        let got = task::block_on(md5_hash_files(path, opts, || {})).unwrap();
        let mut tuples: Vec<(md5::Digest, Vec<DirEntry>)> = got.inner.into_iter().collect();
        tuples.sort_by(|a, b| format!("{:?}", a.1[0]).cmp(&format!("{:?}", b.1[0])));
        let want = r#"[
    (
        b743cd81f58d58406a0874356eb3d03f,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/a",
                },
            ),
        ],
    ),
    (
        ff227372abc3f8c57a807e5064d79b59,
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
        9942b78186753bd8f9c0e8185df35cd4,
        [
            DirEntry(
                PathBuf {
                    inner: "./test_fixtures/alpha/bravo/charlie/c",
                },
            ),
        ],
    ),
    (
        5070a59f0f65446b614921f0655125cf,
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
        assert_eq!(format!("{:#?}", tuples), want);
        // println!("{:#?}", tuples);
    }

    #[test]
    fn test_count_files() {
        let path = Path::new("./test_fixtures");
        let max_depth = -1;
        let got = task::block_on(count_files(path, max_depth));
        assert_eq!(6u64, got.unwrap());
    }
}
