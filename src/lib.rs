use async_recursion::async_recursion;
use async_std::fs::{self, File};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;
use async_std::task;
use async_std::io::{BufReader, SeekFrom};
use futures::future::{self, Future};
use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::mpsc::{self, Sender, Receiver};

pub type HashFNameMapInner<T> = HashMap<T, Vec<PathBuf>>;
pub type DupVecInner = Vec<Vec<PathBuf>>;

#[derive(Debug)]
pub struct FileHash<T: Hash + Eq + Send + Sync>(Option<T>, Option<PathBuf>);

pub enum SortOrder {
    Lexicographic,
    Size,
}

#[derive(Debug)]
pub struct DupVec {
    inner: Vec<Vec<PathBuf>>,
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
            SortOrder::Lexicographic => self.inner.sort_by(|a, b| {
                a[0].cmp(&b[0])}),
        }
        self
    }

    pub fn into_inner(self) -> DupVecInner {
        self.inner
    }
}

#[derive(Debug)]
pub struct HashFNameMap<T: Hash + Eq> {
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
    follow_symlinks: bool,
    map_fn: &'async_recursion dyn Fn(PathBuf) -> F,
) -> Result<(), std::io::Error>
where
    T: Send + Sync,
    F: Future<Output = T>,
{
    max_depth -= 1;
    let mut entries = fs::read_dir(path).await?;
    while let Some(entry) = entries.next().await {
        let next_path = entry.as_ref().unwrap().path();

        let f_sym_meta = next_path.symlink_metadata().await?;
        let f_type = f_sym_meta.file_type();
        if f_type.is_symlink() && !follow_symlinks {
            // return Err(std::io::Error::new(ErrorKind::Other, "follow symlinks disabled"));
            continue;
        } else if next_path.is_dir().await {
            if max_depth == 0 { return Ok(()) };
            recursive_file_map(sender.clone(), next_path.as_path(), max_depth, follow_symlinks, &map_fn).await?;
        } else {
            sender.send(map_fn(entry.unwrap().path()).await).unwrap();
        }
    }
    Ok(())
}

pub async fn crawl_fs(path: impl AsRef<Path>, max_depth: i16, follow_symlinks: bool) -> Result<Vec<PathBuf>, std::io::Error>
{
    let (sender, receiver): (Sender<PathBuf>, Receiver<PathBuf>)  = mpsc::channel();
    let mut file_vec = Vec::new();

    let reader_handle = task::spawn(async move {
        while let Ok(path) = receiver.recv() {
            file_vec.push(path);
        }
        file_vec
    });

    let writer_handle = recursive_file_map(sender, &path, max_depth, follow_symlinks, &|d: PathBuf| async move {
        d
    });

    let results = futures::join!(
        reader_handle,
        writer_handle
    );
    Ok(results.0)
}

pub async fn hash_file_vec<C, F, T>(mut files: Vec<PathBuf>, batch_size: u16, hash_fn: &dyn Fn(PathBuf) -> F, mut loop_cb: C,
) -> Result<HashFNameMap<T>, std::io::Error>
where
    C: FnMut() + Send + Sync + 'static,
    T: Hash + Eq + Send + Sync,
    F: Future<Output = Result<FileHash<T>, std::io::Error>>,
{
    let num_files =  files.len();
    let mut files_processed: usize = 0;
    let mut hash_fname_map: HashFNameMap<T> = HashFNameMap::new();
    let mut join_handle: Vec<F> = Vec::new();

    async fn join_all<F, T>(handles: Vec<F>, hfm: &mut HashFNameMap<T>
    ) -> Result<(), std::io::Error>
    where
        T: Hash + Eq + Send + Sync,
        F: Future<Output = Result<FileHash<T>, std::io::Error>>,
    {
        let results = future::join_all(handles.into_iter()).await;
        for res in results.into_iter() {
            match res {
                Ok(file_hash) => {
                    let entry = hfm.inner.entry(file_hash.0.unwrap()).or_insert(Vec::new());
                    entry.push(file_hash.1.unwrap());
                },
                Err(e) => {
                    eprintln!("{:?}", e);
                },
            };
        }
        Ok(())
    }

    while let Some(f) = files.pop() {
        files_processed += 1;
        loop_cb();
        join_handle.push(hash_fn(f));
        if files_processed % batch_size as usize == 0 {
            join_all(join_handle, &mut hash_fname_map).await?;
            join_handle = Vec::new();
        }
    }
    join_all(join_handle, &mut hash_fname_map).await?;
    for _ in 0..=num_files-files_processed {
        loop_cb();
    }

    Ok(hash_fname_map)
}

#[derive(Copy, Clone, Debug)]
pub struct MD5HashFileOpts {
    pub max_depth: i16,
    pub follow_symlinks: bool,
    pub read_buf_size: usize,
    pub sample_rate: u64,
    pub sample_threshold: u64,
    pub batch_size: u16,
}

pub async fn md5_hash_file_vec<C>(files: Vec<PathBuf>, opts: MD5HashFileOpts, loop_cb: C,
) -> Result<HashFNameMap<md5::Digest>, std::io::Error>
where
    C: FnMut() + Send + Sync + 'static,
{

    let hash_fn = |e: PathBuf| async move {
        let f_meta = e.metadata().await?;
        let f_size = f_meta.len();
        let f_handle = File::open(&e).await?;
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

        while 0 <  buf_reader.read(&mut read_buf[..]).await? {
            fdigest = hash_it(&mut read_buf, fdigest);
            buf_reader.seek(SeekFrom::Current(skip_bytes)).await?;
        }

        // if the buffer can't be filled close to the EOF we still have to get the remaining data
        buf_reader.read_to_end(&mut read_buf).await?;
        fdigest = hash_it(&mut read_buf, fdigest);

        Ok(FileHash(Some(fdigest), Some(e)))
    };

    let mut hash_fname_map = hash_file_vec(files, opts.batch_size, &hash_fn, loop_cb).await?;
    for v in hash_fname_map.inner.values_mut() {
        v.sort_by(|a, b| {
            a.partial_cmp(b).unwrap()
        });
    }

    Ok(hash_fname_map)
}

mod test {
    use super::*;

    #[test]
    fn test_dup_vec() {
        let path = Path::new("./test_fixtures");
        let opts = MD5HashFileOpts{
            max_depth: -1,
            follow_symlinks: false,
            read_buf_size: 256,
            sample_rate: 10,
            sample_threshold: 1024,
            batch_size: 2
        };
        let files = task::block_on(crawl_fs(path, opts.max_depth, false)).unwrap();
        let got = task::block_on(md5_hash_file_vec(files, opts, || {})).unwrap().duplicates().sort(SortOrder::Size);
        let want = r#"DupVec {
    inner: [
        [
            PathBuf {
                inner: "./test_fixtures/alpha/b",
            },
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/charlie/b",
            },
        ],
        [
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/charlie/d",
            },
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/d",
            },
        ],
    ],
}"#;

        // println!("{:#?}", got);
        assert_eq!(format!("{:#?}", got), want);
    }

    #[test]
    fn test_md5_hash_file_vec() {
        let path = Path::new("./test_fixtures");
        let max_depth = -1;
        let files = task::block_on(crawl_fs(path, max_depth, false)).unwrap();
        let opts = MD5HashFileOpts{
            max_depth: -1,
            follow_symlinks: false,
            read_buf_size: 256,
            sample_rate: 10,
            sample_threshold: 1024,
            batch_size: 2
        };
        let got = task::block_on(md5_hash_file_vec(files, opts, || {})).unwrap();
        let mut tuples: Vec<(md5::Digest, Vec<PathBuf>)> = got.inner.into_iter().collect();
        tuples.sort_by(|a, b| format!("{:?}", a.1[0]).cmp(&format!("{:?}", b.1[0])));
        let want = r#"[
    (
        b743cd81f58d58406a0874356eb3d03f,
        [
            PathBuf {
                inner: "./test_fixtures/alpha/a",
            },
        ],
    ),
    (
        ff227372abc3f8c57a807e5064d79b59,
        [
            PathBuf {
                inner: "./test_fixtures/alpha/b",
            },
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/charlie/b",
            },
        ],
    ),
    (
        9942b78186753bd8f9c0e8185df35cd4,
        [
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/charlie/c",
            },
        ],
    ),
    (
        5070a59f0f65446b614921f0655125cf,
        [
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/charlie/d",
            },
            PathBuf {
                inner: "./test_fixtures/alpha/bravo/d",
            },
        ],
    ),
]"#;
        // println!("{:#?}", tuples);
        assert_eq!(want, format!("{:#?}", tuples));
    }

    #[test]
    fn test_crawl_fs() {
        let path = Path::new("./test_fixtures");
        let max_depth = -1;
        let got = task::block_on(crawl_fs(path, max_depth, false)).unwrap();
        let want = r#"[
    PathBuf {
        inner: "./test_fixtures/alpha/a",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/b",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/c",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/d",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/b",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/d",
    },
]"#;
        assert_eq!(want, format!("{:#?}", got));
        // println!("{:#?}", got)
    }

    #[test]
    fn test_crawl_fs_symlinks() {
        let path = Path::new("./test_fixtures");
        let max_depth = -1;
        let got = task::block_on(crawl_fs(path, max_depth, true)).unwrap();
        let want = r#"[
    PathBuf {
        inner: "./test_fixtures/alpha/a",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/la",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/b",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/c",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/d",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/charlie/b",
    },
    PathBuf {
        inner: "./test_fixtures/alpha/bravo/d",
    },
]"#;
        assert_eq!(want, format!("{:#?}", got));
        // println!("{:#?}", got)
    }
}
