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

pub type HashFNameMap<T> = HashMap<T, Vec<String>>;

pub fn run_hash_files<T, U>(path: impl AsRef<Path>, hash_fn: impl FnMut(DirEntry) -> T)
where
    T: Future<Output = U>,
    U: Hash + Send + Sync + Debug,
{
    async fn hash_files<T, U>(path: impl AsRef<Path>, mut hash_fn: impl FnMut(DirEntry) -> T)
    where
        T: Future<Output = U>,
        U: Hash + Send + Sync + Debug,
    {
        fn inner_fn<'a, T, U>(
            path: &'a Path,
            hash_fn: &'a mut dyn FnMut(DirEntry) -> T,
        ) -> LocalBoxFuture<'a, ()>
        where
            T: Future<Output = U>,
            U: Hash + Send + Sync + Debug,
        {
            async move {
                let mut entries = fs::read_dir(path).await.unwrap();
                while let Some(path) = entries.next().await {
                    let entry = path.unwrap();
                    let path = entry.path();
                    if path.is_file().await {
                        let hash = hash_fn(entry).await;
                        println!("{:?}", hash);
                    } else {
                        inner_fn(&path, hash_fn).await
                    }
                }
            }
            .boxed_local()
        }
        inner_fn(path.as_ref(), &mut hash_fn).await
    }
    executor::block_on({ hash_files(path, hash_fn) });
}

mod test {
    use super::*;

    #[test]
    fn test_md5_file_map() {
        run_hash_files(Path::new("./target"), |entry| async move {
            let contents = fs::read(entry.path()).await;
            md5::compute(contents.unwrap())
        })
    }
}
