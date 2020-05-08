mod lib;
use async_std::task;
use async_std::fs::DirEntry;
use structopt::StructOpt;
use std::path::PathBuf;
use indicatif::ProgressBar;

#[derive(StructOpt, Debug)]
struct Cli {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}


fn main() {
    let args = Cli::from_args();
    println!("{:?}", args);

    let num_files = task::block_on(lib::count_files(&args.path)).unwrap();

    let progress = ProgressBar::new(num_files);
    task::block_on(
        lib::md5_hash_files(&args.path, |hf| {
            let results: Vec<Vec<DirEntry>> = hf.unwrap().into_iter().filter(|(_, v)| {
                v.len() > 1
            })
            .map(|(_, v)| v)
            .collect();

            for res in results.iter() {
                for duplicate in res.iter() {
                    println!("{}", duplicate.path().to_str().unwrap());
                }
                println!();
            }
        }, move || {progress.inc(1)})
    );
}
