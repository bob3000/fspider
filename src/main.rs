mod lib;
use async_std::task;
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
    let hf = task::block_on( lib::md5_hash_files(&args.path, move || {progress.inc(1)})).unwrap();
    let results: lib::DupVec = hf.duplicates().sort(lib::SortOrder::Size);

    for res in results.into_inner().iter() {
        for duplicate in res.iter() {
            println!("{}", duplicate.path().to_str().unwrap());
        }
        println!();
    }
}
