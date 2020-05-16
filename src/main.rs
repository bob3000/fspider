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

    let opts = lib::MD5HashFileOpts{ max_depth: -1, read_buf_size: 512, sample_rate: 10, sample_threshold: 1024, };
    let num_files = task::block_on(lib::count_files(&args.path, opts.max_depth)).unwrap();

    let progress = ProgressBar::new(num_files);
    let hf = task::block_on( lib::md5_hash_files(&args.path, opts, move || {progress.inc(1)})).unwrap();
    let results: lib::DupVec = hf.duplicates().sort(lib::SortOrder::Size);

    for res in results.into_inner().iter() {
        for duplicate in res.iter() {
            println!("{}", duplicate.path().to_str().unwrap());
        }
        println!();
    }
}
