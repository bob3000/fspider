mod lib;
use async_std::task;
use structopt::StructOpt;
use std::path::PathBuf;
use indicatif::ProgressBar;

#[derive(StructOpt, Debug)]
struct Cli {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "d", long = "max-depth", default_value = "-1")]
    max_depth: i16,
    #[structopt(short = "s", long = "follow-links")]
    follow_symlinks: bool,
}


fn main() {
    let args = Cli::from_args();
    println!("{:?}", args);
    let opts = lib::MD5HashFileOpts{
        max_depth: args.max_depth,
        follow_symlinks: args.follow_symlinks,
        read_buf_size: 512,
        sample_rate: 10,
        sample_threshold: 1024 * 1024,
        batch_size: 128,
    };
    let files_to_hash = task::block_on(lib::crawl_fs(&args.path, opts.max_depth, opts.follow_symlinks)).unwrap();
    let num_files = files_to_hash.len();

    let progress_hashing = ProgressBar::new(num_files as u64);
    let hf = task::block_on( lib::md5_hash_file_vec(files_to_hash, opts, move || {progress_hashing.inc(1)})).unwrap();
    let results: lib::DupVec = hf.duplicates().sort(lib::SortOrder::Size);

    for res in results.into_inner().iter() {
        for duplicate in res.iter() {
            println!("{}", duplicate.to_str().unwrap());
        }
        println!();
    }
}
