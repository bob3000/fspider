mod lib;
use async_std::task;
use structopt::StructOpt;
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};
use console::style;
use exitfailure::ExitFailure;

#[derive(StructOpt, Debug)]
struct Cli {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "d", long = "max-depth", default_value = "-1")]
    max_depth: i16,
    #[structopt(short = "l", long = "follow-links")]
    follow_symlinks: bool,
    #[structopt(short = "s", long = "sort-order", default_value = "size")]
    sort_order: lib::SortOrder,
}

fn main() -> Result<(), ExitFailure> {
    let args = Cli::from_args();
    let opts = lib::MD5HashFileOpts{
        max_depth: args.max_depth,
        follow_symlinks: args.follow_symlinks,
        sort_order: args.sort_order,
        read_buf_size: 512,
        sample_rate: 10,
        sample_threshold: 1024 * 1024,
        batch_size: 128,
    };

    eprintln!("{}", style("Reading file tree ...").green());
    let progress_crawling = ProgressBar::new_spinner();
    let files_to_hash = task::block_on(
        lib::crawl_fs(&args.path, opts.max_depth, opts.follow_symlinks, &mut move || {progress_crawling.inc(1)})).unwrap();
    let num_files = files_to_hash.len();

    eprintln!("{}", style("Generating check sums ...").green());
    let progress_hashing = ProgressBar::new(num_files as u64);
    progress_hashing.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-"));
    let (hf, errors) = task::block_on( lib::md5_hash_file_vec(files_to_hash, opts, move || {progress_hashing.inc(1)})).unwrap();
    let results: lib::DupVec = hf.duplicates().sort(args.sort_order);

    if !errors.is_empty() {
        eprintln!("{}", style("Encountered errors ...").red());
        for err in errors.iter() {
            println!("{}", err.to_string());
        }
        println!()
    }

    if !results.inner().is_empty() {
        eprintln!("{}", style("Found duplicates sums ...").green());
        for res in results.into_inner().iter() {
            for duplicate in res.iter() {
                println!("{}", duplicate.to_str().unwrap());
            }
            println!();
        }
    }
    Ok(())
}
