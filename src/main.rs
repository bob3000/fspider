mod lib;
use async_std::task;
use structopt::StructOpt;
use std::path::PathBuf;
use indicatif::{ProgressBar, ProgressStyle};
use console::Term;
use exitfailure::ExitFailure;

#[derive(StructOpt, Debug)]
struct Cli {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(short = "d", long = "max-depth", default_value = "-1")]
    max_depth: i16,
    #[structopt(short = "s", long = "follow-links")]
    follow_symlinks: bool,
}

fn main() -> Result<(), ExitFailure> {
    let args = Cli::from_args();
    let opts = lib::MD5HashFileOpts{
        max_depth: args.max_depth,
        follow_symlinks: args.follow_symlinks,
        read_buf_size: 512,
        sample_rate: 10,
        sample_threshold: 1024 * 1024,
        batch_size: 128,
    };

    let term_err = Term::stderr();
    let term_out = Term::stdout();

    term_err.write_line("Reading file tree ...")?;
    let progress_crawling = ProgressBar::new_spinner();
    let files_to_hash = task::block_on(
        lib::crawl_fs(&args.path, opts.max_depth, opts.follow_symlinks, &mut move || {progress_crawling.inc(1)})).unwrap();
    let num_files = files_to_hash.len();

    term_err.write_line("Generating check sums ...")?;
    let progress_hashing = ProgressBar::new(num_files as u64);
    progress_hashing.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-"));
    let (hf, errors) = task::block_on( lib::md5_hash_file_vec(files_to_hash, opts, move || {progress_hashing.inc(1)})).unwrap();
    let results: lib::DupVec = hf.duplicates().sort(lib::SortOrder::Size);

    if !errors.is_empty() {
        term_err.write_line("Encountered errors ...")?;
        for err in errors.iter() {
            term_err.write_line(&format!("{}\n", err.to_string()))?;
        }
        term_out.write_line("")?;
    }

    term_err.write_line("Found duplicates ...")?;
    for res in results.into_inner().iter() {
        for duplicate in res.iter() {
            term_out.write_line(&format!("{}", duplicate.to_str().unwrap()))?;
        }
        term_out.write_line("")?;
    }
    Ok(())
}
