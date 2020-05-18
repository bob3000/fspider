# FSpider

Crawls the file system for duplicate files

## Install

```sh
git clone https://github.com/bob3000/fspider.git
cd fspider
cargo build --release
```

## Usage

```sh
$ target/release/fspider --help
fspider 0.1.0

USAGE:
    fspider [FLAGS] [OPTIONS] <path>

FLAGS:
    -l, --follow-links
    -h, --help            Prints help information
    -V, --version         Prints version information

OPTIONS:
    -d, --max-depth <max-depth>       [default: -1]
    -s, --sort-order <sort-order>     [default: size]

ARGS:
    <path>



$ target/release/fspider test_fixtures
Reading file tree ...
Generating check sums ...
Found duplicates sums ...
test_fixtures/alpha/b
test_fixtures/alpha/bravo/charlie/b

test_fixtures/alpha/bravo/charlie/d
test_fixtures/alpha/bravo/d
```
