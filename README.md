[![Rust CI](https://github.com/jcsherin/datablok/actions/workflows/ci.yml/badge.svg)](https://github.com/jcsherin/datablok/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Datablok** is a collection of experiments in novel and high-performance
applications of the Rust database building blocks (Apache DataFusion, Arrow
& Parquet).

## Highlights

* [parquet-parallel-nested](./crates/parquet-parallel-nested/README.md) -
  Writing 1 billion nested records to Parquet with a per-core throughput of ~1.3
  million records per second, using a multi-stage parallel pipeline.
* [tantivy-byte-array-index](./crates/tantivy-byte-array-index/README.md) -
  Embedding arbitrary data in Parquet and exploiting it to improve
  DataFusion query performance. In this instance we embed a Tantivy
  full-text index to accelerate `LIKE` queries.

## Project Goals

* Test the performance limits of single-node data processing.
* Explore novel ways of composing database building blocks.
* Find and contribute improvements to the underlying libraries.

## Usage

To run a specific experiment, use the `-p` or `--package` flag for cargo from
the root of the repository.

For example, to run the `hello-datafusion` doodle:

```sh
cargo run -p hello-datafusion
```

## Local Development

The `verify.sh` script mirrors the CI pipeline. Running this script is a good
practice before pushing code changes to
prevent failures in CI. Catching errors locally is much faster than waiting for
the CI pipeline to discover them.

```zsh
# Run all checks on the 'hello-datafusion' package
./scripts/verify.sh hello-datafusion

# For a more detailed output, use the --verbose flag
./scripts/verify.sh --verbose hello-datafusion
```
