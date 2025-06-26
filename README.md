[![Rust CI](https://github.com/jcsherin/rusty-doodles/actions/workflows/ci.yml/badge.svg)](https://github.com/jcsherin/rusty-doodles/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A monorepo experimenting with the internals of data systems in Rust, focusing on Apache DataFusion, Arrow, and Parquet.

## Usage

To run a specific experiment, use the `-p` or `--package` flag for cargo from the root of the repository.

For example, to run the `hello-datafusion` doodle:

```sh
cargo run -p hello-datafusion
```

## Local Development

The `verify.sh` script mirrors the CI pipeline. Running this script is a good practice before pushing code changes to
prevent failures in CI. Catching errors locally is much faster than waiting for the CI pipeline to discover them.

```zsh
# Run all checks on the 'hello-datafusion' package
./scripts/verify.sh hello-datafusion

# For a more detailed output, use the --verbose flag
./scripts/verify.sh --verbose hello-datafusion
```
