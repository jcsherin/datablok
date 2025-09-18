[![Rust CI](https://github.com/jcsherin/datablok/actions/workflows/ci.yml/badge.svg)](https://github.com/jcsherin/datablok/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A collection of one-off projects built using database building blocks:
[DataFusion], [Arrow] and [Parquet].

[DataFusion]: https://datafusion.apache.org/

[Arrow]: https://github.com/apache/arrow-rs

[Parquet]: https://parquet.apache.org/

## Projects (latest first)

* Embed a full-text index within a Parquet file and use [Datafusion] to accelerate
  wildcard
  `LIKE` queries. See the [README for parquet-embed-tantivy] more details.

* A fast, parallel, [Zipfian-like] Parquet file generator for nested data
  structures. See the [README For parquet-nested-parallel] more details.

[README for parquet-nested-parallel]: /crates/parquet-nested-parallel/README.md

[README for parquet-embed-tantivy]: /crates/parquet-embed-tantivy/README.md

[Zipfian-like]: https://en.wikipedia.org/wiki/Zipf%27s_law
