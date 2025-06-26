## Hello, Tantivy!

This experiment demonstrates the fundamental workflow of the Tantivy search engine library. It covers creating an
in-memory index, defining a schema, adding several documents, and performing basic search queries.

The code is a direct adaptation of
the [basic_search.rs](https://github.com/quickwit-oss/tantivy/blob/main/examples/basic_search.rs) example from the
official
Tantivy repository.

### How to Run

From the root of the monorepo, you can run this experiment using its package name. The `RUST_LOG` variable is used to
control the log output.

```zsh
RUST_LOG=info cargo run -p hello-tantivy
```

To check the code for formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh hello-tantivy
```

### Expected Output

When you run the code, you will see output confirming the major steps of the process.

_(Note: Log timestamps, temporary index path, hashed filenames will differ on your machine. The verbose internal logs
from the tantivy library have been omitted below for clarity.)_

```text
[<tz> INFO  hello_tantivy] Index created at: /path/to/tmp/.tmpXXXXXX
[<tz> INFO  tantivy::indexer::segment_updater] save metas
[<tz> INFO  tantivy::indexer::index_writer] Preparing commit
[<tz> INFO  tantivy::indexer::index_writer] Prepared commit 6
[<tz> INFO  tantivy::indexer::prepared_commit] committing 6
[<tz> INFO  tantivy::indexer::segment_updater] save metas
[<tz> INFO  tantivy::indexer::segment_updater] Running garbage collection
[<tz> INFO  tantivy::directory::managed_directory] Garbage collect
[<tz> INFO  hello_tantivy] Commit: 3 documents written to index
...
[<tz> INFO  hello_tantivy] Found file: "meta.json"
...
[<tz> INFO  hello_tantivy] score: 0.8781843, doc_address: {"title":["The Old Man and the Sea"]}
...
```
