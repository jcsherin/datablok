## Parquet Nested Data

This experiment demonstrates how to serialize nested Rust data structures into a __Parquet__ file using the __Apache
Arrow__ memory format as an intermediate representation.

It demonstrates the Record Shredding API for storing nested objects like `Contact` in a columnar format like Parquet.

### How to Run

From the root of the monorepo, you can run this experiment using its package name. The `RUST_LOG` variable is used to
control the log output.

```zsh
RUST_LOG=info cargo run -p parquet-nested-data
```

To check the code for formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh parquet-nested-data
```

### Expected Output

Running the experiment will create a `contacts.parquet` file in root directory of this repository. You can verify the
contents using this wonderful online parquet
viewer: [https://parquet-viewer.xiangpeng.systems/](https://parquet-viewer.xiangpeng.systems/)

```text
[timestamp INFO  parquet_nested_data] Created 5 contacts.
...
[timestamp INFO  parquet_nested_data] Created parquet here: contacts.parquet
[timestamp INFO  parquet_nested_data] Fin.
```

### DuckDB SQL

An example SQL query executed on `contacts.parquet` using __DuckDB__.

```text
// @formatter:off
D select name, unnest(phones, recursive := true) from read_parquet('contacts.parquet');
┌─────────┬──────────┬────────────┐
│  name   │  number  │ phone_type │
│ varchar │ varchar  │  varchar   │
├─────────┼──────────┼────────────┤
│ Alice   │ 555-1234 │ Home       │
│ Alice   │ 555-5678 │ Work       │
│ Diana   │ 555-9999 │ Work       │
│ NULL    │ NULL     │ Home       │
└─────────┴──────────┴────────────┘
// @formatter:on
```

_(Note: The query and output above were generated with the following DuckDB version)_

```text
D
.
version
DuckDB v1.3.0-dev3259 e0e4cc2cc8
clang-17.0.0
```
