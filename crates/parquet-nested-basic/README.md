## Parquet Nested Data

This experiment demonstrates how to serialize nested Rust data structures into a __Parquet__ file using the __Apache
Arrow__ memory format as an intermediate representation.

It demonstrates the Record Shredding API for storing nested objects like `Contact` in a columnar format like Parquet.

```rust
struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

enum PhoneType {
    Home,
    Work,
}
```

The structure of the generated `contacts.parquet` file can be inspected with this wonderful online
Parquet viewer: [https://parquet-viewer.xiangpeng.systems/](https://parquet-viewer.xiangpeng.systems/). The nested data
will be represented as follows:

![Screenshot showing the nested structure of contacts.parquet in an online viewer](./parquet_viewer.png)

Alternatively you can query the file directly using SQL and the `UNNEST` function to flatten the nested data structures
stored in the Parquet file.

```text
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
```

The previous query returns rows only if `phones` is present. To return all the contacts, we need to join each one with
the rows generated from its flattened `phones` list. This is fundamentally a `LATERAL` join.

```text
D SELECT
      t.name,
      p.phone.number,
      p.phone.phone_type
  FROM
      read_parquet('contacts.parquet') AS t
  LEFT JOIN
      UNNEST(t.phones) AS p(phone) ON true
  ORDER BY t.name;
┌─────────┬──────────┬────────────┐
│  name   │  number  │ phone_type │
│ varchar │ varchar  │  varchar   │
├─────────┼──────────┼────────────┤
│ Alice   │ 555-1234 │ Home       │
│ Alice   │ 555-5678 │ Work       │
│ Bob     │ NULL     │ NULL       │
│ Charlie │ NULL     │ NULL       │
│ Diana   │ 555-9999 │ Work       │
│ NULL    │ NULL     │ Home       │
└─────────┴──────────┴────────────┘
```

_(Note: The query and output above were generated with the following DuckDB version)_

```text
D
.
version
DuckDB v1.3.0-dev3259 e0e4cc2cc8
clang-17.0.0
```

### How to Run

From the root of the monorepo, you can run this experiment using its package name. The `RUST_LOG` variable is used to
control the log output.

```zsh
RUST_LOG=info cargo run -p parquet-nested-basic
```

To check the code for formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh parquet-nested-basic
```

### Expected Output

Running the experiment will create a `contacts.parquet` file in root directory of this repository.

```text
[timestamp INFO  parquet_nested_data] Created 5 contacts.
...
[timestamp INFO  parquet_nested_data] Created parquet here: contacts.parquet
[timestamp INFO  parquet_nested_data] Fin.
```
