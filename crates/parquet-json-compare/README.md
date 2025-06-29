A direct comparison between JSON and Parquet file sizes for storing nested data structures.

### How to Run

From the root of the monorepo, you can run this experiment using the package name:

```zsh
RUST_LOG=info cargo run -p parquet-json-compare
```

To check the code formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh parquet-json-compare
```

### Expected Output

_TODO_

### Schema

```rust
#[derive(Debug, Clone, PartialEq)]
enum PhoneType {
    Mobile,
    Home,
    Work,
}

struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}
```

### Data Distribution

The data skew of `phone_type` and `phone_number` fields are [Zipfian-like](https://en.wikipedia.org/wiki/Zipf%27s_law).

The value distribution in real-world datasets are not uniform. Rather it has a long-tail and few items account for
most of the occurrences.

| Field                 | Distribution                                                           | Description                                               | 
|-----------------------|------------------------------------------------------------------------|-----------------------------------------------------------|
| `name`                | 20% `NULL` <br/> 80% unique names                                      | Significant number of contacts are missing a name.        |
| `phones.phone_type`   | 55% Mobile <br/> 35% Work <br/> 10% Home                               | Skewed distribution is more realistic than a uniform one. |
| `phones.phone_number` | 40% zero phones <br/> 45% 1 phone <br/> 10% 2 phone <br/> 5% 3+ phones | Skewed towards most records having only 1 phone or none.  | 

| `phone_number` | `phone_type` | Probability |
|----------------|--------------|-------------|
| Some(_)        | Some(_)      | 90%         |
| Some(_)        | None         | 5%          |
| None           | Some(_)      | 4%          |
| None           | None         | 1%          |

### Cardinality

| Field                 | Cardinality | Description                                                     |
|-----------------------|-------------|-----------------------------------------------------------------|
| `name`                | High        | Names are mostly unique.                                        | 
| `phones.phone_type`   | Low         | It has only three possible values: `["Home", "Work", "Mobile"]` | 
| `phones.phone_number` | High        | Phone numbers are unique.                                       |

### Nullability

| Field                 | Nullablle |
|-----------------------|-----------|
| `name`                | true      |
| `phones.phone_type`   | true      |
| `phones.phone_number` | true      |

### Representing Optional Fields in JSON

In Rust, the absence of a value is represented by `Option::None`. When serialized to JSON, this maps to an explicit
`null` value, ensuring the key is always present.

_Q. Why not omit the `null` in the JSON representation?_

The reason to omit `null` value keys is that it will reduce the storage size. But we will follow existing practice
of being explicit. It removes ambiguity regarding the which keys are present in a structure and the semantics of the
value. For readers can identify a valid property has not value at present. For writers can identify if the key is to
be zeroed out if it has an existing value. But if it were absent then the writer has to guess the intent.

__Rust Struct__:

```rust
Contact { name: None, phones: None, }
```

__Resulting JSON__:

```json
{
  "name": null,
  "phones": null
}
```
