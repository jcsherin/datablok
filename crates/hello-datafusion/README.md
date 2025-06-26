## Hello, DataFusion!

This experiment demonstrates the most basic setup of an Apache DataFusion SessionContext. It serves as the "hello,
world!" of this repository.

### How to Run

From the root of the monorepo, you can run this experiment using the package name:

```zsh
RUST_LOG=info cargo run -p hello-datafusion
```

To check the code formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh hello-datafusion
```

### Expected Output

When you run the code, you should see the following output, confirming that the setup is working:

```text
Hello, DataFusion!

DataFusion Session Configuration:
---------------------------------
Batch Size: 8192
Target Partitions: 8
Timezone: +00:00

Setup is working correctly!
```

_(Note: The value for Target Partitions may vary depending on the number of CPU cores on your machine.)_
