#!/bin/bash
# This script runs criterion benchmarks on specific commits to compare performance.
set -e

# Save the current branch name to return to it later
original_branch=$(git rev-parse --abbrev-ref HEAD)

# Commits to compare
baseline_commit="6140f49535411973e10d9e0de6f8af87b6af032e"
perf11_commit="da4a725c28b08d13f2f8ead7ef42e43ecedc7cf0"
perf12_commit="2f285c1bb475e7b1a734490a65ce0e10958b97d4"

# Package name is consistent across all commits being tested
pkg_name="parquet-nested-parallel"

# Clean old benchmark results to ensure a fresh baseline
echo "Cleaning old benchmark data from target/criterion..."
rm -rf target/criterion

# --- Run Baseline ---
echo "
====================================================================
Checking out baseline commit ($baseline_commit) and running benchmarks...
====================================================================
"
git checkout "$baseline_commit"
echo "--- Running datagen benchmark ---"
cargo bench -p "$pkg_name" --bench datagen
echo "--- Running pipeline benchmark ---"
cargo bench -p "$pkg_name" --bench pipeline
echo "Baseline run complete."

# --- Run Perf 11 vs Baseline ---
echo "
====================================================================
Checking out perf 11 commit ($perf11_commit) and comparing to baseline...
====================================================================
"
git checkout "$perf11_commit"
echo "--- Running datagen benchmark ---"
cargo bench -p "$pkg_name" --bench datagen
echo "--- Running pipeline benchmark ---"
cargo bench -p "$pkg_name" --bench pipeline
echo "Perf 11 comparison complete."

# --- Run Perf 12 vs Perf 11 ---
echo "
====================================================================
Checking out perf 12 commit ($perf12_commit) and comparing to perf 11...
====================================================================
"
git checkout "$perf12_commit"
echo "--- Running datagen benchmark ---"
cargo bench -p "$pkg_name" --bench datagen
echo "--- Running pipeline benchmark ---"
cargo bench -p "$pkg_name" --bench pipeline
echo "Perf 12 comparison complete."

# Return to the original branch
echo "
====================================================================
Returning to original branch: $original_branch
====================================================================
"
git checkout "$original_branch"

echo "Script finished."
