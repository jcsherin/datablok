#!/bin/bash

# A unified script to parse a git log, identify the correct package for each commit,
# and run a full suite of performance benchmarks and profiling.

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
LOG_FILE="log.txt"
GOOD_CARGO_TOML="Cargo.toml.good-version"
OUTPUT_DIR="performance_results"
POSSIBLE_PACKAGES=("parquet-nested-parallel" "parquet-parallel-nested")
FLAMEGRAPH_DIR="${HOME}/FlameGraph"
PERF_RECORDS_DIR="${HOME}/perf-records"
PERF_STAT_EVENTS="cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses"

# --- Sanity Checks ---
if [ ! -f "$LOG_FILE" ]; then
    echo "ERROR: Input file '$LOG_FILE' not found." >&2
    exit 1
fi
if [ ! -f "$GOOD_CARGO_TOML" ]; then
    echo "ERROR: The template file '$GOOD_CARGO_TOML' is missing." >&2
    exit 1
fi

# --- Script Logic ---
echo "INFO: Beginning benchmark process. You may be prompted for your password for 'sudo'."

ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "INFO: Stashing changes. Will return to branch '$ORIGINAL_BRANCH' upon completion."

mkdir -p "$OUTPUT_DIR"
mkdir -p "$PERF_RECORDS_DIR"
OUTPUT_DIR=$(realpath "$OUTPUT_DIR")

git stash &> /dev/null

echo "INFO: Processing commits from '$LOG_FILE' in chronological order."

# MODIFICATION: Initialize a counter for sequential prefixes.
COUNTER=1

# MODIFICATION: Add 'tac' to reverse the log, processing oldest to newest.
grep "^commit " "$LOG_FILE" | awk '{print $2}' | tac | while IFS= read -r commit_hash; do
    if [ -z "$commit_hash" ]; then continue; fi # Skip any empty lines

    echo "========================================================================"
    echo "PROCESSING COMMIT: $commit_hash"
    echo "========================================================================"

    git checkout "$commit_hash" --force --quiet

    found_package=""
    for pkg in "${POSSIBLE_PACKAGES[@]}"; do
        if cargo check -p "$pkg" --quiet &> /dev/null; then
            echo "✅ Found valid package for this commit: '$pkg'"
            found_package="$pkg"
            break
        fi
    done

    if [ -z "$found_package" ]; then
        echo "⚠️ WARNING: Could not find a valid package for commit $commit_hash. Skipping." >&2
        continue
    fi

    package_name="$found_package"

    echo "🔧 Resetting Cargo.toml to known-good version..."
    cp "$GOOD_CARGO_TOML" Cargo.toml

    echo "🏗️ Building package '$package_name'..."
    cargo build --release -p "$package_name"
    BINARY_PATH="target/release/$package_name"

    # MODIFICATION: Create directory name with a sequential prefix for perfect sorting.
    COMMIT_DATE=$(git log -1 --format=%cs "$commit_hash")
    SHORT_HASH=$(git rev-parse --short=7 "$commit_hash")
    COMMIT_DIR_NAME=$(printf "%02d-%s-%s" "$COUNTER" "$COMMIT_DATE" "$SHORT_HASH")
    COMMIT_OUTPUT_DIR="$OUTPUT_DIR/$COMMIT_DIR_NAME"
    mkdir -p "$COMMIT_OUTPUT_DIR"

    echo "⏱️ Running hyperfine benchmark (will require sudo)..."
    sudo /home/jcsherin/.cargo/bin/hyperfine --warmup 1 --runs 10 \
      --prepare 'sync; echo 3 > /proc/sys/vm/drop_caches' \
      --export-json "$COMMIT_OUTPUT_DIR/hyperfine_results.json" \
      "$BINARY_PATH" &> /dev/null

    echo "📊 Running perf stat (will require sudo)..."
    sudo perf stat -e "$PERF_STAT_EVENTS" -o "$COMMIT_OUTPUT_DIR/perf_stat.txt" -- "$BINARY_PATH"

    echo "🔥 Generating flamegraph (will require sudo)..."
    PERF_DATA_FILE="$PERF_RECORDS_DIR/perf-$SHORT_HASH.data"
    FLAMEGRAPH_FILE="$COMMIT_OUTPUT_DIR/flamegraph-${COMMIT_DIR_NAME}.svg"
    sudo perf record -g -o "$PERF_DATA_FILE" -- "$BINARY_PATH"
    sudo perf script -i "$PERF_DATA_FILE" | \
      "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" | \
      /home/jcsherin/.cargo/bin/rustfilt | \
      "$FLAMEGRAPH_DIR/flamegraph.pl" > "$FLAMEGRAPH_FILE"

    sudo chown -R "$USER:$USER" "$COMMIT_OUTPUT_DIR" "$PERF_DATA_FILE"

    echo "✅ Successfully processed commit $commit_hash"
    echo "   Results saved in $COMMIT_OUTPUT_DIR"

    # MODIFICATION: Increment the counter.
    COUNTER=$((COUNTER + 1))

done

# --- Cleanup ---
echo "========================================================================"
echo "INFO: All commits processed. Cleaning up."
git checkout "$ORIGINAL_BRANCH" --force --quiet
git stash pop &> /dev/null || echo "INFO: No stash to pop."

echo "🎉 All done! Your performance data has been generated."
echo -e "\n👉 To copy the results to your local machine, use a command like this (replace with your user/server):\n"
echo "   rsync -avz --progress YOUR_USER@YOUR_SERVER:'$OUTPUT_DIR' ."
echo ""
