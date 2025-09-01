#!/usr/bin/env python3
import subprocess
import json
import itertools
import re
import shlex
import os
from pathlib import Path

# --- Configuration ---
BENCH_BIN = "./target/release/parquet-nested-parallel"
HYPERFINE_BIN = f"{Path.home()}/.cargo/bin/hyperfine"
REPORTS_DIR = Path("reports")

# Parameters to scan
WRITER_LEVELS = [1, 2, 4, 6, 8]
RECORD_LEVELS = [100000, 1000000, 10000000]
BATCH_SIZE_LEVELS = [1024, 5120, 10240, 20480]


def parse_custom_metrics(output_text: str) -> dict:
    """Parses text to find custom metrics."""
    metrics = {}
    patterns = {
        "total_time_ms": r"Total generation and write time: ([\d.]+)ms",
        "record_throughput_m_sec": r"Record Throughput: ([\d.]+)M records/sec",
        "mem_throughput_gb_sec": r"In-Memory Throughput: ([\d.]+) GB/s",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, output_text)
        if match:
            try:
                metrics[key] = float(match.group(1))
            except (ValueError, IndexError):
                print(f"Warning: Could not parse value for metric '{key}'")
    return metrics


def inject_metrics_into_json(json_file: Path, metrics: dict):
    """Adds the custom metrics to the hyperfine JSON report."""
    if not metrics:
        print("    -> No custom metrics found to inject.")
        return
    try:
        with open(json_file, "r+") as f:
            data = json.load(f)
            if data.get("results"):
                data["results"][0]["custom_metrics"] = metrics
                f.seek(0)
                json.dump(data, f, indent=2)
                f.truncate()
                print(f"    -> Injected custom metrics into {json_file.name}")
    except (FileNotFoundError, json.JSONDecodeError, PermissionError) as e:
        print(f"ERROR: Could not process JSON file {json_file}: {e}")


def run_batch_scan():
    """Runs a batch of benchmarks, saving an enriched report for each."""
    print("Building the latest version of the program in release mode...")
    subprocess.run(["cargo", "build", "--release", "-p", "parquet-nested-parallel"], check=True)

    REPORTS_DIR.mkdir(exist_ok=True)
    param_combinations = list(itertools.product(WRITER_LEVELS, RECORD_LEVELS, BATCH_SIZE_LEVELS))
    total_runs = len(param_combinations)

    print("=" * 60)
    print(f"Running batch benchmark scan ({total_runs} configurations)...")
    print("=" * 60)

    successful_runs = 0
    # Get the current user and group ID to own the final report file
    try:
        user_id = os.environ.get("SUDO_UID", str(os.getuid()))
        group_id = os.environ.get("SUDO_GID", str(os.getgid()))
    except (TypeError, ValueError):
        print("Warning: Could not determine original user ID. Files may remain owned by root.")
        user_id, group_id = None, None

    for i, (writers, records, batch_size) in enumerate(param_combinations):
        print(f"--- [{i+1}/{total_runs}] Benchmarking: {writers}w, {records}r, {batch_size}b ---")
        output_filename = REPORTS_DIR / f"run-W{writers}-R{records}-B{batch_size}.json"
        bench_command = f"{BENCH_BIN} --num-writers {writers} --target-records {records} --record-batch-size {batch_size}"

        full_command_str = (
            f"RUST_LOG=info {shlex.quote(HYPERFINE_BIN)} "
            f"--warmup 1 --runs 10 "
            f"--prepare 'sync; echo 3 > /proc/sys/vm/drop_caches' "
            f"--show-output --export-json {shlex.quote(str(output_filename))} "
            f"{shlex.quote(bench_command)}"
        )
        command_to_run = ["sudo", "/bin/sh", "-c", full_command_str]

        try:
            process = subprocess.Popen(command_to_run, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            captured_output = [line for line in process.stdout]

            return_code = process.wait()
            if return_code != 0:
                # Print the captured output before raising the error
                print("".join(captured_output))
                raise subprocess.CalledProcessError(return_code, command_to_run)

            # --- KEY CHANGE: Fix file ownership ---
            # The report file was created by root, so we change its ownership back to the user.
            if user_id and group_id:
                chown_command = ["sudo", "chown", f"{user_id}:{group_id}", str(output_filename)]
                subprocess.run(chown_command, check=True)

            full_output_str = "".join(captured_output)
            custom_metrics = parse_custom_metrics(full_output_str)
            inject_metrics_into_json(output_filename, custom_metrics)

            successful_runs += 1
            print(f"    -> OK. Report saved to {output_filename.name}")

        except subprocess.CalledProcessError:
            print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("!!! Benchmark command FAILED!")
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    print("=" * 60)
    print("Batch scan complete.")
    print(f"Successfully generated {successful_runs} of {total_runs} reports in '{REPORTS_DIR}'.")

if __name__ == "__main__":
    run_batch_scan()