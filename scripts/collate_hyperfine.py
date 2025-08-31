#!/usr/bin/env python3

import os
import glob
import re

def parse_hyperfine_value(value_str, unit):
    """
    Parses a value string from hyperfine, which can be a single number
    or a mean ± stddev.
    Returns a tuple (mean, std_dev). std_dev will be 0.0 if not present.
    Converts units from ms to s.
    """
    parts = [p.strip() for p in value_str.split("±")]
    main_value_str = parts[0]

    if not main_value_str:
        return 0.0, 0.0

    mean = float(main_value_str)
    std_dev = 0.0

    if len(parts) > 1:
        std_dev_str = parts[1]
        if std_dev_str:
            std_dev = float(std_dev_str)

    if unit == "ms":
        mean /= 1000.0
        std_dev /= 1000.0

    return mean, std_dev

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    
    report_dir = os.path.join(project_root, "crates/parquet-nested-parallel/report_linux_amd")
    search_path = os.path.join(report_dir, "*/hyperfine_summary.md")
    output_file = os.path.join(report_dir, "hyperfine_summary_collated.md")

    files = sorted(glob.glob(search_path))

    with open(output_file, 'w') as out_f:
        out_f.write("| Version | Mean ± StdDev [s] | Min [s] | Max [s] |\n")
        out_f.write("|:---|:---:|---:|---:|\n")

        for f_path in files:
            with open(f_path, 'r') as in_f:
                lines = in_f.readlines()
                if len(lines) < 3:
                    continue

                header = lines[0]
                data_row = lines[2]

                unit_match = re.search(r'\[(s|ms)\]', header)
                if not unit_match:
                    continue
                unit = unit_match.group(1)

                parts = [p.strip() for p in data_row.split('|')]
                if len(parts) < 7:
                    continue
                
                version = os.path.basename(os.path.dirname(f_path))
                mean_str = parts[2]
                min_str = parts[3]
                max_str = parts[4]

                mean_s, std_dev_s = parse_hyperfine_value(mean_str, unit)
                min_s, _ = parse_hyperfine_value(min_str, unit)
                max_s, _ = parse_hyperfine_value(max_str, unit)
                
                if std_dev_s > 0:
                    mean_cell = f"{mean_s:.3f} ± {std_dev_s:.3f}"
                else:
                    mean_cell = f"{mean_s:.3f}"

                out_f.write(f"| {version} | {mean_cell} | {min_s:.3f} | {max_s:.3f} |\n")

    print(f"Collated summary written to {output_file}")

if __name__ == "__main__":
    main()
