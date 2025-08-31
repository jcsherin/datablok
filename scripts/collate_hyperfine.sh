#!/bin/bash

set -e

# Directory containing the reports
REPORT_DIR="crates/parquet-nested-parallel/report_linux_amd"
OUTPUT_FILE="${REPORT_DIR}/hyperfine_summary_collated.md"

# Write header
echo "| Version | Mean [s] | Min [s] | Max [s] |" > "${OUTPUT_FILE}"
echo "|:---|---:|---:|---:|" >> "${OUTPUT_FILE}"

# Find all summary files and process them
for summary_file in $(find "${REPORT_DIR}" -name "hyperfine_summary.md" | sort); do
  # Extract version from the path
  version=$(basename $(dirname "${summary_file}"))
  
  # Extract the data row (3rd line) and process it
  data_row=$(sed -n '3p' "${summary_file}")
  
  # Use awk to parse the markdown table row
  # and reformat it.
  echo "${data_row}" | awk -v ver="${version}" -F '|' '{ 
    # Trim whitespace from columns
    gsub(/^[ 	]+|[ 	]+$/, "", $3);
    gsub(/^[ 	]+|[ 	]+$/, "", $4);
    gsub(/^[ 	]+|[ 	]+$/, "", $5);
    
    # Print the new row
    printf "| %s | %s | %s | %s |\n", ver, $3, $4, $5
  }' >> "${OUTPUT_FILE}"
done

echo "Collated summary written to ${OUTPUT_FILE}"
