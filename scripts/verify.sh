#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

run_command() {
  printf "\n🚀 Executing: %s\n" "$*"
  "$@"
}


QUIET_FLAG="--quiet"
PACKAGE_NAME=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -v|--verbose)
      QUIET_FLAG=""
      shift
      ;;
    *)
      if [ -z "$PACKAGE_NAME" ]; then
        PACKAGE_NAME="$1"
      fi
      shift
      ;;
  esac
done


# Show usage if PACKAGE_NAME is not provided
if [ -z "$PACKAGE_NAME" ]; then
  printf "Usage: %s [--verbose] <package-name>\n" "$0" >&2
  printf "Example (Quiet): %s hello-datafusion\n" "$0" >&2
  printf "Example (Verbose): %s --verbose hello-datafusion\n" "$0" >&2
  exit 1
fi

run_command cargo fmt --check -p "$PACKAGE_NAME"
run_command cargo check $QUIET_FLAG -p "$PACKAGE_NAME"
run_command cargo clippy $QUIET_FLAG -p "$PACKAGE_NAME" -- -D warnings
run_command cargo test $QUIET_FLAG -p "$PACKAGE_NAME"
run_command cargo build $QUIET_FLAG -p "$PACKAGE_NAME"
run_command cargo run -p "$PACKAGE_NAME"

printf "\n🎉 All steps completed successfully for '%s'!\n" "$PACKAGE_NAME"