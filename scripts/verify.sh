#!/bin/sh

# Exit immediately if a command exits with a non-zero status.
set -e

run_command() {
  printf "\n🚀 Executing: %s\n" "$*"
  "$@"
}


QUIET_FLAG="--quiet"
DOODLE_NAME=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    -v|--verbose)
      QUIET_FLAG=""
      shift
      ;;
    *)
      if [ -z "$DOODLE_NAME" ]; then
        DOODLE_NAME="$1"
      fi
      shift
      ;;
  esac
done


# Show usage if DOODLE_NAME is not provided
if [ -z "$DOODLE_NAME" ]; then
  printf "Usage: %s [--verbose] <doodle-name>\n" "$0" >&2
  printf "Example (Quiet): %s hello-datafusion\n" "$0" >&2
  printf "Example (Verbose): %s --verbose hello-datafusion\n" "$0" >&2
  exit 1
fi

run_command cargo fmt --check -p "$DOODLE_NAME"
run_command cargo check $QUIET_FLAG -p "$DOODLE_NAME"
run_command cargo build $QUIET_FLAG -p "$DOODLE_NAME"
run_command cargo run -p "$DOODLE_NAME"

printf "\n🎉 All steps completed successfully for '%s'!\n" "$DOODLE_NAME"