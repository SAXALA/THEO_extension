#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload_dir=$(cd "${script_dir}/.." && pwd)
replay_script="${workload_dir}/replay.sh"

if [ ! -x "$replay_script" ]; then
	echo "replay.sh not found or not executable: ${replay_script}" >&2
	exit 1
fi

set -euo pipefail

# Optional overrides:
#   TRACE_FILE=cache|nocache|nocache_snap
#   DB_TYPE=all|aol|prefixdb|pebble
#   START_BLOCK_ID=<block>
#   END_BLOCK_ID=<block>
#   COMMIT_BLOCK_INTERVAL=<n>

echo "Run crash recovery for THEO"
"$replay_script" recovery theo