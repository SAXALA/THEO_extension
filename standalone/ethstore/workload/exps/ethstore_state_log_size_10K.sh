#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
script_path="${script_dir}/$(basename "${BASH_SOURCE[0]}")"
TEST_RUN_ROUNDS="${TEST_RUN_ROUNDS:-3}"
export TEST_RUN_ROUNDS

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	set -euo pipefail
	exec "${script_dir}/../multiple_replay.sh" replay ethstore cache "$script_path"
fi

# Experiment 1:
# - ethstore only
# - state-store only (PrefixDB-handled data types)
# - cache trace only
# - block window 20500000-20510000
# - chunk sizes 4 KiB / 8 KiB / 16 KiB

DB_TYPE="prefixdb"
BACKEND_CANDIDATES=(ethstore)
TRACE_FILE_CANDIDATES=(cache)
CACHE_SIZE_CANDIDATES=(16)
CACHE_COUNT_CANDIDATES=(0)
COMMIT_BLOCK_INTERVAL_CANDIDATES=(1)
REPLAY_CGROUP_CASE_CANDIDATES=(false)
CHUNK_FILE_SIZE_BYTES_CANDIDATES=(4096 8192 16384)
BLOCK_RANGE_CANDIDATES=("20500000:20510000")
