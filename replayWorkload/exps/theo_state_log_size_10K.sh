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
	exec "${script_dir}/../multiple_replay.sh" replay all all "$script_path"
fi

# Experiment 1:
# - theo only
# - state-store only (PrefixDB-handled data types)
# - cache trace only
# - block window 20500000-20510000
# - chunk sizes 32 KiB / 64 KiB

DB_TYPE="prefixdb"
THEO_PREFIXDB_PEBBLE_SOURCE_DIR="/mnt/gen3/theo-ssd-backup/index/accountHash_key_pebble"
BACKEND_CANDIDATES=(theo)
TRACE_FILE_CANDIDATES=(nocache_snap)
CACHE_SIZE_CANDIDATES=(16)
CACHE_COUNT_CANDIDATES=(0)
COMMIT_BLOCK_INTERVAL_CANDIDATES=(1)
REPLAY_CGROUP_CASE_CANDIDATES=(false)
CHUNK_FILE_SIZE_BYTES_CANDIDATES=(4096 8192 32768 65536)
BLOCK_RANGE_CANDIDATES=("20500000:20510000")
