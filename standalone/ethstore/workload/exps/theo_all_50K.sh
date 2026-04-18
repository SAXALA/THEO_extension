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
# - block window 20500000-20550000
# - cache sizes 32 MiB / 512 MiB

DB_TYPE="all"
ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR="/mnt/gen3/ethstore-ssd-backup/index/accountHash_key_pebble"
BACKEND_CANDIDATES=(ethstore)
TRACE_FILE_CANDIDATES=(cache nocache_snap nocache)
CACHE_SIZE_CANDIDATES=(16)
CACHE_COUNT_CANDIDATES=(0)
COMMIT_BLOCK_INTERVAL_CANDIDATES=(1)
REPLAY_CGROUP_CASE_CANDIDATES=(false)
CHUNK_FILE_SIZE_BYTES_CANDIDATES=(16384 8192)
BLOCK_RANGE_CANDIDATES=("20500000:20550000")
