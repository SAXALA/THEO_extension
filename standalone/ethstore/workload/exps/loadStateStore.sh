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

loaded_root="${LOADED_ROOT:-/mnt/ssd2/loaded}"
state_store_sizes=(32768 65536)

state_dirname_for_size() {
	case "$1" in
		4096) echo "database_statedb4KB" ;;
		8192) echo "database_statedb8KB" ;;
		16384) echo "database_statedb16KB" ;;
		32768) echo "database_statedb32KB" ;;
		65536) echo "database_statedb64KB" ;;
		*)
			echo "Unsupported CHUNK_FILE_SIZE_BYTES: $1" >&2
			exit 1
			;;
	esac
}

for state_store_size in "${state_store_sizes[@]}"; do
	state_dirname=$(state_dirname_for_size "$state_store_size")
	state_dir="${loaded_root}/ethstore/${state_dirname}"
	backup_dir="${state_dir}_bak"

	echo "Load account data with CHUNK_FILE_SIZE_BYTES=${state_store_size}"
	CHUNK_FILE_SIZE_BYTES="$state_store_size" TOTAL_CACHE_SIZE_MIB=16 "$replay_script" load-account prefixdb

	sleep 10

	echo "Backup loaded account data to ${backup_dir}"
	cp -r "$state_dir" "$backup_dir"

	sleep 10

	echo "Load storage data with CHUNK_FILE_SIZE_BYTES=${state_store_size}"
	CHUNK_FILE_SIZE_BYTES="$state_store_size" TOTAL_CACHE_SIZE_MIB=16 PREFIXDB_ACCOUNT_STATE_DIR="$state_dir" "$replay_script" load-storage prefixdb
done

