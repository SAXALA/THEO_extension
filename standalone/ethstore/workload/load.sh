#!/usr/bin/env bash

set -euo pipefail

loaded_root="${LOADED_ROOT:-/mnt/ssd2/loaded}"
state_store_sizes=(4096 16384)

state_dirname_for_size() {
	case "$1" in
		4096) echo "database_statedb4KB" ;;
		16384) echo "database_statedb16KB" ;;
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
	CHUNK_FILE_SIZE_BYTES="$state_store_size" TOTAL_CACHE_SIZE_MIB=16 ./replay.sh load-account prefixdb

	sleep 10

	echo "Backup loaded account data to ${backup_dir}"
	cp -r "$state_dir" "$backup_dir"

	sleep 10

	echo "Load storage data with CHUNK_FILE_SIZE_BYTES=${state_store_size}"
	CHUNK_FILE_SIZE_BYTES="$state_store_size" TOTAL_CACHE_SIZE_MIB=16 PREFIXDB_ACCOUNT_STATE_DIR="$state_dir" ./replay.sh load-storage prefixdb
done

