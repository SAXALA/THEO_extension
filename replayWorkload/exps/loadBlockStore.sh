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

read -r -a chunk_file_sizes <<< "${AOL_CHUNK_FILE_SIZE_BYTES_CANDIDATES:-16384}"

for chunk_file_size in "${chunk_file_sizes[@]}"; do
	echo "Load AOL block data with CHUNK_FILE_SIZE_BYTES=${chunk_file_size}"
	CHUNK_FILE_SIZE_BYTES="$chunk_file_size" \
	TOTAL_CACHE_SIZE_MIB="${AOL_TOTAL_CACHE_SIZE_MIB:-${TOTAL_CACHE_SIZE_MIB:-16}}" \
	"$replay_script" load aol
done