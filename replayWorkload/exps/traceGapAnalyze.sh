#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload_dir=$(cd "${script_dir}/.." && pwd)

config_path="${CONFIG_PATH:-${workload_dir}/replay_config.json}"
trace_candidates=(cache nocache nocache_snap)

start_block_id="${START_BLOCK_ID:-0}"
end_block_id="${END_BLOCK_ID:-0}"
max_ops="${MAX_OPS:-0}"
gap_lru_cap="${GAP_LRU_CAP:-1000000}"
gap_pebble_root="${GAP_PEBBLE_DIR_ROOT:-/mnt/ssd2/analyze/gap_trace_index}"
clean_gap_dir="${CLEAN_GAP_DIR:-false}"

build_replay_binary() {
	cd "$workload_dir" || exit 1
	if ! go mod download; then
		echo "Dependency download failed" >&2
		exit 1
	fi
	mkdir -p ./bin
	if ! GOAMD64=v4 go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go; then
		echo "Failed to build replayWorkload" >&2
		exit 1
	fi
}

if [ ! -x "${workload_dir}/bin/replayWorkload" ] || [ "${REBUILD_REPLAY_BIN:-false}" = "true" ]; then
	build_replay_binary
fi

if [ ! -f "$config_path" ]; then
	echo "Config file not found: $config_path" >&2
	exit 1
fi

mkdir -p "$gap_pebble_root"

for trace in "${trace_candidates[@]}"; do
	gap_dir="${gap_pebble_root}/${trace}"
	if [ "$clean_gap_dir" = "true" ] && [ -d "$gap_dir" ]; then
		rm -rf "$gap_dir"
	fi
	mkdir -p "$gap_dir"

	echo "Analyze trace=${trace} start=${start_block_id} end=${end_block_id} max_ops=${max_ops} gap_lru_cap=${gap_lru_cap} gap_dir=${gap_dir}"
	"${workload_dir}/bin/replayWorkload" \
		-mode analyzeTrace \
		-config "$config_path" \
		-trace-file "$trace" \
		-start-block-id "$start_block_id" \
		-end-block-id "$end_block_id" \
		-max-ops "$max_ops" \
		-gap-lru-cap "$gap_lru_cap" \
		-gap-pebble-dir "$gap_dir"
done
