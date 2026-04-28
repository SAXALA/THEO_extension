#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

# Usage:
#   ./multiple_replay.sh [action] [backend|all] [trace-file|all] [config-script]
# Examples:
#   ./multiple_replay.sh replay ethstore nocache_snap
#   ./multiple_replay.sh recovery ethstore
#   ./multiple_replay.sh replay all all
#   TEST_RUN_ROUNDS=3 ./multiple_replay.sh replay ethstore nocache_snap
#   ./multiple_replay.sh load-account prefixdb all ./my_experiment.sh
#   PREFIXDB_ACCOUNT_STATE_DIR=/mnt/ssd2/loaded/ethstore/database_statedb8KB \
#     ./multiple_replay.sh load-storage prefixdb all ./my_experiment.sh

ACTION="${1:-replay}"
BACKEND_SELECTOR="${2:-}"

TRACE_SELECTOR="${3:-all}"
CONFIG_SCRIPT="${4:-}"
DB_TYPE="${DB_TYPE:-all}"
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-0}"
TEST_RUN_ROUNDS="${TEST_RUN_ROUNDS:-1}"

# Fill these arrays with candidate values (MiB / count).
CACHE_SIZE_CANDIDATES=(16)      # e.g. 64 256
CACHE_COUNT_CANDIDATES=(0)      # e.g. 64
COMMIT_BLOCK_INTERVAL_CANDIDATES=(1)
BACKEND_CANDIDATES=(chainkv)   # pebble ethstore chainkv
TRACE_FILE_CANDIDATES=(cache nocache_snap nocache)        # cache nocache_snap nocache
REPLAY_CGROUP_CASE_CANDIDATES=(false)

# Chunk file size candidates in bytes (used by ethstore/prefixdb).
CHUNK_FILE_SIZE_BYTES_CANDIDATES=(16384)

# Replay block windows in start:end form.
BLOCK_RANGE_CANDIDATES=("20500000:20550000")

if [ -n "$CONFIG_SCRIPT" ]; then
	if [ ! -f "$CONFIG_SCRIPT" ]; then
		echo "Config script not found: $CONFIG_SCRIPT" >&2
		exit 1
	fi
	# shellcheck disable=SC1090
	source "$CONFIG_SCRIPT"
fi

if [ -z "$BACKEND_SELECTOR" ]; then
	BACKEND_SELECTOR="all"
fi

if [ -z "$TRACE_SELECTOR" ]; then
	if [ "$ACTION" = "replay" ]; then
		TRACE_SELECTOR="all"
	else
		TRACE_SELECTOR="${TRACE_FILE_CANDIDATES[0]}"
	fi
fi

CURRENT_REPLAY_SH_PID=""

terminate_pid_tree() {
	local pid="$1"
	if [ -z "$pid" ]; then
		return 0
	fi
	if ! kill -0 "$pid" 2>/dev/null; then
		return 0
	fi

	local children
	children=$(pgrep -P "$pid" 2>/dev/null || true)
	if [ -n "$children" ]; then
		local child
		for child in $children; do
			terminate_pid_tree "$child"
		done
	fi

	kill -TERM "$pid" 2>/dev/null || true
	sleep 0.2
	if kill -0 "$pid" 2>/dev/null; then
		kill -KILL "$pid" 2>/dev/null || true
	fi
}

cleanup_running_processes() {
	terminate_pid_tree "$CURRENT_REPLAY_SH_PID"
	CURRENT_REPLAY_SH_PID=""
}

handle_interrupt() {
	echo
	echo "Interrupted, stopping running processes..."
	cleanup_running_processes
	exit 130
}

trap 'handle_interrupt' INT TERM
trap 'cleanup_running_processes' EXIT

usage() {
	cat <<EOF
Usage: $0 [action] [backend|all] [trace-file|all] [config-script]

action:       load | load-account | load-storage | restore | replay | recovery | gc  (default: replay)
backend:      ethstore | chainkv | pebble | all            (default: all when omitted)
trace-file:   cache | nocache | nocache_snap | all         (default: all for replay)
config-script path to a bash script that defines experiment arrays
		      (optional; when omitted, built-in defaults are used)

The config script must define these arrays:
  CACHE_SIZE_CANDIDATES=(...)          # values in MiB
  CACHE_COUNT_CANDIDATES=(...)
  COMMIT_BLOCK_INTERVAL_CANDIDATES=(...)
  BACKEND_CANDIDATES=(...)
  TRACE_FILE_CANDIDATES=(...)
  REPLAY_CGROUP_CASE_CANDIDATES=(...)
	CHUNK_FILE_SIZE_BYTES_CANDIDATES=(...)  # e.g. 4096 8192 16384
	BLOCK_RANGE_CANDIDATES=("start:end" ...)

Optional config script overrides:
	DB_TYPE=prefixdb
	WORKLOAD_MAX_OPS=0

Environment flags:
  TEST_RUN_ROUNDS=1    # each parameter combination runs this many rounds
EOF
}

if [ "$ACTION" = "-h" ] || [ "$ACTION" = "--help" ]; then
	usage
	exit 0
fi

# Validate sourced arrays exist and are non-empty.
for required_arr in CACHE_SIZE_CANDIDATES CACHE_COUNT_CANDIDATES \
		COMMIT_BLOCK_INTERVAL_CANDIDATES BACKEND_CANDIDATES \
		TRACE_FILE_CANDIDATES REPLAY_CGROUP_CASE_CANDIDATES \
		CHUNK_FILE_SIZE_BYTES_CANDIDATES BLOCK_RANGE_CANDIDATES; do
	eval "arr_len=\${#${required_arr}[@]}"
	if [ "$arr_len" -eq 0 ]; then
		echo "${required_arr} is empty" >&2
		exit 1
	fi
done

if ! [[ "$TEST_RUN_ROUNDS" =~ ^[0-9]+$ ]] || [ "$TEST_RUN_ROUNDS" -le 0 ]; then
	echo "Invalid TEST_RUN_ROUNDS: $TEST_RUN_ROUNDS"
	exit 1
fi

contains_value() {
	local needle="$1"
	shift
	local value
	for value in "$@"; do
		if [ "$value" = "$needle" ]; then
			return 0
		fi
	done
	return 1
}

resolve_backends() {
	if [ "$BACKEND_SELECTOR" = "all" ]; then
		printf '%s\n' "${BACKEND_CANDIDATES[@]}"
		return 0
	fi
	if ! contains_value "$BACKEND_SELECTOR" "${BACKEND_CANDIDATES[@]}"; then
		echo "Invalid backend selector: $BACKEND_SELECTOR" >&2
		exit 1
	fi
	printf '%s\n' "$BACKEND_SELECTOR"
}

resolve_traces() {
	if [ "$ACTION" != "replay" ]; then
		printf '%s\n' "${TRACE_FILE_CANDIDATES[0]}"
		return 0
	fi
	if [ "$TRACE_SELECTOR" = "all" ]; then
		printf '%s\n' "${TRACE_FILE_CANDIDATES[@]}"
		return 0
	fi
	if ! contains_value "$TRACE_SELECTOR" "${TRACE_FILE_CANDIDATES[@]}"; then
		echo "Invalid trace-file selector: $TRACE_SELECTOR" >&2
		exit 1
	fi
	printf '%s\n' "$TRACE_SELECTOR"
}

resolve_backend_cache_mib() {
	local backend="$1"
	local cache_mib="$2"
	case "$backend" in
		chainkv)
			echo "$cache_mib"
			;;
		pebble)
			echo "$cache_mib"
			;;
		*)
			echo "0"
			;;
	esac
}

resolve_cache_count_candidates() {
	local backend="$1"
	case "$backend" in
		ethstore)
			printf '%s\n' "${CACHE_COUNT_CANDIDATES[@]}"
			;;
		*)
			echo "${CACHE_COUNT_CANDIDATES[0]}"
			;;
	esac
}

resolve_replay_cgroup_cases() {
	if [ "$ACTION" = "replay" ] || [ "$ACTION" = "recovery" ]; then
		printf '%s\n' "${REPLAY_CGROUP_CASE_CANDIDATES[@]}"
		return 0
	fi
	printf '%s\n' "${REPLAY_CGROUP_IO_LIMIT_ENABLED:-true}"
}

resolve_chunk_file_size_candidates() {
	local backend="$1"
	case "$backend" in
		ethstore|prefixdb)
			printf '%s\n' "${CHUNK_FILE_SIZE_BYTES_CANDIDATES[@]}"
			;;
		*)
			echo "${CHUNK_FILE_SIZE_BYTES_CANDIDATES[0]}"
			;;
	esac
}

parse_block_range() {
	local range="$1"
	local start_block end_block

	IFS=':' read -r start_block end_block <<< "$range"
	if ! [[ "$start_block" =~ ^[0-9]+$ ]] || ! [[ "$end_block" =~ ^[0-9]+$ ]]; then
		echo "Invalid BLOCK_RANGE candidate: $range (expected start:end)" >&2
		exit 1
	fi
	if [ "$end_block" -ne 0 ] && [ "$start_block" -ne 0 ] && [ "$end_block" -lt "$start_block" ]; then
		echo "Invalid BLOCK_RANGE candidate: $range (end < start)" >&2
		exit 1
	fi
	printf '%s %s\n' "$start_block" "$end_block"
}

count_total_runs() {
	local total=0
	local backend trace_file cache_size_mib cache_count commit_block_interval replay_cgroup_enabled round_idx
	local chunk_file_size_bytes block_range start_block_id end_block_id
	local backend_cache_mib
	local -a cache_count_values chunk_file_size_values

	for ((round_idx = 1; round_idx <= TEST_RUN_ROUNDS; round_idx++)); do
		for trace_file in "${SELECTED_TRACES[@]}"; do
			for backend in "${SELECTED_BACKENDS[@]}"; do
				mapfile -t cache_count_values < <(resolve_cache_count_candidates "$backend")
				mapfile -t chunk_file_size_values < <(resolve_chunk_file_size_candidates "$backend")
				for chunk_file_size_bytes in "${chunk_file_size_values[@]}"; do
					if ! [[ "$chunk_file_size_bytes" =~ ^[0-9]+$ ]] || [ "$chunk_file_size_bytes" -le 0 ]; then
						echo "Invalid CHUNK_FILE_SIZE_BYTES candidate: $chunk_file_size_bytes" >&2
						exit 1
					fi
					for block_range in "${BLOCK_RANGE_CANDIDATES[@]}"; do
						read -r start_block_id end_block_id < <(parse_block_range "$block_range")
						for cache_size_mib in "${CACHE_SIZE_CANDIDATES[@]}"; do
							backend_cache_mib="$(resolve_backend_cache_mib "$backend" "$cache_size_mib")"
							if ! [[ "$backend_cache_mib" =~ ^[0-9]+$ ]]; then
								echo "Invalid derived backend cache for $backend: $backend_cache_mib" >&2
								exit 1
							fi
							for cache_count in "${cache_count_values[@]}"; do
								for commit_block_interval in "${COMMIT_BLOCK_INTERVAL_CANDIDATES[@]}"; do
									for replay_cgroup_enabled in "${REPLAY_CGROUP_CASES[@]}"; do
										total=$((total + 1))
									done
								done
							done
						done
					done
				done
			done
		done
	done

	printf '%s\n' "$total"
}

mapfile -t SELECTED_BACKENDS < <(resolve_backends)
mapfile -t SELECTED_TRACES < <(resolve_traces)
mapfile -t REPLAY_CGROUP_CASES < <(resolve_replay_cgroup_cases)

run_idx=0
total_runs="$(count_total_runs)"

for ((round_idx = 1; round_idx <= TEST_RUN_ROUNDS; round_idx++)); do
	for trace_file in "${SELECTED_TRACES[@]}"; do
		for backend in "${SELECTED_BACKENDS[@]}"; do
			mapfile -t CACHE_COUNT_VALUES < <(resolve_cache_count_candidates "$backend")
			mapfile -t CHUNK_FILE_SIZE_VALUES < <(resolve_chunk_file_size_candidates "$backend")
			for chunk_file_size_bytes in "${CHUNK_FILE_SIZE_VALUES[@]}"; do
				if ! [[ "$chunk_file_size_bytes" =~ ^[0-9]+$ ]] || [ "$chunk_file_size_bytes" -le 0 ]; then
					echo "Invalid CHUNK_FILE_SIZE_BYTES candidate: $chunk_file_size_bytes"
					exit 1
				fi

				for block_range in "${BLOCK_RANGE_CANDIDATES[@]}"; do
					read -r start_block_id end_block_id < <(parse_block_range "$block_range")
					for cache_size_mib in "${CACHE_SIZE_CANDIDATES[@]}"; do
						if ! [[ "$cache_size_mib" =~ ^[0-9]+$ ]] || [ "$cache_size_mib" -le 0 ]; then
							echo "Invalid CACHE_SIZE candidate: $cache_size_mib"
							exit 1
						fi

						backend_cache_mib="$(resolve_backend_cache_mib "$backend" "$cache_size_mib")"
						if ! [[ "$backend_cache_mib" =~ ^[0-9]+$ ]]; then
							echo "Invalid derived backend cache for $backend: $backend_cache_mib"
							exit 1
						fi

						for cache_count in "${CACHE_COUNT_VALUES[@]}"; do
							if ! [[ "$cache_count" =~ ^[0-9]+$ ]]; then
								echo "Invalid CACHE_COUNT candidate: $cache_count"
								exit 1
							fi

							for commit_block_interval in "${COMMIT_BLOCK_INTERVAL_CANDIDATES[@]}"; do
								if ! [[ "$commit_block_interval" =~ ^[0-9]+$ ]] || [ "$commit_block_interval" -le 0 ]; then
									echo "Invalid COMMIT_BLOCK_INTERVAL candidate: $commit_block_interval"
									exit 1
								fi

								for replay_cgroup_enabled in "${REPLAY_CGROUP_CASES[@]}"; do
									run_idx=$((run_idx + 1))
									if [ "$backend" = "ethstore" ]; then
										echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file BLOCK_RANGE=${start_block_id}-${end_block_id} CHUNK_FILE_SIZE_BYTES=${chunk_file_size_bytes} TOTAL_CACHE_SIZE=${cache_size_mib}MiB CACHE_COUNT=${cache_count} COMMIT_BLOCK_INTERVAL=${commit_block_interval} REPLAY_CGROUP_IO_LIMIT_ENABLED=${replay_cgroup_enabled} RUN_ROUND=${round_idx}/${TEST_RUN_ROUNDS}"
										TOTAL_CACHE_SIZE_MIB="$cache_size_mib" \
										CACHE_COUNT="$cache_count" \
										COMMIT_BLOCK_INTERVAL="$commit_block_interval" \
										TRACE_FILE="$trace_file" \
										DB_TYPE="$DB_TYPE" \
										ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR="$ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR" \
										WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
										CHUNK_FILE_SIZE_BYTES="$chunk_file_size_bytes" \
										START_BLOCK_ID="$start_block_id" \
										END_BLOCK_ID="$end_block_id" \
										REPLAY_CGROUP_IO_LIMIT_ENABLED="$replay_cgroup_enabled" \
										RUN_ROUND="$round_idx" \
										RUN_ROUNDS="$TEST_RUN_ROUNDS" \
										./replay.sh "$ACTION" "$backend" &
									else
										echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file BLOCK_RANGE=${start_block_id}-${end_block_id} CHUNK_FILE_SIZE_BYTES=${chunk_file_size_bytes} CACHE_SIZE=${cache_size_mib}MiB BACKEND_CACHE=${backend_cache_mib}MiB COMMIT_BLOCK_INTERVAL=${commit_block_interval} REPLAY_CGROUP_IO_LIMIT_ENABLED=${replay_cgroup_enabled} RUN_ROUND=${round_idx}/${TEST_RUN_ROUNDS}"
										if [ "$backend" = "chainkv" ]; then
											CHAINKV_CACHE_MB="$backend_cache_mib" \
											COMMIT_BLOCK_INTERVAL="$commit_block_interval" \
											TRACE_FILE="$trace_file" \
											DB_TYPE="$DB_TYPE" \
											WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
											CHUNK_FILE_SIZE_BYTES="$chunk_file_size_bytes" \
											START_BLOCK_ID="$start_block_id" \
											END_BLOCK_ID="$end_block_id" \
											REPLAY_CGROUP_IO_LIMIT_ENABLED="$replay_cgroup_enabled" \
											RUN_ROUND="$round_idx" \
											RUN_ROUNDS="$TEST_RUN_ROUNDS" \
											./replay.sh "$ACTION" "$backend" &
										else
											PEBBLE_CACHE_MB="$backend_cache_mib" \
											COMMIT_BLOCK_INTERVAL="$commit_block_interval" \
											TRACE_FILE="$trace_file" \
											DB_TYPE="$DB_TYPE" \
											WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
											CHUNK_FILE_SIZE_BYTES="$chunk_file_size_bytes" \
											START_BLOCK_ID="$start_block_id" \
											END_BLOCK_ID="$end_block_id" \
											REPLAY_CGROUP_IO_LIMIT_ENABLED="$replay_cgroup_enabled" \
											RUN_ROUND="$round_idx" \
											RUN_ROUNDS="$TEST_RUN_ROUNDS" \
											./replay.sh "$ACTION" "$backend" &
										fi
									fi
									CURRENT_REPLAY_SH_PID=$!
									wait "$CURRENT_REPLAY_SH_PID"
									CURRENT_REPLAY_SH_PID=""

									echo "[$run_idx/$total_runs] done"
									echo
								done
							done
						done
					done
				done
			done
		done
	done
done

echo "All runs finished: $total_runs"
