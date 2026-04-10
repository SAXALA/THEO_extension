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
#   ./multiple_replay.sh replay all all
#   TEST_RUN_ROUNDS=3 ./multiple_replay.sh replay ethstore nocache_snap
#   ./multiple_replay.sh load-account prefixdb all ./my_experiment.sh
#   PREFIXDB_ACCOUNT_STATE_DIR=/mnt/ssd2/loaded/ethstore/database_statedb8KB \
#     ./multiple_replay.sh load-storage prefixdb all ./my_experiment.sh

ACTION="${1:-replay}"
BACKEND_SELECTOR="${2:-}"

TRACE_SELECTOR="${3:-all}"
DB_TYPE="${DB_TYPE:-all}"
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-0}"
TEST_RUN_ROUNDS="${TEST_RUN_ROUNDS:-2}"

# Optional 4th argument: path to a config script that defines the candidate arrays.
# Defaults to the bundled replay_experiment_config.sh.
CONFIG_SCRIPT="${4:-${script_dir}/replay_experiment_config.sh}"

if [ ! -f "$CONFIG_SCRIPT" ]; then
	echo "Config script not found: $CONFIG_SCRIPT" >&2
	exit 1
fi

# Source the config to get CACHE_SIZE_CANDIDATES, BACKEND_CANDIDATES, etc.
# shellcheck source=/dev/null
source "$CONFIG_SCRIPT"

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

action:       load | load-account | load-storage | restore | replay | gc  (default: replay)
backend:      ethstore | chainkv | pebble | all            (default: all when omitted)
trace-file:   cache | nocache | nocache_snap | all         (default: all for replay)
config-script path to a bash script that defines experiment arrays
              (default: ${script_dir}/replay_experiment_config.sh)

The config script must define these arrays:
  CACHE_SIZE_CANDIDATES=(...)          # values in MiB
  CACHE_COUNT_CANDIDATES=(...)
  COMMIT_BLOCK_INTERVAL_CANDIDATES=(...)
  BACKEND_CANDIDATES=(...)
  TRACE_FILE_CANDIDATES=(...)
  REPLAY_CGROUP_CASE_CANDIDATES=(...)
  CHUNK_FILE_SIZE_BYTES=<bytes>

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
		TRACE_FILE_CANDIDATES REPLAY_CGROUP_CASE_CANDIDATES; do
	eval "arr_len=\${#${required_arr}[@]}"
	if [ "$arr_len" -eq 0 ]; then
		echo "${required_arr} is empty (defined in ${CONFIG_SCRIPT})" >&2
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
	if [ "$ACTION" = "replay" ]; then
		printf '%s\n' "${REPLAY_CGROUP_CASE_CANDIDATES[@]}"
		return 0
	fi
	printf '%s\n' "${REPLAY_CGROUP_IO_LIMIT_ENABLED:-true}"
}

count_total_runs() {
	local total=0
	local backend trace_file cache_size_mib cache_count commit_block_interval replay_cgroup_enabled round_idx
	local backend_cache_mib
	local -a cache_count_values

	for ((round_idx = 1; round_idx <= TEST_RUN_ROUNDS; round_idx++)); do
		for trace_file in "${SELECTED_TRACES[@]}"; do
			for backend in "${SELECTED_BACKENDS[@]}"; do
				mapfile -t cache_count_values < <(resolve_cache_count_candidates "$backend")
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
								echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file TOTAL_CACHE_SIZE=${cache_size_mib}MiB CACHE_COUNT=${cache_count} COMMIT_BLOCK_INTERVAL=${commit_block_interval} REPLAY_CGROUP_IO_LIMIT_ENABLED=${replay_cgroup_enabled} RUN_ROUND=${round_idx}/${TEST_RUN_ROUNDS}"
								TOTAL_CACHE_SIZE_MIB="$cache_size_mib" \
								CACHE_COUNT="$cache_count" \
								COMMIT_BLOCK_INTERVAL="$commit_block_interval" \
								TRACE_FILE="$trace_file" \
								DB_TYPE="$DB_TYPE" \
								WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
								CHUNK_FILE_SIZE_BYTES="$CHUNK_FILE_SIZE_BYTES" \
								REPLAY_CGROUP_IO_LIMIT_ENABLED="$replay_cgroup_enabled" \
								RUN_ROUND="$round_idx" \
								RUN_ROUNDS="$TEST_RUN_ROUNDS" \
								./replay.sh "$ACTION" "$backend" &
							else
								echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file CACHE_SIZE=${cache_size_mib}MiB BACKEND_CACHE=${backend_cache_mib}MiB COMMIT_BLOCK_INTERVAL=${commit_block_interval} REPLAY_CGROUP_IO_LIMIT_ENABLED=${replay_cgroup_enabled} RUN_ROUND=${round_idx}/${TEST_RUN_ROUNDS}"
								if [ "$backend" = "chainkv" ]; then
									CHAINKV_CACHE_MB="$backend_cache_mib" \
									COMMIT_BLOCK_INTERVAL="$commit_block_interval" \
									TRACE_FILE="$trace_file" \
									DB_TYPE="$DB_TYPE" \
									WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
									CHUNK_FILE_SIZE_BYTES="$CHUNK_FILE_SIZE_BYTES" \
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
									CHUNK_FILE_SIZE_BYTES="$CHUNK_FILE_SIZE_BYTES" \
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

echo "All runs finished: $total_runs"
