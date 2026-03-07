#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

# Usage:
#   ./multiple_replay.sh [action] [backend|all] [trace-file|all]
# Examples:
#   ./multiple_replay.sh replay ethstore nocache_snap
#   ./multiple_replay.sh replay all all

ACTION="${1:-replay}"
BACKEND_SELECTOR="${2:-}"

# Fill these arrays with candidate values (MiB / count).
STORAGE_CACHE_SIZE_CANDIDATES=(12 48)
CACHE_COUNT_CANDIDATES=(32)
SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES=(4)
BACKEND_CANDIDATES=(ethstore pebble)
TRACE_FILE_CANDIDATES=(cache nocache_snap nocache)

TRACE_SELECTOR="${3:-all}"
DB_TYPE="${DB_TYPE:-all}"
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-0}"
CHUNK_FILE_SIZE="${CHUNK_FILE_SIZE:-16384}"

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
Usage: $0 [action] [backend|all] [trace-file|all]

action:     load | restore | replay | gc                 (default: replay)
backend:    ethstore | chainkv | pebble | all            (default: all when omitted)
trace-file: cache | nocache | nocache_snap | all         (default: all for replay when omitted)

Edit arrays in this file:
  STORAGE_CACHE_SIZE_CANDIDATES=(...)
  CACHE_COUNT_CANDIDATES=(...)
	SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES=(...)
	BACKEND_CANDIDATES=(...)
	TRACE_FILE_CANDIDATES=(...)

NODE_CACHE_SIZE is derived automatically as STORAGE_CACHE_SIZE/3.
CHAINKV_CACHE_MB and PEBBLE_CACHE_MB are derived automatically as STORAGE_CACHE_SIZE*4/3.
EOF
}

if [ "$ACTION" = "-h" ] || [ "$ACTION" = "--help" ]; then
	usage
	exit 0
fi

if [ "${#STORAGE_CACHE_SIZE_CANDIDATES[@]}" -eq 0 ]; then
	echo "STORAGE_CACHE_SIZE_CANDIDATES is empty"
	exit 1
fi
if [ "${#CACHE_COUNT_CANDIDATES[@]}" -eq 0 ]; then
	echo "CACHE_COUNT_CANDIDATES is empty"
	exit 1
fi

if [ "${#SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES[@]}" -eq 0 ]; then
	echo "SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES is empty"
	exit 1
fi

if [ "${#BACKEND_CANDIDATES[@]}" -eq 0 ]; then
	echo "BACKEND_CANDIDATES is empty"
	exit 1
fi

if [ "${#TRACE_FILE_CANDIDATES[@]}" -eq 0 ]; then
	echo "TRACE_FILE_CANDIDATES is empty"
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
	local storage_mib="$2"
	case "$backend" in
		chainkv)
			echo $((storage_mib * 4 / 3))
			;;
		pebble)
			echo $((storage_mib * 4 / 3))
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

mapfile -t SELECTED_BACKENDS < <(resolve_backends)
mapfile -t SELECTED_TRACES < <(resolve_traces)

run_idx=0
total_runs=0
for backend in "${SELECTED_BACKENDS[@]}"; do
	mapfile -t cache_count_candidates < <(resolve_cache_count_candidates "$backend")
	total_runs=$((total_runs + ${#STORAGE_CACHE_SIZE_CANDIDATES[@]} * ${#cache_count_candidates[@]} * ${#SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES[@]} * ${#SELECTED_TRACES[@]}))
done

for trace_file in "${SELECTED_TRACES[@]}"; do
	for backend in "${SELECTED_BACKENDS[@]}"; do
		mapfile -t CACHE_COUNT_VALUES < <(resolve_cache_count_candidates "$backend")
		for storage_mib in "${STORAGE_CACHE_SIZE_CANDIDATES[@]}"; do
			if ! [[ "$storage_mib" =~ ^[0-9]+$ ]] || [ "$storage_mib" -le 0 ]; then
				echo "Invalid STORAGE_CACHE_SIZE candidate: $storage_mib"
				exit 1
			fi

			# Keep node cache proportional to storage cache.
			node_mib=$((storage_mib / 3))
			if [ "$node_mib" -le 0 ]; then
				echo "Derived NODE_CACHE_SIZE is invalid for STORAGE_CACHE_SIZE=$storage_mib"
				exit 1
			fi

			backend_cache_mib="$(resolve_backend_cache_mib "$backend" "$storage_mib")"
			if ! [[ "$backend_cache_mib" =~ ^[0-9]+$ ]] || [ "$backend_cache_mib" -lt 0 ]; then
				echo "Invalid derived backend cache for $backend: $backend_cache_mib"
				exit 1
			fi

			for cache_count in "${CACHE_COUNT_VALUES[@]}"; do
				if ! [[ "$cache_count" =~ ^[0-9]+$ ]] || [ "$cache_count" -le 0 ]; then
					echo "Invalid CACHE_COUNT candidate: $cache_count"
					exit 1
				fi

				for segment_index_cache_size_mib in "${SEGMENT_INDEX_CACHE_SIZE_MIB_CANDIDATES[@]}"; do
					if ! [[ "$segment_index_cache_size_mib" =~ ^[0-9]+$ ]] || [ "$segment_index_cache_size_mib" -le 0 ]; then
						echo "Invalid segment index cache candidate: $segment_index_cache_size_mib"
						exit 1
					fi

					run_idx=$((run_idx + 1))
					echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file STORAGE_CACHE_SIZE=${storage_mib}MiB NODE_CACHE_SIZE=${node_mib}MiB SEGMENT_INDEX_CACHE_SIZE_MIB=${segment_index_cache_size_mib}MiB CACHE_COUNT=${cache_count} BACKEND_CACHE=${backend_cache_mib}MiB"

					STORAGE_CACHE_SIZE="$storage_mib" \
					NODE_CACHE_SIZE="$node_mib" \
					SEGMENT_INDEX_CACHE_SIZE_MIB="$segment_index_cache_size_mib" \
					CACHE_COUNT="$cache_count" \
					CHAINKV_CACHE_MB="$backend_cache_mib" \
					PEBBLE_CACHE_MB="$backend_cache_mib" \
					TRACE_FILE="$trace_file" \
					DB_TYPE="$DB_TYPE" \
					WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
					CHUNK_FILE_SIZE="$CHUNK_FILE_SIZE" \
					./replay.sh "$ACTION" "$backend" &
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

echo "All runs finished: $total_runs"
