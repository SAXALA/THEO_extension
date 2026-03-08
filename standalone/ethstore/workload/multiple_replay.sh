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
CACHE_SIZE_CANDIDATES=(16 64)
CACHE_COUNT_CANDIDATES=(32)
BACKEND_CANDIDATES=(ethstore pebble)
TRACE_FILE_CANDIDATES=(cache nocache_snap)

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
	CACHE_SIZE_CANDIDATES=(...)  # values must be multiples of 16
  CACHE_COUNT_CANDIDATES=(...)
	BACKEND_CANDIDATES=(...)
	TRACE_FILE_CANDIDATES=(...)

CHAINKV_CACHE_MB and PEBBLE_CACHE_MB are derived automatically as CACHE_SIZE.
For ethstore:
	STORAGE_CACHE_SIZE = CACHE_SIZE * 12 / 16
	NODE_CACHE_SIZE = CACHE_SIZE * 3 / 16
	SEGMENT_INDEX_CACHE_SIZE_MIB = CACHE_SIZE * 1 / 16
EOF
}

if [ "$ACTION" = "-h" ] || [ "$ACTION" = "--help" ]; then
	usage
	exit 0
fi

if [ "${#CACHE_SIZE_CANDIDATES[@]}" -eq 0 ]; then
	echo "CACHE_SIZE_CANDIDATES is empty"
	exit 1
fi
if [ "${#CACHE_COUNT_CANDIDATES[@]}" -eq 0 ]; then
	echo "CACHE_COUNT_CANDIDATES is empty"
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

mapfile -t SELECTED_BACKENDS < <(resolve_backends)
mapfile -t SELECTED_TRACES < <(resolve_traces)

run_idx=0
total_runs=0
for backend in "${SELECTED_BACKENDS[@]}"; do
	mapfile -t cache_count_candidates < <(resolve_cache_count_candidates "$backend")
	total_runs=$((total_runs + ${#CACHE_SIZE_CANDIDATES[@]} * ${#cache_count_candidates[@]} * ${#SELECTED_TRACES[@]}))
done

for trace_file in "${SELECTED_TRACES[@]}"; do
	for backend in "${SELECTED_BACKENDS[@]}"; do
		mapfile -t CACHE_COUNT_VALUES < <(resolve_cache_count_candidates "$backend")
		for cache_size_mib in "${CACHE_SIZE_CANDIDATES[@]}"; do
			if ! [[ "$cache_size_mib" =~ ^[0-9]+$ ]] || [ "$cache_size_mib" -le 0 ]; then
				echo "Invalid CACHE_SIZE candidate: $cache_size_mib"
				exit 1
			fi
			if (( cache_size_mib % 16 != 0 )); then
				echo "CACHE_SIZE candidate must be a multiple of 16, got: $cache_size_mib"
				exit 1
			fi

			# Derive ethstore cache split from total cache budget.
			storage_mib=$((cache_size_mib * 12 / 16))
			node_mib=$((cache_size_mib * 3 / 16))
			segment_index_cache_size_mib=$((cache_size_mib / 16))

			if [ "$storage_mib" -le 0 ]; then
				echo "Derived STORAGE_CACHE_SIZE is invalid for CACHE_SIZE=$cache_size_mib"
				exit 1
			fi
			if [ "$node_mib" -le 0 ]; then
				echo "Derived NODE_CACHE_SIZE is invalid for CACHE_SIZE=$cache_size_mib"
				exit 1
			fi
			if [ "$segment_index_cache_size_mib" -le 0 ]; then
				echo "Derived SEGMENT_INDEX_CACHE_SIZE_MIB is invalid for CACHE_SIZE=$cache_size_mib"
				exit 1
			fi

			backend_cache_mib="$(resolve_backend_cache_mib "$backend" "$cache_size_mib")"
			if ! [[ "$backend_cache_mib" =~ ^[0-9]+$ ]] || [ "$backend_cache_mib" -lt 0 ]; then
				echo "Invalid derived backend cache for $backend: $backend_cache_mib"
				exit 1
			fi

			for cache_count in "${CACHE_COUNT_VALUES[@]}"; do
				if ! [[ "$cache_count" =~ ^[0-9]+$ ]] || [ "$cache_count" -le 0 ]; then
					echo "Invalid CACHE_COUNT candidate: $cache_count"
					exit 1
				fi

				run_idx=$((run_idx + 1))
				echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$backend TRACE_FILE=$trace_file CACHE_SIZE=${cache_size_mib}MiB STORAGE_CACHE_SIZE=${storage_mib}MiB NODE_CACHE_SIZE=${node_mib}MiB SEGMENT_INDEX_CACHE_SIZE_MIB=${segment_index_cache_size_mib}MiB CACHE_COUNT=${cache_count} BACKEND_CACHE=${backend_cache_mib}MiB"

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

echo "All runs finished: $total_runs"
