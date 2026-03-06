#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

# Usage:
#   ./multiple_replay.sh [action] [backend]
# Example:
#   ./multiple_replay.sh replay ethstore

ACTION="${1:-replay}"
BACKEND="${2:-ethstore}"

# Fill these arrays with candidate values (MiB / count).
STORAGE_CACHE_SIZE_CANDIDATES=(24 48)
CACHE_COUNT_CANDIDATES=(32 64)

# Optional fixed env passthrough for replay.sh (customize if needed).
TRACE_FILE="${TRACE_FILE:-nocache_snap}"
DB_TYPE="${DB_TYPE:-all}"
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-0}"
CHUNK_FILE_SIZE="${CHUNK_FILE_SIZE:-16384}"

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
Usage: $0 [action] [backend]

action:  load | restore | replay | gc   (default: replay)
backend: ethstore | chainkv | pebble | all (default: ethstore)

Edit arrays in this file:
  STORAGE_CACHE_SIZE_CANDIDATES=(...)
  CACHE_COUNT_CANDIDATES=(...)

NODE_CACHE_SIZE is derived automatically as STORAGE_CACHE_SIZE/4.
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

run_idx=0
total_runs=$((${#STORAGE_CACHE_SIZE_CANDIDATES[@]} * ${#CACHE_COUNT_CANDIDATES[@]}))

for storage_mib in "${STORAGE_CACHE_SIZE_CANDIDATES[@]}"; do
	if ! [[ "$storage_mib" =~ ^[0-9]+$ ]] || [ "$storage_mib" -le 0 ]; then
		echo "Invalid STORAGE_CACHE_SIZE candidate: $storage_mib"
		exit 1
	fi

	# Enforce NODE_CACHE_SIZE relationship required by user.
	node_mib=$((storage_mib / 3))
	if [ "$node_mib" -le 0 ]; then
		echo "Derived NODE_CACHE_SIZE is invalid for STORAGE_CACHE_SIZE=$storage_mib"
		exit 1
	fi

	for cache_count in "${CACHE_COUNT_CANDIDATES[@]}"; do
		if ! [[ "$cache_count" =~ ^[0-9]+$ ]] || [ "$cache_count" -le 0 ]; then
			echo "Invalid CACHE_COUNT candidate: $cache_count"
			exit 1
		fi

		run_idx=$((run_idx + 1))
		echo "[$run_idx/$total_runs] ACTION=$ACTION BACKEND=$BACKEND STORAGE_CACHE_SIZE=${storage_mib}MiB NODE_CACHE_SIZE=${node_mib}MiB CACHE_COUNT=${cache_count}"

		STORAGE_CACHE_SIZE="$storage_mib" \
		NODE_CACHE_SIZE="$node_mib" \
		CACHE_COUNT="$cache_count" \
		TRACE_FILE="$TRACE_FILE" \
		DB_TYPE="$DB_TYPE" \
		WORKLOAD_MAX_OPS="$WORKLOAD_MAX_OPS" \
		CHUNK_FILE_SIZE="$CHUNK_FILE_SIZE" \
		./replay.sh "$ACTION" "$BACKEND" &
		CURRENT_REPLAY_SH_PID=$!
		wait "$CURRENT_REPLAY_SH_PID"
		CURRENT_REPLAY_SH_PID=""

		echo "[$run_idx/$total_runs] done"
		echo
	done
done

echo "All runs finished: $total_runs"
