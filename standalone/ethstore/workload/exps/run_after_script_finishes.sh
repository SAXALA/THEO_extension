#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

usage() {
	echo "Usage: $(basename "$0") <script-a> <script-b>" >&2
	echo "   or: $(basename "$0") <script-a> [script-a-args ...] -- <script-b>" >&2
	echo "Examples:" >&2
	echo "  $(basename "$0") ./script_a.sh ./script_b.sh" >&2
	echo "  $(basename "$0") ./loadPebbleWithSnapshot.sh chainkv -- ./script_b.sh" >&2
	echo "Environment: MONITOR_INTERVAL_MINUTES=<minutes> (default: 1)" >&2
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

monitor_args=()
run_arg=""
if [ "$#" -eq 2 ]; then
	monitor_args=("$1")
	run_arg="$2"
else
	separator_seen=false
	for arg in "$@"; do
		if [ "$separator_seen" = false ] && [ "$arg" = "--" ]; then
			separator_seen=true
			continue
		fi
		if [ "$separator_seen" = false ]; then
			monitor_args+=("$arg")
		elif [ -z "$run_arg" ]; then
			run_arg="$arg"
		else
			usage
			exit 1
		fi
	done
	if [ "${#monitor_args[@]}" -eq 0 ] || [ -z "$run_arg" ] || [ "$separator_seen" = false ]; then
		usage
		exit 1
	fi
fi

monitor_arg="${monitor_args[*]}"
interval_minutes="${MONITOR_INTERVAL_MINUTES:-1}"

if ! [[ "$interval_minutes" =~ ^[0-9]+$ ]] || [ "$interval_minutes" -lt 1 ]; then
	echo "MONITOR_INTERVAL_MINUTES must be an integer >= 1, got: $interval_minutes" >&2
	exit 1
fi

resolve_script_path() {
	local input="$1"
	if [[ "$input" == */* ]]; then
		if [ -f "$input" ]; then
			printf '%s/%s\n' "$(cd "$(dirname "$input")" && pwd)" "$(basename "$input")"
			return 0
		fi
	else
		if [ -f "$PWD/$input" ]; then
			echo "$PWD/$input"
			return 0
		fi
		if [ -f "$script_dir/$input" ]; then
			echo "$script_dir/$input"
			return 0
		fi
	fi
	return 1
}

monitor_path=""

monitor_script_token="$monitor_arg"
monitor_args_suffix=""
if [[ "$monitor_arg" == *" "* ]]; then
	monitor_script_token="${monitor_arg%% *}"
	monitor_args_suffix="${monitor_arg#* }"
fi

if resolved=$(resolve_script_path "$monitor_script_token"); then
	monitor_path="$resolved"
	monitor_pattern="$monitor_path"
else
	monitor_pattern="$monitor_script_token"
fi

monitor_basename="$(basename "$monitor_script_token")"

if ! run_path=$(resolve_script_path "$run_arg"); then
	echo "Script B not found: $run_arg" >&2
	exit 1
fi

find_matching_pids() {
	local pattern="$1"
	local resolved_path="$2"
	local basename_pattern="$3"
	local args_suffix="$4"
	ps -eo pid=,args= | while IFS= read -r line; do
		local pid cmd
		line="${line#${line%%[![:space:]]*}}"
		pid="${line%% *}"
		cmd="${line#* }"
		if [ -z "$pid" ] || [ "$pid" = "$$" ]; then
			continue
		fi
		if [[ "$cmd" != *"$pattern"* ]] && [[ -n "$resolved_path" && "$cmd" != *"$resolved_path"* ]] && [[ "$cmd" != *"$basename_pattern"* ]]; then
			continue
		fi
		if [ -n "$args_suffix" ] && [[ "$cmd" != *"$args_suffix"* ]]; then
			continue
		fi
		printf '%s\n' "$pid"
	done
}

echo "Monitoring script A: $monitor_arg"
if [ -n "$monitor_path" ]; then
	echo "Resolved script A path: $monitor_path"
fi
echo "Script B will run after A finishes: $run_path"
echo "Polling interval: ${interval_minutes} minute(s)"

while true; do
	if pids=$(find_matching_pids "$monitor_pattern" "$monitor_path" "$monitor_basename" "$monitor_args_suffix") && [ -n "$pids" ]; then
		echo "[$(date '+%F %T')] script A still running, check again in ${interval_minutes} minute(s): $pids"
		sleep "$((interval_minutes * 60))"
		continue
	fi
	break
done

echo "[$(date '+%F %T')] script A has finished, starting script B"
exec bash "$run_path"