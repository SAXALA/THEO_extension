#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

usage() {
	echo "Usage: $(basename "$0") <script-a> <script-b>" >&2
	echo "Environment: MONITOR_INTERVAL_MINUTES=<minutes> (default: 1)" >&2
}

if [ "$#" -ne 2 ]; then
	usage
	exit 1
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
monitor_arg="$1"
run_arg="$2"
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
if resolved=$(resolve_script_path "$monitor_arg"); then
	monitor_path="$resolved"
	monitor_pattern="$monitor_path"
else
	monitor_pattern="$monitor_arg"
fi

if ! run_path=$(resolve_script_path "$run_arg"); then
	echo "Script B not found: $run_arg" >&2
	exit 1
fi

find_matching_pids() {
	local pattern="$1"
	pgrep -f -- "$pattern" 2>/dev/null | while IFS= read -r pid; do
		if [ -n "$pid" ] && [ "$pid" != "$$" ]; then
			printf '%s\n' "$pid"
		fi
	done
}

echo "Monitoring script A: $monitor_arg"
if [ -n "$monitor_path" ]; then
	echo "Resolved script A path: $monitor_path"
fi
echo "Script B will run after A finishes: $run_path"
echo "Polling interval: ${interval_minutes} minute(s)"

while true; do
	if pids=$(find_matching_pids "$monitor_pattern") && [ -n "$pids" ]; then
		echo "[$(date '+%F %T')] script A still running, check again in ${interval_minutes} minute(s): $pids"
		sleep "$((interval_minutes * 60))"
		continue
	fi
	break
done

echo "[$(date '+%F %T')] script A has finished, starting script B"
exec bash "$run_path"