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
monitor_script_token="${monitor_args[0]}"
monitor_script_args=()
if [ "${#monitor_args[@]}" -gt 1 ]; then
	monitor_script_args=("${monitor_args[@]:1}")
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

resolve_process_arg_path() {
	local proc_dir="$1"
	local arg="$2"
	local proc_cwd=""
	if [[ "$arg" == */* ]]; then
		if [[ "$arg" = /* ]]; then
			if [ -f "$arg" ]; then
				printf '%s/%s\n' "$(cd "$(dirname "$arg")" && pwd)" "$(basename "$arg")"
				return 0
			fi
		else
			proc_cwd=$(readlink "$proc_dir/cwd" 2>/dev/null || true)
			if [ -n "$proc_cwd" ] && [ -f "$proc_cwd/$arg" ]; then
				printf '%s/%s\n' "$(cd "$proc_cwd/$(dirname "$arg")" && pwd)" "$(basename "$arg")"
				return 0
			fi
		fi
	fi
	return 1
}

get_proc_ppid() {
	local status_path="$1"
	local field value rest
	while read -r field value rest; do
		if [ "$field" = "PPid:" ]; then
			printf '%s\n' "$value"
			return 0
		fi
	done < "$status_path"
	return 1
}

find_matching_pids() {
	local resolved_path="$1"
	local basename_pattern="$2"
	shift 2
	local expected_args=("$@")
	local excluded_pid_set=" "
	local chain_cursor="${BASHPID:-$$}"
	while [ -n "$chain_cursor" ] && [ "$chain_cursor" != "0" ]; do
		excluded_pid_set+="$chain_cursor "
		local chain_status="/proc/$chain_cursor/status"
		if [ ! -r "$chain_status" ]; then
			break
		fi
		chain_cursor=$(get_proc_ppid "$chain_status") || break
	done
	local proc_dir pid
	for proc_dir in /proc/[0-9]*; do
		pid="${proc_dir##*/}"
		if [[ "$excluded_pid_set" == *" $pid "* ]]; then
			continue
		fi
		if [ ! -r "$proc_dir/cmdline" ]; then
			continue
		fi

		local argv=()
		mapfile -d '' -t argv < "$proc_dir/cmdline" 2>/dev/null || continue
		if [ "${#argv[@]}" -eq 0 ]; then
			continue
		fi

		if [ "${#argv[@]}" -ge 2 ] && [[ "${argv[1]}" == "-c" || "${argv[1]}" == "-lc" || "${argv[1]}" == "-ic" ]]; then
			continue
		fi

		local match_index="-1"
		local i token resolved_token
		for i in "${!argv[@]}"; do
			token="${argv[$i]}"
			if [ -n "$resolved_path" ]; then
				if resolved_token=$(resolve_process_arg_path "$proc_dir" "$token"); then
					if [ "$resolved_token" = "$resolved_path" ]; then
						match_index="$i"
						break
					fi
				fi
			fi
			if [ "$token" = "$basename_pattern" ] || [ "$token" = "$monitor_script_token" ]; then
				match_index="$i"
				break
			fi
		done

		if [ "$match_index" -lt 0 ]; then
			continue
		fi

		if [ "${#expected_args[@]}" -gt 0 ]; then
			local start_index=$((match_index + 1))
			local remaining=$(( ${#argv[@]} - start_index ))
			if [ "$remaining" -lt "${#expected_args[@]}" ]; then
				continue
			fi
			local args_match=true
			local j
			for j in "${!expected_args[@]}"; do
				if [ "${argv[$((start_index + j))]}" != "${expected_args[$j]}" ]; then
					args_match=false
					break
				fi
			done
			if [ "$args_match" != true ]; then
				continue
			fi
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
	if pids=$(find_matching_pids "$monitor_path" "$monitor_basename" "${monitor_script_args[@]}") && [ -n "$pids" ]; then
		echo "[$(date '+%F %T')] script A still running, check again in ${interval_minutes} minute(s): $pids"
		sleep "$((interval_minutes * 60))"
		continue
	fi
	break
done

echo "[$(date '+%F %T')] script A has finished, starting script B"
exec bash "$run_path"