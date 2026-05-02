#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload_dir=$(cd "${script_dir}/.." && pwd)
cd "$workload_dir" || exit 1

set -Eeuo pipefail

# 用法:
#   ./loadPebbleWithSnapshot.sh [chainkv|pebble|all]
# 示例:
#   ./loadPebbleWithSnapshot.sh all
#   ./loadPebbleWithSnapshot.sh chainkv

BACKEND="${1:-all}"
VALID_BACKENDS="chainkv pebble all"

if [[ " ${VALID_BACKENDS} " != *" ${BACKEND} "* ]]; then
	echo "Invalid backend: ${BACKEND}"
	echo "Usage: $0 [chainkv|pebble|all]"
	exit 1
fi

CONFIG_PATH="${CONFIG_PATH:-${workload_dir}/replay_config.json}"

# 复用 replay.sh 中的常用参数默认值
CHAINKV_CACHE_MB="${CHAINKV_CACHE_MB:-16}"
CHAINKV_HANDLES="${CHAINKV_HANDLES:-32768}"
CHAINKV_STATE="${CHAINKV_STATE:-true}"
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"

PEBBLE_CACHE_MB="${PEBBLE_CACHE_MB:-16}"
PEBBLE_HANDLES="${PEBBLE_HANDLES:-32768}"

export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"

normalize_bool_flag() {
	local value="${1:-}"
	value=$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')
	case "$value" in
		true|1|yes|y|on)
			printf 'true'
			;;
		false|0|no|n|off)
			printf 'false'
			;;
		*)
			return 1
			;;
	esac
}

normalize_chainkv_state_flag() {
	local normalized
	if ! normalized=$(normalize_bool_flag "$CHAINKV_STATE"); then
		echo "Invalid CHAINKV_STATE=${CHAINKV_STATE}; forcing true" >&2
		CHAINKV_STATE="true"
		return 0
	fi
	if [ "$normalized" != "true" ]; then
		echo "CHAINKV_STATE=${CHAINKV_STATE} would disable ChainKV state path; forcing true" >&2
	fi
	CHAINKV_STATE="true"
}

normalize_chainkv_state_flag

build_binary() {
	mkdir -p ./bin
	rm -f ./replayWorkload ./workload
	find ./bin -maxdepth 1 -type f ! -name replayWorkload -delete
	go mod download
	GOAMD64=v4 go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go
}

run_chainkv_with_snapshot() {
	echo "==== load chainkv (with snapshots) ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend chainkv \
		-ckv-cache "$CHAINKV_CACHE_MB" \
		-ckv-handles "$CHAINKV_HANDLES" \
		-ckv-state "$CHAINKV_STATE" \
		-ckv-limit "$CHAINKV_LOAD_LIMIT"
}

run_pebble_with_snapshot() {
	echo "==== load pebble (with snapshots) ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend pebble \
		-pebble-cache "$PEBBLE_CACHE_MB" \
		-pebble-handles "$PEBBLE_HANDLES"
}

main() {
	build_binary

	case "$BACKEND" in
		pebble)
			run_pebble_with_snapshot
			;;
		chainkv)
			run_chainkv_with_snapshot
			;;
		all)
			run_pebble_with_snapshot
			run_chainkv_with_snapshot
			;;
	esac
}

main