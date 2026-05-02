#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload_dir=$(cd "${script_dir}/.." && pwd)
cd "$workload_dir" || exit 1

set -Eeuo pipefail

# 用法:
#   ./loadWithoutSnapshot.sh [chainkv|pebble|theoPebble|all]
# 示例:
#   ./loadWithoutSnapshot.sh all
#   ./loadWithoutSnapshot.sh chainkv

BACKEND="${1:-all}"
VALID_BACKENDS="chainkv pebble theoPebble all"

if [[ " ${VALID_BACKENDS} " != *" ${BACKEND} "* ]]; then
	echo "Invalid backend: ${BACKEND}"
	echo "Usage: $0 [chainkv|pebble|theoPebble|all]"
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

run_chainkv_without_snapshot() {
	echo "==== load chainkvWithoutSnapshots ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend chainkvWithoutSnapshots \
		-ckv-cache "$CHAINKV_CACHE_MB" \
		-ckv-handles "$CHAINKV_HANDLES" \
		-ckv-state "$CHAINKV_STATE" \
		-ckv-limit "$CHAINKV_LOAD_LIMIT"
}

run_pebble_without_snapshot() {
	echo "==== load pebbleWithoutSnapshots ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend pebbleWithoutSnapshots \
		-pebble-cache "$PEBBLE_CACHE_MB" \
		-pebble-handles "$PEBBLE_HANDLES"
}

run_theo_pebble_without_snapshot() {
	echo "==== load theoPebbleWithoutSnapshots ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend theoPebbleWithoutSnapshots \
		-pebble-cache "$PEBBLE_CACHE_MB" \
		-pebble-handles "$PEBBLE_HANDLES"
}

main() {
	build_binary

	case "$BACKEND" in
		pebble)
			run_pebble_without_snapshot
			;;
		theoPebble)
			run_theo_pebble_without_snapshot
			;;
        chainkv)
			run_chainkv_without_snapshot
			;;
		all)
			run_pebble_without_snapshot
            run_chainkv_without_snapshot
			run_theo_pebble_without_snapshot
			;;
	esac
}

main
