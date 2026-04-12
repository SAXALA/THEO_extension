#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

set -Eeuo pipefail

# 用法:
#   ./loadWithoutSnapshot.sh [chainkv|pebble|ethstorePebble|all]
# 示例:
#   ./loadWithoutSnapshot.sh all
#   ./loadWithoutSnapshot.sh chainkv

BACKEND="${1:-all}"
VALID_BACKENDS="chainkv pebble ethstorePebble all"

if [[ " ${VALID_BACKENDS} " != *" ${BACKEND} "* ]]; then
	echo "Invalid backend: ${BACKEND}"
	echo "Usage: $0 [chainkv|pebble|ethstorePebble|all]"
	exit 1
fi

CONFIG_PATH="${CONFIG_PATH:-replay_config.json}"

# 复用 replay.sh 中的常用参数默认值
CHAINKV_CACHE_MB="${CHAINKV_CACHE_MB:-16}"
CHAINKV_HANDLES="${CHAINKV_HANDLES:-32768}"
CHAINKV_STATE="${CHAINKV_STATE:-true}"
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"

PEBBLE_CACHE_MB="${PEBBLE_CACHE_MB:-16}"
PEBBLE_HANDLES="${PEBBLE_HANDLES:-32768}"

export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"

build_binary() {
	mkdir -p ./bin
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

run_ethstore_pebble_without_snapshot() {
	echo "==== load ethstorePebbleWithoutSnapshots ===="
	./bin/replayWorkload \
		-config "$CONFIG_PATH" \
		-mode ld \
		-backend ethstorePebbleWithoutSnapshots \
		-pebble-cache "$PEBBLE_CACHE_MB" \
		-pebble-handles "$PEBBLE_HANDLES"
}

main() {
	build_binary

	case "$BACKEND" in
		pebble)
			run_pebble_without_snapshot
			;;
		ethstorePebble)
			run_ethstore_pebble_without_snapshot
			;;
        chainkv)
			run_chainkv_without_snapshot
			;;
		all)
			run_pebble_without_snapshot
            run_chainkv_without_snapshot
			run_ethstore_pebble_without_snapshot
			;;
	esac
}

main
