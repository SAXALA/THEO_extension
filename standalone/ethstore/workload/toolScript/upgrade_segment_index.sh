#!/usr/bin/env bash

set -Eeuo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workload_dir=$(cd "${script_dir}/.." && pwd)

SOURCE_STATE_DIR="${1:-${SOURCE_STATE_DIR:-}}"
TARGET_STATE_DIR="${2:-${TARGET_STATE_DIR:-}}"

usage() {
    echo "Usage: $0 <source-state-db-dir> [target-state-db-dir]"
    echo "Example: $0 /mnt/ssd2/loaded/ethstore/database_statedb16KB_gced"
    echo "Example: $0 /mnt/ssd2/loaded/ethstore/database_statedb16KB_gced /mnt/ssd2/loaded/ethstore/database_statedb16KB_gced_upgrade_copy"
}

# sudo 密码；留空则使用交互式 sudo
SUDO_PASSWD="${SUDO_PASSWD:-qwe123}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

copy_state_dir() {
    local source_dir="$1"
    local target_dir="$2"

    mkdir -p "${target_dir}"

    if command -v rsync >/dev/null 2>&1; then
        sudo_run rsync -a --delete "${source_dir}/" "${target_dir}/"
        return 0
    fi

    rm -rf "${target_dir}"
    mkdir -p "${target_dir}"
    sudo_run cp -a "${source_dir}/." "${target_dir}/"
}

if [ -z "${SOURCE_STATE_DIR}" ]; then
    usage
    exit 1
fi

if [ ! -d "${SOURCE_STATE_DIR}" ]; then
    echo "Source statedb directory does not exist: ${SOURCE_STATE_DIR}"
    exit 1
fi

if [ -z "${TARGET_STATE_DIR}" ]; then
    TARGET_STATE_DIR="${SOURCE_STATE_DIR%/}_upgrade_copy"
fi

target_parent_dir=$(dirname "${TARGET_STATE_DIR}")
mkdir -p "${target_parent_dir}"

source_abs_dir=$(cd "${SOURCE_STATE_DIR}" && pwd)
target_abs_dir=$(cd "${target_parent_dir}" && pwd)/$(basename "${TARGET_STATE_DIR}")

if [ "${source_abs_dir}" = "${target_abs_dir}" ]; then
    echo "Target statedb directory must differ from source: ${TARGET_STATE_DIR}"
    exit 1
fi

echo "Copy statedb before upgrade"
echo "  source: ${SOURCE_STATE_DIR}"
echo "  target: ${TARGET_STATE_DIR}"
copy_state_dir "${SOURCE_STATE_DIR}" "${TARGET_STATE_DIR}"

cd "${workload_dir}"
GC_WORKERS="${GC_WORKERS:-32}" \
STORAGE_GC_THRESHOLD="${STORAGE_GC_THRESHOLD:-1.5}" \
UPGRADE_STATE_DIR="${TARGET_STATE_DIR}" \
./replay.sh upgrade-index ethstore

echo "Upgrade finished on copied statedb: ${TARGET_STATE_DIR}"