#!/bin/bash

# --- 1. Parameter Parsing and Process Lookup ---
TARGET=$1
INTERVAL=${2:-1}
LOGFILE=${3:-"monitor_${TARGET}.log"}
SSD_TARGET=${4:-"/mnt/ssd2"}
STATFILE="${LOGFILE}.stat"

if [ -z "$TARGET" ]; then
    echo "Usage: $0 <process-name|PID> [interval/s] [log-file-name] [SSD-mount-point|block-device]"
    exit 1
fi

# sudo password; leave empty to use interactive sudo
SUDO_PASSWD="${SUDO_PASSWD:-admin}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

# Support passing PID or process name
if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    MAIN_PID="$TARGET"
    PROCESS_DESC="PID $MAIN_PID"
else
    # Find the latest PID by process name
    MAIN_PID=$(pgrep -n "$TARGET")
    PROCESS_DESC="$TARGET (PID: $MAIN_PID)"
fi

if [ -z "$MAIN_PID" ]; then
    echo "Error: no process found for target '$TARGET'."
    exit 1
fi

if [ ! -d "/proc/$MAIN_PID" ]; then
    echo "Error: PID $MAIN_PID does not exist."
    exit 1
fi

echo "Start monitoring process: $PROCESS_DESC"
echo "Sampling interval: ${INTERVAL}s | Log saved to: $LOGFILE"

resolve_root_block_device() {
    local dev_name="$1"
    local candidate="$dev_name"
    local next

    if [ ! -e "/dev/${candidate}" ]; then
        return 1
    fi

    while true; do
        next=$(lsblk -ndo PKNAME "/dev/${candidate}" 2>/dev/null | head -n1)
        if [ -z "$next" ]; then
            break
        fi
        candidate="$next"
    done

    echo "$candidate"
}

resolve_block_device() {
    local target="$1"
    local dev

    if [ -b "$target" ]; then
        dev="$target"
    else
        dev=$(df -P "$target" 2>/dev/null | tail -1 | awk '{print $1}')
    fi

    if [ -z "$dev" ]; then
        return 1
    fi

    # Normalize the device name for use with /sys/class/block/<name>/stat.
    dev=$(basename "$dev")
    if [ -f "/sys/class/block/${dev}/stat" ]; then
        echo "$dev"
        return 0
    fi

    return 1
}

read_device_model() {
    local dev_name="$1"
    local root_dev
    local model_file

    root_dev=$(resolve_root_block_device "$dev_name" 2>/dev/null || true)
    if [ -z "$root_dev" ]; then
        root_dev="$dev_name"
    fi

    model_file="/sys/class/block/${root_dev}/device/model"
    if [ ! -r "$model_file" ]; then
        return 1
    fi

    tr -d '[:space:]' < "$model_file"
}

is_huawei_ssd_model() {
    local model_upper

    model_upper=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
    [[ "$model_upper" == *HUAWEI* || "$model_upper" == HWE* ]]
}

is_intel_p4510_model() {
    local model_upper

    model_upper=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
    [[ "$model_upper" == *INTEL* && ( "$model_upper" == *P4510* || "$model_upper" == *SSDPE2KX* ) ]]
}

read_disk_bytes() {
    local dev_name="$1"
    local stat_path="/sys/class/block/${dev_name}/stat"
    local sectors_read
    local sectors_write

    sectors_read=$(awk '{print $3}' "$stat_path")
    sectors_write=$(awk '{print $7}' "$stat_path")
    echo "$((sectors_read * 512)) $((sectors_write * 512))"
}

read_fs_io_counters() {
    local pid="$1"
    local syscr
    local syscw

    if [ ! -r "/proc/${pid}/io" ]; then
        return 1
    fi

    syscr=$(awk '/syscr/ {print $2}' "/proc/${pid}/io" 2>/dev/null || true)
    syscw=$(awk '/syscw/ {print $2}' "/proc/${pid}/io" 2>/dev/null || true)
    if [ -z "$syscr" ] || [ -z "$syscw" ]; then
        return 1
    fi
    echo "$syscr $syscw"
}

resolve_hioadm_device_name() {
    local dev_name="$1"
    local candidate

    candidate=$(resolve_root_block_device "$dev_name" 2>/dev/null || true)
    if [ -z "$candidate" ]; then
        return 1
    fi

    if [[ "$candidate" =~ ^nvme[0-9]+$ ]] || [[ "$candidate" =~ ^nvme[0-9]+n[0-9]+$ ]]; then
        echo "$candidate"
        return 0
    fi

    return 1
}

resolve_nvme_controller_name() {
    local dev_name="$1"
    local candidate

    candidate=$(resolve_root_block_device "$dev_name" 2>/dev/null || true)
    if [ -z "$candidate" ]; then
        return 1
    fi

    if [[ "$candidate" =~ ^(nvme[0-9]+)n[0-9]+$ ]]; then
        echo "${BASH_REMATCH[1]}"
        return 0
    fi

    if [[ "$candidate" =~ ^nvme[0-9]+$ ]]; then
        echo "$candidate"
        return 0
    fi

    return 1
}

normalize_counter_value() {
    local raw

    raw=$(echo "$1" | tr -d '[:space:]')
    if [ -z "$raw" ]; then
        return 1
    fi

    if [[ ! "$raw" =~ ^(0[xX][0-9A-Fa-f]+|[0-9]+)$ ]]; then
        return 1
    fi

    printf '%s\n' "$((raw))"
}

run_hioadm_extend_smart() {
    local dev_name="$1"
    local output

    if ! command -v hioadm >/dev/null 2>&1; then
        return 1
    fi

    if [ "$(id -u)" -eq 0 ]; then
        output=$(hioadm info -d "$dev_name" -e 2>/dev/null || true)
    else
        output=$(sudo_run hioadm info -d "$dev_name" -e 2>/dev/null || true)
    fi

    if [ -z "$output" ]; then
        return 1
    fi

    printf '%s\n' "$output"
}

read_hioadm_nand_rw_counters() {
    local dev_name="$1"
    local ext_out total_read_raw io_read_raw gc_read_raw total_write_raw io_write_raw gc_write_raw
    local total_read io_read gc_read total_write io_write gc_write

    ext_out=$(run_hioadm_extend_smart "$dev_name") || return 1
    total_read_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*read_count[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')
    io_read_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*bs_read_count[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')
    gc_read_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*gc_read_count[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')
    total_write_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*write_count[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')
    io_write_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*IO_write_cnt[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')
    gc_write_raw=$(printf '%s\n' "$ext_out" | awk -F: '/^[[:space:]]*GC_write_cnt[[:space:]]*:/ {gsub(/[[:space:]]/, "", $2); print $2; exit}')

    total_read=$(normalize_counter_value "$total_read_raw" 2>/dev/null || true)
    io_read=$(normalize_counter_value "$io_read_raw" 2>/dev/null || true)
    gc_read=$(normalize_counter_value "$gc_read_raw" 2>/dev/null || true)
    total_write=$(normalize_counter_value "$total_write_raw" 2>/dev/null || true)
    io_write=$(normalize_counter_value "$io_write_raw" 2>/dev/null || true)
    gc_write=$(normalize_counter_value "$gc_write_raw" 2>/dev/null || true)

    if [ -z "$total_read" ] && [ -z "$io_read" ] && [ -z "$gc_read" ] && [ -z "$total_write" ] && [ -z "$io_write" ] && [ -z "$gc_write" ]; then
        return 1
    fi

    printf '%s|%s|%s|%s|%s|%s|%s\n' "$total_read" "$io_read" "$gc_read" "$total_write" "$io_write" "$gc_write" "hioadm"
}

run_nvme_intel_smart_log_add() {
    local dev_name="$1"
    local output

    if ! command -v nvme >/dev/null 2>&1; then
        return 1
    fi

    if [ "$(id -u)" -eq 0 ]; then
        output=$(nvme intel smart-log-add "/dev/${dev_name}" 2>/dev/null || true)
    else
        output=$(sudo_run nvme intel smart-log-add "/dev/${dev_name}" 2>/dev/null || true)
    fi

    if [ -z "$output" ]; then
        return 1
    fi

    printf '%s\n' "$output"
}

run_nvme_intel_smart_log_add_json() {
    local dev_name="$1"
    local output

    if ! command -v nvme >/dev/null 2>&1; then
        return 1
    fi

    if [ "$(id -u)" -eq 0 ]; then
        output=$(nvme intel smart-log-add -j "/dev/${dev_name}" 2>/dev/null || true)
    else
        output=$(sudo_run nvme intel smart-log-add -j "/dev/${dev_name}" 2>/dev/null || true)
    fi

    if [ -z "$output" ]; then
        return 1
    fi

    printf '%s\n' "$output"
}

extract_nvme_intel_json_raw_counter() {
    local text="$1"
    local key="$2"

    printf '%s\n' "$text" | awk -v wanted="$key" '
        BEGIN {
            in_target = 0
        }
        $0 ~ "\"" wanted "\"[[:space:]]*:[[:space:]]*\\{" {
            in_target = 1
            next
        }
        in_target && $0 ~ /"raw"[[:space:]]*:/ {
            line = $0
            gsub(/,/, "", line)
            if (match(line, /"raw"[[:space:]]*:[[:space:]]*"?([0-9]+(\.[0-9]+)?)"?/, m)) {
                print m[1]
                exit
            }
        }
        in_target && $0 ~ /}/ {
            in_target = 0
        }
    '
}

extract_nvme_intel_counter() {
    local text="$1"
    local key="$2"

    printf '%s\n' "$text" | awk -v wanted="$key" '
        {
            line=tolower($0)
            gsub(/,/, "", line)
        }
        line ~ "^[[:space:]]*" wanted "[[:space:]]*:" {
            if (match(line, /(sectors|sector|units|unit|bytes|byte|count|raw):[[:space:]]*([0-9]+)/, m)) {
                print m[2]
                exit
            }

            count=split(line, parts, /[^0-9]+/)
            for (i=count; i>=1; i--) {
                if (parts[i] != "") {
                    print parts[i]
                    exit
                }
            }
        }
    '
}

read_intel_p4510_nand_rw_counters() {
    local dev_name="$1"
    local ext_out json_out total_read_raw total_write_raw total_read total_write

    json_out=$(run_nvme_intel_smart_log_add_json "$dev_name" || true)
    if [ -n "$json_out" ]; then
        total_read_raw=$(extract_nvme_intel_json_raw_counter "$json_out" "media_bytes_read")
        total_write_raw=$(extract_nvme_intel_json_raw_counter "$json_out" "nand_bytes_written")
    fi

    if [ -z "$total_read_raw" ] || [ -z "$total_write_raw" ]; then
        ext_out=$(run_nvme_intel_smart_log_add "$dev_name" || true)
        if [ -z "$total_read_raw" ]; then
            total_read_raw=$(extract_nvme_intel_counter "$ext_out" "media_bytes_read")
        fi
        if [ -z "$total_write_raw" ]; then
            total_write_raw=$(extract_nvme_intel_counter "$ext_out" "nand_bytes_written")
        fi
    fi

    total_read=$(normalize_counter_value "$total_read_raw" 2>/dev/null || true)
    total_write=$(normalize_counter_value "$total_write_raw" 2>/dev/null || true)

    if [ -z "$total_read" ] && [ -z "$total_write" ]; then
        return 1
    fi

    printf '%s|%s|%s|%s|%s|%s|%s\n' "$total_read" "" "" "$total_write" "" "" "nvme-cli"
}

read_nand_rw_counters() {
    local method="$1"
    local dev_name="$2"

    case "$method" in
        huawei-hioadm)
            read_hioadm_nand_rw_counters "$dev_name"
            ;;
        intel-p4510-nvme)
            read_intel_p4510_nand_rw_counters "$dev_name"
            ;;
        *)
            return 1
            ;;
    esac
}

BLOCK_DEVICE=$(resolve_block_device "$SSD_TARGET")
if [ -z "$BLOCK_DEVICE" ]; then
    echo "Error: cannot resolve block device from '$SSD_TARGET'. Please provide a valid mount point or block device path (e.g. /mnt/ssd2 or /dev/nvme0n1p1)."
    exit 1
fi

SSD_MODEL=$(read_device_model "$BLOCK_DEVICE" 2>/dev/null || true)
NAND_READ_METHOD=""
NAND_DEVICE_NAME=""

if [ -n "$SSD_MODEL" ] && is_huawei_ssd_model "$SSD_MODEL"; then
    NAND_READ_METHOD="huawei-hioadm"
    NAND_DEVICE_NAME=$(resolve_hioadm_device_name "$BLOCK_DEVICE" || true)
elif [ -n "$SSD_MODEL" ] && is_intel_p4510_model "$SSD_MODEL"; then
    NAND_READ_METHOD="intel-p4510-nvme"
    NAND_DEVICE_NAME=$(resolve_nvme_controller_name "$BLOCK_DEVICE" || true)
fi

echo "SSD stats target: $SSD_TARGET (device: $BLOCK_DEVICE, model: ${SSD_MODEL:-unknown}) | Stats log: $STATFILE"

# Write CSV header for later analysis
if [ ! -f "$LOGFILE" ]; then
    echo "TIMESTAMP,CPU_USAGE,RSS_KB,CPU_JIFFIES,CPU_SEC,IO_READ_KB,IO_WRITE_KB,FS_READ_OPS,FS_WRITE_OPS,TOTAL_SYSCR,TOTAL_SYSCW,TOTAL_RCHAR,TOTAL_WCHAR" > "$LOGFILE"
fi

if [ ! -f "$STATFILE" ]; then
    echo "START_TIMESTAMP,END_TIMESTAMP,DEVICE,BLOCK_READ_BYTES_DIFF,BLOCK_WRITE_BYTES_DIFF,BLOCK_READ_BYTES_START,BLOCK_WRITE_BYTES_START,BLOCK_READ_BYTES_END,BLOCK_WRITE_BYTES_END,FS_READ_OPS_DIFF,FS_WRITE_OPS_DIFF,FS_READ_OPS_START,FS_WRITE_OPS_START,FS_READ_OPS_END,FS_WRITE_OPS_END,NAND_TOTAL_READ_DIFF,NAND_TOTAL_READ_START,NAND_TOTAL_READ_END,NAND_IO_READ_DIFF,NAND_IO_READ_START,NAND_IO_READ_END,NAND_GC_READ_DIFF,NAND_GC_READ_START,NAND_GC_READ_END,NAND_TOTAL_WRITE_DIFF,NAND_TOTAL_WRITE_START,NAND_TOTAL_WRITE_END,NAND_IO_WRITE_DIFF,NAND_IO_WRITE_START,NAND_IO_WRITE_END,NAND_GC_WRITE_DIFF,NAND_GC_WRITE_START,NAND_GC_WRITE_END,NAND_COUNTER_SOURCE" > "$STATFILE"
fi

# Get system clock frequency HZ
HZ=$(getconf CLK_TCK)

# SSD stats are recorded only at process start and end, outputting the total diff.
START_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
START_DISK_BYTES=($(read_disk_bytes "$BLOCK_DEVICE"))
SSD_START_READ=${START_DISK_BYTES[0]}
SSD_START_WRITE=${START_DISK_BYTES[1]}
START_FS_IO=($(read_fs_io_counters "$MAIN_PID" 2>/dev/null || echo "0 0"))
FS_START_READ_OPS=${START_FS_IO[0]}
FS_START_WRITE_OPS=${START_FS_IO[1]}
NAND_COUNTER_SOURCE="unavailable"
NAND_TOTAL_START_READ=""
NAND_IO_START_READ=""
NAND_GC_START_READ=""
NAND_TOTAL_START_WRITE=""
NAND_IO_START_WRITE=""
NAND_GC_START_WRITE=""
NAND_START_OK=0

NAND_COUNTER_START_INFO=""
if [ -n "$NAND_READ_METHOD" ] && [ -n "$NAND_DEVICE_NAME" ]; then
    NAND_COUNTER_START_INFO=$(read_nand_rw_counters "$NAND_READ_METHOD" "$NAND_DEVICE_NAME" || true)
fi
if [ -n "$NAND_COUNTER_START_INFO" ]; then
    IFS='|' read -r NAND_TOTAL_START_READ NAND_IO_START_READ NAND_GC_START_READ NAND_TOTAL_START_WRITE NAND_IO_START_WRITE NAND_GC_START_WRITE NAND_COUNTER_SOURCE <<< "$NAND_COUNTER_START_INFO"
    NAND_START_OK=1
fi

if [ "$NAND_START_OK" -eq 0 ]; then
    if [ -z "$NAND_READ_METHOD" ]; then
        echo "Warning: device ${SSD_MODEL:-unknown} did not match any supported NAND stats collection method; currently only Huawei SSD (hioadm) and Intel P4510 (nvme intel smart-log-add) are supported." >&2
    else
        echo "Warning: failed to read NAND R/W stats from /dev/${NAND_DEVICE_NAME:-$BLOCK_DEVICE} via ${NAND_READ_METHOD}; check sudo permissions, ensure the tool is installed, or verify the device exposes the vendor-specific fields." >&2
    fi
fi

# --- 2. Monitor Loop ---
while [ -d "/proc/$MAIN_PID" ]; do
    if [ ! -r "/proc/$MAIN_PID/stat" ] || [ ! -r "/proc/$MAIN_PID/io" ] || [ ! -r "/proc/$MAIN_PID/status" ]; then
        break
    fi

    # Record data before the sampling interval
    # When reading /proc/$PID/stat, we extract fields 14 and 15 (utime, stime)
    CPU_TOTAL_BEFORE=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    PROC_STAT_BEFORE=($(cat /proc/$MAIN_PID/stat))
    UTIME_BEFORE=${PROC_STAT_BEFORE[13]}
    STIME_BEFORE=${PROC_STAT_BEFORE[14]}
    
    IO_BEFORE_READ=$(awk '/rchar/ {print $2}' /proc/$MAIN_PID/io 2>/dev/null || echo 0)
    IO_BEFORE_WRITE=$(awk '/wchar/ {print $2}' /proc/$MAIN_PID/io 2>/dev/null || echo 0)
    FS_IO_BEFORE=($(read_fs_io_counters "$MAIN_PID" 2>/dev/null || echo "0 0"))
    SYSCR_BEFORE=${FS_IO_BEFORE[0]}
    SYSCW_BEFORE=${FS_IO_BEFORE[1]}

    sleep "${INTERVAL}"

    # Check if the process exited during sleep
    if [ ! -d "/proc/$MAIN_PID" ]; then break; fi
    if [ ! -r "/proc/$MAIN_PID/stat" ] || [ ! -r "/proc/$MAIN_PID/io" ] || [ ! -r "/proc/$MAIN_PID/status" ]; then break; fi

    # Record data after the sampling interval
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    CPU_TOTAL_AFTER=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    PROC_STAT_AFTER=($(cat /proc/$MAIN_PID/stat))
    UTIME_AFTER=${PROC_STAT_AFTER[13]}
    STIME_AFTER=${PROC_STAT_AFTER[14]}
    
    IO_AFTER_READ=$(awk '/rchar/ {print $2}' /proc/$MAIN_PID/io 2>/dev/null || echo "$IO_BEFORE_READ")
    IO_AFTER_WRITE=$(awk '/wchar/ {print $2}' /proc/$MAIN_PID/io 2>/dev/null || echo "$IO_BEFORE_WRITE")
    FS_IO_AFTER=($(read_fs_io_counters "$MAIN_PID" 2>/dev/null || echo "$SYSCR_BEFORE $SYSCW_BEFORE"))
    SYSCR_AFTER=${FS_IO_AFTER[0]}
    SYSCW_AFTER=${FS_IO_AFTER[1]}
    RSS=$(awk '/VmRSS/ {print $2}' /proc/$MAIN_PID/status 2>/dev/null || echo 0)

    # --- 3. Data Calculation ---
    # CPU usage calculation
    CPU_USAGE=$(awk -v ut_b=$UTIME_BEFORE -v st_b=$STIME_BEFORE \
                    -v ut_a=$UTIME_AFTER -v st_a=$STIME_AFTER \
                    -v tot_b=$CPU_TOTAL_BEFORE -v tot_a=$CPU_TOTAL_AFTER \
                    'BEGIN {
                        proc_diff = (ut_a + st_a) - (ut_b + st_b);
                        tot_diff = tot_a - tot_b;
                        if (tot_diff > 0) printf "%.2f", (proc_diff / tot_diff) * 100;
                        else print "0.00";
                    }')

    # Accumulated CPU seconds consumed
    CPU_TIME_SEC=$(awk -v j=$((UTIME_AFTER + STIME_AFTER)) -v h=$HZ 'BEGIN {printf "%.2f", j/h}')
    
    # I/O delta in this interval (KB)
    IO_READ_KB=$(((IO_AFTER_READ - IO_BEFORE_READ) / 1024))
    IO_WRITE_KB=$(((IO_AFTER_WRITE - IO_BEFORE_WRITE) / 1024))
    FS_READ_OPS=$((SYSCR_AFTER - SYSCR_BEFORE))
    FS_WRITE_OPS=$((SYSCW_AFTER - SYSCW_BEFORE))

    # --- 4. Output and Save ---
    DATA_LINE="$TIMESTAMP, $CPU_USAGE, $RSS, $((UTIME_AFTER + STIME_AFTER)), $CPU_TIME_SEC, $IO_READ_KB, $IO_WRITE_KB, $FS_READ_OPS, $FS_WRITE_OPS, $SYSCR_AFTER, $SYSCW_AFTER, $IO_AFTER_READ, $IO_AFTER_WRITE"
    echo "$DATA_LINE" >> "$LOGFILE"
    
    # Optional: print abbreviated info to screen in real time
    # echo "[$(date +%T)] CPU: $CPU_USAGE% | RSS: ${RSS}KB | IO Read: ${IO_READ_KB}KB"
done

END_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
END_DISK_BYTES=($(read_disk_bytes "$BLOCK_DEVICE"))
SSD_END_READ=${END_DISK_BYTES[0]}
SSD_END_WRITE=${END_DISK_BYTES[1]}
SSD_READ_DIFF=$((SSD_END_READ - SSD_START_READ))
SSD_WRITE_DIFF=$((SSD_END_WRITE - SSD_START_WRITE))
END_FS_IO=($(read_fs_io_counters "$MAIN_PID" 2>/dev/null || echo ""))
FS_END_READ_OPS=${END_FS_IO[0]}
FS_END_WRITE_OPS=${END_FS_IO[1]}
if [ -z "$FS_END_READ_OPS" ] || [ -z "$FS_END_WRITE_OPS" ]; then
    FS_END_READ_OPS=${SYSCR_AFTER:-$FS_START_READ_OPS}
    FS_END_WRITE_OPS=${SYSCW_AFTER:-$FS_START_WRITE_OPS}
fi
FS_READ_OPS_DIFF=$((FS_END_READ_OPS - FS_START_READ_OPS))
FS_WRITE_OPS_DIFF=$((FS_END_WRITE_OPS - FS_START_WRITE_OPS))

NAND_TOTAL_END_WRITE=""
NAND_TOTAL_WRITE_DIFF=""
NAND_IO_END_WRITE=""
NAND_IO_WRITE_DIFF=""
NAND_GC_END_WRITE=""
NAND_GC_WRITE_DIFF=""
NAND_TOTAL_END_READ=""
NAND_TOTAL_READ_DIFF=""
NAND_IO_END_READ=""
NAND_IO_READ_DIFF=""
NAND_GC_END_READ=""
NAND_GC_READ_DIFF=""
NAND_COUNTER_END_INFO=""
if [ -n "$NAND_READ_METHOD" ] && [ -n "$NAND_DEVICE_NAME" ]; then
    NAND_COUNTER_END_INFO=$(read_nand_rw_counters "$NAND_READ_METHOD" "$NAND_DEVICE_NAME" || true)
fi
if [ -n "$NAND_COUNTER_END_INFO" ]; then
    IFS='|' read -r NAND_TOTAL_END_READ NAND_IO_END_READ NAND_GC_END_READ NAND_TOTAL_END_WRITE NAND_IO_END_WRITE NAND_GC_END_WRITE _ <<< "$NAND_COUNTER_END_INFO"
    if [ -n "$NAND_TOTAL_START_READ" ] && [ -n "$NAND_TOTAL_END_READ" ]; then
        NAND_TOTAL_READ_DIFF=$((NAND_TOTAL_END_READ - NAND_TOTAL_START_READ))
    fi
    if [ -n "$NAND_IO_START_READ" ] && [ -n "$NAND_IO_END_READ" ]; then
        NAND_IO_READ_DIFF=$((NAND_IO_END_READ - NAND_IO_START_READ))
    fi
    if [ -n "$NAND_GC_START_READ" ] && [ -n "$NAND_GC_END_READ" ]; then
        NAND_GC_READ_DIFF=$((NAND_GC_END_READ - NAND_GC_START_READ))
    fi
    if [ -n "$NAND_TOTAL_START_WRITE" ] && [ -n "$NAND_TOTAL_END_WRITE" ]; then
        NAND_TOTAL_WRITE_DIFF=$((NAND_TOTAL_END_WRITE - NAND_TOTAL_START_WRITE))
    fi
    if [ -n "$NAND_IO_START_WRITE" ] && [ -n "$NAND_IO_END_WRITE" ]; then
        NAND_IO_WRITE_DIFF=$((NAND_IO_END_WRITE - NAND_IO_START_WRITE))
    fi
    if [ -n "$NAND_GC_START_WRITE" ] && [ -n "$NAND_GC_END_WRITE" ]; then
        NAND_GC_WRITE_DIFF=$((NAND_GC_END_WRITE - NAND_GC_START_WRITE))
    fi
fi

STAT_LINE="$START_TIMESTAMP,$END_TIMESTAMP,$BLOCK_DEVICE,$SSD_READ_DIFF,$SSD_WRITE_DIFF,$SSD_START_READ,$SSD_START_WRITE,$SSD_END_READ,$SSD_END_WRITE,$FS_READ_OPS_DIFF,$FS_WRITE_OPS_DIFF,$FS_START_READ_OPS,$FS_START_WRITE_OPS,$FS_END_READ_OPS,$FS_END_WRITE_OPS,$NAND_TOTAL_READ_DIFF,$NAND_TOTAL_START_READ,$NAND_TOTAL_END_READ,$NAND_IO_READ_DIFF,$NAND_IO_START_READ,$NAND_IO_END_READ,$NAND_GC_READ_DIFF,$NAND_GC_START_READ,$NAND_GC_END_READ,$NAND_TOTAL_WRITE_DIFF,$NAND_TOTAL_START_WRITE,$NAND_TOTAL_END_WRITE,$NAND_IO_WRITE_DIFF,$NAND_IO_START_WRITE,$NAND_IO_END_WRITE,$NAND_GC_WRITE_DIFF,$NAND_GC_START_WRITE,$NAND_GC_END_WRITE,$NAND_COUNTER_SOURCE"
echo "$STAT_LINE" >> "$STATFILE"

echo -e "\nProcess exited. Monitor logs saved to: $LOGFILE and $STATFILE"