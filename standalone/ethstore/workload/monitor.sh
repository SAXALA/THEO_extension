#!/bin/bash

# --- 1. 参数与进程查找 ---
TARGET=$1
INTERVAL=${2:-1}
LOGFILE=${3:-"monitor_${TARGET}.log"}
SSD_TARGET=${4:-"/mnt/ssd2"}
STATFILE="${LOGFILE}.stat"

if [ -z "$TARGET" ]; then
    echo "使用方法: $0 <进程名|PID> [间隔/s] [日志文件名] [SSD挂载点|块设备]"
    exit 1
fi

# sudo 密码；留空则使用交互式 sudo
SUDO_PASSWD="${SUDO_PASSWD:-qwe123}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

# 支持传入 PID 或进程名
if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
    MAIN_PID="$TARGET"
    PROCESS_DESC="PID $MAIN_PID"
else
    # 根据进程名查找最新的 PID
    MAIN_PID=$(pgrep -n "$TARGET")
    PROCESS_DESC="$TARGET (PID: $MAIN_PID)"
fi

if [ -z "$MAIN_PID" ]; then
    echo "错误: 未找到目标 '$TARGET' 对应的进程。"
    exit 1
fi

if [ ! -d "/proc/$MAIN_PID" ]; then
    echo "错误: PID $MAIN_PID 不存在。"
    exit 1
fi

echo "开始监控进程: $PROCESS_DESC"
echo "采样间隔: ${INTERVAL}s | 日志保存至: $LOGFILE"

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

    # 统一设备名，供 /sys/class/block/<name>/stat 使用。
    dev=$(basename "$dev")
    if [ -f "/sys/class/block/${dev}/stat" ]; then
        echo "$dev"
        return 0
    fi

    return 1
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

    if [[ "$candidate" =~ ^nvme[0-9]+$ ]] || [[ "$candidate" =~ ^nvme[0-9]+n[0-9]+$ ]]; then
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

    printf '%s|%s|%s|%s|%s|%s|%s\n' "$total_read" "$io_read" "$gc_read" "$total_write" "$io_write" "$gc_write" "hioadm-extend-smart-counts"
}

BLOCK_DEVICE=$(resolve_block_device "$SSD_TARGET")
if [ -z "$BLOCK_DEVICE" ]; then
    echo "错误: 无法从 '$SSD_TARGET' 解析块设备，请传入有效挂载点或块设备路径（如 /mnt/ssd2 或 /dev/nvme0n1p1）。"
    exit 1
fi

echo "SSD 统计目标: $SSD_TARGET (device: $BLOCK_DEVICE) | 统计日志: $STATFILE"

# 写入 CSV 表头以便后续分析
if [ ! -f "$LOGFILE" ]; then
    echo "TIMESTAMP,CPU_USAGE,RSS_KB,CPU_JIFFIES,CPU_SEC,IO_READ_KB,IO_WRITE_KB,FS_READ_OPS,FS_WRITE_OPS,TOTAL_SYSCR,TOTAL_SYSCW,TOTAL_RCHAR,TOTAL_WCHAR" > "$LOGFILE"
fi

if [ ! -f "$STATFILE" ]; then
    echo "START_TIMESTAMP,END_TIMESTAMP,DEVICE,BLOCK_READ_BYTES_DIFF,BLOCK_WRITE_BYTES_DIFF,BLOCK_READ_BYTES_START,BLOCK_WRITE_BYTES_START,BLOCK_READ_BYTES_END,BLOCK_WRITE_BYTES_END,FS_READ_OPS_DIFF,FS_WRITE_OPS_DIFF,FS_READ_OPS_START,FS_WRITE_OPS_START,FS_READ_OPS_END,FS_WRITE_OPS_END,NAND_TOTAL_READ_DIFF,NAND_TOTAL_READ_START,NAND_TOTAL_READ_END,NAND_IO_READ_DIFF,NAND_IO_READ_START,NAND_IO_READ_END,NAND_GC_READ_DIFF,NAND_GC_READ_START,NAND_GC_READ_END,NAND_TOTAL_WRITE_DIFF,NAND_TOTAL_WRITE_START,NAND_TOTAL_WRITE_END,NAND_IO_WRITE_DIFF,NAND_IO_WRITE_START,NAND_IO_WRITE_END,NAND_GC_WRITE_DIFF,NAND_GC_WRITE_START,NAND_GC_WRITE_END,NAND_COUNTER_SOURCE" > "$STATFILE"
fi

# 获取系统时钟频率 HZ
HZ=$(getconf CLK_TCK)

# SSD 统计只记录进程开始与结束两点，输出总差值。
START_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
START_DISK_BYTES=($(read_disk_bytes "$BLOCK_DEVICE"))
SSD_START_READ=${START_DISK_BYTES[0]}
SSD_START_WRITE=${START_DISK_BYTES[1]}
START_FS_IO=($(read_fs_io_counters "$MAIN_PID" 2>/dev/null || echo "0 0"))
FS_START_READ_OPS=${START_FS_IO[0]}
FS_START_WRITE_OPS=${START_FS_IO[1]}
HIOADM_DEVICE=$(resolve_hioadm_device_name "$BLOCK_DEVICE" || true)
NAND_COUNTER_SOURCE="unavailable"
NAND_TOTAL_START_READ=""
NAND_IO_START_READ=""
NAND_GC_START_READ=""
NAND_TOTAL_START_WRITE=""
NAND_IO_START_WRITE=""
NAND_GC_START_WRITE=""
HIOADM_START_OK=0

NAND_COUNTER_START_INFO=""
if [ -n "$HIOADM_DEVICE" ]; then
    NAND_COUNTER_START_INFO=$(read_hioadm_nand_rw_counters "$HIOADM_DEVICE" || true)
fi
if [ -n "$NAND_COUNTER_START_INFO" ]; then
    IFS='|' read -r NAND_TOTAL_START_READ NAND_IO_START_READ NAND_GC_START_READ NAND_TOTAL_START_WRITE NAND_IO_START_WRITE NAND_GC_START_WRITE NAND_COUNTER_SOURCE <<< "$NAND_COUNTER_START_INFO"
    HIOADM_START_OK=1
fi

if [ "$HIOADM_START_OK" -eq 0 ]; then
    echo "警告: 无法通过 hioadm 读取 /dev/${HIOADM_DEVICE:-$BLOCK_DEVICE} 的 NAND 读写统计；请检查 SUDO_PASSWD 是否可用、是否已执行 sudo -v，或确认设备是否暴露相应厂商字段。" >&2
fi

# --- 2. 循环监控 ---
while [ -d "/proc/$MAIN_PID" ]; do
    if [ ! -r "/proc/$MAIN_PID/stat" ] || [ ! -r "/proc/$MAIN_PID/io" ] || [ ! -r "/proc/$MAIN_PID/status" ]; then
        break
    fi

    # 记录采样开始前的数据
    # 使用 /proc/$PID/stat 时，我们要提取的是第 14 和 15 个字段 (utime, stime)
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

    # 检查进程在 sleep 期间是否退出
    if [ ! -d "/proc/$MAIN_PID" ]; then break; fi
    if [ ! -r "/proc/$MAIN_PID/stat" ] || [ ! -r "/proc/$MAIN_PID/io" ] || [ ! -r "/proc/$MAIN_PID/status" ]; then break; fi

    # 记录采样结束后的数据
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

    # --- 3. 数据计算 ---
    # CPU 使用率计算
    CPU_USAGE=$(awk -v ut_b=$UTIME_BEFORE -v st_b=$STIME_BEFORE \
                    -v ut_a=$UTIME_AFTER -v st_a=$STIME_AFTER \
                    -v tot_b=$CPU_TOTAL_BEFORE -v tot_a=$CPU_TOTAL_AFTER \
                    'BEGIN {
                        proc_diff = (ut_a + st_a) - (ut_b + st_b);
                        tot_diff = tot_a - tot_b;
                        if (tot_diff > 0) printf "%.2f", (proc_diff / tot_diff) * 100;
                        else print "0.00";
                    }')

    # 累计消耗的 CPU 秒数
    CPU_TIME_SEC=$(awk -v j=$((UTIME_AFTER + STIME_AFTER)) -v h=$HZ 'BEGIN {printf "%.2f", j/h}')
    
    # 本次间隔内的 I/O 增量 (KB)
    IO_READ_KB=$(((IO_AFTER_READ - IO_BEFORE_READ) / 1024))
    IO_WRITE_KB=$(((IO_AFTER_WRITE - IO_BEFORE_WRITE) / 1024))
    FS_READ_OPS=$((SYSCR_AFTER - SYSCR_BEFORE))
    FS_WRITE_OPS=$((SYSCW_AFTER - SYSCW_BEFORE))

    # --- 4. 输出与保存 ---
    DATA_LINE="$TIMESTAMP, $CPU_USAGE, $RSS, $((UTIME_AFTER + STIME_AFTER)), $CPU_TIME_SEC, $IO_READ_KB, $IO_WRITE_KB, $FS_READ_OPS, $FS_WRITE_OPS, $SYSCR_AFTER, $SYSCW_AFTER, $IO_AFTER_READ, $IO_AFTER_WRITE"
    echo "$DATA_LINE" >> "$LOGFILE"
    
    # 可选：在屏幕上实时打印缩略信息
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
if [ -n "$HIOADM_DEVICE" ]; then
    NAND_COUNTER_END_INFO=$(read_hioadm_nand_rw_counters "$HIOADM_DEVICE" || true)
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

echo -e "\n进程已结束。监控日志已保存至: $LOGFILE 和 $STATFILE"