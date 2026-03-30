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

resolve_smart_device_path() {
    local dev_name="$1"
    local dev_path="/dev/${dev_name}"
    local parent

    parent=$(lsblk -ndo PKNAME "$dev_path" 2>/dev/null | head -n1)
    if [ -n "$parent" ]; then
        echo "/dev/${parent}"
        return 0
    fi

    echo "$dev_path"
}

extract_last_integer() {
    sed -nE 's/.*([0-9][0-9,]*).*/\1/p' | tr -d ','
}

human_size_to_bytes() {
    local human="$1"
    local value unit

    value=$(echo "$human" | sed -nE 's/^([0-9]+(\.[0-9]+)?).*/\1/p')
    unit=$(echo "$human" | sed -nE 's/^[0-9]+(\.[0-9]+)?[[:space:]]*([[:alpha:]]+).*$/\2/p' | tr '[:lower:]' '[:upper:]')

    if [ -z "$value" ] || [ -z "$unit" ]; then
        return 1
    fi

    awk -v v="$value" -v u="$unit" 'BEGIN {
        mul = 1
        if (u == "B") mul = 1
        else if (u == "KB" || u == "KIB") mul = 1024
        else if (u == "MB" || u == "MIB") mul = 1024 * 1024
        else if (u == "GB" || u == "GIB") mul = 1024 * 1024 * 1024
        else if (u == "TB" || u == "TIB") mul = 1024 * 1024 * 1024 * 1024
        else if (u == "PB" || u == "PIB") mul = 1024 * 1024 * 1024 * 1024 * 1024
        else exit 1
        printf "%.0f", v * mul
    }'
}

read_nand_write_bytes() {
    local dev_path="$1"
    local smart_out line bracket_val raw_value bytes

    if ! command -v smartctl >/dev/null 2>&1; then
        return 1
    fi

    smart_out=$(smartctl -a "$dev_path" 2>/dev/null || true)
    if [ -z "$smart_out" ]; then
        return 1
    fi

    # Some NVMe drives expose physical media writes with a human-readable size in brackets.
    line=$(echo "$smart_out" | grep -iE 'Physical media units written|NAND bytes written|NAND writes|Flash writes' | head -n1)
    if [ -n "$line" ]; then
        bracket_val=$(echo "$line" | sed -nE 's/.*\[([0-9]+(\.[0-9]+)?[[:space:]]*[KMGTPE]?I?B)\].*/\1/p')
        if [ -n "$bracket_val" ]; then
            bytes=$(human_size_to_bytes "$bracket_val" 2>/dev/null || true)
            if [ -n "$bytes" ]; then
                echo "$bytes bracket-size"
                return 0
            fi
        fi
    fi

    # Common SATA SMART vendor attributes that directly report NAND writes in GiB.
    line=$(echo "$smart_out" | awk '/Total_NAND_Writes_GiB|NAND_Writes_1GiB|NAND_Writes_GiB|Flash_Writes_GiB/ {print; exit}')
    if [ -n "$line" ]; then
        raw_value=$(echo "$line" | extract_last_integer)
        if [ -n "$raw_value" ]; then
            bytes=$((raw_value * 1024 * 1024 * 1024))
            echo "$bytes smart-attr-gib"
            return 0
        fi
    fi

    # Some drives expose a direct NAND bytes counter.
    line=$(echo "$smart_out" | awk -F: '/NAND[[:space:]_]*Bytes[[:space:]_]*Written/ {print $2; exit}')
    if [ -n "$line" ]; then
        raw_value=$(echo "$line" | extract_last_integer)
        if [ -n "$raw_value" ]; then
            echo "$raw_value smart-attr-bytes"
            return 0
        fi
    fi

    return 1
}

BLOCK_DEVICE=$(resolve_block_device "$SSD_TARGET")
if [ -z "$BLOCK_DEVICE" ]; then
    echo "错误: 无法从 '$SSD_TARGET' 解析块设备，请传入有效挂载点或块设备路径（如 /mnt/ssd2 或 /dev/nvme0n1p1）。"
    exit 1
fi

echo "SSD 统计目标: $SSD_TARGET (device: $BLOCK_DEVICE) | 统计日志: $STATFILE"

# 写入 CSV 表头以便后续分析
if [ ! -f "$LOGFILE" ]; then
    echo "TIMESTAMP,CPU_USAGE,RSS_KB,CPU_JIFFIES,CPU_SEC,IO_READ_KB,IO_WRITE_KB,TOTAL_RCHAR,TOTAL_WCHAR" > "$LOGFILE"
fi

if [ ! -f "$STATFILE" ]; then
    echo "START_TIMESTAMP,END_TIMESTAMP,DEVICE,BLOCK_READ_BYTES_DIFF,BLOCK_WRITE_BYTES_DIFF,BLOCK_READ_BYTES_START,BLOCK_WRITE_BYTES_START,BLOCK_READ_BYTES_END,BLOCK_WRITE_BYTES_END,NAND_WRITE_BYTES_DIFF,NAND_WRITE_BYTES_START,NAND_WRITE_BYTES_END,NAND_SOURCE" > "$STATFILE"
fi

# 获取系统时钟频率 HZ
HZ=$(getconf CLK_TCK)

# SSD 统计只记录进程开始与结束两点，输出总差值。
START_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
START_DISK_BYTES=($(read_disk_bytes "$BLOCK_DEVICE"))
SSD_START_READ=${START_DISK_BYTES[0]}
SSD_START_WRITE=${START_DISK_BYTES[1]}
SMART_DEVICE_PATH=$(resolve_smart_device_path "$BLOCK_DEVICE")
NAND_SOURCE="unavailable"
NAND_START_WRITE=""

NAND_START_INFO=$(read_nand_write_bytes "$SMART_DEVICE_PATH" || true)
if [ -n "$NAND_START_INFO" ]; then
    NAND_START_WRITE=$(echo "$NAND_START_INFO" | awk '{print $1}')
    NAND_SOURCE=$(echo "$NAND_START_INFO" | awk '{print $2}')
else
    echo "警告: 无法从 ${SMART_DEVICE_PATH} 读取 NAND 物理写入计数（smartctl 字段可能不支持该盘）。" >&2
fi

# --- 2. 循环监控 ---
while [ -d "/proc/$MAIN_PID" ]; do
    # 记录采样开始前的数据
    # 使用 /proc/$PID/stat 时，我们要提取的是第 14 和 15 个字段 (utime, stime)
    CPU_TOTAL_BEFORE=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    PROC_STAT_BEFORE=($(cat /proc/$MAIN_PID/stat))
    UTIME_BEFORE=${PROC_STAT_BEFORE[13]}
    STIME_BEFORE=${PROC_STAT_BEFORE[14]}
    
    IO_BEFORE_READ=$(awk '/rchar/ {print $2}' /proc/$MAIN_PID/io)
    IO_BEFORE_WRITE=$(awk '/wchar/ {print $2}' /proc/$MAIN_PID/io)

    sleep "${INTERVAL}"

    # 检查进程在 sleep 期间是否退出
    if [ ! -d "/proc/$MAIN_PID" ]; then break; fi

    # 记录采样结束后的数据
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    CPU_TOTAL_AFTER=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    PROC_STAT_AFTER=($(cat /proc/$MAIN_PID/stat))
    UTIME_AFTER=${PROC_STAT_AFTER[13]}
    STIME_AFTER=${PROC_STAT_AFTER[14]}
    
    IO_AFTER_READ=$(awk '/rchar/ {print $2}' /proc/$MAIN_PID/io)
    IO_AFTER_WRITE=$(awk '/wchar/ {print $2}' /proc/$MAIN_PID/io)
    RSS=$(awk '/VmRSS/ {print $2}' /proc/$MAIN_PID/status)

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

    # --- 4. 输出与保存 ---
    DATA_LINE="$TIMESTAMP, $CPU_USAGE, $RSS, $((UTIME_AFTER + STIME_AFTER)), $CPU_TIME_SEC, $IO_READ_KB, $IO_WRITE_KB, $IO_AFTER_READ, $IO_AFTER_WRITE"
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

NAND_END_WRITE=""
NAND_WRITE_DIFF=""
NAND_END_INFO=$(read_nand_write_bytes "$SMART_DEVICE_PATH" || true)
if [ -n "$NAND_END_INFO" ]; then
    NAND_END_WRITE=$(echo "$NAND_END_INFO" | awk '{print $1}')
    if [ -n "$NAND_START_WRITE" ]; then
        NAND_WRITE_DIFF=$((NAND_END_WRITE - NAND_START_WRITE))
    fi
fi

STAT_LINE="$START_TIMESTAMP,$END_TIMESTAMP,$BLOCK_DEVICE,$SSD_READ_DIFF,$SSD_WRITE_DIFF,$SSD_START_READ,$SSD_START_WRITE,$SSD_END_READ,$SSD_END_WRITE,$NAND_WRITE_DIFF,$NAND_START_WRITE,$NAND_END_WRITE,$NAND_SOURCE"
echo "$STAT_LINE" >> "$STATFILE"

echo -e "\n进程已结束。监控日志已保存至: $LOGFILE 和 $STATFILE"