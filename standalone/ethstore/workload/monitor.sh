#!/bin/bash

# --- 1. 参数与进程查找 ---
PROCESS_NAME=$1
INTERVAL=${2:-1}
LOGFILE=${3:-"monitor_${PROCESS_NAME}.log"}

if [ -z "$PROCESS_NAME" ]; then
    echo "使用方法: $0 <进程名> [间隔/s] [日志文件名]"
    exit 1
fi

# 根据进程名查找最新的 PID
MAIN_PID=$(pgrep -n "$PROCESS_NAME")

if [ -z "$MAIN_PID" ]; then
    echo "错误: 未找到名为 '$PROCESS_NAME' 的进程。"
    exit 1
fi

echo "开始监控进程: $PROCESS_NAME (PID: $MAIN_PID)"
echo "采样间隔: ${INTERVAL}s | 日志保存至: $LOGFILE"

# 写入 CSV 表头以便后续分析
if [ ! -f "$LOGFILE" ]; then
    echo "TIMESTAMP,CPU_USAGE,RSS_KB,CPU_JIFFIES,CPU_SEC,IO_READ_KB,IO_WRITE_KB,TOTAL_RCHAR,TOTAL_WCHAR" > "$LOGFILE"
fi

# 获取系统时钟频率 HZ
HZ=$(getconf CLK_TCK)

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
    echo "[$(date +%T)] CPU: $CPU_USAGE% | RSS: ${RSS}KB | IO Read: ${IO_READ_KB}KB"
done

echo -e "\n进程已结束。监控日志已保存至: $LOGFILE"