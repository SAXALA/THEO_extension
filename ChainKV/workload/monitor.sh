#!/bin/bash

# --- 1. 参数与进程查找 ---
TARGET=$1
MODE="name"

if [ -z "$TARGET" ]; then
    echo "使用方法:"
    echo "  按进程名监控: $0 <进程名> [间隔/s] [日志文件名]"
    echo "  按 PID 监控:   $0 --pid <PID> [间隔/s] [日志文件名]"
    echo "  按进程树监控: $0 --pid-tree <PID> [间隔/s] [日志文件名]"
    exit 1
fi

if [ "$TARGET" = "--pid" ]; then
    MODE="pid"
    MAIN_PID=$2
    INTERVAL=${3:-1}
    LOGFILE=${4:-"monitor_pid_${MAIN_PID}.log"}

    if [ -z "$MAIN_PID" ]; then
        echo "错误: --pid 模式需要提供 PID。"
        exit 1
    fi

    if [ ! -d "/proc/$MAIN_PID" ]; then
        echo "错误: PID $MAIN_PID 不存在或已退出。"
        exit 1
    fi

    PROCESS_NAME=$(cat /proc/$MAIN_PID/comm 2>/dev/null)
    if [ -z "$PROCESS_NAME" ]; then
        PROCESS_NAME="pid_${MAIN_PID}"
    fi
elif [ "$TARGET" = "--pid-tree" ]; then
    MODE="pid-tree"
    MAIN_PID=$2
    INTERVAL=${3:-1}
    LOGFILE=${4:-"monitor_tree_${MAIN_PID}.log"}

    if [ -z "$MAIN_PID" ]; then
        echo "错误: --pid-tree 模式需要提供 PID。"
        exit 1
    fi

    if [ ! -d "/proc/$MAIN_PID" ]; then
        echo "错误: PID $MAIN_PID 不存在或已退出。"
        exit 1
    fi

    PROCESS_NAME=$(cat /proc/$MAIN_PID/comm 2>/dev/null)
    if [ -z "$PROCESS_NAME" ]; then
        PROCESS_NAME="pid_${MAIN_PID}"
    fi
else
    PROCESS_NAME=$TARGET
    INTERVAL=${2:-1}
    LOGFILE=${3:-"monitor_${PROCESS_NAME}.log"}

    # 根据进程名查找最新的 PID
    MAIN_PID=$(pgrep -n "$PROCESS_NAME")

    if [ -z "$MAIN_PID" ]; then
        echo "错误: 未找到名为 '$PROCESS_NAME' 的进程。"
        exit 1
    fi
fi

if [ "$MODE" = "pid-tree" ]; then
    echo "开始监控进程树: $PROCESS_NAME (Root PID: $MAIN_PID)"
else
    echo "开始监控进程: $PROCESS_NAME (PID: $MAIN_PID)"
fi
echo "采样间隔: ${INTERVAL}s | 日志保存至: $LOGFILE"

# 写入 CSV 表头以便后续分析
if [ ! -f "$LOGFILE" ]; then
    echo "TIMESTAMP,CPU_USAGE,RSS_KB,CPU_JIFFIES,CPU_SEC,IO_READ_KB,IO_WRITE_KB,TOTAL_RCHAR,TOTAL_WCHAR,PROC_COUNT" > "$LOGFILE"
fi

# 获取系统时钟频率 HZ
HZ=$(getconf CLK_TCK)

get_tree_pids() {
    local root_pid=$1
    local -a queue=()
    local -a result=()
    local current_pid
    local child
    local children

    queue=("$root_pid")

    while [ ${#queue[@]} -gt 0 ]; do
        current_pid="${queue[0]}"
        queue=("${queue[@]:1}")

        if [ ! -d "/proc/$current_pid" ]; then
            continue
        fi

        result+=("$current_pid")
        children=$(pgrep -P "$current_pid" 2>/dev/null || true)
        for child in $children; do
            queue+=("$child")
        done
    done

    printf '%s\n' "${result[@]}"
}

collect_totals() {
    local pids=("$@")
    local proc_count=0
    local total_utime=0
    local total_stime=0
    local total_rss=0
    local total_rchar=0
    local total_wchar=0
    local pid
    local utime
    local stime
    local rss
    local rchar
    local wchar

    for pid in "${pids[@]}"; do
        if [ ! -d "/proc/$pid" ]; then
            continue
        fi

        utime=$(awk '{print $14}' /proc/$pid/stat 2>/dev/null || echo 0)
        stime=$(awk '{print $15}' /proc/$pid/stat 2>/dev/null || echo 0)
        rss=$(awk '/VmRSS/ {print $2}' /proc/$pid/status 2>/dev/null || echo 0)
        rchar=$(awk '/rchar/ {print $2}' /proc/$pid/io 2>/dev/null || echo 0)
        wchar=$(awk '/wchar/ {print $2}' /proc/$pid/io 2>/dev/null || echo 0)

        total_utime=$((total_utime + utime))
        total_stime=$((total_stime + stime))
        total_rss=$((total_rss + rss))
        total_rchar=$((total_rchar + rchar))
        total_wchar=$((total_wchar + wchar))
        proc_count=$((proc_count + 1))
    done

    echo "$proc_count $total_utime $total_stime $total_rss $total_rchar $total_wchar"
}

# --- 2. 循环监控 ---
while true; do
    if [ "$MODE" != "pid-tree" ] && [ ! -d "/proc/$MAIN_PID" ]; then
        break
    fi

    if [ "$MODE" = "pid-tree" ]; then
        mapfile -t BEFORE_PIDS < <(get_tree_pids "$MAIN_PID")
    else
        BEFORE_PIDS=("$MAIN_PID")
    fi

    if [ ${#BEFORE_PIDS[@]} -eq 0 ]; then
        break
    fi

    # 记录采样开始前的数据
    # 使用 /proc/$PID/stat 时，我们要提取的是第 14 和 15 个字段 (utime, stime)
    CPU_TOTAL_BEFORE=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    read -r PROC_COUNT_BEFORE UTIME_BEFORE STIME_BEFORE RSS_BEFORE IO_BEFORE_READ IO_BEFORE_WRITE <<< "$(collect_totals "${BEFORE_PIDS[@]}")"

    sleep "${INTERVAL}"

    # 检查进程在 sleep 期间是否退出
    if [ "$MODE" = "pid-tree" ]; then
        mapfile -t AFTER_PIDS < <(get_tree_pids "$MAIN_PID")
        if [ ${#AFTER_PIDS[@]} -eq 0 ]; then
            break
        fi
    else
        if [ ! -d "/proc/$MAIN_PID" ]; then
            break
        fi
        AFTER_PIDS=("$MAIN_PID")
    fi

    # 记录采样结束后的数据
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    CPU_TOTAL_AFTER=$(grep '^cpu ' /proc/stat | awk '{print $2+$3+$4+$5+$6+$7+$8+$9+$10}')
    read -r PROC_COUNT_AFTER UTIME_AFTER STIME_AFTER RSS IO_AFTER_READ IO_AFTER_WRITE <<< "$(collect_totals "${AFTER_PIDS[@]}")"

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
    DATA_LINE="$TIMESTAMP,$CPU_USAGE,$RSS,$((UTIME_AFTER + STIME_AFTER)),$CPU_TIME_SEC,$IO_READ_KB,$IO_WRITE_KB,$IO_AFTER_READ,$IO_AFTER_WRITE,$PROC_COUNT_AFTER"
    echo "$DATA_LINE" >> "$LOGFILE"
    
    # 可选：在屏幕上实时打印缩略信息
    if [ "$MODE" = "pid-tree" ]; then
        echo "[$(date +%T)] CPU: $CPU_USAGE% | RSS: ${RSS}KB | IO Read: ${IO_READ_KB}KB | Proc: $PROC_COUNT_AFTER"
    else
        echo "[$(date +%T)] CPU: $CPU_USAGE% | RSS: ${RSS}KB | IO Read: ${IO_READ_KB}KB"
    fi
done

echo -e "\n进程已结束。监控日志已保存至: $LOGFILE"