#!/bin/bash
# analyze_go.sh

set -e

APP_NAME="workload"
MAIN_FILE="replayWorkload.go"
RESULT_DIR="./vtune_results_$(date +%Y%m%d_%H%M%S)"
DURATION=30

echo "=== Go 程序 VTune 分析 ==="
echo

# 1. 编译
echo "1. 编译 Go 程序（禁用优化和内联）..."
go build -gcflags "-N -l" -o "$APP_NAME" "$MAIN_FILE"

if [ ! -f "$APP_NAME" ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功: $(ls -lh $APP_NAME)"
echo

# 2. 创建结果目录
mkdir -p "$RESULT_DIR"
echo "2. 结果目录: $RESULT_DIR"
echo

# 3. 运行 Hotspots 分析
echo "3. 运行 Hotspots 分析..."
vtune -collect hotspots \
  -result-dir "$RESULT_DIR/hotspots" \
  -duration $DURATION \
  -knob sampling-mode=hw \
  -knob enable-stack-collection=true \
  -knob stack-size=0.5 \
  -- "./$APP_NAME" 2>&1 | tee "$RESULT_DIR/hotspots.log"

echo "✅ Hotspots 分析完成"
echo

# 4. 运行 Memory Access 分析
echo "4. 运行 Memory Access 分析..."
vtune -collect memory-access \
  -result-dir "$RESULT_DIR/memory" \
  -duration $DURATION \
  -knob analyze-mem-objects=true \
  -- "./$APP_NAME" 2>&1 | tee "$RESULT_DIR/memory.log"

echo "✅ Memory Access 分析完成"
echo

# 5. 生成报告
echo "5. 生成分析报告..."
vtune -report summary -result-dir "$RESULT_DIR/hotspots" -format text > "$RESULT_DIR/summary.txt"
vtune -report hotspots -result-dir "$RESULT_DIR/hotspots" -format text -group-by function > "$RESULT_DIR/hotspots_by_function.txt"
vtune -report callstacks -result-dir "$RESULT_DIR/hotspots" -format text > "$RESULT_DIR/callstacks.txt"

# 6. 提取 Go 特定的信息
echo "6. 提取 Go 运行时信息..."
# 查找 runtime 相关的函数
grep -i "runtime\\." "$RESULT_DIR/hotspots_by_function.txt" | head -20 > "$RESULT_DIR/go_runtime.txt"
grep -i "gc\\|malloc\\|heap" "$RESULT_DIR/hotspots_by_function.txt" | head -20 > "$RESULT_DIR/go_gc.txt"

echo "✅ 分析完成！"
echo
echo "=== 报告文件 ==="
echo "总结报告: cat $RESULT_DIR/summary.txt"
echo "热点函数: cat $RESULT_DIR/hotspots_by_function.txt"
echo "调用栈: cat $RESULT_DIR/callstacks.txt"
echo "Go 运行时: cat $RESULT_DIR/go_runtime.txt"
echo "GC 相关: cat $RESULT_DIR/go_gc.txt"
echo
echo "要查看详细 GUI 报告:"
echo "vtune-gui $RESULT_DIR/hotspots"