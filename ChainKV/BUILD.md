# ChainKV 编译指南

## ✅ 编译步骤

### 1. 初始化项目（已完成）

```bash
cd ./ChainKV

# 已创建 go.mod
# 已创建 cache 符号链接（指向 cache-sgc）
# 已配置 Go 环境变量
```

### 2. 编译 Workload 工具

```bash
cd workload
go build -o replayWorkload replayWorkload.go
```

### 3. 运行测试

```bash
# 快速测试
./replayWorkload -db /tmp/test -bench 100

# 使用 State 模式
./replayWorkload -db /tmp/test -state -bench 100

# 加载数据
./replayWorkload -db /tmp/test -load test_data.txt
```

## 📦 项目结构说明

```
ChainKV/
├── go.mod                    # Go 模块文件（已配置）
├── go.sum                    # 依赖校验文件
├── goleveldb/                # LevelDB 实现
│   └── leveldb/
│       ├── cache -> cache-sgc  # 符号链接（SGC 缓存策略）
│       ├── cache-lru/          # LRU 缓存实现
│       ├── cache-sgc/          # SGC 缓存实现 ✓
│       └── ethdb/              # 以太坊数据库接口
├── workload/                 # 工作负载工具
│   ├── replayWorkload        # 可执行文件（编译产物）
│   ├── replayWorkload.go     # 源代码
│   ├── replay_config.json    # 配置文件
│   ├── test_data.txt         # 测试数据
│   └── README.md             # 使用文档
└── README.md                 # 项目说明
```

## 🔧 已完成的配置

1. ✅ **创建 go.mod**：在项目根目录
2. ✅ **创建 cache 符号链接**：`goleveldb/leveldb/cache -> cache-sgc`
3. ✅ **配置 Go 环境**：
   ```bash
   go env -w GOPRIVATE=theo.local
   go env -w GONOSUMDB=theo.local
   ```
4. ✅ **完善 replayWorkload.go**：添加完整的 main 函数和功能
5. ✅ **编译成功**：生成可执行文件
6. ✅ **测试通过**：基准测试、State 模式、数据加载均正常

## 🚀 使用示例

### 示例 1: 性能基准测试

```bash
./replayWorkload -db /tmp/benchmark -cache 512 -bench 10000
```

**输出示例：**
```
ChainKV Workload Replay Tool
=============================
Database path: /tmp/benchmark
Cache size: 512 MB
File handles: 128
State mode: false

Opening ChainKV database...
Database opened successfully!

=== Running Benchmark (10000 operations) ===
PUT: 10000 ops in 40ms (250000 ops/sec)
GET: 10000 ops in 3ms (3333333 ops/sec, 10000 hits)

Done!
```

### 示例 2: State 数据测试

```bash
./replayWorkload -db /tmp/statedb -state -bench 5000
```

### 示例 3: 批量数据加载

```bash
./replayWorkload -db /tmp/datadb -load mydata.txt -limit 100000
```

## 📊 性能特点

根据测试结果：
- **PUT 性能**：~250,000 ops/sec
- **GET 性能**：~3,000,000 ops/sec
- **命中率**：100%（缓存命中）

## 🛠 其他编译选项

### 编译整个项目的测试

```bash
# 编译 goleveldb 测试
cd goleveldb/leveldb
go test -c

# 运行特定测试
go test -v -run TestDB
```

### 编译优化版本

```bash
# 带优化的编译
go build -ldflags="-s -w" -o replayWorkload replayWorkload.go

# 静态编译（如需部署到其他机器）
CGO_ENABLED=0 go build -o replayWorkload replayWorkload.go
```

## ⚠️ 注意事项
   
1. **本地模块**：项目使用本地路径，不能推送到 GitHub 作为可下载模块
   - 如需发布，需要调整导入路径

2. **缓存选择**：默认使用 SGC（Space Game Cache）策略
   - 如需使用 LRU，修改符号链接：`ln -sf cache-lru cache`

3. **状态分离**：ChainKV 的核心特性
   - 状态数据：使用 `-state` 标志和 Put_s/Get_s
   - 非状态数据：默认模式，使用 Put/Get

## 📚 参考文档

- [项目 README](../README.md)
- [Workload 工具文档](workload/README.md)
- [ChainKV 论文（SIGMOD 2024）](https://dl.acm.org/doi/10.1145/3588908)

## 🐛 故障排除

### 编译错误：找不到 cache 包
```bash
cd goleveldb/leveldb
ln -sf cache-sgc cache
```

### 运行时错误：无法打开数据库
```bash
# 确保目录存在且有写权限
mkdir -p /tmp/testdb
chmod 755 /tmp/testdb
```

### 性能不佳
```bash
# 增加缓存和文件句柄
./replayWorkload -db /path/to/db -cache 2048 -handles 512 -bench 10000
```

---

**编译完成！** 🎉

现在你可以使用 ChainKV 进行性能测试和工作负载重放了。
