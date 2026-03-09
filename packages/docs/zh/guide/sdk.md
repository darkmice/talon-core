# SDK 概览

Talon 提供 **5 种语言** 的官方 SDK，全部实现 10 个引擎模块的 100% API 覆盖。

所有 SDK 通过统一的 `talon_execute` C ABI 与 Talon 引擎通信 — 相同的 JSON 命令协议，跨语言行为一致。

## 支持语言

| 语言 | 安装方式 | 绑定方式 | Native Lib |
|------|---------|---------|------------|
| [Go](/zh/guide/sdk-go) | `go get` | cgo（静态链接） | 编译进二进制 |
| [Python](/zh/guide/sdk-python) | `pip install talon-db` | ctypes | 首次使用时自动下载 |
| [Node.js](/zh/guide/sdk-nodejs) | `npm install talon-db` | koffi | 安装时自动下载 |
| [Java](/zh/guide/sdk-java) | 源码构建 | JNA | 从 `lib/` 目录加载 |
| [.NET](/zh/guide/sdk-dotnet) | `dotnet add package TalonDb` | P/Invoke | NuGet 包内置 |

## 模块覆盖

全部 SDK 覆盖完整的 10 个引擎模块：

| 模块 | 方法数 | 说明 |
|------|--------|------|
| SQL | 1 | 关系型查询 |
| KV | 17 | 键值存储（TTL / 原子操作 / 分页扫描） |
| Vector | 7 | HNSW 向量索引与搜索 |
| TS | 7 | 时序引擎 |
| MQ | 9 | 消息队列 |
| AI | 30+ | Session / Context / Memory / RAG / Agent / Trace / LLM 配置 / 自动 Embedding |
| FTS | 16 | 全文搜索（BM25 + 模糊 + 混合搜索） |
| Geo | 10 | 地理空间（半径 / 矩形 / 围栏） |
| Graph | 19 | 属性图（CRUD + BFS + 最短路径 + PageRank） |
| Cluster + Ops | 10 | 集群管理 / 统计 / 备份 |

## 预编译库

`talon-sdk` 的 `lib/` 目录包含 4 个平台的预编译库，由 CI 自动构建推送：

| 系统 | 架构 | 静态库 | 动态库 |
|------|------|--------|--------|
| macOS | arm64 (Apple Silicon) | `.a` ✅ | `.dylib` ✅ |
| macOS | amd64 (Intel) | `.a` ✅ | `.dylib` ✅ |
| Linux | amd64（海光/兆芯） | `.a` ✅ | `.so` ✅ |
| Linux | arm64（鲲鹏/飞腾） | `.a` ✅ | `.so` ✅ |
| Windows | amd64 | `.lib` ✅ | `.dll` ✅ |
| Windows | arm64 | `.lib` ✅ | `.dll` ✅ |
| Linux | loongarch64（龙芯） | `.a` ✅ | `.so` ✅ |
| Linux | riscv64 | `.a` ✅ | `.so` ✅ |

### 信创平台支持

| 芯片 | 架构 | 目录 |
|------|------|------|
| 鲲鹏 (Kunpeng) / 飞腾 (Phytium) | arm64 | `linux_arm64` |
| 海光 (Hygon) / 兆芯 (Zhaoxin) | x86_64 | `linux_amd64` |
| 龙芯 (Loongson) | loongarch64 | `linux_loongarch64` |
| RISC-V | riscv64 | `linux_riscv64` |

### 库查找优先级

1. `TALON_LIB_PATH` 环境变量（直接指定文件路径）
2. `talon-sdk/lib/{platform}/` 内嵌库（自动检测平台）
3. 系统搜索路径

## 架构

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Go SDK     │  │ Python SDK   │  │ Node.js SDK  │
│   (cgo)      │  │  (ctypes)    │  │  (koffi)     │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       ▼                 ▼                 ▼
  ┌─────────────────────────────────────────────┐
  │          talon_execute(db, json_cmd)         │
  │              C ABI (FFI 层)                  │
  └──────────────────┬──────────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────────┐
  │              Talon 引擎 (Rust)               │
  │  SQL │ KV │ TS │ MQ │ Vec │ FTS │ Geo │ Graph │
  └─────────────────────────────────────────────┘
```
