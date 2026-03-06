# 嵌入式 vs Server 模式

Talon 支持两种运行模式，功能完全对等。

## 对比

| 特性 | 嵌入式模式 | Server 模式 |
|------|-----------|-------------|
| 部署方式 | 作为 Rust 库链接 | 独立二进制进程 |
| 访问协议 | 直接函数调用 | HTTP / TCP / Redis |
| 延迟 | 零网络开销 | 网络往返 |
| 多语言支持 | Rust / C (FFI) | 任何语言 |
| 并发模型 | 进程内多线程 | 多客户端连接 |
| 适用场景 | 移动端、桌面、CLI、嵌入式设备 | Web 后端、微服务、多客户端 |

## 嵌入式模式

```rust
use talon::Talon;

let db = Talon::open("./data")?;
db.run_sql("SELECT 1 + 1")?;
db.kv()?.set(b"key", b"value", None)?;
```

**优势：**
- 零网络延迟
- 零外部依赖
- 单二进制分发
- 适合资源受限环境

## Server 模式

```bash
talon-server --data ./data --http-port 8080
```

```bash
# 任何语言通过 HTTP 访问
curl http://localhost:8080/api/sql -d '{"sql":"SELECT 1+1"}'
```

**优势：**
- 多客户端并发访问
- 多语言支持（Python、Node.js、Go 等）
- 支持 Primary-Replica 集群
- Redis 协议兼容

## 功能对等性

所有 9 大引擎在两种模式下功能完全对等：

```
嵌入式：db.kv()?.set(key, value, ttl)
Server：POST /api/kv/set {"key":"...", "value":"...", "ttl":3600}
Server：redis-cli SET key value EX 3600
```

## 集群模式

Server 模式支持 Primary-Replica 集群：

```rust
use talon::{Talon, StorageConfig, ClusterConfig, ClusterRole};

let db = Talon::open_with_cluster(
    "./data",
    StorageConfig::default(),
    ClusterConfig {
        role: ClusterRole::Primary,
        ..Default::default()
    },
)?;
```

- **Primary** — 读写节点
- **Replica** — 只读节点，通过 OpLog 同步
- **故障转移** — `db.promote()` 将 Replica 提升为 Primary
