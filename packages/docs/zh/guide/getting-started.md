# 快速开始

## 添加依赖

在 `Cargo.toml` 中添加 Talon：

```toml
[dependencies]
talon = { git = "https://github.com/darkmice/talon-bin.git", tag = "v0.1.10", package = "talon-sys" }
```

## 嵌入式模式

```rust
use talon::Talon;

fn main() -> Result<(), talon::Error> {
    // 打开（或创建）数据库
    let db = Talon::open("./my-data")?;

    // SQL 引擎
    db.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    db.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    let rows = db.run_sql("SELECT * FROM users WHERE age > 25")?;
    println!("查询结果: {:?}", rows);

    // KV 引擎
    db.kv()?.set(b"hello", b"world", None)?;
    let val = db.kv_read()?.get(b"hello")?;
    println!("KV 值: {:?}", val);

    // 向量引擎
    db.run_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, emb VECTOR(384))")?;
    db.run_sql("CREATE VECTOR INDEX idx ON docs(emb) USING HNSW")?;

    // 全文搜索引擎
    db.fts()?.index_doc("articles", &FtsDoc {
        id: "doc1".into(),
        fields: vec![("content".into(), "Talon 是 AI 原生数据库".into())],
    })?;

    // AI 引擎
    let ai = db.ai()?;
    ai.create_session("chat-001", Default::default(), None)?;

    Ok(())
}
```

## 服务端模式

启动 Talon Server：

```bash
talon-server --data ./my-data --http-port 8080 --tcp-port 9090 --redis-port 6380
```

### HTTP API 调用

```bash
# SQL 查询
curl -X POST http://localhost:8080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM users"}'

# KV 操作
curl -X POST http://localhost:8080/api/kv/set \
  -d '{"key": "hello", "value": "world"}'

# 向量搜索
curl -X POST http://localhost:8080/api/vector/search \
  -d '{"index": "idx", "vector": [0.1, 0.2, ...], "k": 10}'
```

### Redis 客户端

```bash
redis-cli -p 6380
127.0.0.1:6380> SET user:1 '{"name":"Alice"}'
OK
127.0.0.1:6380> GET user:1
"{\"name\":\"Alice\"}"
```

## 九大引擎概览

| 引擎 | 获取方式 | 用途 |
|------|----------|------|
| [SQL](/zh/engines/sql) | `db.run_sql()` | 关系型查询 |
| [KV](/zh/engines/kv) | `db.kv()? / db.kv_read()?` | 键值缓存 |
| [时序](/zh/engines/timeseries) | `db.create_timeseries()` | 时间序列 |
| [消息队列](/zh/engines/message-queue) | `db.mq()?` | 消息发布/消费 |
| [向量](/zh/engines/vector) | `db.vector(name)?` | ANN 近似搜索 |
| [全文搜索](/zh/engines/full-text-search) | `db.fts()?` | BM25 搜索 |
| [GEO](/zh/engines/geo) | `db.geo()?` | 地理空间 |
| [图](/zh/engines/graph) | `db.graph()?` | 图遍历/分析 |
| [AI](/zh/engines/ai) | `db.ai()?` | 会话/记忆/RAG |

## 下一步

- [安装指南](/zh/guide/installation) — 安装与配置
- [嵌入式 vs Server 模式](/zh/guide/embedded-vs-server) — 选择合适的模式
- [HTTP API](/zh/server/http-api) — 完整 HTTP 端点参考
