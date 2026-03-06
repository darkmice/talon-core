# TCP 协议

Talon 的二进制 TCP 协议，适用于高吞吐量的 Rust/C 客户端。

## 概述

TCP 协议提供紧凑的二进制帧协议以实现最大吞吐量。主要由 Rust 和 C SDK 使用。

## 连接

```
tcp://localhost:9090
```

## 帧格式

```
[4 字节: 净荷长度 (big-endian u32)]
[N 字节: 净荷 (MessagePack 编码)]
```

## 操作

所有操作都是请求-响应模式。请求包含：

```json
{
  "op": "sql",
  "sql": "SELECT 1 + 1"
}
```

支持的 `op` 值：`sql`、`kv_set`、`kv_get`、`kv_del`、`fts_search`、`vector_search`、`geo_search`、`graph_query`、`ai_session` 等。

## Rust SDK 使用

```rust
use talon::TcpClient;

let client = TcpClient::connect("127.0.0.1:9090")?;
let rows = client.run_sql("SELECT * FROM users")?;
```
