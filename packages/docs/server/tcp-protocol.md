# TCP Protocol

Talon's binary TCP protocol for high-throughput Rust/C clients.

## Overview

The TCP protocol provides a compact binary framing protocol for maximum throughput. It is primarily used by the Rust and C SDKs.

## Connection

```
tcp://localhost:9090
```

## Frame Format

```
[4 bytes: payload length (big-endian u32)]
[N bytes: payload (MessagePack encoded)]
```

## Operations

All operations are request-response. The request contains:

```json
{
  "op": "sql",
  "sql": "SELECT 1 + 1"
}
```

Supported `op` values: `sql`, `kv_set`, `kv_get`, `kv_del`, `fts_search`, `vector_search`, `geo_search`, `graph_query`, `ai_session`, etc.

## Usage with Rust SDK

```rust
use talon::TcpClient;

let client = TcpClient::connect("127.0.0.1:9090")?;
let rows = client.run_sql("SELECT * FROM users")?;
```
