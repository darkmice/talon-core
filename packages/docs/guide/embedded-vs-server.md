# Embedded vs Server Mode

Talon supports two deployment modes to fit different use cases.

## Embedded Mode

Directly link Talon as a Rust library. Zero network overhead, zero serialization cost.

```rust
use talon::Talon;

let db = Talon::open("./data")?;
db.run_sql("SELECT 1")?;
```

**Best for:**
- Desktop applications (Tauri, Electron with native bindings)
- CLI tools
- Edge/IoT devices
- Single-process applications
- Maximum performance (no network overhead)

## Server Mode

Run Talon as a standalone server exposing HTTP, TCP, and Redis protocol interfaces.

```bash
./talon-server --http-port 8080 --tcp-port 9090 --redis-port 6380
```

**Protocols:**

| Protocol | Port | Use Case |
|----------|------|----------|
| HTTP REST | 8080 | Web apps, microservices, any language |
| TCP Binary | 9090 | High-throughput Rust/C clients |
| Redis RESP | 6380 | KV operations via any Redis client |

**Best for:**
- Multi-language applications (Python, Node.js, Go, etc.)
- Microservice architectures
- Shared database across processes
- Remote access

## Feature Parity

Both modes expose identical functionality. Every engine API available in embedded mode has a corresponding HTTP endpoint in server mode.

| Feature | Embedded | Server |
|---------|----------|--------|
| SQL Engine | `db.run_sql()` | `POST /api/sql` |
| KV Engine | `db.kv()?.set()` | `POST /api/kv/set` or Redis `SET` |
| Vector Search | `db.vector()?.search()` | `POST /api/vector/search` |
| FTS | `db.fts()?.search()` | `POST /api/fts/search` |
| AI Engine | `db.ai()?.create_session()` | `POST /api/ai/session` |
| Cluster | `db.cluster_status()` | `GET /cluster/status` |
