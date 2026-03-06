# Getting Started

Talon is an AI-Native Multi-Model Data Engine that unifies 9 data engines into a single binary.

## Installation

### As a Rust Dependency

```toml
[dependencies]
talon = { git = "https://github.com/darkmice/talon-bin.git", tag = "v0.1.8", package = "talon-sys" }
```

## Quick Start (Embedded Mode)

```rust
use talon::Talon;

fn main() -> Result<(), talon::Error> {
    // Open or create a database
    let db = Talon::open("./my_data")?;

    // SQL Engine
    db.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.run_sql("INSERT INTO users VALUES (1, 'Alice')")?;
    let rows = db.run_sql("SELECT * FROM users")?;

    // KV Engine
    db.kv()?.set(b"session:abc", b"token_data", Some(3600))?;
    let val = db.kv_read()?.get(b"session:abc")?;

    // TimeSeries Engine
    use talon::TsSchema;
    let schema = TsSchema::new(vec!["cpu".into(), "mem".into()]);
    let ts = db.create_timeseries("metrics", schema)?;

    // MessageQueue Engine
    db.mq()?.create_topic("events", 0)?;
    db.mq()?.publish("events", b"user_login")?;

    // Vector Engine
    db.run_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, emb VECTOR(384))")?;
    db.run_sql("CREATE VECTOR INDEX idx ON docs(emb) USING HNSW")?;

    // Full-Text Search Engine
    db.fts()?.index("articles", "doc1", "Talon is a multi-model database")?;
    let hits = db.fts()?.search("articles", "database", 10)?;

    // GEO Engine
    db.geo()?.create("places")?;
    db.geo()?.geo_add("places", "office", 116.4074, 39.9042)?;

    // Graph Engine
    db.graph()?.add_vertex("social", None, Some("person"), None)?;

    // AI Engine
    db.ai()?.create_session("s1", Default::default(), None)?;

    // Flush to disk
    db.persist()?;
    Ok(())
}
```

## Quick Start (Server Mode)

```bash
# Start the HTTP server
talon-server --port 8080

# SQL via HTTP
curl -X POST http://localhost:8080/api/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT 1 + 1"}'

# KV via Redis protocol
redis-cli -p 6380 SET mykey myvalue
redis-cli -p 6380 GET mykey
```

## Next Steps

- [Engine APIs](/engines/sql) — Detailed API reference for all 9 engines
- [Server API](/server/http-api) — HTTP REST API documentation
- [AI Docs](/ai/overview) — AI-consumable documentation for LLM integration
