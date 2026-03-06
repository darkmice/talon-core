<div align="center">

# Talon

**AI-Native Multi-Model Data Engine**

SQL + KV + TimeSeries + MessageQueue + Vector + Full-Text Search + GEO + Graph + AI
— all in a single binary with zero external dependencies.

[![Rust](https://img.shields.io/badge/Rust-2021-orange)](https://www.rust-lang.org/)
[![License: SSPL/Commercial](https://img.shields.io/badge/license-Dual%20License-blue)](LICENSE)
[![Test](https://img.shields.io/badge/tests-1362%2B-green)]()

[English](#why-talon) · [中文文档](README_zh.md)

</div>

---

## Why Talon?

Building AI applications typically requires stitching together **5+ infrastructure components**:

| Need | Traditional Solution |
|------|---------------------|
| Structured data | SQLite / PostgreSQL |
| Cache & sessions | Redis |
| Vector search | Qdrant / Milvus |
| Time-series logs | InfluxDB / TimescaleDB |
| Message queue | RabbitMQ / Kafka |
| Full-text search | Elasticsearch |
| Geospatial | PostGIS |
| Knowledge graph | Neo4j |
| AI primitives | Custom code + multiple DBs |

**Talon unifies all nine capabilities into a single engine:**

- **Single Binary** — one executable, zero external dependencies, embed or run as a server (data stored as a directory via LSM-Tree)
- **AI-Native** — built-in Session, Context, Memory, Trace, RAG, and Agent abstractions
- **High Performance** — PK lookup 0.007ms, KV 744K ops/s, MQ 1.6M msg/s, vector KNN P95=0.1ms
- **Multi-Language** — Rust, C/C++, Python, Node.js, Go, Java, .NET via FFI
- **Redis Compatible** — drop-in RESP protocol support, use `redis-cli` directly
- **Elasticsearch Compatible** — `_bulk` NDJSON ingest API

## Table of Contents

- [Quick Start](#quick-start)
- [Nine Data Engines](#nine-data-engines)
- [SQL Engine](#sql-engine)
- [KV Engine](#kv-engine)
- [TimeSeries Engine](#timeseries-engine)
- [Message Queue Engine](#message-queue-engine)
- [Vector Engine](#vector-engine)
- [Full-Text Search Engine](#full-text-search-engine)
- [GEO Engine](#geo-engine)
- [Graph Engine](#graph-engine)
- [AI Engine](#ai-engine)
- [Cross-Engine Queries](#cross-engine-queries)
- [Data Import](#data-import)
- [Server Mode](#server-mode)
- [CLI Client](#cli-client)
- [Multi-Language SDKs](#multi-language-sdks)
- [Cluster & Replication](#cluster--replication)
- [Diagnostics & Monitoring](#diagnostics--monitoring)
- [Performance Benchmarks](#performance-benchmarks)
- [Architecture](#architecture)
- [Building from Source](#building-from-source)
- [Feature Boundaries & Large-Scale Strategy](#feature-boundaries--large-scale-strategy)
- [License](#license)

## Quick Start

### Embedded Mode (Rust)

```rust
use talon::Talon;

let db = Talon::open("./my_data")?;

// SQL — relational queries
db.run_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;
db.run_sql("INSERT INTO users VALUES (1, 'Alice')")?;
let rows = db.run_sql("SELECT * FROM users WHERE id = 1")?;

// KV — Redis-style key-value with TTL
let kv = db.kv()?;
kv.set(b"session:abc", b"token_data", Some(3600))?;
let val = kv.get(b"session:abc")?;

// Vector — HNSW approximate nearest neighbor search
db.run_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, emb VECTOR(128))")?;
db.run_sql("CREATE VECTOR INDEX idx_emb ON docs(emb) USING HNSW WITH (metric='cosine')")?;

// TimeSeries — time-stamped metrics
let ts = db.create_timeseries("metrics", schema)?;
ts.insert(&point)?;

// Message Queue — persistent pub/sub
let mq = db.mq()?;
mq.create_topic("events", 0)?;
mq.publish("events", b"hello")?;

// Full-Text Search — BM25 scoring
let fts = db.fts()?;
fts.create_index("articles", &FtsConfig::default())?;
fts.index_doc("articles", &doc)?;
let hits = fts.search("articles", "rust database", 10)?;

// GEO — geospatial queries
let geo = db.geo()?;
geo.create("places")?;
geo.geo_add("places", "cafe", 116.397, 39.908)?;
let nearby = geo.geo_search("places", 116.4, 39.9, 1000.0, GeoUnit::Meters, Some(10))?;

// Graph — property graph with traversals
let graph = db.graph()?;
graph.create("social")?;
let alice = graph.add_vertex("social", "person", &props)?;

// AI — session, memory, RAG
let ai = db.ai()?;
ai.create_session("s1", Default::default(), None)?;
ai.append_message("s1", &ContextMessage { role: "user".into(), content: "Hello".into(), ..Default::default() })?;

db.persist()?;
```

### Server Mode

```bash
# Start server
talon --data ./my_data --addr 0.0.0.0:7720 --token my_secret

# HTTP API
curl -X POST http://localhost:7720/api/sql \
  -H "Authorization: Bearer my_secret" \
  -d '{"action":"query","params":{"sql":"SELECT * FROM users"}}'
```

### CLI Client

```bash
talon-cli "talon://:my_secret@localhost:7720"
talon> SELECT * FROM users;
talon> :kv get session:abc
talon> :stats
talon> :help
```

## Nine Data Engines

| Engine | Use Case | Replaces |
|--------|----------|----------|
| **SQL** | Structured data, metadata, config | SQLite, PostgreSQL |
| **KV** | Sessions, cache, prompt templates | Redis |
| **TimeSeries** | Token usage, chat logs, metrics | InfluxDB |
| **Message Queue** | Agent communication, task scheduling | RabbitMQ |
| **Vector** | Embeddings, semantic search, RAG | Qdrant, Milvus |
| **Full-Text Search** | Document search, BM25 ranking | Elasticsearch |
| **GEO** | Location-based queries, geofencing | PostGIS, Redis GEO |
| **Graph** | Knowledge graphs, relationships | Neo4j |
| **AI** | Session, Memory, RAG, Agent, Intent | Custom code + multiple DBs |

## SQL Engine

Full relational database with rich SQL dialect:

```sql
-- DDL
CREATE TABLE docs (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT,
    metadata JSONB,
    embedding VECTOR(1536),
    location GEOPOINT,
    created_at TIMESTAMP DEFAULT NOW()
);
ALTER TABLE docs ADD COLUMN category TEXT DEFAULT 'general';
ALTER TABLE docs DROP COLUMN category;
ALTER TABLE docs RENAME COLUMN content TO body;
CREATE INDEX idx_title ON docs(title);
DROP INDEX idx_title;

-- DML
INSERT INTO docs (id, title) VALUES (1, 'Hello');
INSERT OR REPLACE INTO docs (id, title) VALUES (1, 'Updated');         -- UPSERT
INSERT INTO docs (id, title) ON CONFLICT DO UPDATE SET title = 'New';  -- ON CONFLICT
UPDATE docs SET title = 'World' WHERE id = 1;
UPDATE docs SET views = views + 1 WHERE id = 1;                        -- Arithmetic
DELETE FROM docs WHERE id = 1;
TRUNCATE TABLE docs;

-- Queries
SELECT id, title FROM docs WHERE title LIKE '%hello%' ORDER BY id DESC LIMIT 10 OFFSET 5;
SELECT DISTINCT category FROM docs;
SELECT * FROM docs WHERE id IN (1, 2, 3);
SELECT * FROM docs WHERE id BETWEEN 10 AND 20;
SELECT * FROM docs WHERE title IS NOT NULL;

-- Aggregates (O(1) via column stats)
SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM docs;

-- JOINs (chained multi-table)
SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id;
SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id;
SELECT a.*, b.name, c.total FROM t1 a JOIN t2 b ON a.id = b.aid JOIN t3 c ON b.id = c.bid;

-- GROUP BY / HAVING
SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category HAVING COUNT(*) > 5;

-- Subqueries
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);

-- UNION
SELECT name FROM customers UNION SELECT name FROM suppliers;

-- FULL OUTER JOIN
SELECT a.*, b.* FROM t1 a FULL OUTER JOIN t2 b ON a.id = b.id;

-- Expression columns
SELECT id, price * quantity AS total, a + b AS sum FROM orders;

-- Window functions
SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users;
SELECT id, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees;
SELECT id, LAG(salary, 1) OVER (ORDER BY id) AS prev_salary FROM employees;
SELECT id, SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) AS running_total FROM orders;

-- DISTINCT ON (PostgreSQL compatible)
SELECT DISTINCT ON (dept) id, dept, salary FROM employees ORDER BY dept, salary DESC;

-- Advanced aggregates
SELECT ARRAY_AGG(name) FROM users GROUP BY dept;
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;
SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY response_ms) FROM metrics;

-- INSERT / UPDATE RETURNING
INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING *;
UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING id, name;

-- Multi-table UPDATE (MySQL compatible)
UPDATE t1 JOIN t2 ON t1.id = t2.t1_id SET t1.status = 'done' WHERE t2.flag = 1;

-- Multi-table DELETE (MySQL + PostgreSQL)
DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t2.expired = 1;
DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id AND t2.expired = 1;

-- ALTER TABLE enhancements
ALTER TABLE docs ADD CONSTRAINT uq_title UNIQUE (title);
ALTER TABLE docs ALTER COLUMN content TYPE TEXT;
ALTER TABLE docs ALTER COLUMN status SET DEFAULT 'active';
ALTER TABLE docs ALTER COLUMN status DROP DEFAULT;
COMMENT ON TABLE docs IS 'Document storage';
COMMENT ON COLUMN docs.title IS 'Document title';

-- PostgreSQL $1/$2 parameter placeholders (auto-converted to ?)
-- REPLACE INTO (MySQL compatible)

-- Transactions
BEGIN;
INSERT INTO docs VALUES (2, 'tx1');
INSERT INTO docs VALUES (3, 'tx2');
COMMIT;  -- or ROLLBACK

-- Vector search in SQL
SELECT id, vec_cosine(embedding, [0.1, 0.2, ...]) AS score
FROM docs ORDER BY score DESC LIMIT 10;

-- Geospatial in SQL
SELECT *, ST_DISTANCE(location, GEOPOINT(39.9, 116.4)) AS dist
FROM docs WHERE ST_WITHIN(location, GEOPOINT(39.9, 116.4), 1000) ORDER BY dist;

-- Parameterized queries
db.run_sql_param("SELECT * FROM docs WHERE id = ?", &[Value::Integer(1)])?;

-- Batch operations (skip SQL parsing, direct encode + WriteBatch)
db.batch_insert_rows("docs", &["id", "title"], rows)?;

-- Query plan
EXPLAIN SELECT * FROM docs WHERE id = 1;

-- Metadata
SHOW TABLES;
```

**Supported Types**: `INTEGER` · `FLOAT` · `TEXT` · `BLOB` · `BOOLEAN` · `JSONB` · `VECTOR(dim)` · `TIMESTAMP` · `GEOPOINT`

## KV Engine

Redis-compatible key-value store with TTL and background expiration:

```rust
let kv = db.kv()?;

kv.set(b"key", b"value", Some(3600))?;     // SET with TTL (seconds)
let val = kv.get(b"key")?;                  // GET
kv.del(b"key")?;                            // DEL
kv.exists(b"key")?;                         // EXISTS

kv.mset(&[(b"k1", b"v1"), (b"k2", b"v2")])?;  // MSET
let vals = kv.mget(&[b"k1", b"k2"])?;          // MGET

kv.incr(b"counter")?;                       // INCR
kv.incrby(b"counter", 10)?;                 // INCRBY
kv.decrby(b"counter", 5)?;                  // DECRBY
kv.setnx(b"lock", b"1")?;                   // SETNX

kv.expire(b"key", 60)?;                     // EXPIRE
let ttl = kv.ttl(b"key")?;                  // TTL

let keys = kv.keys_prefix(b"session:")?;    // KEYS prefix scan
let keys = kv.keys_match("user:*")?;        // KEYS glob pattern
```

**Redis RESP Protocol**: Talon includes a built-in Redis-compatible server. Connect with `redis-cli`:

```bash
redis-cli -h 127.0.0.1 -p 6380
127.0.0.1:6380> SET mykey "hello"
127.0.0.1:6380> GET mykey
127.0.0.1:6380> INCR counter
```

Supported commands: `GET` · `SET` · `DEL` · `MGET` · `MSET` · `EXISTS` · `EXPIRE` · `TTL` · `KEYS` · `INCR` · `DECR` · `PING` · `INFO`

## TimeSeries Engine

Time-series storage with tag-based filtering and aggregation:

```rust
use talon::{TsEngine, TsSchema, TsQuery, TsAggQuery, AggFunc, DataPoint};

// Create with schema
let schema = TsSchema {
    tags: vec!["host".into(), "region".into()],
    fields: vec!["cpu".into(), "mem".into()],
    retention_ms: Some(86400 * 30 * 1000), // 30-day retention
};
let ts = db.create_timeseries("metrics", schema)?;

// Insert data points
let point = DataPoint {
    timestamp_ms: 1700000000000,
    tags: btreemap!{"host" => "srv1", "region" => "us-east"},
    fields: btreemap!{"cpu" => 85.5, "mem" => 72.3},
};
ts.insert(&point)?;

// Query with time range + tag filter
let query = TsQuery {
    start_ms: Some(1700000000000),
    end_ms: Some(1700100000000),
    tag_filters: vec![("host".into(), "srv1".into())],
    order_asc: true,
    limit: Some(100),
};
let results = ts.query(&query)?;

// Aggregation (SUM, AVG, COUNT, MIN, MAX with interval grouping)
let agg = TsAggQuery {
    field: "cpu".into(),
    func: AggFunc::Avg,
    interval_ms: Some(3600_000), // 1-hour buckets
    ..Default::default()
};
let buckets = ts.aggregate(&agg)?;
```

**InfluxDB Line Protocol** ingest is supported:

```rust
use talon::parse_line_protocol;
let points = parse_line_protocol("cpu,host=srv1 value=85.5 1700000000000000000")?;
```

## Message Queue Engine

Persistent pub/sub with consumer groups and at-least-once delivery:

```rust
let mq = db.mq()?;

mq.create_topic("events", 0)?;                     // Create topic (0 = unlimited)
mq.publish("events", b"hello world")?;              // Publish message
mq.subscribe("events", "group1")?;                  // Subscribe consumer group

let msgs = mq.poll("events", "group1", "c1", 10)?;  // Poll messages
mq.ack("events", "group1", "c1", msg_id)?;          // Acknowledge

let len = mq.len("events")?;                        // Queue length
let topics = mq.list_topics()?;                      // List all topics
mq.drop_topic("events")?;                           // Drop topic
```

## Vector Engine

HNSW-based approximate nearest neighbor search:

```rust
use talon::VectorEngine;

// Create via SQL
db.run_sql("CREATE VECTOR INDEX my_idx ON docs(emb) USING HNSW WITH (
    metric='cosine', m=16, ef_construction=200, ef_search=100
)")?;

// Or via API
let ve = db.vector("my_idx")?;
ve.insert(1, &embedding_vec)?;
ve.batch_insert(&[(2, &vec2), (3, &vec3)])?;

// KNN search
let results = ve.search(&query_vec, 10)?;  // top-10 nearest neighbors
for hit in &results {
    println!("id={}, distance={}", hit.id, hit.distance);
}

// SQL-integrated vector search
db.run_sql("SELECT id, vec_cosine(emb, [0.1, 0.2, ...]) AS score FROM docs ORDER BY score DESC LIMIT 10")?;
db.run_sql("SELECT id, vec_l2(emb, [...]) AS dist FROM docs ORDER BY dist LIMIT 10")?;
db.run_sql("SELECT id, vec_dot(emb, [...]) AS sim FROM docs ORDER BY sim DESC LIMIT 10")?;

// Recommend API (Qdrant-compatible) — find similar items
let results = ve.recommend(&[&pos_vec1, &pos_vec2], &[&neg_vec], 10)?;

// Discover API (context search) — target + context pairs for reranking
let results = ve.discover(&target_vec, &[(&positive, &negative)], 10)?;

// Metadata filter — search with field-level filtering
use talon::{MetaFilter, MetaFilterOp, MetaValue};
ve.insert_with_metadata(1, &vec, &hashmap!{"category" => MetaValue::String("ai".into())})?;
let results = ve.search_with_filter(&query_vec, 10, &[
    MetaFilter { field: "category".into(), op: MetaFilterOp::Eq(MetaValue::String("ai".into())) },
])?;

// SQ8 scalar quantization (4:1 compression, <2% accuracy loss)
ve.enable_quantization()?;

// Snapshot-consistent search (reads isolated from concurrent writes)
let snap = db.store().snapshot();
let results = ve.snapshot_search(&snap, &query_vec, 10)?;
```

**Distance metrics**: `cosine` · `l2` (Euclidean) · `dot` (Inner Product)
**Advanced**: Recommend · Discover · Metadata Filter · SQ8 Quantization · Snapshot Search

## Full-Text Search Engine

BM25-based full-text search with inverted index, highlighting, and fuzzy matching:

```rust
use talon::{FtsEngine, FtsConfig, FtsDoc, Analyzer};

let fts = db.fts()?;

// Create index
fts.create_index("articles", &FtsConfig { analyzer: Analyzer::Standard })?;

// Index documents
let doc = FtsDoc {
    doc_id: "1".into(),
    fields: btreemap!{
        "title" => "Introduction to Rust",
        "body" => "Rust is a systems programming language...",
    },
};
fts.index_doc("articles", &doc)?;
fts.index_doc_batch("articles", &docs)?;

// BM25 search with highlighting
let hits = fts.search("articles", "rust programming", 10)?;
for hit in &hits {
    println!("doc_id={}, score={:.4}", hit.doc_id, hit.score);
    println!("highlights: {:?}", hit.highlights);  // <em>Rust</em> is a ...
}

// Fuzzy search (edit distance ≤ 2)
let fuzzy = fts.search_fuzzy("articles", "ruts", 2, 10)?;

// Bool Query (Elasticsearch-compatible must/should/must_not)
use talon::BoolQuery;
let hits = fts.search_bool("articles", &BoolQuery {
    must: vec!["rust".into()],
    should: vec!["database".into(), "engine".into()],
    must_not: vec!["deprecated".into()],
}, 10)?;

// Phrase Query (exact phrase matching with position awareness)
let hits = fts.search_phrase("articles", "rust database", 10)?;

// Term Query (exact field-value match, no tokenization)
let hits = fts.search_term("articles", "category", "programming", 10)?;

// Range Query (numeric/string range filtering)
use talon::fts::range::RangeQuery;
let hits = fts.search_range("articles", &RangeQuery {
    field: "price".into(), gte: Some("10".into()), lte: Some("100".into()),
    ..Default::default()
}, 10)?;

// Regexp / Wildcard queries
let hits = fts.search_regexp("articles", r"v\d+\.\d+", 10)?;
let hits = fts.search_wildcard("articles", "rust*", 10)?;

// Multi-field query
use talon::MultiFieldQuery;
let hits = fts.search_multi_field("articles", &MultiFieldQuery {
    query: "rust".into(), fields: vec!["title".into(), "body".into()],
}, 10)?;

// Aggregations (terms, sorted search)
let buckets = fts.aggregate_terms("articles", "category", Some(10))?;
let suggestions = fts.suggest("articles", "rus", 5)?;

// Chinese tokenizer (jieba)
fts.create_index("cn_docs", &FtsConfig { analyzer: Analyzer::Chinese })?;
```

**Index Management** (Elasticsearch-compatible):

```rust
// Index aliases (POST /_aliases)
fts.add_alias("articles_v2", "articles")?;  // articles → articles_v2

// Close / Open index (POST /_close, /_open)
fts.close_index("articles")?;
fts.open_index("articles")?;

// Reindex (rebuild inverted index, e.g. after analyzer change)
fts.reindex("articles")?;

// Get mapping / List indexes
let mapping = fts.get_mapping("articles")?;
let indexes = fts.list_indexes()?;

// Update / Delete by query
fts.update_by_query("articles", "old_tag", &new_fields, 100)?;
fts.delete_by_query("articles", "deprecated", 100)?;
```

**Elasticsearch `_bulk` API**: Ingest data using ES NDJSON format:

```rust
use talon::parse_es_bulk;
let items = parse_es_bulk(r#"
{"index":{"_index":"docs","_id":"1"}}
{"title":"hello","body":"world"}
"#)?;
```

**Hybrid Search** (BM25 + Vector RRF fusion with pre-filtering):

```rust
use talon::hybrid_search;
let hits = hybrid_search(&store, &HybridQuery {
    fts_index: "articles",
    vec_index: "emb_idx",
    query_text: "rust database",
    query_vec: &query_embedding,
    limit: 10,
    pre_filter: Some(vec![("namespace", "tenant_a")]),  // field-level pre-filtering
    ..Default::default()
})?;
```

## GEO Engine

Geohash-based spatial index, compatible with Redis GEO commands:

```rust
let geo = db.geo()?;
geo.create("shops")?;

// GEOADD
geo.geo_add("shops", "cafe_a", 116.397, 39.908)?;
geo.geo_add_batch("shops", &[("cafe_b", 116.405, 39.912), ("cafe_c", 116.389, 39.905)])?;

// GEOPOS
let pos = geo.geo_pos("shops", "cafe_a")?;

// GEODIST
let dist = geo.geo_dist("shops", "cafe_a", "cafe_b", GeoUnit::Meters)?;

// GEOSEARCH (circle)
let nearby = geo.geo_search("shops", 116.4, 39.9, 2.0, GeoUnit::Kilometers, Some(10))?;

// GEOSEARCH (bounding box)
let in_box = geo.geo_search_box("shops", 116.38, 39.90, 116.41, 39.92, Some(20))?;

// Geofencing
let inside = geo.geo_fence("shops", "cafe_a", 116.4, 39.9, 1000.0, GeoUnit::Meters)?;

// GEOSEARCHSTORE (Redis 7.0 compatible — search + store results)
geo.geo_search_store("shops", "nearby_cache", 116.4, 39.9, 2.0, GeoUnit::Kilometers, Some(20))?;
```

## Graph Engine

Property graph with vertices, edges, label indexes, and traversal algorithms:

```rust
use talon::{GraphEngine, Direction};

let graph = db.graph()?;
graph.create("social")?;

// Add vertices
let alice = graph.add_vertex("social", "person", &btreemap!{"name" => "Alice"})?;
let bob = graph.add_vertex("social", "person", &btreemap!{"name" => "Bob"})?;

// Add edges
graph.add_edge("social", alice, bob, "knows", &btreemap!{"since" => "2024"})?;

// Query neighbors
let friends = graph.neighbors("social", alice, Direction::Out)?;

// Label queries
let people = graph.vertices_by_label("social", "person")?;

// BFS traversal
let reachable = graph.bfs("social", alice, 3, Direction::Out)?;

// BFS with property filter (callback controls expansion)
let filtered = graph.bfs_filter("social", alice, 3, Direction::Out, |v| {
    v.label == "person" && v.properties.get("active") == Some(&"true".into())
})?;

// Shortest path (unweighted BFS)
let path = graph.shortest_path("social", alice, bob, 10, Direction::Both)?;

// Weighted shortest path (Dijkstra, edge weight from property)
let wpath = graph.weighted_shortest_path("social", alice, bob, 10, Direction::Out, "cost")?;

// Degree centrality (out-degree, in-degree, total — sorted by total)
let top = graph.degree_centrality("social", 10)?;

// PageRank (configurable damping factor and iterations)
let ranks = graph.pagerank("social", 0.85, 20, 10)?;
```

## AI Engine

Full-featured AI-native engine with first-class abstractions for LLM application development:

```rust
let ai = db.ai()?;

// ── Session Management ──────────────────────────────
ai.create_session("s1", metadata, Some(86400))?;  // with TTL
ai.update_session("s1", &new_metadata)?;
let sessions = ai.list_sessions()?;
ai.delete_session("s1")?;                          // cascade delete context + trace
ai.cleanup_expired_sessions()?;                     // batch cleanup

// Session tags & archive
ai.add_session_tags("s1", &["important".into(), "customer-support".into()])?;
ai.archive_session("s1")?;                          // soft archive
ai.unarchive_session("s1")?;

// Session export & stats
let exported = ai.export_session("s1")?;            // full session snapshot
let stats = ai.session_stats("s1")?;                // message count, token usage

// ── Conversation Context ────────────────────────────
ai.append_message("s1", &ContextMessage {
    role: "user".into(), content: "Hello".into(), token_count: Some(5),
})?;
let history = ai.get_recent_messages("s1", 10)?;
let window = ai.get_context_window("s1", 4096)?;   // fit within token limit
ai.clear_context("s1")?;                            // reset conversation

// ── Semantic Memory (vector-backed long-term memory) ─
ai.store_memory(&MemoryEntry {
    id: 1, session_id: "s1".into(), content: "User prefers Rust".into(),
    ..Default::default()
}, &embedding)?;
let memories = ai.search_memory(&query_vec, 5)?;
ai.update_memory(1, Some("Updated content"), None)?;

// Memory dedup & TTL
let duplicates = ai.find_duplicate_memories(0.05)?;  // cosine distance threshold (0.05~0.1)
ai.cleanup_expired_memories()?;                       // TTL-based cleanup
let stats = ai.memory_stats()?;                       // count & usage

// ── RAG Document Management ─────────────────────────
ai.store_rag_document(&doc_with_chunks)?;
let results = ai.search_rag(&query_vec, 10)?;

// ── Agent Primitives ────────────────────────────────
// Tool call caching (avoid redundant API calls)
ai.cache_tool_result("weather_api", "input_hash", "result_json", Some(3600))?;
let cached = ai.get_cached_tool_result("weather_api", "input_hash")?;
ai.invalidate_tool_cache("weather_api")?;           // clear cache for a tool

// Agent step tracking (persistent state machine)
ai.save_agent_state("agent_1", "step_1", "search", "{\"query\": \"rust\"}")?;
let latest = ai.get_agent_state("agent_1")?;
let steps = ai.list_agent_steps("agent_1")?;
ai.rollback_agent_to_step("agent_1", "step_1")?;    // undo to checkpoint

// ── Intent Recognition ──────────────────────────────
let intent = ai.query_intent(&IntentQuery {
    text: "find similar docs".into(), session_id: Some("s1".into()),
})?;

// ── Execution Trace & Observability ─────────────────
ai.log_trace("s1", "run_1", "llm_call", &payload)?;
let traces = ai.query_traces_by_session("s1")?;
let traces = ai.query_traces_by_run("run_1")?;
let traces = ai.query_traces_by_operation("llm_call")?;
let report = ai.trace_performance_report(Some("s1"), 1000)?;  // slow threshold ms
let stats = ai.trace_stats(Some("s1"))?;                       // count & breakdown

// ── Embedding Cache ─────────────────────────────────
ai.cache_embedding("text_hash", &embedding)?;
let cached_emb = ai.get_cached_embedding("text_hash")?;
```

## Cross-Engine Queries

Powerful multi-engine fusion queries:

```rust
use talon::cross::*;

// GEO + Vector: Find semantically similar items near a location
let hits = geo_vector_search(&store, &GeoVectorQuery {
    geo_name: "shops", center_lng: 116.4, center_lat: 39.9,
    radius: 1000.0, vector_index: "emb", query_vec: &vec, k: 10,
})?;

// Graph + Vector: GraphRAG — traverse graph then rank by vector similarity
let hits = graph_vector_search(&store, &GraphVectorQuery {
    graph: "knowledge", start_vertex: 1, max_depth: 3,
    vector_index: "emb", query_vec: &vec, k: 10,
})?;

// Graph + FTS: Knowledge graph full-text search
let hits = graph_fts_search(&store, &GraphFtsQuery {
    graph: "knowledge", start_vertex: 1, max_depth: 2,
    fts_index: "docs", query: "rust database", k: 10,
})?;

// GEO + Graph + Vector: Triple engine fusion
let hits = triple_search(&store, &TripleQuery { .. })?;
```

## Data Import

### CSV → SQL Table

```rust
use talon::import::import_csv;
let stats = import_csv(&db, "users", reader, true)?;
// Auto-detects column types (Integer/Float/Text), creates table, batch inserts
```

### JSONL → Full-Text Index

```rust
use talon::import::import_jsonl;
let stats = import_jsonl(&db, "articles", reader, true)?;
// Each line: {"doc_id":"1", "title":"...", "body":"..."}
```

### SQL Dump Import

```rust
db.import_sql_file("./dump.sql")?;  // Supports SQLite .dump format
```

### InfluxDB Line Protocol

```rust
let points = parse_line_protocol("cpu,host=srv1 value=85.5 1700000000000000000")?;
```

### Elasticsearch `_bulk` Format

```rust
let items = parse_es_bulk(ndjson_str)?;
```

## Server Mode

### HTTP/JSON API

```bash
talon --data ./my_data --addr 0.0.0.0:7720 --token my_secret
```

**API Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/sql` | Execute SQL |
| `POST` | `/api/kv` | KV operations |
| `POST` | `/api/ts` | TimeSeries operations |
| `POST` | `/api/mq` | Message Queue operations |
| `POST` | `/api/vector` | Vector operations |
| `POST` | `/api/fts` | Full-Text Search |
| `POST` | `/api/geo` | Geospatial operations |
| `POST` | `/api/graph` | Graph engine operations |
| `POST` | `/api/ai` | AI engine operations |
| `POST` | `/api/backup` | Backup & restore |
| `GET`  | `/api/stats` | Database statistics |
| `GET`  | `/health` | Health check |

**Example:**

```bash
# SQL query
curl -X POST http://localhost:7720/api/sql \
  -H "Authorization: Bearer my_secret" \
  -d '{"action":"query","params":{"sql":"SELECT * FROM users"}}'

# Parameterized query
curl -X POST http://localhost:7720/api/sql \
  -d '{"action":"query","params":{"sql":"SELECT * FROM users WHERE id = ?","bind":[{"Integer":1}]}}'

# KV set
curl -X POST http://localhost:7720/api/kv \
  -d '{"action":"set","params":{"key":"hello","value":"world","ttl":3600}}'

# Health check
curl http://localhost:7720/health
```

### TCP Binary Protocol

```bash
talon --data ./my_data --addr 0.0.0.0:7720 --tcp-addr 0.0.0.0:7721
```

### Server Options

| Flag | Description | Default |
|------|-------------|---------|
| `--data <path>` | Data directory | `talon_data` |
| `--addr <host:port>` | HTTP listen address | `127.0.0.1:7720` |
| `--tcp-addr <host:port>` | TCP binary protocol address | (disabled) |
| `--token <token>` | Auth token | (no auth) |
| `--role <role>` | Cluster role: `standalone`/`primary`/`replica` | `standalone` |
| `--repl-addr <host:port>` | Replication address | — |
| `--repl-token <token>` | Replication auth token | — |

## CLI Client

Interactive database shell:

```bash
talon-cli "talon://:my_secret@localhost:7720"
# or
talon-cli --url "talon://localhost:7720"
# or
TALON_URL="talon://localhost:7720" talon-cli
```

**Commands:**

| Command | Description |
|---------|-------------|
| SQL statements (end with `;`) | Execute SQL |
| `:kv get <key>` | KV read |
| `:kv set <key> <value>` | KV write |
| `:kv del <key>` | KV delete |
| `:kv keys <prefix>` | List keys by prefix |
| `:mq topics` | List MQ topics |
| `:mq len <topic>` | Queue length |
| `:stats` | Database statistics |
| `:help` | Show help |
| `:quit` / `:exit` | Exit |

## Multi-Language SDKs

| Language | Path | Integration | Engine Coverage |
|----------|------|-------------|-----------------|
| **Rust** | Direct crate dependency | Embedded | All engines |
| **C/C++** | `include/talon.h` + `libtalon` | FFI | All engines |
| **Python** | `sdk/python/` | ctypes / FFI | All engines |
| **Node.js** | `sdk/nodejs/` | FFI (N-API) | All engines |
| **Go** | `sdk/go/` | cgo | All engines |
| **Java** | `sdk/java/` | JNI | All engines |
| **.NET** | `sdk/dotnet/` | P/Invoke | All engines |

All SDKs share a unified JSON command interface (`talon_execute`) that covers every engine operation.

**Node.js Example:**

```javascript
const { Talon } = require('talon-sdk');
const db = new Talon('./my_data');

db.sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
db.sql("INSERT INTO users VALUES (1, 'Alice')");
const rows = db.sql("SELECT * FROM users");

db.kvSet("session:1", "token_abc", 3600);
const val = db.kvGet("session:1");

db.close();
```

**Go Example:**

```go
import "github.com/darkmice/talon-core/sdk/go"

db, _ := talon.Open("./my_data")
defer db.Close()

rows, _ := db.SQL("SELECT * FROM users")
db.KVSet("key", "value", 3600)
val, _ := db.KVGet("key")
```

## Cluster & Replication

OpLog-based async replication with automatic read-only protection:

```bash
# Primary node
talon --data ./primary --addr 0.0.0.0:7720 \
  --role primary --repl-addr 0.0.0.0:7730 --repl-token my_repl_secret

# Replica node
talon --data ./replica --addr 0.0.0.0:7721 \
  --role replica --repl-addr primary_host:7730 --repl-token my_repl_secret
```

**Features:**
- **OpLog async replication** — binary-encoded operation log streaming
- **Automatic read-only protection** — Replica nodes reject all write operations
- **Online promote** — `POST /api/cluster/promote` to promote Replica → Primary
- **Cluster status** — `GET /api/cluster/status` for real-time replication health
- **Multi-replica** — one Primary can serve multiple Replicas

## Diagnostics & Monitoring

```rust
// Database statistics (KV keys, SQL tables, TS series, MQ topics, cache, disk)
let stats = db.database_stats()?;

// Health check (validates each engine is readable/writable)
let health = db.health_check();

// Runtime stats (cache hit rate, version)
let info = db.stats();
```

**HTTP endpoints:**
- `GET /api/stats` — full database statistics
- `GET /health` — engine-level health check

## Performance Benchmarks

Tested at **million-row scale** with `persist()` flush + close→reopen→verify validation:

### SQL Engine (71-column wide table)

| Benchmark | Target | Actual | Margin |
|-----------|--------|--------|--------|
| Point query by PK (1M rows) | P95 < 5ms | **0.007ms** | 714x |
| Range query with index | P95 < 10ms | **0.139ms** | 72x |
| Single INSERT (71 cols) | > 10K QPS | **46,667 QPS** | 4.7x |
| Native batch INSERT | > 100K rows/s | **241,697 rows/s** | 2.4x |
| JOIN (100K × 1K) | P95 < 50ms | **8.6ms** | 5.8x |
| Aggregates (1M rows) | P95 < 500ms | **< 1ms** | 500x+ |

### Other Engines

| Benchmark | Target | Actual | Margin |
|-----------|--------|--------|--------|
| KV batch SET (1M keys) | > 400K ops/s | **744K ops/s** | 1.9x |
| TS batch INSERT (1M pts) | > 200K pts/s | **540K pts/s** | 2.7x |
| TS query (ASC+LIMIT) | P95 < 50ms | **1.0ms** | 50x |
| MQ publish (1M msgs, batch) | > 50K msg/s | **1,611K msg/s** | 32x |
| Vector INSERT (100K, HNSW) | > 1K vec/s | **1,057 vec/s** | ✅ |
| Vector KNN (k=10, 100K vecs) | P95 < 50ms | **0.1ms** | 500x |
| Graph vertex write (1M) | — | **127K ops/s** | — |
| Graph edge write (2M) | — | **74K ops/s** | — |
| Graph vertex read | — | **935K ops/s** | — |
| Graph neighbor query | — | **110K qps** | — |
| Graph BFS traversal | — | **6.5K qps** | — |

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         Public API (lib.rs / Talon)                       │
│                Embedded Rust API · C ABI / FFI · HTTP · TCP               │
├────────┬──────┬──────┬──────┬────────┬──────┬──────┬───────┬─────────────┤
│  SQL   │  KV  │  TS  │  MQ  │ Vector │ FTS  │ GEO  │ Graph │     AI      │
│ Engine │Engine│Engine│Engine│ Engine │Engine│Engine│Engine │   Engine    │
│        │      │      │      │        │      │      │       │Session·RAG  │
│        │      │      │      │        │      │      │       │Memory·Agent │
├────────┴──────┴──────┴──────┴────────┴──────┴──────┴───────┴─────────────┤
│                       Cross-Engine Query Layer                            │
│       GEO+Vector · Graph+Vector · Graph+FTS · Hybrid Search · Triple     │
├───────────────────────────────────────────────────────────────────────────┤
│                     Types Layer (Value / Schema / ColumnType)              │
├───────────────────────────────────────────────────────────────────────────┤
│                  Storage Layer (fjall LSM-Tree · Keyspace · Batch)         │
├───────────────────────────────────────────────────────────────────────────┤
│                 Server Layer (HTTP · TCP · Redis RESP · CLI)               │
├───────────────────────────────────────────────────────────────────────────┤
│             Cluster Layer (OpLog · ReplSender · ReplReceiver)              │
└───────────────────────────────────────────────────────────────────────────┘
```

**Tech Stack:**
- **Language**: Rust 2021 edition
- **Storage**: fjall 3.x — pure-Rust LSM-Tree (WAL-based crash recovery)
- **Vector Index**: Custom HNSW implementation
- **Serialization**: serde + serde_json; binary encoding for vectors
- **Compression**: LZ4 (lz4_flex)
- **Error Handling**: thiserror (public API) + anyhow (internal/tests)
- **External Dependencies**: **Zero** runtime dependencies beyond Rust stdlib

## Building from Source

```bash
# Clone
git clone https://github.com/darkmice/talon-core.git
cd talon

# Build release binary
cargo build --release

# Run unit tests
cargo test --lib

# Run integration tests
cargo test --tests

# Run benchmarks
cargo test --test bench_p0 --release -- --nocapture
cargo test --test bench_engines_1m --release -- --nocapture

# Build shared library for FFI
cargo build --release  # produces libtalon.dylib / libtalon.so / talon.dll
```

**Output binaries:**
- `target/release/talon` — Server binary
- `target/release/talon-cli` — CLI client
- `target/release/libtalon.dylib` — Shared library (macOS)

## Feature Boundaries & Large-Scale Strategy

### Not Supported

The following are architecturally excluded:

- User access control (RBAC)
- Distributed transactions (async replication only)
- Cloud-managed service (local-first / self-hosted)

### Fully Implemented, Use Judgment at Scale

The following SQL advanced features are **fully implemented** and work well at small-to-medium scale (10K–1M rows). At very large scale (100M+ rows), these operations may involve full table scans or significant memory usage — evaluate based on your data size:

| Feature | Status | Large-Scale Note |
|---------|--------|------------------|
| `GROUP BY` / `HAVING` | ✅ Implemented | HashMap grouping, memory proportional to group count |
| Subqueries `WHERE x IN (SELECT ...)` | ✅ Implemented | Subquery result set loaded into memory |
| Multi-table JOIN (chained, any count) | ✅ Implemented | Nested Loop, no cost-based optimizer |
| `UNION` / `UNION ALL` | ✅ Implemented | HashSet dedup |
| Expression columns `SELECT a + b` | ✅ Implemented | No extra overhead |
| Aggregates `SUM`/`AVG`/`COUNT` | ✅ Implemented | O(1) column stats (without WHERE) |
| Stored procedures / triggers | ❌ Not supported | Intentionally excluded |
| Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, aggregate `OVER`) | ✅ Implemented | Partition + ORDER BY in memory |
| Views / materialized views | ❌ Not supported | Intentionally excluded |
| Foreign key constraints | ❌ Not supported | Intentionally excluded |

## Free Use & AI Disclaimers

By using this software, you agree to the following terms and the Dual License Agreement. If you do not agree, do not use the software.

**AI-Specific Disclaimers**
1. **Algorithmic Probabilities**: The user understands and agrees that the AI Engine (such as Context shortening, RAG retrieval) is probabilistic. The developers do not guarantee absolute accuracy or complete equivalence to the original meaning of the results.
2. **High-Risk Exclusion**: It is strictly forbidden to use this software in high-risk environments such as medical life-saving, nuclear facilities, or aviation control. If the user violates this use case, all resulting loss of life or property is solely borne by the user.
3. **Third-Party Model Isolation**: If this software connects to third-party models (e.g., OpenAI, DeepSeek), the developers are not responsible for the availability, privacy compliance, or legality of the output content from those third-party services.
4. **Data Backup Obligation**: When using this software to store or process data, the user must establish a comprehensive backup mechanism. The developers are not liable for any data loss or damage caused by any reason.

---

## License

[Talon Community Dual License Agreement (SSPL / Commercial)](LICENSE)
