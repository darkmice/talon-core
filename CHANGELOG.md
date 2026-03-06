# Changelog

All notable changes to Talon will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-02-27

### Added

#### Five-Engine Architecture
- **SQL Engine**: Full relational database with CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
  - WHERE clauses: `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, `IS NOT NULL`
  - AND / OR / parenthesized conditions
  - ORDER BY (ASC/DESC), LIMIT, OFFSET, DISTINCT
  - JOIN: INNER, LEFT, RIGHT with table aliases (`AS` or implicit)
  - Aggregates: COUNT, SUM, AVG, MIN, MAX (O(1) via column stats)
  - CREATE INDEX / DROP INDEX for secondary indexes
  - ALTER TABLE ADD/DROP/RENAME COLUMN
  - INSERT OR REPLACE, ON CONFLICT DO UPDATE (UPSERT)
  - UPDATE SET arithmetic expressions (`col = col + 1`)
  - Transactions: BEGIN / COMMIT / ROLLBACK
  - EXPLAIN query plan
  - Types: INTEGER, FLOAT, TEXT, BLOB, BOOLEAN, JSONB, VECTOR(dim), TIMESTAMP, GEOPOINT

- **KV Engine**: Redis-compatible key-value store
  - SET / GET / DEL / MSET / MGET / EXISTS
  - TTL with automatic background expiration cleanup
  - INCR / INCRBY / DECRBY / SETNX
  - KEYS prefix / glob pattern matching
  - EXPIRE / TTL query
  - Namespace isolation (`ns:key` convention)
  - Snapshot reads (MVCC)

- **TimeSeries Engine**: Time-series data storage
  - CREATE TIMESERIES with TAG fields and value fields
  - Batch INSERT with millisecond precision timestamps
  - Time range queries with TAG filtering
  - Aggregation: SUM, AVG, COUNT, MIN, MAX with interval grouping
  - Retention policy with automatic background cleanup

- **Message Queue Engine**: Persistent pub/sub messaging
  - CREATE / DROP topics with optional MAXLEN
  - PUBLISH / POLL with consumer groups
  - ACK message confirmation (at-least-once delivery)
  - Multiple consumers per group

- **Vector Engine**: HNSW-based approximate nearest neighbor search
  - CREATE VECTOR INDEX with configurable HNSW parameters (m, ef_construction, ef_search)
  - Distance metrics: cosine, L2 (Euclidean), dot product
  - SQL-integrated vector search: `vec_distance()`, `vec_cosine()`, `vec_l2()`, `vec_dot()`
  - Hybrid queries: scalar filtering + vector KNN in single SELECT

#### AI-Native Abstractions
- **Session management**: Create, query, list AI sessions
- **Context/Memory**: Store and retrieve conversation context and long-term memory
- **Trace/Run tracking**: Record AI agent execution traces
- **RAG support**: Document chunking, storage, and semantic search
- **Tool cache**: Cache tool call results with TTL
- **Intent queries**: Natural language intent-based data retrieval

#### Infrastructure
- **Embedded mode**: Single `Talon::open()` call, zero external dependencies
- **Server mode**: TCP binary protocol + HTTP/JSON API with token authentication
- **C ABI / FFI**: Complete foreign function interface for Python/Node.js/Go integration
- **CLI client**: Interactive command-line client for all five engines
- **Backup/Restore**: Export/import keyspaces as JSONL files
- **Cluster support**: Primary/Replica replication via OpLog
  - Standalone / Primary / Replica roles
  - OpLog-based replication with binary encoding
  - Manual failover (promote Replica to Primary)
- **Thread safety**: All engines protected by Mutex/RwLock, safe for concurrent access
- **Crash recovery**: WAL-based durability via fjall storage engine

#### Additional Engines (M150+)
- **FTS Engine**: BM25 full-text search with inverted index
  - Standard / Chinese analyzer, phrase search, fuzzy search, wildcard, regexp
  - Bool queries (must/should/must_not), multi-field weighted search
  - Range queries, term queries, sorted search
  - Index alias, close/open index, reindex
  - Highlights with `<em>` tags

- **GEO Engine**: Geohash-based spatial index (Redis GEO compatible)
  - GEOADD / GEOPOS / GEODIST / GEOSEARCH / GEODEL
  - Circle and rectangle range search
  - Geofence detection, GEOHASH encoding
  - NX/XX/CH conditional writes, batch operations

- **Graph Engine**: Triple-store based knowledge graph
  - Vertex / Edge CRUD with properties
  - BFS/DFS traversal, shortest path
  - Cross-engine queries: graph+vector, graph+FTS

#### Window Functions & Advanced SQL (M170+)
- ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- PARTITION BY + ORDER BY
- CTE (WITH ... AS), UNION ALL
- Subqueries in WHERE IN (SELECT ...)

### Fixed

#### M200-M202: Performance Optimization & Bug Fixes
- **TS partial tag query**: 163ms → 352µs (**455x** improvement) — tag registry index lookup
- **TS regex tag query**: 494ms → 244µs (**2025x**) — same optimization
- **TS no-tag time range**: 169ms → 1.07ms (**158x**) — multi-prefix time range scan
- **FTS search**: 27 → 48 qps — O(k) reverse lookup + zero-alloc HashMap + high-df skip
- **Vector HNSW recall bug**: 0.2% → **100%** — fixed `prune_neighbors` silently dropping
  reverse connections when `load_vec` returned None for unpersisted vectors

### Performance (105 benchmark metrics, release build)
- **KV**: SET 1.46M ops/s, GET 737K ops/s, batch SET 1M 1.46M ops/s
- **SQL**: INSERT 308K rows/s, PK lookup 252K ops/s, COUNT(*) O(1)
- **TS**: INSERT 657K pts/s, single-tag query 352µs, aggregation <5ms
- **Vector**: KNN 795 qps (recall=100%), INSERT 230 vecs/s (dim=128)
- **FTS**: Index 17K docs/s, search 48 qps (100K docs)
- **MQ**: Publish 1.57M msg/s (batch), poll P95 <1ms
- **GEO**: ADD 515K ops/s, POS 550K ops/s, fence 786K ops/s
- Cold start: 234ms, persist: 0.75ms avg

[0.1.0]: https://github.com/user/talon/releases/tag/v0.1.0
