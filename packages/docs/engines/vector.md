# Vector Engine

Self-built HNSW index with recommend, discover, metadata filtering, SQ8 quantization, and sub-millisecond search.

## Overview

The Vector Engine provides approximate nearest neighbor (ANN) search using a custom HNSW implementation. It supports multiple distance metrics, SQL integration, metadata filtering, scalar quantization, snapshot-consistent search, and Qdrant-compatible recommend/discover APIs.

## Quick Start

```rust
let db = Talon::open("./data")?;

// Create table with vector column
db.run_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, emb VECTOR(384))")?;
db.run_sql("CREATE VECTOR INDEX idx ON docs(emb) USING HNSW")?;

// Insert vectors
db.run_sql("INSERT INTO docs VALUES (1, '[0.1, 0.2, ...]')")?;

// Search via SQL
let results = db.run_sql("SELECT id, vec_cosine(emb, '[0.1, 0.2, ...]') AS score FROM docs ORDER BY score LIMIT 10")?;

// Search via Engine API
let ve = db.vector("idx")?;
let query = vec![0.1f32; 384];
let hits = ve.search(&query, 10)?;
```

## API Reference

### `Talon::vector`

Get vector engine handle (write mode). Replica nodes return `Error::ReadOnly`.

```rust
pub fn vector(&self, name: &str) -> Result<VectorEngine, Error>
```

### `Talon::vector_read`

Get vector engine handle (read-only). Available on Replica nodes.

```rust
pub fn vector_read(&self, name: &str) -> Result<VectorEngine, Error>
```

### `Talon::vector_set_ef_search`

Set runtime search width for a vector index.

```rust
pub fn vector_set_ef_search(&self, name: &str, ef_search: usize) -> Result<(), Error>
```

### `VectorEngine::search`

K-nearest neighbor search.

```rust
pub fn search(&self, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `&[f32]` | Query vector |
| `k` | `usize` | Number of results |
| `metric` | `&str` | `"cosine"`, `"l2"`, or `"dot"` |

### `VectorEngine::batch_search`

Batch search multiple queries at once.

```rust
pub fn batch_search(&self, queries: &[&[f32]], k: usize, metric: &str) -> Result<Vec<Vec<(u64, f32)>>, Error>
```

### `VectorEngine::insert`

Insert a vector with ID.

```rust
pub fn insert(&self, id: u64, vec: &[f32]) -> Result<(), Error>
```

### `VectorEngine::insert_batch`

Batch insert vectors.

```rust
pub fn insert_batch(&self, items: &[(u64, &[f32])]) -> Result<(), Error>
```

### `VectorEngine::insert_with_metadata`

Insert a vector with associated metadata for filtered search.

```rust
pub fn insert_with_metadata(&self, id: u64, vec: &[f32], metadata: HashMap<String, MetaValue>) -> Result<(), Error>
```

### `VectorEngine::delete`

Delete a vector by ID.

```rust
pub fn delete(&self, id: u64) -> Result<(), Error>
```

### `VectorEngine::get_vector`

Retrieve a stored vector by ID.

```rust
pub fn get_vector(&self, id: u64) -> Result<Option<Vec<f32>>, Error>
```

### `VectorEngine::count`

Get total number of vectors in the index.

```rust
pub fn count(&self) -> Result<u64, Error>
```

### `VectorEngine::recommend`

Find similar items using positive and negative examples (Qdrant-compatible).

```rust
pub fn recommend(&self, positive: &[&[f32]], negative: &[&[f32]], k: usize) -> Result<Vec<RecommendHit>, Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `positive` | `&[&[f32]]` | Vectors to be similar to |
| `negative` | `&[&[f32]]` | Vectors to avoid |
| `k` | `usize` | Number of results |

**Example:**
```rust
let liked = vec![0.1f32; 384];
let disliked = vec![0.9f32; 384];
let hits = ve.recommend(&[&liked], &[&disliked], 10)?;
```

### `VectorEngine::discover`

Context-based reranking search (Qdrant-compatible).

```rust
pub fn discover(&self, target: &[f32], context: &[(&[f32], &[f32])], k: usize) -> Result<Vec<DiscoverHit>, Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `target` | `&[f32]` | Target vector |
| `context` | `&[(&[f32], &[f32])]` | Positive-negative pairs for reranking |
| `k` | `usize` | Number of results |

### `VectorEngine::search_with_filter`

Search with metadata filtering.

```rust
pub fn search_with_filter(&self, query: &[f32], k: usize, filter: &[MetaFilter]) -> Result<Vec<SearchResult>, Error>
```

**MetaFilter operators:** `Eq`, `Ne`, `Gt`, `Lt`, `Gte`, `Lte`, `In`

```rust
use talon::{MetaFilter, MetaFilterOp, MetaValue};

let filter = vec![
    MetaFilter { field: "category".into(), op: MetaFilterOp::Eq, value: MetaValue::Text("tech".into()) },
];
let hits = ve.search_with_filter(&query, 10, &filter)?;
```

### `VectorEngine::enable_quantization`

Enable SQ8 scalar quantization (4:1 compression, <2% accuracy loss).

```rust
pub fn enable_quantization(&self) -> Result<(), Error>
```

### `VectorEngine::set_ef_search`

Set search-time ef parameter (higher = more accurate, slower).

```rust
pub fn set_ef_search(&self, ef_search: usize) -> Result<(), Error>
```

### `VectorEngine::disable_quantization`

Disable SQ8 quantization and revert to full-precision vectors.

```rust
pub fn disable_quantization(&self) -> Result<(), Error>
```

### `VectorEngine::is_quantized`

Check if quantization is currently enabled.

```rust
pub fn is_quantized(&self) -> Result<bool, Error>
```

### `VectorEngine::rebuild_index`

Rebuild the HNSW index from scratch. Returns the number of vectors reindexed.

```rust
pub fn rebuild_index(&self) -> Result<u64, Error>
```

### Metadata Operations

#### `set_metadata`
```rust
pub fn set_metadata(&self, id: u64, metadata: HashMap<String, MetaValue>) -> Result<(), Error>
```
Set metadata for a vector (overwrites existing).

#### `get_metadata`
```rust
pub fn get_metadata(&self, id: u64) -> Result<Option<HashMap<String, MetaValue>>, Error>
```

#### `delete_metadata`
```rust
pub fn delete_metadata(&self, id: u64) -> Result<(), Error>
```

### Snapshot Search

#### `snapshot_search`
```rust
pub fn snapshot_search(&self, snapshot: &Snapshot, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, Error>
```
Search against a point-in-time snapshot for read consistency. Useful when concurrent writes are happening.

## SQL Integration

```sql
-- Vector distance functions
SELECT id, vec_cosine(emb, '[0.1, 0.2, ...]') AS score FROM docs ORDER BY score LIMIT 10;
SELECT id, vec_l2(emb, '[0.1, 0.2, ...]') AS dist FROM docs ORDER BY dist LIMIT 10;
SELECT id, vec_dot(emb, '[0.1, 0.2, ...]') AS sim FROM docs ORDER BY sim DESC LIMIT 10;

-- Hybrid query: scalar filter + vector KNN
SELECT id, vec_cosine(emb, '[0.1, ...]') AS score
FROM docs WHERE category = 'tech' ORDER BY score LIMIT 10;
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | HNSW max connections per node |
| `ef_construction` | 200 | Build-time search width |
| `ef_search` | 50 | Runtime search width |
| Distance metric | Cosine | Also supports L2, Inner Product |

## Milvus / Qdrant / Pinecone Compatibility

### Feature Comparison

| Feature | Milvus | Qdrant | Pinecone | Talon Vector |
|---------|--------|--------|----------|--------------|
| HNSW index | ✅ | ✅ | ✅ | ✅ |
| Cosine / L2 / Dot metrics | ✅ | ✅ | ✅ | ✅ |
| Metadata filtering | ✅ | ✅ | ✅ | ✅ |
| Batch insert / delete | ✅ | ✅ | ✅ | ✅ |
| Snapshot-consistent reads | ❌ | ❌ | ❌ | ✅ |
| SQL hybrid query | ❌ | ❌ | ❌ | ✅ native |
| Product quantization (PQ) | ✅ | ✅ | ✅ | ✅ |
| IVF index | ✅ | ❌ | ❌ | ❌ |
| DiskANN | ✅ | ❌ | ❌ | ❌ |
| GPU acceleration | ✅ | ❌ | ✅ | ❌ |
| Distributed sharding | ✅ | ✅ | ✅ (managed) | ❌ |
| Embedded mode | ❌ | ❌ | ❌ | ✅ |
| Single binary | ❌ | ✅ | ❌ (SaaS) | ✅ |
| Multi-model (SQL+KV+FTS) | ❌ | ❌ | ❌ | ✅ |

### Talon-Only Features

- **SQL-native vector search** — `SELECT vec_cosine(emb, ...) FROM docs WHERE category='ai'` in standard SQL
- **Cross-engine fusion** — vector + FTS hybrid search (RRF), vector + graph traversal
- **Snapshot search** — point-in-time consistent reads during concurrent writes
- **Embedded deployment** — in-process, zero network overhead for AI applications
- **Unified data engine** — vectors, metadata, full-text, relational data in one binary

## Performance

| Benchmark | Result |
|-----------|--------|
| INSERT (100K, HNSW) | 1,057 vec/s |
| KNN search (k=10, 100K vectors) | P95 0.1ms |
