# Full-Text Search Engine

Inverted index + BM25 scoring with Elasticsearch-compatible queries, Chinese tokenizer, and hybrid search.

## Overview

The FTS Engine provides full-text search with BM25 relevance scoring, multiple query types (bool, phrase, term, range, regexp, wildcard), aggregations, index management, Chinese tokenization (jieba), and hybrid search combining BM25 + vector KNN via Reciprocal Rank Fusion (RRF).

## Quick Start

```rust
let db = Talon::open("./data")?;

// Index documents
db.fts()?.index("articles", "doc1", "Talon is an AI-native database")?;
db.fts()?.index("articles", "doc2", "Vector search with HNSW algorithm")?;

// Basic search
let hits = db.fts()?.search("articles", "database", 10)?;
for hit in &hits {
    println!("id={}, score={:.2}", hit.id, hit.score);
}
```

## API Reference

### Index Management

#### `create_index`
```rust
pub fn create_index(&self, name: &str, config: &FtsConfig) -> Result<(), Error>
```
Create an index with explicit configuration (analyzer, field mappings).

#### `drop_index`
```rust
pub fn drop_index(&self, name: &str) -> Result<(), Error>
```
Delete an index and all its documents.

### Document Operations

#### `index_doc`
```rust
pub fn index_doc(&self, name: &str, doc: &FtsDoc) -> Result<(), Error>
```
Index a structured document. Auto-creates the index if it doesn't exist.

#### `index_doc_batch`
```rust
pub fn index_doc_batch(&self, name: &str, docs: &[FtsDoc]) -> Result<(), Error>
```
Batch index multiple documents in a single WriteBatch.

#### `get_doc`
```rust
pub fn get_doc(&self, name: &str, doc_id: &str) -> Result<Option<FtsDoc>, Error>
```
Retrieve a stored document by ID.

#### `update_doc`
```rust
pub fn update_doc(&self, name: &str, doc_id: &str, doc: &FtsDoc) -> Result<(), Error>
```
Update an existing document (delete + re-index).

#### `delete_doc`
```rust
pub fn delete_doc(&self, name: &str, doc_id: &str) -> Result<bool, Error>
```
Delete a document. Returns `true` if found and deleted.

### Search Queries

#### `search` (Basic)
```rust
pub fn search(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
BM25 full-text search with Unicode tokenization.

#### `search_bool` (Boolean Query)
```rust
pub fn search_bool(&self, name: &str, query: &BoolQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Elasticsearch-compatible boolean query with `must`, `should`, `must_not` clauses.

```rust
use talon::BoolQuery;

let query = BoolQuery {
    must: vec!["database".into()],
    should: vec!["AI".into(), "vector".into()],
    must_not: vec!["legacy".into()],
};
let hits = db.fts()?.search_bool("articles", &query, 10)?;
```

#### `search_phrase` (Phrase Query)
```rust
pub fn search_phrase(&self, name: &str, phrase: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Exact phrase matching with position awareness.

#### `search_term` (Term Query)
```rust
pub fn search_term(&self, name: &str, field: &str, term: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Exact field-value match without tokenization.

#### `search_range` (Range Query)
```rust
pub fn search_range(&self, name: &str, query: &RangeQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Numeric or string range filtering.

#### `search_regexp` (Regular Expression)
```rust
pub fn search_regexp(&self, name: &str, pattern: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_wildcard` (Wildcard)
```rust
pub fn search_wildcard(&self, name: &str, pattern: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
`*` matches any characters, `?` matches single character.

#### `search_fuzzy` (Fuzzy Search)
```rust
pub fn search_fuzzy(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Fuzzy search with edit distance tolerance for typo correction.

#### `search_multi_field` (Multi-Field)
```rust
pub fn search_multi_field(&self, name: &str, query: &MultiFieldQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Search across multiple fields with per-field boosting.

### Aggregations

#### `aggregate_terms`
```rust
pub fn aggregate_terms(&self, name: &str, field: &str, size: usize) -> Result<Vec<TermBucket>, Error>
```
Terms aggregation — top N values by frequency.

#### `suggest`
```rust
pub fn suggest(&self, name: &str, prefix: &str, limit: usize) -> Result<Vec<String>, Error>
```
Auto-complete suggestions based on prefix.

#### `search_sorted`
```rust
pub fn search_sorted(&self, name: &str, query: &str, sort_field: &str, ascending: bool, limit: usize) -> Result<Vec<SearchHit>, Error>
```
Search with custom sort order on a specific field.

#### `search_terms` (Multi-Term)
```rust
pub fn search_terms(&self, name: &str, field: &str, terms: &[&str], limit: usize) -> Result<Vec<SearchHit>, Error>
```
Match documents containing any of the given terms in a field.

### Index Management

#### `list_indexes`
```rust
pub fn list_indexes(&self) -> Result<Vec<FtsIndexInfo>, Error>
```

#### `get_mapping`
```rust
pub fn get_mapping(&self, name: &str) -> Result<FtsMapping, Error>
```
Get index field mapping (schema).

#### `add_alias`
```rust
pub fn add_alias(&self, alias: &str, index: &str) -> Result<(), Error>
```
Add an alias to an index (useful for RAG knowledge base version switching).

#### `remove_alias`
```rust
pub fn remove_alias(&self, alias: &str) -> Result<(), Error>
```
Remove an index alias.

#### `close_index` / `open_index`
```rust
pub fn close_index(&self, name: &str) -> Result<(), Error>
pub fn open_index(&self, name: &str) -> Result<(), Error>
```

#### `reindex`
```rust
pub fn reindex(&self, name: &str) -> Result<u64, Error>
```
Rebuild index (e.g., after tokenizer change). Returns doc count.

#### `update_by_query`
```rust
pub fn update_by_query(&self, name: &str, query: &str, field: &str, value: &str) -> Result<u64, Error>
```
Bulk update documents matching a query.

#### `delete_by_query`
```rust
pub fn delete_by_query(&self, name: &str, query: &str) -> Result<u64, Error>
```
Bulk delete documents matching a query.

### Elasticsearch Bulk API

```rust
use talon::parse_es_bulk;

let ndjson = r#"
{"index":{"_index":"articles","_id":"1"}}
{"title":"Talon","body":"AI database"}
{"index":{"_index":"articles","_id":"2"}}
{"title":"HNSW","body":"Vector search algorithm"}
"#;

let items = parse_es_bulk(ndjson)?;
for item in items {
    db.fts()?.index_json(&item.index, &item.id, &item.doc)?;
}
```

### Hybrid Search (BM25 + Vector)

Combines full-text BM25 and vector KNN results via Reciprocal Rank Fusion (RRF).

```rust
use talon::hybrid_search;

let hits = hybrid_search(&db.store_ref(), &HybridQuery {
    fts_index: "articles",
    vec_index: "emb_idx",
    query_text: "AI database",
    query_vec: &embedding,
    limit: 10,
    pre_filter: Some(vec![("namespace", "tenant_a")]),
    ..Default::default()
})?;
```

### Analyzers

| Analyzer | Description |
|----------|-------------|
| `Standard` | Unicode segmentation (default) |
| `Jieba` | Chinese tokenization |
| `Whitespace` | Split on whitespace |
| `Keyword` | No tokenization (exact match) |

Configure per-index:
```rust
let config = FtsConfig {
    analyzer: Analyzer::Jieba,
    ..Default::default()
};
db.fts()?.create_index_with_config("zh_docs", config)?;
```

### SearchHit Structure

```rust
pub struct SearchHit {
    pub id: String,        // Document ID
    pub score: f32,        // BM25 score
    pub highlights: Vec<String>, // Highlighted snippets with <em> tags
    pub doc: Option<serde_json::Value>, // Original document (if stored)
}
```

## Elasticsearch Compatibility

### ES `_bulk` API

Talon supports Elasticsearch NDJSON bulk format for easy migration:

```rust
use talon::parse_es_bulk;

let ndjson = r#"{"index":{"_index":"articles","_id":"1"}}
{"title":"Hello","body":"World"}
{"index":{"_index":"articles","_id":"2"}}
{"title":"Talon","body":"Database"}
"#;
let items = parse_es_bulk(ndjson)?;
for item in &items {
    db.fts()?.index_doc(&item.index, &item.doc)?;
}
```

Supported actions: `index`, `create`. Unsupported: `delete`, `update` (silently skipped).

### Feature Comparison

| Feature | Elasticsearch | Talon FTS |
|---------|--------------|-----------|
| BM25 scoring | ✅ | ✅ |
| Term / phrase / bool query | ✅ | ✅ |
| Wildcard / regexp / fuzzy | ✅ | ✅ |
| Range query | ✅ | ✅ |
| Multi-field search | ✅ | ✅ |
| Terms aggregation | ✅ | ✅ |
| Auto-complete (suggest) | ✅ | ✅ |
| Index aliases | ✅ | ✅ |
| `_bulk` NDJSON API | ✅ | ✅ |
| Hybrid search (BM25 + vector RRF) | ❌ | ✅ |
| Complex aggregation pipeline | ✅ | ❌ |
| Nested / parent-child docs | ✅ | ❌ |
| Distributed sharding | ✅ | ❌ |
| REST API (full) | ✅ | HTTP subset |
| JVM dependency | ✅ (Java) | ❌ (single binary) |
| Chinese tokenizer (Jieba) | plugin | ✅ built-in |

### Talon-Only Features

- **Hybrid search** — BM25 + vector via Reciprocal Rank Fusion (RRF) in a single query
- **Cross-engine fusion** — FTS + Graph, FTS + Vector, triple search
- **Zero external dependencies** — no JVM, no cluster setup
- **Embedded mode** — in-process, no network overhead

## Performance

| Benchmark | Result |
|-----------|--------|
| Index throughput | ~50K docs/s |
| Search latency (100K docs) | < 5ms |
| Hybrid search | < 10ms |
