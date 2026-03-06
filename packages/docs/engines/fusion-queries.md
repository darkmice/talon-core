# Cross-Engine Fusion Queries

Talon's unique multi-engine fusion query capability combines results across different engines in a single operation.

## Overview

Cross-engine queries allow combining the strengths of different engines — for example, geographic filtering followed by vector similarity ranking, or graph traversal followed by full-text scoring.

## Available Combinations

| Combination | Function | Use Case |
|-------------|----------|----------|
| GEO + Vector | `geo_vector_search` | Nearby semantic search |
| GEO + Vector (Box) | `geo_box_vector_search` | Bounding box + vector |
| Graph + Vector | `graph_vector_search` | GraphRAG |
| Graph + FTS | `graph_fts_search` | Knowledge graph retrieval |
| FTS + Vector | `hybrid_search` | Hybrid retrieval (RRF) |
| Graph + FTS + Vector | `triple_search` | Triple fusion search |

## API Reference

### `geo_vector_search`

Geographic circle filter → vector similarity ranking.

```rust
use talon::{geo_vector_search, GeoVectorQuery};

let hits = geo_vector_search(&store, &GeoVectorQuery {
    geo_name: "places",
    vec_name: "embeddings",
    lng: 116.4074,
    lat: 39.9042,
    radius_m: 1000.0,
    query_vec: &embedding,
    k: 10,
})?;

for hit in &hits {
    println!("member={}, distance={}m, similarity={:.3}", hit.member, hit.geo_dist, hit.vec_score);
}
```

### `geo_box_vector_search`

Geographic bounding box filter → vector similarity ranking.

```rust
use talon::{geo_box_vector_search, GeoBoxVectorQuery};

let hits = geo_box_vector_search(&store, &GeoBoxVectorQuery {
    geo_name: "places",
    vec_name: "embeddings",
    lng: 116.4074,
    lat: 39.9042,
    width_m: 2000.0,
    height_m: 2000.0,
    query_vec: &embedding,
    k: 10,
})?;
```

### `graph_vector_search`

Graph traversal → vector similarity ranking (GraphRAG).

```rust
use talon::{graph_vector_search, GraphVectorQuery, Direction};

let hits = graph_vector_search(&store, &GraphVectorQuery {
    graph: "knowledge",
    vec_name: "embeddings",
    start: root_vertex_id,
    max_depth: 3,
    direction: Direction::Out,
    query_vec: &embedding,
    k: 10,
})?;

for hit in &hits {
    println!("vertex={}, depth={}, similarity={:.3}", hit.vertex_id, hit.depth, hit.vec_score);
}
```

### `graph_fts_search`

Graph traversal → BM25 full-text ranking.

```rust
use talon::{graph_fts_search, GraphFtsQuery, Direction};

let hits = graph_fts_search(&store, &GraphFtsQuery {
    graph: "knowledge",
    fts_name: "articles",
    start: root_vertex_id,
    max_depth: 2,
    direction: Direction::Out,
    query: "AI database",
    k: 10,
})?;
```

### `hybrid_search`

BM25 full-text + vector KNN via Reciprocal Rank Fusion (RRF).

```rust
use talon::{hybrid_search, HybridQuery};

let hits = hybrid_search(&store, &HybridQuery {
    fts_index: "articles",
    vec_index: "emb_idx",
    query_text: "AI database",
    query_vec: &embedding,
    limit: 10,
    pre_filter: Some(vec![("namespace", "tenant_a")]),
    ..Default::default()
})?;
```

### `triple_search`

Triple fusion: Graph traversal + FTS + Vector combined.

```rust
use talon::{triple_search, TripleQuery, Direction};

let hits = triple_search(&store, &TripleQuery {
    graph: "knowledge",
    fts_name: "articles",
    vec_name: "embeddings",
    start: root_vertex_id,
    max_depth: 2,
    direction: Direction::Out,
    text_query: "AI database",
    vec_query: &embedding,
    k: 10,
})?;
```

## AI Application Patterns

### RAG Pipeline
```
User Query → Embedding → hybrid_search(FTS + Vector) → LLM Context
```

### GraphRAG
```
User Query → Identify Root Node → graph_vector_search(Graph + Vector) → LLM Context
```

### Location-Aware AI
```
User Location → geo_vector_search(GEO + Vector) → Nearby Semantic Results → LLM
```
