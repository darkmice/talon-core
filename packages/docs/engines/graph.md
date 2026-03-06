# Graph Engine

Property graph with BFS/DFS traversal, shortest path, PageRank, and 935K reads/s.

## Overview

The Graph Engine implements a labeled property graph model with vertices (nodes), edges (relationships), label indexes, and graph traversal algorithms. It supports directed edges with O(1) neighbor lookups via adjacency indexes.

## Quick Start

```rust
use talon::{Talon, Direction};

let db = Talon::open("./data")?;

// Add vertices
let alice = db.graph()?.add_vertex("social", None, Some("person"), Some(&props))?;
let bob = db.graph()?.add_vertex("social", None, Some("person"), Some(&props))?;

// Add edge
db.graph()?.add_edge("social", alice, bob, Some("follows"), None)?;

// Traverse
let friends = db.graph()?.bfs("social", alice, 2, Direction::Out)?;
```

## API Reference

### `Talon::graph` / `Talon::graph_read`

```rust
pub fn graph(&self) -> Result<GraphEngine, Error>      // Write mode
pub fn graph_read(&self) -> Result<GraphEngine, Error>  // Read-only
```

### Vertex Operations

#### `add_vertex`
```rust
pub fn add_vertex(
    &self, graph: &str,
    id: Option<u64>,
    label: Option<&str>,
    properties: Option<&serde_json::Value>,
) -> Result<u64, Error>
```
Add a vertex. `id = None` for auto-increment. Returns vertex ID.

#### `get_vertex`
```rust
pub fn get_vertex(&self, graph: &str, id: u64) -> Result<Option<Vertex>, Error>
```

#### `update_vertex`
```rust
pub fn update_vertex(&self, graph: &str, id: u64, properties: &serde_json::Value) -> Result<(), Error>
```

#### `delete_vertex`
```rust
pub fn delete_vertex(&self, graph: &str, id: u64) -> Result<(), Error>
```
Deletes vertex and all connected edges.

### Graph Namespace

#### `create`
```rust
pub fn create(&self, name: &str) -> Result<(), Error>
```
Create a new graph namespace.

### Edge Operations

#### `add_edge`
```rust
pub fn add_edge(
    &self, graph: &str,
    from: u64, to: u64,
    label: Option<&str>,
    properties: Option<&serde_json::Value>,
) -> Result<u64, Error>
```
Add a directed edge. Returns edge ID.

#### `get_edge`
```rust
pub fn get_edge(&self, graph: &str, id: u64) -> Result<Option<Edge>, Error>
```

#### `delete_edge`
```rust
pub fn delete_edge(&self, graph: &str, edge_id: u64) -> Result<(), Error>
```

#### `out_edges`
```rust
pub fn out_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error>
```
Get all outgoing edges from a vertex.

#### `in_edges`
```rust
pub fn in_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error>
```
Get all incoming edges to a vertex.

### Traversal Algorithms

#### `bfs` (Breadth-First Search)
```rust
pub fn bfs(&self, graph: &str, start: u64, max_depth: u32, direction: Direction) -> Result<Vec<u64>, Error>
```
Returns visited vertex IDs in BFS order.

**Direction:** `Out`, `In`, `Both`

#### `bfs_filter` (Filtered BFS)
```rust
pub fn bfs_filter<F>(&self, graph: &str, start: u64, max_depth: u32, direction: Direction, filter: F) -> Result<Vec<u64>, Error>
where F: Fn(&Vertex) -> bool
```
BFS with a vertex predicate filter. Only visits vertices that pass the filter.

#### `k_hop_neighbors`
```rust
pub fn k_hop_neighbors(&self, graph: &str, start: u64, k: u32, direction: Direction) -> Result<Vec<u64>, Error>
```
Get all vertices within k hops from start vertex.

#### `shortest_path`
```rust
pub fn shortest_path(&self, graph: &str, from: u64, to: u64, direction: Direction) -> Result<Option<Vec<u64>>, Error>
```
Unweighted shortest path (BFS). Returns vertex sequence or `None`.

#### `neighbors`
```rust
pub fn neighbors(&self, graph: &str, vertex_id: u64, direction: Direction) -> Result<Vec<u64>, Error>
```
Direct neighbors (1-hop). O(1) via adjacency index.

### Graph Analytics

#### `pagerank`
```rust
pub fn pagerank(&self, graph: &str, iterations: u32, damping: f64) -> Result<Vec<(u64, f64)>, Error>
```
PageRank scores. Default damping = 0.85, iterations = 20.

#### `degree_centrality`
```rust
pub fn degree_centrality(&self, graph: &str, direction: Direction) -> Result<Vec<(u64, f64)>, Error>
```
Compute degree centrality for all vertices. Returns (vertex_id, centrality) pairs sorted descending.

#### `weighted_shortest_path`
```rust
pub fn weighted_shortest_path(&self, graph: &str, from: u64, to: u64, weight_key: &str) -> Result<Option<(Vec<u64>, f64)>, Error>
```
Dijkstra shortest path using edge property as weight. Returns (vertex path, total weight) or `None`.

#### `vertex_count` / `edge_count`
```rust
pub fn vertex_count(&self, graph: &str) -> Result<u64, Error>
pub fn edge_count(&self, graph: &str) -> Result<u64, Error>
```

### Label Operations

#### `vertices_by_label`
```rust
pub fn vertices_by_label(&self, graph: &str, label: &str) -> Result<Vec<Vertex>, Error>
```

#### `edges_by_label`
```rust
pub fn edges_by_label(&self, graph: &str, label: &str) -> Result<Vec<Edge>, Error>
```
Get all edges with a specific label.

### Cross-Engine Queries

```rust
use talon::{graph_vector_search, GraphVectorQuery};

// Graph + Vector: graph traversal → vector similarity ranking (GraphRAG)
let hits = graph_vector_search(&db.store_ref(), &GraphVectorQuery {
    graph: "knowledge",
    vec_name: "embeddings",
    start: root_id,
    max_depth: 3,
    direction: Direction::Out,
    query_vec: &embedding,
    k: 10,
})?;
```

### Data Types

```rust
pub struct Vertex {
    pub id: u64,
    pub label: Option<String>,
    pub properties: Option<serde_json::Value>,
}

pub struct Edge {
    pub id: u64,
    pub from: u64,
    pub to: u64,
    pub label: Option<String>,
    pub properties: Option<serde_json::Value>,
}

pub enum Direction {
    Out,
    In,
    Both,
}
```

## Neo4j / JanusGraph Compatibility

### Feature Comparison

| Feature | Neo4j | JanusGraph | Talon Graph |
|---------|-------|------------|-------------|
| Property graph model | ✅ | ✅ | ✅ |
| Vertex labels | ✅ | ✅ | ✅ |
| Edge labels | ✅ | ✅ | ✅ |
| JSON properties | ✅ | ✅ | ✅ |
| BFS traversal | ✅ | ✅ | ✅ |
| BFS with filter | ✅ | ✅ | ✅ |
| K-hop neighbors | ✅ | ✅ | ✅ |
| Shortest path | ✅ | ✅ | ✅ |
| Weighted shortest path | ✅ | ✅ | ✅ (Dijkstra) |
| PageRank | ✅ (GDS) | ✅ | ✅ native |
| Degree centrality | ✅ (GDS) | ✅ | ✅ native |
| Cypher query language | ✅ | ❌ | ❌ |
| Gremlin query language | ❌ | ✅ | ❌ |
| ACID transactions | ✅ | ✅ | ✅ |
| Distributed | ✅ (Enterprise) | ✅ | ❌ |
| GraphRAG fusion | ❌ | ❌ | ✅ |
| Embedded mode | ✅ (Java) | ❌ | ✅ (Rust) |
| Single binary | ❌ (JVM) | ❌ (JVM) | ✅ |
| Multi-model (SQL+KV+Vector) | ❌ | ❌ | ✅ |

### Talon-Only Features

- **GraphRAG** — graph traversal + vector search + FTS in unified queries
- **Cross-engine fusion** — graph + SQL joins, graph + geo spatial queries
- **Rust API** — zero-overhead embedded graph, no JVM
- **AI-native** — knowledge graphs for RAG, agent reasoning chains

## Performance

| Benchmark | Result |
|-----------|--------|
| Vertex write (1M) | 127K ops/s |
| Edge write (2M) | 74K ops/s |
| Vertex read | 935K ops/s |
| Neighbor query | 110K qps |
| BFS traversal | 6.5K qps |
