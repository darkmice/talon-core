# 图引擎

属性图模型，支持 BFS/DFS 遍历、最短路径、PageRank，935K 读/秒。

## 概述

图引擎实现了带标签的属性图模型，包含顶点（节点）、边（关系）、标签索引和图遍历算法。支持有向边，通过邻接索引实现 O(1) 邻居查找。

## 快速开始

```rust
use talon::{Talon, Direction};

let db = Talon::open("./data")?;

let graph = db.graph()?;
graph.create("social")?;

let alice = graph.add_vertex("social", None, Some("person"), Some(&props))?;
let bob = graph.add_vertex("social", None, Some("person"), Some(&props))?;

graph.add_edge("social", alice, bob, Some("follows"), None)?;

let friends = graph.bfs("social", alice, 2, Direction::Out)?;
```

## API 参考

### 获取引擎句柄

```rust
pub fn graph(&self) -> Result<GraphEngine, Error>      // 写模式
pub fn graph_read(&self) -> Result<GraphEngine, Error>  // 只读模式
```

### 图命名空间

```rust
pub fn create(&self, name: &str) -> Result<(), Error>
```

### 顶点操作

```rust
pub fn add_vertex(&self, graph: &str, id: Option<u64>, label: Option<&str>, properties: Option<&serde_json::Value>) -> Result<u64, Error>
pub fn get_vertex(&self, graph: &str, id: u64) -> Result<Option<Vertex>, Error>
pub fn update_vertex(&self, graph: &str, id: u64, properties: &serde_json::Value) -> Result<(), Error>
pub fn delete_vertex(&self, graph: &str, id: u64) -> Result<(), Error>  // 级联删除关联边
```

### 边操作

```rust
pub fn add_edge(&self, graph: &str, from: u64, to: u64, label: Option<&str>, properties: Option<&serde_json::Value>) -> Result<u64, Error>
pub fn get_edge(&self, graph: &str, id: u64) -> Result<Option<Edge>, Error>
pub fn delete_edge(&self, graph: &str, edge_id: u64) -> Result<(), Error>
pub fn out_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error>  // 出边
pub fn in_edges(&self, graph: &str, vertex_id: u64) -> Result<Vec<Edge>, Error>   // 入边
```

### 遍历算法

```rust
pub fn bfs(&self, graph: &str, start: u64, max_depth: u32, direction: Direction) -> Result<Vec<u64>, Error>
pub fn bfs_filter<F>(&self, graph: &str, start: u64, max_depth: u32, direction: Direction, filter: F) -> Result<Vec<u64>, Error>
    where F: Fn(&Vertex) -> bool
pub fn k_hop_neighbors(&self, graph: &str, start: u64, k: u32, direction: Direction) -> Result<Vec<u64>, Error>
pub fn shortest_path(&self, graph: &str, from: u64, to: u64, direction: Direction) -> Result<Option<Vec<u64>>, Error>
pub fn neighbors(&self, graph: &str, vertex_id: u64, direction: Direction) -> Result<Vec<u64>, Error>
```

- `bfs` — 广度优先遍历
- `bfs_filter` — 带顶点谓词过滤的 BFS
- `k_hop_neighbors` — 获取 k 跳范围内的所有顶点
- `shortest_path` — 无权最短路径（BFS）
- `neighbors` — 直接邻居（1 跳），O(1) 邻接索引

**Direction：** `Out`（出）、`In`（入）、`Both`（双向）

### 图分析

```rust
pub fn pagerank(&self, graph: &str, iterations: u32, damping: f64) -> Result<Vec<(u64, f64)>, Error>
pub fn degree_centrality(&self, graph: &str, direction: Direction) -> Result<Vec<(u64, f64)>, Error>
pub fn weighted_shortest_path(&self, graph: &str, from: u64, to: u64, weight_key: &str) -> Result<Option<(Vec<u64>, f64)>, Error>
pub fn vertex_count(&self, graph: &str) -> Result<u64, Error>
pub fn edge_count(&self, graph: &str) -> Result<u64, Error>
```

- `degree_centrality` — 度中心性，返回 (顶点ID, 中心性) 降序排列
- `weighted_shortest_path` — Dijkstra 加权最短路径，返回 (路径, 总权重)

### 标签操作

```rust
pub fn vertices_by_label(&self, graph: &str, label: &str) -> Result<Vec<Vertex>, Error>
pub fn edges_by_label(&self, graph: &str, label: &str) -> Result<Vec<Edge>, Error>
```

### 跨引擎查询（GraphRAG）

```rust
use talon::{graph_vector_search, GraphVectorQuery};

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

### 数据类型

```rust
pub struct Vertex { pub id: u64, pub label: Option<String>, pub properties: Option<serde_json::Value> }
pub struct Edge { pub id: u64, pub from: u64, pub to: u64, pub label: Option<String>, pub properties: Option<serde_json::Value> }
pub enum Direction { Out, In, Both }
```

## Neo4j / JanusGraph 兼容性

### 功能对比

| 功能 | Neo4j | JanusGraph | Talon Graph |
|------|-------|------------|-------------|
| 属性图模型 | ✅ | ✅ | ✅ |
| 顶点/边标签 | ✅ | ✅ | ✅ |
| JSON 属性 | ✅ | ✅ | ✅ |
| BFS 遍历 | ✅ | ✅ | ✅ |
| BFS 带过滤 | ✅ | ✅ | ✅ |
| K 跳邻居 | ✅ | ✅ | ✅ |
| 最短路径 | ✅ | ✅ | ✅ |
| 带权最短路径 | ✅ | ✅ | ✅（Dijkstra） |
| PageRank | ✅（GDS） | ✅ | ✅ 原生 |
| 度中心性 | ✅（GDS） | ✅ | ✅ 原生 |
| Cypher 查询语言 | ✅ | ❌ | ❌ |
| Gremlin 查询语言 | ❌ | ✅ | ❌ |
| GraphRAG 融合 | ❌ | ❌ | ✅ |
| 嵌入式模式 | ✅（Java） | ❌ | ✅（Rust） |
| 单二进制 | ❌（JVM） | ❌（JVM） | ✅ |
| 多模融合（SQL+KV+Vector） | ❌ | ❌ | ✅ |

### Talon 独有特性

- **GraphRAG** — 图遍历 + 向量搜索 + FTS 统一查询
- **跨引擎融合** — 图 + SQL 联合、图 + 地理空间查询
- **Rust API** — 零开销嵌入式图引擎，无 JVM
- **AI 原生** — 知识图谱用于 RAG、Agent 推理链

## 性能

| 基准测试 | 结果 |
|----------|------|
| 顶点写入（1M） | 127K ops/s |
| 边写入（2M） | 74K ops/s |
| 顶点读取 | 935K ops/s |
| 邻居查询 | 110K qps |
| BFS 遍历 | 6.5K qps |
