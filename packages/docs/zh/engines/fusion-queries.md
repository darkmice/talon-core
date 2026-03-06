# 跨引擎融合查询

Talon 独有的多引擎融合查询能力，在单次操作中组合不同引擎的结果。

## 概述

跨引擎查询允许组合不同引擎的优势 — 例如地理过滤后进行向量相似度排序，或图遍历后进行全文评分。

## 可用组合

| 组合 | 函数 | 用途 |
|------|------|------|
| GEO + 向量 | `geo_vector_search` | 附近语义搜索 |
| GEO + 向量（矩形） | `geo_box_vector_search` | 矩形区域 + 向量 |
| 图 + 向量 | `graph_vector_search` | GraphRAG |
| 图 + 全文 | `graph_fts_search` | 知识图谱检索 |
| 全文 + 向量 | `hybrid_search` | 混合检索（RRF） |
| 图 + 全文 + 向量 | `triple_search` | 三重融合搜索 |

## API 参考

### `geo_vector_search`

地理圆形过滤 → 向量相似度排序。

```rust
let hits = geo_vector_search(&store, &GeoVectorQuery {
    geo_name: "places",
    vec_name: "embeddings",
    lng: 116.4074, lat: 39.9042,
    radius_m: 1000.0,
    query_vec: &embedding,
    k: 10,
})?;
```

### `geo_box_vector_search`

地理矩形过滤 → 向量相似度排序。

```rust
let hits = geo_box_vector_search(&store, &GeoBoxVectorQuery {
    geo_name: "places", vec_name: "embeddings",
    lng: 116.4074, lat: 39.9042,
    width_m: 2000.0, height_m: 2000.0,
    query_vec: &embedding, k: 10,
})?;
```

### `graph_vector_search`

图遍历 → 向量相似度排序（GraphRAG）。

```rust
let hits = graph_vector_search(&store, &GraphVectorQuery {
    graph: "knowledge", vec_name: "embeddings",
    start: root_vertex_id,
    max_depth: 3, direction: Direction::Out,
    query_vec: &embedding, k: 10,
})?;
```

### `graph_fts_search`

图遍历 → BM25 全文排序。

```rust
let hits = graph_fts_search(&store, &GraphFtsQuery {
    graph: "knowledge", fts_name: "articles",
    start: root_vertex_id,
    max_depth: 2, direction: Direction::Out,
    query: "AI 数据库", k: 10,
})?;
```

### `hybrid_search`

BM25 全文 + 向量 KNN 通过 Reciprocal Rank Fusion (RRF) 融合。

```rust
let hits = hybrid_search(&store, &HybridQuery {
    fts_index: "articles", vec_index: "emb_idx",
    query_text: "AI 数据库",
    query_vec: &embedding,
    limit: 10,
    ..Default::default()
})?;
```

### `triple_search`

三重融合：图遍历 + 全文 + 向量。

```rust
let hits = triple_search(&store, &TripleQuery {
    graph: "knowledge",
    fts_name: "articles", vec_name: "embeddings",
    start: root_vertex_id,
    max_depth: 2, direction: Direction::Out,
    text_query: "AI 数据库",
    vec_query: &embedding, k: 10,
})?;
```

## AI 应用模式

### RAG 管道
```
用户查询 → Embedding → hybrid_search(全文 + 向量) → LLM 上下文
```

### GraphRAG
```
用户查询 → 识别根节点 → graph_vector_search(图 + 向量) → LLM 上下文
```

### 位置感知 AI
```
用户位置 → geo_vector_search(GEO + 向量) → 附近语义结果 → LLM
```
