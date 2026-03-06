# 向量引擎

自研 HNSW 索引，支持推荐/发现 API、元数据过滤、SQ8 量化和亚毫秒级搜索。

## 概述

向量引擎使用自研 HNSW 实现提供近似最近邻（ANN）搜索，支持多种距离度量、SQL 集成、元数据过滤、标量量化、快照一致性搜索，以及 Qdrant 兼容的推荐/发现 API。

## 快速开始

```rust
let db = Talon::open("./data")?;

db.run_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, emb VECTOR(384))")?;
db.run_sql("CREATE VECTOR INDEX idx ON docs(emb) USING HNSW")?;

db.run_sql("INSERT INTO docs VALUES (1, '[0.1, 0.2, ...]')")?;

let ve = db.vector("idx")?;
let query = vec![0.1f32; 384];
let hits = ve.search(&query, 10, "cosine")?;
```

## API 参考

### 获取引擎句柄

```rust
pub fn vector(&self, name: &str) -> Result<VectorEngine, Error>      // 写模式
pub fn vector_read(&self, name: &str) -> Result<VectorEngine, Error>  // 只读模式
pub fn vector_set_ef_search(&self, name: &str, ef_search: usize) -> Result<(), Error>
```

### 搜索

#### `search`
```rust
pub fn search(&self, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, Error>
```
K 最近邻搜索。`metric` 支持 `"cosine"`、`"l2"`、`"dot"`。

#### `batch_search`
```rust
pub fn batch_search(&self, queries: &[&[f32]], k: usize, metric: &str) -> Result<Vec<Vec<(u64, f32)>>, Error>
```
批量搜索多个查询向量。

#### `search_with_filter`
```rust
pub fn search_with_filter(&self, query: &[f32], k: usize, filter: &[MetaFilter]) -> Result<Vec<SearchResult>, Error>
```
带元数据过滤的搜索。

**MetaFilter 操作符：** `Eq`, `Ne`, `Gt`, `Lt`, `Gte`, `Lte`, `In`

```rust
let filter = vec![
    MetaFilter { field: "category".into(), op: MetaFilterOp::Eq, value: MetaValue::Text("tech".into()) },
];
let hits = ve.search_with_filter(&query, 10, &filter)?;
```

#### `recommend`
```rust
pub fn recommend(&self, positive: &[&[f32]], negative: &[&[f32]], k: usize) -> Result<Vec<RecommendHit>, Error>
```
基于正负样本的推荐（Qdrant 兼容）。

#### `discover`
```rust
pub fn discover(&self, target: &[f32], context: &[(&[f32], &[f32])], k: usize) -> Result<Vec<DiscoverHit>, Error>
```
上下文重排序搜索（Qdrant 兼容）。

### 写入

```rust
pub fn insert(&self, id: u64, vec: &[f32]) -> Result<(), Error>
pub fn insert_batch(&self, items: &[(u64, &[f32])]) -> Result<(), Error>
pub fn insert_with_metadata(&self, id: u64, vec: &[f32], metadata: HashMap<String, MetaValue>) -> Result<(), Error>
pub fn delete(&self, id: u64) -> Result<(), Error>
```

### 读取

```rust
pub fn get_vector(&self, id: u64) -> Result<Option<Vec<f32>>, Error>
pub fn count(&self) -> Result<u64, Error>
```

### 量化

```rust
pub fn enable_quantization(&self) -> Result<(), Error>    // 启用 SQ8（4:1 压缩，<2% 精度损失）
pub fn disable_quantization(&self) -> Result<(), Error>   // 禁用量化
pub fn is_quantized(&self) -> Result<bool, Error>         // 检查是否启用
```

### 索引管理

```rust
pub fn set_ef_search(&self, ef_search: usize) -> Result<(), Error>  // 设置运行时搜索宽度
pub fn rebuild_index(&self) -> Result<u64, Error>                    // 重建 HNSW 索引
```

### 元数据操作

```rust
pub fn set_metadata(&self, id: u64, metadata: HashMap<String, MetaValue>) -> Result<(), Error>
pub fn get_metadata(&self, id: u64) -> Result<Option<HashMap<String, MetaValue>>, Error>
pub fn delete_metadata(&self, id: u64) -> Result<(), Error>
```

### 快照搜索

```rust
pub fn snapshot_search(&self, snapshot: &Snapshot, query: &[f32], k: usize, metric: &str) -> Result<Vec<(u64, f32)>, Error>
```
基于时间点快照搜索，确保读一致性。适用于并发写入场景。

## SQL 集成

```sql
SELECT id, vec_cosine(emb, '[0.1, 0.2, ...]') AS score FROM docs ORDER BY score LIMIT 10;
SELECT id, vec_l2(emb, '[0.1, 0.2, ...]') AS dist FROM docs ORDER BY dist LIMIT 10;
SELECT id, vec_dot(emb, '[0.1, 0.2, ...]') AS sim FROM docs ORDER BY sim DESC LIMIT 10;
```

## 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `m` | 16 | HNSW 每节点最大连接数 |
| `ef_construction` | 200 | 构建时搜索宽度 |
| `ef_search` | 50 | 运行时搜索宽度 |
| 距离度量 | Cosine | 也支持 L2、内积 |

## Milvus / Qdrant / Pinecone 兼容性

### 功能对比

| 功能 | Milvus | Qdrant | Pinecone | Talon Vector |
|------|--------|--------|----------|--------------|
| HNSW 索引 | ✅ | ✅ | ✅ | ✅ |
| Cosine / L2 / Dot 度量 | ✅ | ✅ | ✅ | ✅ |
| 元数据过滤 | ✅ | ✅ | ✅ | ✅ |
| 批量插入/删除 | ✅ | ✅ | ✅ | ✅ |
| 快照一致性读取 | ❌ | ❌ | ❌ | ✅ |
| SQL 混合查询 | ❌ | ❌ | ❌ | ✅ 原生 |
| 乘积量化（PQ） | ✅ | ✅ | ✅ | ✅ |
| IVF 索引 | ✅ | ❌ | ❌ | ❌ |
| GPU 加速 | ✅ | ❌ | ✅ | ❌ |
| 分布式分片 | ✅ | ✅ | ✅（托管） | ❌ |
| 嵌入式模式 | ❌ | ❌ | ❌ | ✅ |
| 单二进制 | ❌ | ✅ | ❌（SaaS） | ✅ |
| 多模融合（SQL+KV+FTS） | ❌ | ❌ | ❌ | ✅ |

### Talon 独有特性

- **SQL 原生向量搜索** — `SELECT vec_cosine(emb, ...) FROM docs WHERE category='ai'`
- **跨引擎融合** — 向量 + FTS 混合搜索（RRF）、向量 + 图遍历
- **快照搜索** — 并发写入时的时间点一致性读取
- **嵌入式部署** — 进程内运行，AI 应用零网络开销

## 性能

| 基准测试 | 结果 |
|----------|------|
| INSERT（100K，HNSW） | 1,057 vec/s |
| KNN 搜索（k=10，100K 向量） | P95 0.1ms |
