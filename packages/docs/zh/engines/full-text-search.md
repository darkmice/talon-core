# 全文搜索引擎

倒排索引 + BM25 评分，支持 ES 兼容查询、中文分词和混合搜索。

## 概述

FTS 引擎提供全文搜索，支持 BM25 相关性评分、多种查询类型（bool/短语/term/范围/正则/通配符/模糊）、聚合、索引管理、中文分词（jieba）、以及结合 BM25 + 向量 KNN 的混合搜索（RRF 融合）。

## 快速开始

```rust
let db = Talon::open("./data")?;

db.fts()?.index_doc("articles", &FtsDoc {
    id: "doc1".into(),
    fields: vec![("content".into(), "Talon 是 AI 原生数据库".into())],
})?;

let hits = db.fts()?.search("articles", "数据库", 10)?;
for hit in &hits {
    println!("id={}, score={:.2}", hit.id, hit.score);
}
```

## API 参考

### 索引管理

```rust
pub fn create_index(&self, name: &str, config: &FtsConfig) -> Result<(), Error>  // 创建索引
pub fn drop_index(&self, name: &str) -> Result<(), Error>                        // 删除索引
pub fn list_indexes(&self) -> Result<Vec<FtsIndexInfo>, Error>                   // 列出所有索引
pub fn get_mapping(&self, name: &str) -> Result<FtsMapping, Error>               // 获取字段映射
pub fn add_alias(&self, alias: &str, index: &str) -> Result<(), Error>           // 添加别名
pub fn remove_alias(&self, alias: &str) -> Result<(), Error>                     // 删除别名
pub fn close_index(&self, name: &str) -> Result<(), Error>                       // 关闭索引
pub fn open_index(&self, name: &str) -> Result<(), Error>                        // 打开索引
pub fn reindex(&self, name: &str) -> Result<u64, Error>                          // 重建索引
```

### 文档操作

```rust
pub fn index_doc(&self, name: &str, doc: &FtsDoc) -> Result<(), Error>           // 索引文档
pub fn index_doc_batch(&self, name: &str, docs: &[FtsDoc]) -> Result<(), Error>  // 批量索引
pub fn get_doc(&self, name: &str, doc_id: &str) -> Result<Option<FtsDoc>, Error> // 获取文档
pub fn update_doc(&self, name: &str, doc_id: &str, doc: &FtsDoc) -> Result<(), Error>  // 更新文档
pub fn delete_doc(&self, name: &str, doc_id: &str) -> Result<bool, Error>        // 删除文档
pub fn update_by_query(&self, name: &str, query: &str, field: &str, value: &str) -> Result<u64, Error>
pub fn delete_by_query(&self, name: &str, query: &str, limit: usize) -> Result<u64, Error>
```

### 搜索查询

#### `search`（基本搜索）
```rust
pub fn search(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_bool`（布尔查询）
```rust
pub fn search_bool(&self, name: &str, query: &BoolQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```
ES 兼容布尔查询，支持 `must`、`should`、`must_not` 子句。

#### `search_phrase`（短语查询）
```rust
pub fn search_phrase(&self, name: &str, phrase: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_term`（精确匹配）
```rust
pub fn search_term(&self, name: &str, field: &str, term: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_range`（范围查询）
```rust
pub fn search_range(&self, name: &str, query: &RangeQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_regexp`（正则搜索）
```rust
pub fn search_regexp(&self, name: &str, pattern: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_wildcard`（通配符搜索）
```rust
pub fn search_wildcard(&self, name: &str, pattern: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```

#### `search_fuzzy`（模糊搜索）
```rust
pub fn search_fuzzy(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, Error>
```
编辑距离容错，用于纠错搜索。

#### `search_multi_field`（多字段搜索）
```rust
pub fn search_multi_field(&self, name: &str, query: &MultiFieldQuery, limit: usize) -> Result<Vec<SearchHit>, Error>
```

### 聚合

```rust
pub fn aggregate_terms(&self, name: &str, field: &str, size: usize) -> Result<Vec<TermBucket>, Error>
pub fn suggest(&self, name: &str, prefix: &str, limit: usize) -> Result<Vec<String>, Error>
pub fn search_sorted(&self, name: &str, query: &str, sort_field: &str, ascending: bool, limit: usize) -> Result<Vec<SearchHit>, Error>
pub fn search_terms(&self, name: &str, field: &str, terms: &[&str], limit: usize) -> Result<Vec<SearchHit>, Error>
```

- `suggest` — 前缀自动补全
- `search_sorted` — 带自定义排序的搜索
- `search_terms` — 多 term 匹配搜索

### 混合搜索（BM25 + 向量）

通过 Reciprocal Rank Fusion (RRF) 融合全文和向量搜索结果。

```rust
use talon::hybrid_search;

let hits = hybrid_search(&db.store_ref(), &HybridQuery {
    fts_index: "articles",
    vec_index: "emb_idx",
    query_text: "AI 数据库",
    query_vec: &embedding,
    limit: 10,
    ..Default::default()
})?;
```

### 分析器

| 分析器 | 说明 |
|--------|------|
| `Standard` | Unicode 分词（默认） |
| `Jieba` | 中文分词 |
| `Whitespace` | 按空格分割 |
| `Keyword` | 不分词（精确匹配） |

### SearchHit 结构

```rust
pub struct SearchHit {
    pub id: String,                   // 文档 ID
    pub score: f32,                   // BM25 分数
    pub highlights: Vec<String>,      // 高亮片段（<em> 标签）
    pub doc: Option<serde_json::Value>, // 原始文档
}
```

## Elasticsearch 兼容性

### ES `_bulk` API

支持 Elasticsearch NDJSON 批量格式，方便迁移：

```rust
use talon::parse_es_bulk;

let ndjson = r#"{"index":{"_index":"articles","_id":"1"}}
{"title":"Hello","body":"World"}
"#;
let items = parse_es_bulk(ndjson)?;
for item in &items {
    db.fts()?.index_doc(&item.index, &item.doc)?;
}
```

支持的 action：`index`、`create`。不支持：`delete`、`update`（静默跳过）。

### 功能对比

| 功能 | Elasticsearch | Talon FTS |
|------|--------------|-----------|
| BM25 评分 | ✅ | ✅ |
| Term / 短语 / 布尔查询 | ✅ | ✅ |
| 通配符 / 正则 / 模糊 | ✅ | ✅ |
| 范围查询 | ✅ | ✅ |
| 多字段搜索 | ✅ | ✅ |
| Terms 聚合 | ✅ | ✅ |
| 自动补全（suggest） | ✅ | ✅ |
| 索引别名 | ✅ | ✅ |
| `_bulk` NDJSON API | ✅ | ✅ |
| 混合搜索（BM25 + 向量 RRF） | ❌ | ✅ |
| 中文分词（结巴） | 插件 | ✅ 内置 |
| 复杂聚合管道 | ✅ | ❌ |
| 嵌套/父子文档 | ✅ | ❌ |
| 分布式分片 | ✅ | ❌ |
| JVM 依赖 | ✅ (Java) | ❌（单二进制） |

### Talon 独有特性

- **混合搜索** — BM25 + 向量通过 RRF 融合，单次查询
- **跨引擎融合** — FTS + Graph、FTS + Vector 三路搜索
- **零外部依赖** — 无 JVM，无集群配置
- **嵌入式模式** — 进程内运行，无网络开销

## 性能

| 基准测试 | 结果 |
|----------|------|
| 索引吞吐 | ~50K docs/s |
| 搜索延迟（100K 文档） | < 5ms |
| 混合搜索 | < 10ms |
