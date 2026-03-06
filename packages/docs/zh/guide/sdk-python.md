# Python SDK

Python SDK 通过 `ctypes` 加载 `libtalon.dylib`/`.so`，无需安装第三方依赖。

## 安装

```bash
pip install talon-db
```

Native library 在首次使用时自动从 GitHub Releases 下载，无需手动编译。

## 快速开始

```python
from talon import Talon

db = Talon("./data")
db.sql("SELECT 1 + 1 AS result")
db.close()
```

## API 速查

### SQL

```python
db.sql("CREATE TABLE users (id INT, name TEXT)")
```

### KV

```python
db.kv_set("key", "value")
val = db.kv_get("key")
db.kv_set_nx("lock", "1", ttl=30)
db.kv_incr_by("counter", 10)
db.kv_decr_by("counter", 5)
keys = db.kv_keys_limit("user:", offset=0, limit=100)
count = db.kv_count()
```

### FTS（全文搜索）

```python
db.fts_create_index("articles")
db.fts_index("articles", "doc1", {"title": "Hello World"})
hits = db.fts_search("articles", "hello", limit=10)
fuzzy = db.fts_search_fuzzy("articles", "helo", max_dist=2)
hybrid = db.fts_hybrid_search("articles", "vecs", "query", vec,
    fts_weight=0.7, vec_weight=0.3)
```

### Geo（地理空间）

```python
db.geo_create("shops")
db.geo_add("shops", "starbucks", lng=121.47, lat=31.23)
nearby = db.geo_search("shops", lng=121.47, lat=31.23, radius=1000)
inside = db.geo_fence("shops", "starbucks", 121.47, 31.23, 500)
members = db.geo_members("shops")
```

### Graph（图引擎）

```python
db.graph_create("social")
v1 = db.graph_add_vertex("social", "person", {"name": "Alice"})
v2 = db.graph_add_vertex("social", "person", {"name": "Bob"})
db.graph_add_edge("social", v1, v2, "knows")
path = db.graph_shortest_path("social", v1, v2)
pagerank = db.graph_pagerank("social", damping=0.85, iterations=20)
```

### AI（Session / Context / Memory / Trace）

```python
db.ai_create_session("s1")
db.ai_append_message("s1", {"role": "user", "content": "hi"})
history = db.ai_get_history("s1")
db.ai_store_memory("s1", {"key": "pref", "value": "dark"})
```

### Vector / TS / MQ / Backup / Ops

```python
db.vector_insert("idx", 1, [0.1, 0.2])
db.ts_insert("metrics", {"cpu": 85.5})
db.mq_publish("events", {"type": "login"})
db.export_db("/backup")
db.persist()
```
