# Python SDK

The Python SDK uses `ctypes` to load `libtalon.dylib`/`.so` at runtime.

## Installation

```bash
pip install talon-db
```

Native library is auto-downloaded from GitHub Releases on first use. No compilation required.

## Quick Start

```python
from talon import Talon

db = Talon("./data")

db.sql("SELECT 1 + 1 AS result")

db.close()
```

## SQL

```python
db.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
db.sql("INSERT INTO users VALUES (1, 'Alice')")
rows = db.sql("SELECT * FROM users WHERE id = 1")
```

## KV

```python
# Basic CRUD
db.kv_set("user:1", "Alice")
val = db.kv_get("user:1")
db.kv_del("user:1")
exists = db.kv_exists("user:1")

# TTL
db.kv_set("session:abc", "token", ttl=3600)
db.kv_expire("session:abc", 1800)
remaining = db.kv_ttl("session:abc")

# Atomic operations
new_val = db.kv_incr("counter")
new_val = db.kv_incr_by("counter", 10)
new_val = db.kv_decr_by("counter", 5)
was_set = db.kv_set_nx("lock:job1", "worker1", ttl=30)

# Batch
db.kv_mset(["k1", "k2"], ["v1", "v2"])
vals = db.kv_mget(["k1", "k2"])

# Scan
keys = db.kv_keys("user:")
keys = db.kv_keys_match("user:*")
keys = db.kv_keys_limit("user:", offset=0, limit=100)
pairs = db.kv_scan_limit("user:", offset=0, limit=100)
count = db.kv_count()
```

## Vector

```python
db.vector_create("embeddings", 384, "cosine")
db.vector_insert("embeddings", 1, [0.1, 0.2, ...])
results = db.vector_search("embeddings", query_vec, k=10)
db.vector_delete("embeddings", 1)
info = db.vector_info("embeddings")
```

## Time Series

```python
db.ts_create("metrics", tags=["host"], fields=["cpu", "mem"])
db.ts_insert("metrics", {"host": "srv1", "cpu": 85.5, "mem": 4096})
points = db.ts_query("metrics", start="2024-01-01T00:00:00Z")
agg = db.ts_aggregate("metrics", "cpu", "avg")
```

## Message Queue

```python
db.mq_create("events")
db.mq_publish("events", {"type": "login", "user": "alice"})
msgs = db.mq_poll("events", count=10)
db.mq_ack("events", msg_id)
db.mq_subscribe("events", "consumer1")
```

## Full-Text Search

```python
db.fts_create_index("articles")
db.fts_index("articles", "doc1", {
    "title": "Introduction to AI",
    "body": "Artificial intelligence is transforming...",
})
hits = db.fts_search("articles", "artificial intelligence", limit=10)
hits = db.fts_search_fuzzy("articles", "artifcial", max_dist=2, limit=10)

# Hybrid search (BM25 + vector)
results = db.fts_hybrid_search(
    "articles", "vectors", "AI search", query_vec,
    metric="cosine", limit=10,
    fts_weight=0.7, vec_weight=0.3,
)

db.fts_add_alias("articles_v2", "articles")
db.fts_reindex("articles")
mapping = db.fts_get_mapping("articles")
indexes = db.fts_list_indexes()
```

## Geospatial

```python
db.geo_create("shops")
db.geo_add("shops", "starbucks", lng=121.4737, lat=31.2304)
db.geo_add_batch("shops", [
    {"key": "mcdonalds", "lng": 121.48, "lat": 31.235},
    {"key": "kfc", "lng": 121.465, "lat": 31.228},
])

# Position & distance
pos = db.geo_pos("shops", "starbucks")      # {"lng": ..., "lat": ...}
dist = db.geo_dist("shops", "starbucks", "mcdonalds", unit="km")

# Search
nearby = db.geo_search("shops", lng=121.47, lat=31.23, radius=1000)
in_box = db.geo_search_box("shops", 121.46, 31.22, 121.49, 31.24)
inside = db.geo_fence("shops", "starbucks", 121.47, 31.23, 500)
members = db.geo_members("shops")
```

## Graph

```python
db.graph_create("social")

# Vertices
v1 = db.graph_add_vertex("social", "person", {"name": "Alice"})
v2 = db.graph_add_vertex("social", "person", {"name": "Bob"})
vertex = db.graph_get_vertex("social", v1)
db.graph_update_vertex("social", v1, {"name": "Alice W."})

# Edges
e1 = db.graph_add_edge("social", v1, v2, "knows", {"since": "2024"})
edge = db.graph_get_edge("social", e1)

# Traversal
neighbors = db.graph_neighbors("social", v1, direction="out")
out_edges = db.graph_out_edges("social", v1)
in_edges = db.graph_in_edges("social", v2)
by_label = db.graph_vertices_by_label("social", "person")

# Algorithms
path = db.graph_shortest_path("social", v1, v2)
w_path = db.graph_weighted_shortest_path("social", v1, v2, weight_key="weight")
bfs = db.graph_bfs("social", v1, max_depth=3)
centrality = db.graph_degree_centrality("social", limit=10)
pagerank = db.graph_pagerank("social", damping=0.85, iterations=20, limit=10)

# Stats
v_count = db.graph_vertex_count("social")
e_count = db.graph_edge_count("social")
```

## AI (Session / Context / Memory / Trace)

```python
# Session management
db.ai_create_session("s1")
session = db.ai_get_session("s1")
db.ai_delete_session("s1")
sessions = db.ai_list_sessions(limit=10)

# Context / Messages
db.ai_append_message("s1", {"role": "user", "content": "What is Talon?"})
history = db.ai_get_history("s1")
db.ai_clear_context("s1")

# Memory
db.ai_store_memory("s1", {"key": "preference", "value": "dark mode"})
memories = db.ai_search_memory("s1", "preference", limit=10)

# Trace
db.ai_log_trace("s1", {"event": "llm_call", "model": "gpt-4", "latency_ms": 230})
traces = db.ai_query_traces("s1")
```

## Backup & Ops

```python
exported = db.export_db("/backup/dir")
imported = db.import_db("/backup/dir")

stats = db.database_stats()
health = db.health_check()
db.persist()
```
