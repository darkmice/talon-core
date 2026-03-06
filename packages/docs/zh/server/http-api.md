# HTTP API

Talon Server 为所有 9 大引擎暴露 RESTful HTTP API。

## 基础 URL

```
http://localhost:8080
```

## 端点

### SQL

```
POST /api/sql
Content-Type: application/json

{"sql": "SELECT * FROM users WHERE age > 25"}
```

**带参数：**
```
POST /api/sql
{"sql": "SELECT * FROM users WHERE age > ?", "params": [25]}
```

### KV

```
POST /api/kv/set    {"key": "user:1", "value": "Alice", "ttl": 3600}
POST /api/kv/get    {"key": "user:1"}
POST /api/kv/del    {"key": "user:1"}
POST /api/kv/mset   {"keys": ["k1","k2"], "values": ["v1","v2"]}
POST /api/kv/mget   {"keys": ["k1","k2"]}
POST /api/kv/incr   {"key": "counter"}
POST /api/kv/keys   {"prefix": "user:"}
```

### 向量

```
POST /api/vector/search    {"index": "idx", "vector": [0.1, ...], "k": 10}
POST /api/vector/insert    {"index": "idx", "id": 1, "vector": [0.1, ...]}
```

### 全文搜索

```
POST /api/fts/index    {"index": "articles", "id": "doc1", "text": "..."}
POST /api/fts/search   {"index": "articles", "query": "database", "limit": 10}
POST /api/fts/_bulk     (Elasticsearch NDJSON 格式)
```

### GEO

```
POST /api/geo/add      {"name": "places", "member": "office", "lng": 116.4, "lat": 39.9}
POST /api/geo/search   {"name": "places", "lng": 116.4, "lat": 39.9, "radius": 500, "unit": "m"}
```

### 图

```
POST /api/graph/vertex    {"graph": "social", "label": "person", "properties": {...}}
POST /api/graph/edge      {"graph": "social", "from": 1, "to": 2, "label": "follows"}
POST /api/graph/bfs       {"graph": "social", "start": 1, "max_depth": 3, "direction": "out"}
```

### AI

```
POST /api/ai/session         {"id": "s1", "metadata": {}}
POST /api/ai/message         {"session_id": "s1", "role": "user", "content": "Hello"}
POST /api/ai/context         {"session_id": "s1", "last_n": 10}
POST /api/ai/memory/store    {"session_id": "s1", "text": "...", "embedding": [...]}
POST /api/ai/memory/search   {"session_id": "s1", "embedding": [...], "k": 5}
```

### 时序

```
POST /api/ts/create    {"name": "metrics", "fields": ["cpu", "mem"]}
POST /api/ts/insert    {"name": "metrics", "timestamp": 1700000000000, "values": [0.85, 0.72]}
POST /api/ts/query     {"name": "metrics", "start": 1700000000000, "limit": 100}
POST /api/ts/write     (InfluxDB 行协议)
```

### 消息队列

```
POST /api/mq/create    {"topic": "events", "max_len": 0}
POST /api/mq/publish   {"topic": "events", "payload": "..."}
POST /api/mq/poll      {"topic": "events", "group": "g1", "consumer": "c1", "count": 10}
POST /api/mq/ack       {"topic": "events", "group": "g1", "consumer": "c1", "message_id": 1}
```

### 健康检查与集群

```
GET  /health              # 健康检查
GET  /cluster/status      # 集群状态
POST /cluster/promote     # 提升 Replica 为 Primary
```

## 响应格式

所有端点返回 JSON：

```json
{
  "ok": true,
  "data": [...],
  "error": null
}
```

错误响应：
```json
{
  "ok": false,
  "data": null,
  "error": "table not found: users"
}
```
