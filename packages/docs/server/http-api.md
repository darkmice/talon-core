# HTTP API

Talon Server exposes a RESTful HTTP API for all 9 engines.

## Base URL

```
http://localhost:8080
```

## Endpoints

### SQL

```
POST /api/sql
Content-Type: application/json

{"sql": "SELECT * FROM users WHERE age > 25"}
```

**With parameters:**
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

### Vector

```
POST /api/vector/search    {"index": "idx", "vector": [0.1, ...], "k": 10}
POST /api/vector/insert    {"index": "idx", "id": 1, "vector": [0.1, ...]}
```

### Full-Text Search

```
POST /api/fts/index    {"index": "articles", "id": "doc1", "text": "..."}
POST /api/fts/search   {"index": "articles", "query": "database", "limit": 10}
POST /api/fts/_bulk     (Elasticsearch NDJSON format)
```

### GEO

```
POST /api/geo/add      {"name": "places", "member": "office", "lng": 116.4, "lat": 39.9}
POST /api/geo/search   {"name": "places", "lng": 116.4, "lat": 39.9, "radius": 500, "unit": "m"}
```

### Graph

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

### TimeSeries

```
POST /api/ts/create    {"name": "metrics", "fields": ["cpu", "mem"]}
POST /api/ts/insert    {"name": "metrics", "timestamp": 1700000000000, "values": [0.85, 0.72]}
POST /api/ts/query     {"name": "metrics", "start": 1700000000000, "limit": 100}
POST /api/ts/write     (InfluxDB Line Protocol)
```

### MessageQueue

```
POST /api/mq/create    {"topic": "events", "max_len": 0}
POST /api/mq/publish   {"topic": "events", "payload": "..."}
POST /api/mq/poll      {"topic": "events", "group": "g1", "consumer": "c1", "count": 10}
POST /api/mq/ack       {"topic": "events", "group": "g1", "consumer": "c1", "message_id": 1}
```

### Health & Cluster

```
GET  /health              # Health check
GET  /cluster/status      # Cluster status
POST /cluster/promote     # Promote replica to primary
```

## Response Format

All endpoints return JSON:

```json
{
  "ok": true,
  "data": [...],
  "error": null
}
```

Error response:
```json
{
  "ok": false,
  "data": null,
  "error": "table not found: users"
}
```
