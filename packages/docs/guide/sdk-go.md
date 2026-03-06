# Go SDK

The Go SDK uses cgo to statically link `libtalon.a`, producing a single binary with zero runtime dependencies.

## Installation

```bash
go get github.com/darkmice/talon-sdk/go
```

## Quick Start

```go
package main

import (
    "fmt"
    talon "github.com/darkmice/talon-sdk/go"
)

func main() {
    db, err := talon.Open("./data")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // SQL
    rows, _ := db.SQL("SELECT 1 + 1 AS result")
    fmt.Println(rows)
}
```

## SQL

```go
result, err := db.SQL("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
result, err = db.SQL("INSERT INTO users VALUES (1, 'Alice')")
result, err = db.SQL("SELECT * FROM users WHERE id = 1")
```

## KV

```go
// Basic CRUD
db.KvSet("user:1", "Alice", nil)
val, _ := db.KvGet("user:1")
db.KvDel("user:1")
exists, _ := db.KvExists("user:1")

// TTL
ttl := uint64(3600)
db.KvSet("session:abc", "token", &ttl)
db.KvExpire("session:abc", 1800)
remaining, _ := db.KvTtl("session:abc")

// Atomic operations
newVal, _ := db.KvIncr("counter")
newVal, _ = db.KvIncrBy("counter", 10)
newVal, _ = db.KvDecrBy("counter", 5)
wasSet, _ := db.KvSetNX("lock:job1", "worker1", &ttl)

// Batch
db.KvMset([]string{"k1", "k2"}, []string{"v1", "v2"})
vals, _ := db.KvMget([]string{"k1", "k2"})

// Scan
keys, _ := db.KvKeys("user:")
keys, _ = db.KvKeysMatch("user:*")
keys, _ = db.KvKeysLimit("user:", 0, 100)    // paginated
pairs, _ := db.KvScanLimit("user:", 0, 100)  // key-value pairs
count, _ := db.KvCount()
```

## Vector

```go
db.VectorCreate("embeddings", 384, "cosine")
db.VectorInsert("embeddings", 1, []float32{0.1, 0.2, ...})
results, _ := db.VectorSearch("embeddings", queryVec, 10, "cosine")
db.VectorDelete("embeddings", 1)
info, _ := db.VectorInfo("embeddings")
```

## Time Series

```go
db.TsCreate("metrics", []string{"host"}, []string{"cpu", "mem"})
db.TsInsert("metrics", map[string]interface{}{
    "host": "srv1", "cpu": 85.5, "mem": 4096,
})
points, _ := db.TsQuery("metrics", map[string]interface{}{
    "start": "2024-01-01T00:00:00Z",
    "end":   "2024-12-31T23:59:59Z",
})
agg, _ := db.TsAggregate("metrics", "cpu", "avg", nil)
```

## Message Queue

```go
db.MqCreate("events")
db.MqPublish("events", map[string]interface{}{"type": "login", "user": "alice"})
msgs, _ := db.MqPoll("events", 10)
db.MqAck("events", msgID)
db.MqSubscribe("events", "consumer1")
```

## Full-Text Search

```go
db.FtsCreateIndex("articles")
db.FtsIndex("articles", "doc1", map[string]string{
    "title": "Introduction to AI",
    "body":  "Artificial intelligence is transforming...",
})
hits, _ := db.FtsSearch("articles", "artificial intelligence", 10)
hits, _ = db.FtsSearchFuzzy("articles", "artifcial", 2, 10)

// Hybrid search (BM25 + vector)
results, _ := db.FtsHybridSearch("articles", "vectors", "AI search",
    queryVec, &talon.HybridSearchOpts{
        Metric: "cosine", Limit: 10,
        FtsWeight: 0.7, VecWeight: 0.3,
    })

db.FtsAddAlias("articles_v2", "articles")
db.FtsReindex("articles")
mapping, _ := db.FtsGetMapping("articles")
indexes, _ := db.FtsListIndexes()
```

## Geospatial

```go
db.GeoCreate("shops")
db.GeoAdd("shops", "starbucks", 121.4737, 31.2304)
db.GeoAddBatch("shops", []talon.GeoMember{
    {Key: "mcdonalds", Lng: 121.4800, Lat: 31.2350},
    {Key: "kfc", Lng: 121.4650, Lat: 31.2280},
})

// Position & distance
lng, lat, found, _ := db.GeoPos("shops", "starbucks")
dist, _ := db.GeoDist("shops", "starbucks", "mcdonalds", "km")

// Search
nearby, _ := db.GeoSearch("shops", 121.47, 31.23, 1000, "m", nil)
inBox, _ := db.GeoSearchBox("shops", 121.46, 31.22, 121.49, 31.24, nil)
inside, _ := db.GeoFence("shops", "starbucks", 121.47, 31.23, 500, "m")
members, _ := db.GeoMembers("shops")
```

## Graph

```go
db.GraphCreate("social")

// Vertices
v1, _ := db.GraphAddVertex("social", "person", map[string]string{"name": "Alice"})
v2, _ := db.GraphAddVertex("social", "person", map[string]string{"name": "Bob"})
vertex, _ := db.GraphGetVertex("social", v1)
db.GraphUpdateVertex("social", v1, map[string]string{"name": "Alice W."})

// Edges
e1, _ := db.GraphAddEdge("social", v1, v2, "knows", map[string]string{"since": "2024"})
edge, _ := db.GraphGetEdge("social", e1)

// Traversal
neighbors, _ := db.GraphNeighbors("social", v1, "out")
outEdges, _ := db.GraphOutEdges("social", v1)
inEdges, _ := db.GraphInEdges("social", v2)
byLabel, _ := db.GraphVerticesByLabel("social", "person")

// Algorithms
path, _ := db.GraphShortestPath("social", v1, v2, 10)
wPath, _ := db.GraphWeightedShortestPath("social", v1, v2, 10, "weight")
bfs, _ := db.GraphBFS("social", v1, 3, "out")
centrality, _ := db.GraphDegreeCentrality("social", 10)
pagerank, _ := db.GraphPageRank("social", 0.85, 20, 10)

// Stats
vCount, _ := db.GraphVertexCount("social")
eCount, _ := db.GraphEdgeCount("social")
```

## AI (Session / Context / Memory / Trace)

```go
// Session management
db.AiCreateSession("s1", nil, nil)
session, _ := db.AiGetSession("s1")
db.AiDeleteSession("s1")
sessions, _ := db.AiListSessions(10, 0)

// Context / Messages
db.AiAppendMessage("s1", map[string]interface{}{
    "role": "user", "content": "What is Talon?",
})
history, _ := db.AiGetHistory("s1", nil)
db.AiClearContext("s1")

// Memory
db.AiStoreMemory("s1", map[string]interface{}{
    "key": "preference", "value": "dark mode",
})
memories, _ := db.AiSearchMemory("s1", "preference", 10)

// Trace
db.AiLogTrace("s1", map[string]interface{}{
    "event": "llm_call", "model": "gpt-4", "latency_ms": 230,
})
traces, _ := db.AiQueryTraces("s1", nil)
```

## Backup

```go
exported, _ := db.ExportDb("/backup/dir", nil)
imported, _ := db.ImportDb("/backup/dir")
```

## Ops

```go
stats, _ := db.Stats()
dbStats, _ := db.DatabaseStats()
health := db.HealthCheck()
db.Persist()
```
