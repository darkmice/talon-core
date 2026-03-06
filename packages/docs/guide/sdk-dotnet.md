# .NET SDK

The .NET SDK uses P/Invoke to load `libtalon.dylib`/`.so` at runtime.

## Installation

```bash
dotnet add package TalonDb
```

Native library is included in the NuGet package (`runtimes/{rid}/native/`). No compilation required.

## Quick Start

```csharp
using TalonDb;

using var db = new TalonClient("./data");

var result = db.Sql("SELECT 1 + 1 AS result");
Console.WriteLine(result);
```

## SQL

```csharp
db.Sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
db.Sql("INSERT INTO users VALUES (1, 'Alice')");
var rows = db.Sql("SELECT * FROM users WHERE id = 1");
```

## KV

```csharp
// Basic CRUD
db.KvSet("user:1", "Alice");
var val = db.KvGet("user:1");
db.KvDel("user:1");
var exists = db.KvExists("user:1");

// TTL
db.KvSet("session:abc", "token", 3600);
db.KvExpire("session:abc", 1800);
var remaining = db.KvTtl("session:abc");

// Atomic operations
var newVal = db.KvIncr("counter");
newVal = db.KvIncrBy("counter", 10);
newVal = db.KvDecrBy("counter", 5);
var wasSet = db.KvSetNx("lock:job1", "worker1", 30);

// Batch
db.KvMset(new[] { "k1", "k2" }, new[] { "v1", "v2" });
var vals = db.KvMget(new[] { "k1", "k2" });

// Scan
var keys = db.KvKeys("user:");
var matched = db.KvKeysMatch("user:*");
var paged = db.KvKeysLimit("user:", 0, 100);
var pairs = db.KvScanLimit("user:", 0, 100);
var count = db.KvCount();
```

## Vector

```csharp
db.VectorCreate("embeddings", 384, "cosine");
db.VectorInsert("embeddings", 1, new float[] { 0.1f, 0.2f });
var results = db.VectorSearch("embeddings", queryVec, 10, "cosine");
db.VectorDelete("embeddings", 1);
var info = db.VectorInfo("embeddings");
```

## Time Series

```csharp
db.TsCreate("metrics", new[] { "host" }, new[] { "cpu", "mem" });
db.TsInsert("metrics", new() { ["host"] = "srv1", ["cpu"] = 85.5, ["mem"] = 4096 });
var points = db.TsQuery("metrics", new() { ["start"] = "2024-01-01T00:00:00Z" });
var agg = db.TsAggregate("metrics", "cpu", "avg");
```

## Message Queue

```csharp
db.MqCreate("events");
db.MqPublish("events", new() { ["type"] = "login", ["user"] = "alice" });
var msgs = db.MqPoll("events", 10);
db.MqAck("events", msgId);
db.MqSubscribe("events", "consumer1");
```

## Full-Text Search

```csharp
db.FtsCreateIndex("articles");
db.FtsIndex("articles", "doc1", new() {
    ["title"] = "Introduction to AI",
    ["body"] = "Artificial intelligence is transforming...",
});
var hits = db.FtsSearch("articles", "artificial intelligence", 10);
var fuzzy = db.FtsSearchFuzzy("articles", "artifcial", 2, 10);

// Hybrid search (BM25 + vector)
var hybrid = db.FtsHybridSearch("articles", "vectors",
    "AI search", queryVec,
    new() { ["metric"] = "cosine", ["limit"] = 10,
            ["fts_weight"] = 0.7, ["vec_weight"] = 0.3 });

db.FtsAddAlias("articles_v2", "articles");
db.FtsReindex("articles");
var mapping = db.FtsGetMapping("articles");
var indexes = db.FtsListIndexes();
```

## Geospatial

```csharp
db.GeoCreate("shops");
db.GeoAdd("shops", "starbucks", 121.4737, 31.2304);
db.GeoAddBatch("shops", new[] {
    new Dictionary<string, object> { ["key"] = "mcdonalds", ["lng"] = 121.48, ["lat"] = 31.235 },
    new Dictionary<string, object> { ["key"] = "kfc", ["lng"] = 121.465, ["lat"] = 31.228 },
});

// Position & distance
var pos = db.GeoPos("shops", "starbucks");
var dist = db.GeoDist("shops", "starbucks", "mcdonalds", "km");

// Search
var nearby = db.GeoSearch("shops", 121.47, 31.23, 1000);
var inBox = db.GeoSearchBox("shops", 121.46, 31.22, 121.49, 31.24);
var inside = db.GeoFence("shops", "starbucks", 121.47, 31.23, 500);
var members = db.GeoMembers("shops");
```

## Graph

```csharp
db.GraphCreate("social");

// Vertices
var v1 = db.GraphAddVertex("social", "person", new() { ["name"] = "Alice" });
var v2 = db.GraphAddVertex("social", "person", new() { ["name"] = "Bob" });
var vertex = db.GraphGetVertex("social", v1);
db.GraphUpdateVertex("social", v1, new() { ["name"] = "Alice W." });

// Edges
var e1 = db.GraphAddEdge("social", v1, v2, "knows", new() { ["since"] = "2024" });
var edge = db.GraphGetEdge("social", e1);

// Traversal
var neighbors = db.GraphNeighbors("social", v1);
var outEdges = db.GraphOutEdges("social", v1);
var inEdges = db.GraphInEdges("social", v2);
var byLabel = db.GraphVerticesByLabel("social", "person");

// Algorithms
var path = db.GraphShortestPath("social", v1, v2);
var wPath = db.GraphWeightedShortestPath("social", v1, v2);
var bfs = db.GraphBfs("social", v1, 3);
var centrality = db.GraphDegreeCentrality("social", 10);
var pagerank = db.GraphPagerank("social", 0.85, 20, 10);

// Stats
var vCount = db.GraphVertexCount("social");
var eCount = db.GraphEdgeCount("social");
```

## AI (Session / Context / Memory / Trace)

```csharp
// Session management
db.AiCreateSession("s1");
var session = db.AiGetSession("s1");
db.AiDeleteSession("s1");
var sessions = db.AiListSessions(10);

// Context / Messages
db.AiAppendMessage("s1", new() { ["role"] = "user", ["content"] = "What is Talon?" });
var history = db.AiGetHistory("s1");
db.AiClearContext("s1");

// Memory
db.AiStoreMemory("s1", new() { ["key"] = "preference", ["value"] = "dark mode" });
var memories = db.AiSearchMemory("s1", "preference", 10);

// Trace
db.AiLogTrace("s1", new() { ["event"] = "llm_call", ["model"] = "gpt-4", ["latency_ms"] = 230 });
var traces = db.AiQueryTraces("s1");
```

## Backup & Ops

```csharp
var exported = db.ExportDb("/backup/dir");
var imported = db.ImportDb("/backup/dir");

var stats = db.DatabaseStats();
var health = db.HealthCheck();
db.Persist();
```
