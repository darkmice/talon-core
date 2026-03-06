# Java SDK

The Java SDK uses JNA to load `libtalon.dylib`/`.so` at runtime.

## Installation

Java SDK is not yet published to Maven Central. Download the JAR from [GitHub Releases](https://github.com/darkmice/talon-bin/releases) and add it to your project:

```xml
<dependency>
    <groupId>io.talon</groupId>
    <artifactId>talon-java</artifactId>
    <version>0.1.8</version>
    <scope>system</scope>
    <systemPath>${project.basedir}/lib/talon-java-0.1.8.jar</systemPath>
</dependency>
```

::: warning
The `lib/` directory must contain the native library for your platform before building.
Libraries are auto-pushed by CI, or can be downloaded from [GitHub Releases](https://github.com/darkmice/talon-bin/releases).
:::

### Library Path

The SDK auto-discovers the native library from:

1. `TALON_LIB_PATH` environment variable
2. Classpath resources (if bundled in JAR)
3. `TALON_SDK_ROOT` environment variable → `lib/{platform}/`
4. Walks up from JAR/class location to find `lib/` directory

## Quick Start

```java
import io.talon.Talon;

try (Talon db = new Talon("./data")) {
    var result = db.sql("SELECT 1 + 1 AS result");
    System.out.println(result);
}
```

## SQL

```java
db.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)");
db.sql("INSERT INTO users VALUES (1, 'Alice')");
var rows = db.sql("SELECT * FROM users WHERE id = 1");
```

## KV

```java
// Basic CRUD
db.kvSet("user:1", "Alice", null);
String val = db.kvGet("user:1");
db.kvDel("user:1");
boolean exists = db.kvExists("user:1");

// TTL
db.kvSet("session:abc", "token", 3600L);
db.kvExpire("session:abc", 1800);
var remaining = db.kvTtl("session:abc");

// Atomic operations
long newVal = db.kvIncr("counter");
newVal = db.kvIncrBy("counter", 10);
newVal = db.kvDecrBy("counter", 5);
boolean wasSet = db.kvSetNx("lock:job1", "worker1", 30L);

// Batch
db.kvMset(List.of("k1", "k2"), List.of("v1", "v2"));
var vals = db.kvMget(List.of("k1", "k2"));

// Scan
var keys = db.kvKeys("user:");
var matched = db.kvKeysMatch("user:*");
var paged = db.kvKeysLimit("user:", 0, 100);
var pairs = db.kvScanLimit("user:", 0, 100);
long count = db.kvCount();
```

## Vector

```java
db.vectorCreate("embeddings", 384, "cosine");
db.vectorInsert("embeddings", 1, new float[]{0.1f, 0.2f});
var results = db.vectorSearch("embeddings", queryVec, 10, "cosine");
db.vectorDelete("embeddings", 1);
var info = db.vectorInfo("embeddings");
```

## Time Series

```java
db.tsCreate("metrics", List.of("host"), List.of("cpu", "mem"));
db.tsInsert("metrics", Map.of("host", "srv1", "cpu", 85.5, "mem", 4096));
var points = db.tsQuery("metrics", Map.of("start", "2024-01-01T00:00:00Z"));
var agg = db.tsAggregate("metrics", "cpu", "avg", null);
```

## Message Queue

```java
db.mqCreate("events");
db.mqPublish("events", Map.of("type", "login", "user", "alice"));
var msgs = db.mqPoll("events", 10);
db.mqAck("events", msgId);
db.mqSubscribe("events", "consumer1");
```

## Full-Text Search

```java
db.ftsCreateIndex("articles");
db.ftsIndex("articles", "doc1", Map.of(
    "title", "Introduction to AI",
    "body", "Artificial intelligence is transforming..."
));
var hits = db.ftsSearch("articles", "artificial intelligence", 10);
var fuzzy = db.ftsSearchFuzzy("articles", "artifcial", 2, 10);

// Hybrid search (BM25 + vector)
var hybrid = db.ftsHybridSearch("articles", "vectors",
    "AI search", queryVec,
    Map.of("metric", "cosine", "limit", 10,
           "fts_weight", 0.7, "vec_weight", 0.3));

db.ftsAddAlias("articles_v2", "articles");
db.ftsReindex("articles");
var mapping = db.ftsGetMapping("articles");
var indexes = db.ftsListIndexes();
```

## Geospatial

```java
db.geoCreate("shops");
db.geoAdd("shops", "starbucks", 121.4737, 31.2304);
db.geoAddBatch("shops", List.of(
    Map.of("key", "mcdonalds", "lng", 121.48, "lat", 31.235),
    Map.of("key", "kfc", "lng", 121.465, "lat", 31.228)
));

// Position & distance
var pos = db.geoPos("shops", "starbucks");
var dist = db.geoDist("shops", "starbucks", "mcdonalds", "km");

// Search
var nearby = db.geoSearch("shops", 121.47, 31.23, 1000, "m", null);
var inBox = db.geoSearchBox("shops", 121.46, 31.22, 121.49, 31.24, null);
var inside = db.geoFence("shops", "starbucks", 121.47, 31.23, 500, "m");
String[] members = db.geoMembers("shops");
```

## Graph

```java
db.graphCreate("social");

// Vertices
long v1 = db.graphAddVertex("social", "person", Map.of("name", "Alice"));
long v2 = db.graphAddVertex("social", "person", Map.of("name", "Bob"));
var vertex = db.graphGetVertex("social", v1);
db.graphUpdateVertex("social", v1, Map.of("name", "Alice W."));

// Edges
long e1 = db.graphAddEdge("social", v1, v2, "knows", Map.of("since", "2024"));
var edge = db.graphGetEdge("social", e1);

// Traversal
var neighbors = db.graphNeighbors("social", v1, "out");
var outEdges = db.graphOutEdges("social", v1);
var inEdges = db.graphInEdges("social", v2);
var byLabel = db.graphVerticesByLabel("social", "person");

// Algorithms
var path = db.graphShortestPath("social", v1, v2, 10);
var wPath = db.graphWeightedShortestPath("social", v1, v2, 10, "weight");
var bfs = db.graphBfs("social", v1, 3, "out");
var centrality = db.graphDegreeCentrality("social", 10);
var pagerank = db.graphPagerank("social", 0.85, 20, 10);

// Stats
long vCount = db.graphVertexCount("social");
long eCount = db.graphEdgeCount("social");
```

## AI (Session / Context / Memory / Trace)

```java
// Session management
db.aiCreateSession("s1", null, null);
var session = db.aiGetSession("s1");
db.aiDeleteSession("s1");
var sessions = db.aiListSessions(10, 0);

// Context / Messages
db.aiAppendMessage("s1", Map.of("role", "user", "content", "What is Talon?"));
var history = db.aiGetHistory("s1", null);
db.aiClearContext("s1");

// Memory
db.aiStoreMemory("s1", Map.of("key", "preference", "value", "dark mode"));
var memories = db.aiSearchMemory("s1", "preference", 10);

// Trace
db.aiLogTrace("s1", Map.of("event", "llm_call", "model", "gpt-4", "latency_ms", 230));
var traces = db.aiQueryTraces("s1", null);
```

## Backup & Ops

```java
long exported = db.exportDb("/backup/dir", null);
long imported = db.importDb("/backup/dir");

var stats = db.databaseStats();
var health = db.healthCheck();
db.persist();
```
