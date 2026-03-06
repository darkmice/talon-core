# .NET SDK

.NET SDK 通过 P/Invoke 加载 `libtalon.dylib`/`.so`。

## 安装

```bash
dotnet add package TalonDb
```

Native library 已内置于 NuGet 包中（`runtimes/{rid}/native/`），无需手动编译。

## 快速开始

```csharp
using TalonDb;

using var db = new TalonClient("./data");
var result = db.Sql("SELECT 1 + 1 AS result");
```

## API 速查

### SQL

```csharp
db.Sql("CREATE TABLE users (id INT, name TEXT)");
```

### KV

```csharp
db.KvSet("key", "value");
var val = db.KvGet("key");
db.KvSetNx("lock", "1", 30);
var newVal = db.KvIncrBy("counter", 10);
newVal = db.KvDecrBy("counter", 5);
var keys = db.KvKeysLimit("user:", 0, 100);
var count = db.KvCount();
```

### FTS（全文搜索）

```csharp
db.FtsCreateIndex("articles");
db.FtsIndex("articles", "doc1", new() { ["title"] = "Hello World" });
var hits = db.FtsSearch("articles", "hello", 10);
var fuzzy = db.FtsSearchFuzzy("articles", "helo", 2, 10);
var hybrid = db.FtsHybridSearch("articles", "vecs", "query", vec,
    new() { ["fts_weight"] = 0.7, ["vec_weight"] = 0.3 });
```

### Geo（地理空间）

```csharp
db.GeoCreate("shops");
db.GeoAdd("shops", "starbucks", 121.47, 31.23);
var nearby = db.GeoSearch("shops", 121.47, 31.23, 1000);
var inside = db.GeoFence("shops", "starbucks", 121.47, 31.23, 500);
var members = db.GeoMembers("shops");
```

### Graph（图引擎）

```csharp
db.GraphCreate("social");
var v1 = db.GraphAddVertex("social", "person", new() { ["name"] = "Alice" });
var v2 = db.GraphAddVertex("social", "person", new() { ["name"] = "Bob" });
db.GraphAddEdge("social", v1, v2, "knows");
var path = db.GraphShortestPath("social", v1, v2);
var pagerank = db.GraphPagerank("social", 0.85, 20, 10);
```

### AI（Session / Context / Memory / Trace）

```csharp
db.AiCreateSession("s1");
db.AiAppendMessage("s1", new() { ["role"] = "user", ["content"] = "hi" });
var history = db.AiGetHistory("s1");
db.AiStoreMemory("s1", new() { ["key"] = "pref", ["value"] = "dark" });
```

### Vector / TS / MQ / Backup / Ops

```csharp
db.VectorInsert("idx", 1, new float[] { 0.1f, 0.2f });
db.TsInsert("metrics", new() { ["cpu"] = 85.5 });
db.MqPublish("events", new() { ["type"] = "login" });
db.ExportDb("/backup");
db.Persist();
```
