# Go SDK

Go SDK 通过 cgo 静态链接 `libtalon.a`，编译后无运行时依赖。

## 安装

```bash
go get github.com/darkmice/talon-sdk/go
```

## 快速开始

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

    rows, _ := db.SQL("SELECT 1 + 1 AS result")
    fmt.Println(rows)
}
```

## API 速查

### SQL

```go
result, _ := db.SQL("CREATE TABLE users (id INT, name TEXT)")
```

### KV

```go
db.KvSet("key", "value", nil)            // 写入
val, _ := db.KvGet("key")                // 读取
db.KvDel("key")                          // 删除
exists, _ := db.KvExists("key")          // 判断存在

ttl := uint64(3600)
db.KvSet("session", "token", &ttl)       // 带 TTL
newVal, _ := db.KvIncr("counter")        // 自增
newVal, _ = db.KvIncrBy("counter", 10)   // 按步长自增
newVal, _ = db.KvDecrBy("counter", 5)    // 按步长自减
wasSet, _ := db.KvSetNX("lock", "1", &ttl)  // SETNX

keys, _ := db.KvKeysLimit("user:", 0, 100)   // 分页扫描
count, _ := db.KvCount()                     // Key 总数
```

### FTS（全文搜索）

```go
db.FtsCreateIndex("articles")
db.FtsIndex("articles", "doc1", map[string]string{"title": "Hello"})
hits, _ := db.FtsSearch("articles", "hello", 10)
fuzzy, _ := db.FtsSearchFuzzy("articles", "helo", 2, 10)
hybrid, _ := db.FtsHybridSearch("articles", "vecs", "query", vec,
    &talon.HybridSearchOpts{FtsWeight: 0.7, VecWeight: 0.3})
```

### Geo（地理空间）

```go
db.GeoCreate("shops")
db.GeoAdd("shops", "starbucks", 121.47, 31.23)
nearby, _ := db.GeoSearch("shops", 121.47, 31.23, 1000, "m", nil)
inside, _ := db.GeoFence("shops", "starbucks", 121.47, 31.23, 500, "m")
members, _ := db.GeoMembers("shops")
```

### Graph（图引擎）

```go
db.GraphCreate("social")
v1, _ := db.GraphAddVertex("social", "person", map[string]string{"name": "Alice"})
v2, _ := db.GraphAddVertex("social", "person", map[string]string{"name": "Bob"})
db.GraphAddEdge("social", v1, v2, "knows", nil)
path, _ := db.GraphShortestPath("social", v1, v2, 10)
pagerank, _ := db.GraphPageRank("social", 0.85, 20, 10)
```

### AI（Session / Context / Memory / Trace）

```go
db.AiCreateSession("s1", nil, nil)
db.AiAppendMessage("s1", map[string]interface{}{"role": "user", "content": "hi"})
history, _ := db.AiGetHistory("s1", nil)
db.AiStoreMemory("s1", map[string]interface{}{"key": "pref", "value": "dark"})
```

### Vector / TS / MQ / Backup / Ops

```go
db.VectorInsert("idx", 1, []float32{0.1, 0.2})
db.TsInsert("metrics", map[string]interface{}{"cpu": 85.5})
db.MqPublish("events", map[string]interface{}{"type": "login"})
db.ExportDb("/backup", nil)
db.Persist()
```
