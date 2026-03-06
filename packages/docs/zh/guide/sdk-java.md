# Java SDK

Java SDK 通过 JNA 加载 `libtalon.dylib`/`.so`。

## 安装

Java SDK 暂未发布到 Maven Central，可从 [GitHub Releases](https://github.com/darkmice/talon-bin/releases) 下载 JAR 包后添加到项目中：

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
构建前需确保 `lib/` 目录包含对应平台的 native library（由 CI 自动推送，或从 [GitHub Releases](https://github.com/darkmice/talon-bin/releases) 手动下载）。
:::

### 库查找

SDK 自动从以下位置查找原生库：
1. `TALON_LIB_PATH` 环境变量
2. Classpath 资源（如内嵌于 JAR 中）
3. `TALON_SDK_ROOT` 环境变量 → `lib/{platform}/`
4. 从 JAR/class 位置向上遍历查找 `lib/` 目录

## 快速开始

```java
import io.talon.Talon;

try (Talon db = new Talon("./data")) {
    var result = db.sql("SELECT 1 + 1 AS result");
}
```

## API 速查

### SQL

```java
db.sql("CREATE TABLE users (id INT, name TEXT)");
```

### KV

```java
db.kvSet("key", "value", null);
String val = db.kvGet("key");
db.kvSetNx("lock", "1", 30L);
long newVal = db.kvIncrBy("counter", 10);
newVal = db.kvDecrBy("counter", 5);
var keys = db.kvKeysLimit("user:", 0, 100);
long count = db.kvCount();
```

### FTS（全文搜索）

```java
db.ftsCreateIndex("articles");
db.ftsIndex("articles", "doc1", Map.of("title", "Hello World"));
var hits = db.ftsSearch("articles", "hello", 10);
var fuzzy = db.ftsSearchFuzzy("articles", "helo", 2, 10);
var hybrid = db.ftsHybridSearch("articles", "vecs", "query", vec,
    Map.of("fts_weight", 0.7, "vec_weight", 0.3));
```

### Geo（地理空间）

```java
db.geoCreate("shops");
db.geoAdd("shops", "starbucks", 121.47, 31.23);
var nearby = db.geoSearch("shops", 121.47, 31.23, 1000, "m", null);
var inside = db.geoFence("shops", "starbucks", 121.47, 31.23, 500, "m");
String[] members = db.geoMembers("shops");
```

### Graph（图引擎）

```java
db.graphCreate("social");
long v1 = db.graphAddVertex("social", "person", Map.of("name", "Alice"));
long v2 = db.graphAddVertex("social", "person", Map.of("name", "Bob"));
db.graphAddEdge("social", v1, v2, "knows", null);
var path = db.graphShortestPath("social", v1, v2, 10);
var pagerank = db.graphPagerank("social", 0.85, 20, 10);
```

### AI（Session / Context / Memory / Trace）

```java
db.aiCreateSession("s1", null, null);
db.aiAppendMessage("s1", Map.of("role", "user", "content", "hi"));
var history = db.aiGetHistory("s1", null);
db.aiStoreMemory("s1", Map.of("key", "pref", "value", "dark"));
```

### Vector / TS / MQ / Backup / Ops

```java
db.vectorInsert("idx", 1, new float[]{0.1f, 0.2f});
db.tsInsert("metrics", Map.of("cpu", 85.5));
db.mqPublish("events", Map.of("type", "login"));
db.exportDb("/backup", null);
db.persist();
```
