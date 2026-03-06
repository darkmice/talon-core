# Talon — 面向本地 AI 的多模融合数据引擎

> 单二进制、零外部依赖、嵌入式 + Server 双模  
> 对标 SQLite + Redis + InfluxDB + Elasticsearch + Qdrant + PostGIS + Neo4j 联合能力

[English](README.md)

---

## 产品定位

Talon 是一款专为 **AI 应用** 设计的多模融合数据引擎。它将关系型数据库、KV 缓存、时序存储、消息队列、向量索引、全文搜索、地理空间、图数据库和 AI 原生引擎九大数据引擎统一到一个引擎中，用一个二进制文件替代传统 AI 技术栈中需要拼装的 5-8 个独立组件。

### 核心理念

- **本地优先**：默认单机/自托管，无强制云依赖，数据主权完全掌控
- **AI 第一性**：以 Session、Context、Memory、Trace、RAG 为设计锚点
- **零依赖**：纯 Rust 实现，单二进制部署（一个可执行文件，数据以 LSM-Tree 目录形式存储），无需安装任何运行时
- **多模融合**：九引擎共享底层存储，支持跨引擎联合查询
- **性能优先**：LSM-Tree 存储引擎，百万级数据毫秒级响应

### 目标用户

- AI 应用开发者（RAG、Agent、对话系统、Copilot）
- 需要轻量级嵌入式数据库的桌面/移动应用
- 边缘计算与 IoT 场景
- 不想运维多个数据库组件的独立开发者和小团队

---

## 为什么需要 Talon？

### AI 开发的痛点

构建一个典型的 AI 应用（如 RAG 聊天机器人），传统方案需要：

| 需求 | 传统方案 | 运维代价 |
|------|---------|---------|
| 用户数据和配置 | PostgreSQL / SQLite | 需要管理 schema 迁移 |
| Session 缓存 | Redis | 额外进程，内存管理 |
| 向量语义搜索 | Qdrant / Milvus / Pinecone | 额外服务，网络延迟 |
| Token 用量统计 | InfluxDB / Prometheus | 时序数据库部署 |
| Agent 任务调度 | RabbitMQ / Kafka | 消息中间件运维 |
| 文档全文检索 | Elasticsearch | 重量级 Java 服务 |
| 地理围栏/LBS | PostGIS | PostgreSQL 扩展 |
| 知识图谱 | Neo4j | 图数据库运维 |
| AI 原语（Session/Memory/RAG） | 自写代码 + 多库拼装 | 高开发成本，难维护 |

**5-8 个独立服务 = 5-8 套部署、监控、升级、故障排查。**

### Talon 的方案

```
一个二进制 = 全部搞定
```

```rust
let db = Talon::open("./my_data")?;

db.run_sql("SELECT * FROM users")?;         // SQL
db.kv()?.set(b"key", b"val", None)?;        // KV
db.create_timeseries("metrics", schema)?;    // 时序
db.mq()?.publish("events", b"msg")?;        // MQ
db.vector("idx")?.search(&vec, 10)?;        // 向量
db.fts()?.search("docs", "关键词", 10)?;     // 全文搜索
db.geo()?.geo_search("places", ...)?;        // GEO
db.graph()?.bfs("social", 1, 3, Out)?;      // 图
db.ai()?.create_session("s1", Default::default(), None)?; // AI
db.ai()?.append_message("s1", &msg)?;               // 对话
```

---

## 九大数据引擎

### 1. SQL 关系型引擎

完整的关系型数据库，支持丰富的 SQL 方言。

**DDL 能力：**
- `CREATE TABLE` — 支持 9 种数据类型
- `ALTER TABLE` — ADD/DROP/RENAME COLUMN、ADD CONSTRAINT UNIQUE、ALTER COLUMN TYPE、SET/DROP DEFAULT
- `CREATE INDEX` / `DROP INDEX` — 二级索引
- `CREATE VECTOR INDEX` — HNSW 向量索引
- `COMMENT ON TABLE/COLUMN` — 表/列注释
- `TRUNCATE TABLE` / `DROP TABLE`

**DML 能力：**
- `INSERT` / `INSERT OR REPLACE` / `ON CONFLICT DO UPDATE`（UPSERT）
- `SELECT` — WHERE / AND / OR / 括号嵌套 / ORDER BY / LIMIT / OFFSET / DISTINCT
- `UPDATE` — 支持算术表达式（`SET col = col + 1`）
- `DELETE` / `TRUNCATE`

**查询能力：**
- 条件：`=` `!=` `<` `>` `<=` `>=` `IN` `BETWEEN` `LIKE` `IS NULL` `IS NOT NULL`
- 聚合：`COUNT` `SUM` `AVG` `MIN` `MAX`（O(1) 列统计加速）
- `GROUP BY` / `HAVING` — 分组聚合 + 条件过滤
- 子查询：`WHERE x IN (SELECT ...)`
- JOIN：`INNER JOIN` / `LEFT JOIN` / `RIGHT JOIN` / `FULL OUTER JOIN`（链式多表，支持别名）
- `UNION` / `UNION ALL` — 结果集合并
- 窗口函数：`ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` / `NTILE` + 聚合窗口（`SUM/AVG/COUNT OVER`）
- `DISTINCT ON (col)` — PostgreSQL 兼容去重
- 高级聚合：`ARRAY_AGG` / `PERCENTILE_CONT` / `PERCENTILE_DISC`
- 表达式列：`SELECT a + b AS total, price * qty`
- `INSERT ... RETURNING` / `UPDATE ... RETURNING` — 返回受影响行
- 多表 UPDATE：`UPDATE t1 JOIN t2 ON ... SET ...`（MySQL 兼容）
- 多表 DELETE：`DELETE t1 FROM t1 JOIN t2 ON ...` / `DELETE FROM t1 USING t2 WHERE ...`（MySQL + PostgreSQL 兼容）
- PostgreSQL `$1/$2` 参数占位符（自动转 `?`）
- `REPLACE INTO`（MySQL 兼容）
- 事务：`BEGIN` / `COMMIT` / `ROLLBACK`（MVCC 快照读）
- 参数化查询：`?` 占位符绑定
- `EXPLAIN` 查询计划
- `SHOW TABLES` 元数据

**数据类型：**

| 类型 | 说明 | 示例 |
|------|------|------|
| `INTEGER` | 64 位整数 | `42` |
| `FLOAT` | 64 位浮点数 | `3.14` |
| `TEXT` | UTF-8 字符串 | `'hello'` |
| `BLOB` | 二进制数据 | `X'DEADBEEF'` |
| `BOOLEAN` | 布尔值 | `TRUE` / `FALSE` |
| `JSONB` | JSON 文档 | `'{"key":"value"}'` |
| `VECTOR(dim)` | 向量（指定维度） | `[0.1, 0.2, 0.3]` |
| `TIMESTAMP` | 时间戳 | `NOW()` |
| `GEOPOINT` | 地理坐标 | `GEOPOINT(39.9, 116.4)` |

### 2. KV 缓存引擎

Redis 兼容的键值存储，支持 TTL 自动过期和后台清理。

**核心操作：**
- `SET` / `GET` / `DEL` / `EXISTS` — 基本读写
- `MSET` / `MGET` — 批量操作
- `INCR` / `INCRBY` / `DECRBY` — 原子计数
- `SETNX` — 分布式锁原语
- `EXPIRE` / `TTL` — 过期管理
- `KEYS` — 前缀扫描 / glob 模式匹配
- Namespace 隔离（`ns:key` 约定）
- MVCC 快照读

**Redis RESP 协议兼容：** 内置 Redis 协议服务器，可直接使用 `redis-cli` 连接：

```bash
redis-cli -h 127.0.0.1 -p 6380
> SET mykey "hello"
> GET mykey
> INCR counter
```

支持命令：`GET` · `SET` · `DEL` · `MGET` · `MSET` · `EXISTS` · `EXPIRE` · `TTL` · `KEYS` · `INCR` · `DECR` · `PING` · `INFO`

### 3. 时序存储引擎

时序数据存储，支持 TAG 过滤和聚合查询。

**核心能力：**
- Schema 定义：TAG 字段（分类索引）+ VALUE 字段（数值）
- 毫秒精度时间戳
- 时间范围查询 + TAG 过滤
- 聚合：SUM / AVG / COUNT / MIN / MAX（支持时间桶分组）
- 保留策略（Retention Policy）+ 后台自动清理
- InfluxDB Line Protocol 兼容导入

### 4. 消息队列引擎

持久化发布/订阅消息队列，支持消费者组和至少一次投递。

**核心能力：**
- Topic 创建/删除，可选 MAXLEN 限制
- PUBLISH / POLL / ACK 三段式消费
- 消费者组（Consumer Group）
- 多消费者并行消费
- 至少一次投递保证（At-Least-Once）

### 5. 向量索引引擎

自研 HNSW（Hierarchical Navigable Small World）近似最近邻搜索。

**核心能力：**
- 可配置 HNSW 参数：`m`（连接数）、`ef_construction`（构建宽度）、`ef_search`（搜索宽度）
- 距离度量：余弦距离 / L2 欧氏距离 / 内积
- SQL 集成：`vec_cosine()` / `vec_l2()` / `vec_dot()` 函数
- 混合查询：标量过滤 + 向量 KNN 在同一 SELECT 中
- 批量插入 / 批量搜索
- Recommend API（对标 Qdrant）— 基于正/负样本查找相似项
- Discover API（上下文搜索，对标 Qdrant）— target + context 对重排序
- 元数据过滤：字段级精确过滤（Eq/Ne/Gt/Lt/In）
- SQ8 标量量化：4:1 压缩比，精度损失 <2%
- 快照一致性搜索：读取与并发写入隔离

### 6. 全文搜索引擎

倒排索引 + BM25 评分，对标 Elasticsearch 核心能力。

**核心能力：**
- 倒排索引 + BM25 相关性评分
- Unicode 标准分词器 + 中文分词（jieba）
- 搜索结果高亮（`<em>` 标记）
- 模糊搜索（编辑距离匹配）
- Bool Query（must/should/must_not，对标 ES）
- Phrase Query（精确短语匹配，位置感知）
- Term Query（精确字段值匹配）
- Range Query（数值/字符串范围过滤）
- Regexp / Wildcard Query（正则/通配符搜索）
- Multi-Field Query（多字段联合搜索）
- 聚合：Terms 聚合、搜索建议（Suggest）、排序搜索
- 混合搜索（BM25 + Vector RRF 融合 + pre_filter 前置过滤）— RAG 核心能力
- Elasticsearch `_bulk` NDJSON 格式兼容

**索引管理（对标 ES）：**
- 索引别名（`add_alias`）— RAG 知识库版本切换
- 关闭/打开索引（`close_index` / `open_index`）
- 重建索引（`reindex`）— 分词器变更后重建
- 获取映射（`get_mapping`）/ 列出索引（`list_indexes`）
- 按查询更新/删除（`update_by_query` / `delete_by_query`）

**混合搜索（Hybrid Search）：**

```rust
// BM25 关键词匹配 + 向量语义相似度，两路 RRF 融合，支持前置过滤
let hits = hybrid_search(&store, &HybridQuery {
    fts_index: "articles",
    vec_index: "emb_idx",
    query_text: "Rust 数据库",
    query_vec: &query_embedding,
    limit: 10,
    pre_filter: Some(vec![("namespace", "tenant_a")]),
    ..Default::default()
})?;
```

### 7. GEO 地理引擎

基于 Geohash + LSM 前缀扫描的空间索引，兼容 Redis GEO 命令语义。

**核心能力：**
- `GEOADD` / `GEOPOS` / `GEODIST` / `GEODEL` — 基本操作
- 圆形范围搜索（按距离排序）
- 矩形范围搜索（Bounding Box）
- 地理围栏检测（Geofencing）
- 批量添加
- `GEOSEARCHSTORE`（对标 Redis 7.0）— 搜索结果写入目标集合
- 52-bit Geohash 编码，精度约 0.6 米

### 8. 图引擎

属性图模型，支持节点/边 CRUD、标签索引和图遍历算法。

**核心能力：**
- 节点（Vertex）：自增 ID、标签、属性（Key-Value）
- 边（Edge）：自增 ID、源→目标、标签、属性
- 出边/入边索引（O(1) 邻居查找）
- 标签索引（按标签查询节点/边）
- 级联删除（删除节点自动删除关联边和索引）

**图算法：**
- BFS 广度优先遍历（可限深度）
- 属性过滤 BFS（回调控制遍历展开）
- 最短路径（无权 BFS）
- 带权最短路径（Dijkstra，边权从属性解析）
- 度中心性（出度/入度/总度排序）
- PageRank（迭代收敛，可配阻尼因子和迭代次数）

**性能基准（release 模式）：**

| 测试项 | 实测 |
|--------|------|
| 节点写入 100 万 | 127K ops/s |
| 边写入 200 万 | 74K ops/s |
| 节点读取 | 935K ops/s |
| 邻居查询 | 110K qps |
| BFS 遍历 | 6.5K qps |

---

## 9. AI 引擎

Talon 的第九大引擎，内置面向 LLM 应用开发的一阶语义抽象，无需额外框架。

### Session 管理
- 创建/查询/列出/删除 AI 会话，支持 TTL 自动过期
- 自定义元数据（`update_session`）
- 标签管理（`add_session_tags` / `remove_session_tags`）
- 归档/恢复（`archive_session` / `unarchive_session`）
- 导出快照（`export_session`）— 备份、迁移、审计
- 会话统计（`session_stats`）— 消息数、Token 用量
- 级联删除（删除 Session 自动清理 Context + Trace）
- 批量清理已过期 Session

### 对话上下文（Context）
- 追加消息（支持 user/assistant/system 角色，含 token_count）
- 获取最近 N 条消息
- Token 窗口管理：自动截断以适配 LLM 上下文长度限制
- 清空上下文（保留 Session）

### 语义记忆（Memory）
- 向量化长期记忆存储 + 语义相似度搜索
- 更新记忆（文本/元数据）
- 记忆去重（`find_duplicate_memories`）— 余弦距离阈值检测
- 记忆 TTL 自动过期（`cleanup_expired_memories`）
- 记忆统计（`memory_stats`）— 总数、已过期数

### RAG 文档管理
- 文档存储与分块（`store_rag_document`）
- 语义向量搜索（`search_rag`）
- 文档版本管理
- 与全文搜索混合检索（Hybrid Search）

### Agent 原语
- 工具调用缓存（`cache_tool_result` / `get_cached_tool_result`）— TTL 自动过期
- 缓存失效（`invalidate_tool_cache`）
- Agent 步骤持久化（`save_agent_state` / `get_agent_state` / `list_agent_steps`）
- 步骤回滚（`rollback_agent_to_step`）— 回到指定检查点

### 执行追踪（Trace）
- 记录执行步骤（LLM 调用、工具调用、embedding 等）
- 多维查询：按 Session / Run / 操作类型 / 时间范围
- 性能报告（`trace_performance_report`）— 延迟统计、慢操作检测
- 聚合统计（`trace_stats`）— 总数、Token 用量、按操作分组

### Embedding 缓存
- 缓存文本的 embedding 向量（`cache_embedding`）— 避免重复调用外部 API
- 按内容哈希查询（`get_cached_embedding`）

### 意图识别（Intent）
- 自然语言意图解析
- 智能路由到合适的引擎

---

## 跨引擎联合查询

Talon 独有的多引擎融合查询能力：

| 组合 | 场景 | 说明 |
|------|------|------|
| **GEO + Vector** | 附近语义搜索 | 先地理过滤，再按向量相似度排序 |
| **Graph + Vector** | GraphRAG | 图遍历获取候选，再按向量相似度排序 |
| **Graph + FTS** | 知识图谱检索 | 图遍历获取候选，再按 BM25 评分排序 |
| **FTS + Vector** | 混合检索 | BM25 + KNN 双路 RRF 融合 |
| **GEO + Graph + Vector** | 三引擎融合 | 地理 + 关系 + 语义三维联合搜索 |

---

## 部署模式

### 嵌入式模式

直接嵌入到 Rust 应用中，一行代码打开数据库：

```rust
let db = Talon::open("./my_data")?;
// 直接使用所有引擎，无需网络通信
```

**适用场景：** 桌面应用、CLI 工具、边缘设备、单元测试

### Server 模式

独立服务进程，提供 HTTP/JSON API + TCP 二进制协议：

```bash
talon --data ./my_data --addr 0.0.0.0:7720 --token my_secret
```

**HTTP API 端点：**

| 端点 | 说明 |
|------|------|
| `POST /api/sql` | SQL 执行 |
| `POST /api/kv` | KV 操作 |
| `POST /api/ts` | 时序操作 |
| `POST /api/mq` | 消息队列操作 |
| `POST /api/vector` | 向量操作 |
| `POST /api/fts` | 全文搜索 |
| `POST /api/geo` | 地理操作 |
| `POST /api/graph` | 图引擎操作 |
| `POST /api/ai` | AI 引擎 |
| `POST /api/backup` | 备份恢复 |
| `GET  /api/stats` | 统计信息 |
| `GET  /health` | 健康检查 |

**特性：**
- Bearer Token 认证
- 最大并发连接限制（默认 256）
- 自动持久化（默认 30 秒间隔）
- 优雅关闭（SIGINT/SIGTERM）
- 零外部 HTTP 框架，纯 std::net 实现

### CLI 客户端

交互式数据库管理 Shell：

```bash
talon-cli "talon://:my_secret@localhost:7720"
talon> SELECT * FROM users;
talon> :kv get session:abc
talon> :stats
talon> :help
```

---

## 多语言 SDK

通过 C ABI / FFI 统一接口，支持 7 种编程语言：

| 语言 | 集成方式 | 引擎覆盖 |
|------|---------|---------|
| **Rust** | 直接 crate 依赖 | 全部 |
| **C/C++** | `talon.h` + `libtalon` | 全部 |
| **Python** | ctypes / FFI | 全部 |
| **Node.js** | FFI (N-API) | 全部 |
| **Go** | cgo | 全部 |
| **Java** | JNI | 全部 |
| **.NET** | P/Invoke | 全部 |

所有 SDK 共享统一的 JSON 命令接口（`talon_execute`），一个函数覆盖全部引擎操作。

**Python 示例：**

```python
from talon import Talon

db = Talon("./my_data")
db.sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
db.sql("INSERT INTO users VALUES (1, 'Alice')")
rows = db.sql("SELECT * FROM users")

db.kv_set("session:1", "token", ttl=3600)
val = db.kv_get("session:1")

db.close()
```

**Node.js 示例：**

```javascript
const { Talon } = require('talon-sdk');
const db = new Talon('./my_data');

db.sql("SELECT * FROM users");
db.kvSet("key", "value", 3600);
db.aiCreateSession("s1");
db.aiAppendMessage("s1", { role: "user", content: "Hello" });

db.close();
```

---

## 集群与复制

基于 OpLog 的异步复制，支持在线故障转移：

```bash
# 主节点
talon --data ./primary --addr 0.0.0.0:7720 \
  --role primary --repl-addr 0.0.0.0:7730

# 从节点
talon --data ./replica --addr 0.0.0.0:7721 \
  --role replica --repl-addr primary_host:7730
```

**特性：**
- **OpLog 异步复制** — 二进制编码操作日志流式传输
- **自动只读保护** — 从节点拒绝所有写操作
- **在线 Promote** — 从节点可在线提升为主节点
- **集群状态监控** — 实时查看复制健康状态
- **多从节点** — 一主多从架构

---

## 数据导入

| 格式 | 目标引擎 | 说明 |
|------|---------|------|
| **CSV** | SQL 表 | 自动推断类型，批量导入 |
| **JSONL** | FTS 索引 | 每行一个 JSON 对象 |
| **SQL Dump** | SQL 表 | 兼容 SQLite `.dump` 格式 |
| **InfluxDB Line Protocol** | 时序表 | 纳秒时间戳自动转毫秒 |
| **ES `_bulk` NDJSON** | FTS 索引 | Elasticsearch 批量导入格式 |

---

## 诊断与监控

```rust
// 全局统计：KV 键数、SQL 表数、TS 序列数、MQ 队列数、缓存、磁盘
let stats = db.database_stats()?;

// 健康检查：验证各引擎可读写
let health = db.health_check();

// 运行时统计：缓存命中率、版本号
let info = db.stats();
```

HTTP 端点：
- `GET /api/stats` — 完整数据库统计
- `GET /health` — 引擎级健康检查

---

## 性能基准

在百万行规模下测试，包含 `persist()` 落盘校验 + close → reopen → verify 精准验证。

### SQL 引擎（71 列宽表）

| 测试项 | 目标 | 实测 | 超目标倍率 |
|--------|------|------|-----------|
| PK 点查询（1M 行） | P95 < 5ms | **0.007ms** | 714x |
| 索引范围查询 | P95 < 10ms | **0.139ms** | 72x |
| 单行 INSERT（71 列） | > 10K QPS | **46,667 QPS** | 4.7x |
| 原生批量 INSERT | > 100K rows/s | **241,697 rows/s** | 2.4x |
| JOIN（100K × 1K） | P95 < 50ms | **8.6ms** | 5.8x |
| 聚合查询（1M 行） | P95 < 500ms | **< 1ms** | 500x+ |

### 其他引擎

| 测试项 | 目标 | 实测 | 超目标倍率 |
|--------|------|------|-----------|
| KV 批量 SET（1M keys） | > 400K ops/s | **744K ops/s** | 1.9x |
| TS 批量 INSERT（1M 点） | > 200K pts/s | **540K pts/s** | 2.7x |
| TS 查询（ASC+LIMIT） | P95 < 50ms | **1.0ms** | 50x |
| MQ 发布（1M 消息） | > 50K msg/s | **1,611K msg/s** | 32x |
| 向量 INSERT（100K, HNSW） | > 1K vec/s | **1,057 vec/s** | ✅ |
| 向量 KNN（k=10, 100K 向量） | P95 < 50ms | **0.1ms** | 500x |

---

## 技术架构

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         公共 API (lib.rs / Talon)                        │
│                嵌入式 Rust API · C ABI / FFI · HTTP · TCP                │
├────────┬──────┬──────┬──────┬────────┬──────┬──────┬───────┬─────────────┤
│  SQL   │  KV  │ 时序  │ 消息  │  向量   │ 全文  │ GEO  │  图   │     AI      │
│  引擎  │ 引擎 │ 引擎  │ 队列  │  引擎   │ 搜索  │ 引擎 │  引擎  │    引擎     │
│       │     │      │      │        │      │     │       │Session·RAG │
│       │     │      │      │        │      │     │       │Memory·Agent│
├────────┴──────┴──────┴──────┴────────┴──────┴──────┴───────┴─────────────┤
│                        跨引擎联合查询层                               │
│       GEO+Vector · Graph+Vector · Graph+FTS · Hybrid Search · Triple  │
├───────────────────────────────────────────────────────────────────────────┤
│                     类型层 (Value / Schema / ColumnType)                │
├───────────────────────────────────────────────────────────────────────────┤
│                  存储层 (fjall LSM-Tree · Keyspace · Batch)              │
├───────────────────────────────────────────────────────────────────────────┤
│                 服务层 (HTTP · TCP · Redis RESP · CLI)                  │
├───────────────────────────────────────────────────────────────────────────┤
│              集群层 (OpLog · ReplSender · ReplReceiver)                 │
└───────────────────────────────────────────────────────────────────────────┘
```

**技术选型：**
- **语言**：Rust 2021
- **存储引擎**：fjall 3.x — 纯 Rust LSM-Tree，WAL 崩溃恢复
- **向量索引**：自研 HNSW 实现
- **序列化**：serde + serde_json；向量数据二进制编码
- **压缩**：LZ4 (lz4_flex)
- **错误处理**：thiserror（公开 API）+ anyhow（内部/测试）
- **外部依赖**：**零**运行时依赖

---

## 功能边界与大数据策略

### 明确不做

以下能力为架构层面刻意裁剪：

- 用户权限管理（RBAC）
- 分布式事务（仅异步复制）
- 云托管服务（本地优先 / 自托管）

### 功能完整，大数据量酌情使用

以下 SQL 高级能力**已全部实现**，在中小数据量（万~百万行）下正常使用。在超大数据量（亿级+）场景下，这些操作可能涉及全表扫描或大量内存消耗，用户需根据实际数据规模评估是否使用：

| 能力 | 状态 | 大数据量说明 |
|------|------|-------------|
| `GROUP BY` / `HAVING` | ✅ 已实现 | HashMap 分组，内存正比于分组数 |
| 子查询 `WHERE x IN (SELECT ...)` | ✅ 已实现 | 子查询结果集加载到内存 |
| 多表 JOIN（链式任意数量表） | ✅ 已实现 | Nested Loop，无成本优化器 |
| `UNION` / `UNION ALL` | ✅ 已实现 | HashSet 去重 |
| 表达式列 `SELECT a + b` | ✅ 已实现 | 无额外开销 |
| 聚合 `SUM` / `AVG` / `COUNT` | ✅ 已实现 | 列统计 O(1) 加速（无 WHERE 时） |
| 存储过程 / 触发器 | ❌ 不支持 | 刻意裁剪 |
| 窗口函数（`ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` / `NTILE` + 聚合窗口） | ✅ 已实现 | Partition + ORDER BY 在内存 |
| 视图 / 物化视图 | ❌ 不支持 | 刻意裁剪 |
| 外键约束 | ❌ 不支持 | 刻意裁剪 |

---

## 与竞品对比

| 特性 | Talon | SQLite | Redis | Qdrant | InfluxDB |
|------|-------|--------|-------|--------|----------|
| SQL 关系型 | ✅ | ✅ | ❌ | ❌ | ❌ |
| KV 缓存 | ✅ | ❌ | ✅ | ❌ | ❌ |
| 时序存储 | ✅ | ❌ | ❌ | ❌ | ✅ |
| 消息队列 | ✅ | ❌ | ✅（Stream） | ❌ | ❌ |
| 向量搜索 | ✅ | ❌ | ❌ | ✅ | ❌ |
| 全文搜索 | ✅ | FTS5 | ❌ | ❌ | ❌ |
| 地理空间 | ✅ | ❌ | GEO | ❌ | ❌ |
| 图查询 | ✅ | ❌ | ❌ | ❌ | ❌ |
| AI 原生抽象 | ✅ | ❌ | ❌ | ❌ | ❌ |
| 跨引擎融合 | ✅ | ❌ | ❌ | ❌ | ❌ |
| 单二进制 | ✅ | ✅ | ✅ | ✅ | ❌ |
| 嵌入式 | ✅ | ✅ | ❌ | ❌ | ❌ |
| 零依赖 | ✅ | ✅ | ❌ | ❌ | ❌ |
| Rust 实现 | ✅ | C | C | Rust | Go |
| 多语言 SDK | 7 种 | 多种 | 多种 | 多种 | 多种 |
| 集群复制 | ✅ | ❌ | ✅ | ✅ | ✅ |
| Redis 协议兼容 | ✅ | ❌ | ✅ | ❌ | ❌ |
| ES Bulk 兼容 | ✅ | ❌ | ❌ | ❌ | ❌ |

---

## 构建与安装

### 从源码构建

```bash
git clone https://github.com/darkmice/talon-core.git
cd talon

# 构建 release 版本
cargo build --release

# 运行测试
cargo test --lib     # 单元测试
cargo test --tests   # 集成测试

# 基准测试
cargo test --test bench_p0 --release -- --nocapture
```

### 输出产物

| 文件 | 说明 |
|------|------|
| `target/release/talon` | Server 二进制 |
| `target/release/talon-cli` | CLI 客户端 |
| `target/release/libtalon.dylib` | 共享库（macOS） |
| `target/release/libtalon.so` | 共享库（Linux） |

---

## 免责与自动同意声明

使用常规的免费版本即表示您明白并同意以下条款：

### AI 专项免责声明 (AI-Specific Disclaimers)

1. **算法概率性**：用户理解并同意，AI 引擎（如 Context 压缩、RAG 检索）具有概率性。开发方不保证处理结果的绝对准确性或与原义的完全等同。
2. **高风险禁区**：严禁将本软件用于医疗救生、核设施、航空控制等高风险环境。若用户违规使用，由此产生的生命财产损失由用户全权承担。
3. **第三方模型隔离**：本软件若连接第三方模型（如 OpenAI, DeepSeek），开发方不对该第三方服务的可用性、隐私合规或输出内容的合法性负责。
4. **数据备份义务**：用户在使用本软件存储或处理数据时，必须建立完善的备份机制。开发方不对任何原因导致的数据丢失或损坏承担赔偿责任。

---

## 许可证

[Talon Community Dual License Agreement (SSPL / Commercial)](LICENSE)
