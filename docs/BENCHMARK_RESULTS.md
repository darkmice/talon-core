# Talon 基准测试结果

> 测试环境：macOS, Apple Silicon, `cargo test --release`
> 版本：**M205** (含 M79-M202 全部优化 + Sprint 6 搜索/缓存性能优化)
> 日期：2026-03-13
>
> **方法论说明**：
> - "标准基准" = 含冷启动（DB open + CREATE TABLE + 数据填充），反映端到端体验
> - "稳态吞吐" = 隔离实测排除 setup，反映引擎真实上限（业界对比基准）
> - 标准基准对短时间操作（<100ms）会显著低估实际性能（setup 占比高）
> - **M100 起所有写入基准测试均包含显式 `db.persist()` 落盘校验**

---

## 1. SQL 引擎

### 1.1 单行操作

| 操作 | 规模 | **M90 标准** | **M90 稳态** | vs SQLite | 评级 |
|------|------|------------|------------|-----------|------|
| INSERT (batch) | 10K | 248,389 | **264,000** | 74K (auto) | ✅ **3.6x 领先** |
| SELECT by PK | 10K | 1,067,359 | **1,067,000** | 759K | ✅ **1.4x 领先** |
| UPDATE by PK | 1K | 17,950 | — | — | ✅ |
| DELETE by PK | 500 | 23,667 | — | — | ✅ |
| INSERT OR REPLACE | 500 | 14,139 | — | — | ✅ |
| DELETE AND (PK+filter) | 200 | 34,489 | — | — | ✅ |

### 1.2 聚合查询 (5K 行表)

| 操作 | M79 前 | M83 | **M90** | M79→M90 |
|------|--------|-----|---------|--------|
| COUNT(*) 无 WHERE | 283 | 2,841 | **2,783** | **9.8x** |
| SUM(val) | 250 | 1,311 | **1,291** | **5.2x** |
| AVG(val) | 252 | 1,373 | **1,277** | **5.1x** |
| MIN(val), MAX(val) | 249 | 1,222 | **1,152** | **4.6x** |
| COUNT(*) WHERE cat='c0' | — | 502 | **953** | ✅ **1.9x** |

### 1.3 复杂查询 (5K 行表)

| 操作 | M83 | M89 | **M90** | M83→M90 |
|------|-----|-----|---------|--------|
| SELECT WHERE index=val | 11,928 | 11,405 | **8,110** | ≈ 稳定 |
| SELECT WHERE AND (index+filter) | 11,222 | 11,839 | **13,981** | ✅ **1.25x** |
| SELECT WHERE + ORDER BY + LIMIT | 12,499 | 14,393 | **14,687** | ✅ **1.18x** |
| SELECT ORDER BY + LIMIT 10 | 763 | 773 | **824** | ✅ 1.08x |
| SELECT LIMIT 10 OFFSET 100 | 37,636 | 39,346 | **38,217** | ≈ 稳定 |
| SELECT WHERE OR | 549 | 483 | **746** | ✅ **1.36x** |
| SELECT WHERE BETWEEN | 684 | 572 | **736** | ✅ 1.08x |
| SELECT WHERE LIKE | 609 | 443 | **746** | ✅ **1.22x** |
| SELECT WHERE IN | 591 | 628 | **877** | ✅ **1.48x** |
| SELECT WHERE IS NULL | 575 | 304 | **163,332** | ✅ IS NULL 快速路径 |
| **SELECT DISTINCT** | 302 | 475 | **394** | ✅ 1.30x |

### 1.4 事务

| 操作 | M83 | M89 | **M90 标准** | **M90 稳态** | 业界参考 |
|------|-----|-----|-----------|-----------|----------|
| **BEGIN+100INSERT+COMMIT** | 49 | 272 | **250 txn/s** | **4,540 txn/s** | SQLite WAL ~20K txn/s |
| BEGIN+50INSERT+ROLLBACK | 6,589 | 10,747 | **2,417** | — | — |
| EXPLAIN | 194,191 | 215,692 | **281,856** | — | — |
| SHOW TABLES | 1,230,769 | 1,405,975 | **1,058,201** | — | — |

> ⚠️ TX 标准基准含 5K 行表 setup 开销，严重低估稳态吞吐。
> 稳态 4,540 txn/s = **454K rows/s**，vs SQLite 2M rows/s 仅差 **4.4x**（非此前报告的 81x）。

### 1.5 DML 批量操作

| 操作 | M83 | M89 | **M90** |
|------|-----|-----|--------|
| UPDATE by index (20 cat) | 264 | 279 | **279** |
| UPDATE AND (index+filter) | 247 | 241 | **250** |
| DELETE by index (10 cat) | 386 | 379 | **259** |

### 1.6 DDL

| 操作 | M83 | M89 | **M90** | 业界参考 |
|------|-----|-----|--------|----------|
| CREATE+DROP TABLE | 30 | 29 | **31** | SQLite ~100-300 ❌ |
| ALTER TABLE (ADD/RENAME/DROP) | 123,495 | 155,373 | **148,033** | SQLite ~1-10K ✅ |
| CREATE INDEX on 1K rows | 22 | 18 | **21** | SQLite ~50-200 ⚠️ |

---

## 2. KV 引擎

| 操作 | 规模 | **M90 标准** | **M90 稳态** | 业界参考 | 评级 |
|------|------|-----------|-----------|----------|------|
| SET (single) | 10K | 26,660 | **200,000** | RocksDB 100-400K | ✅ 达标 |
| SET (batch) | 10K | **644,128** | **674,000** | RocksDB 1-5M | ✅ 同量级 |
| GET | 10K | **2,426,351** | **2,659,000** | RocksDB 500K-2M | ✅ 领先 |
| EXISTS | 10K | **2,160,702** | — | — | ✅ |
| DEL | 10K | **58,666** | — | RocksDB 100-300K | ⚠️ |

### 2.1 大规模 KV (100K)

| 操作 | 吞吐量 |
|------|--------|
| SET batch 100K | **216,885 ops/s** |
| GET random 100K | **1,013,066 ops/s** |
| key_count 100K | **133 ops/s** (7.5ms) |
| scan_prefix_limit(100) from 100K | **64,613 ops/s** |

### 2.2 KV 高级操作

| 操作 | 吞吐量 |
|------|--------|
| MSET ×1K | **227,877 ops/s** |
| MGET ×1K | **3,627,026 ops/s** |
| MSET_BATCH ×1K | **314,399 ops/s** |
| SET with TTL ×5K | **84,105 ops/s** |
| TTL query ×5K | **2,464,268 ops/s** |
| INCR new keys ×5K | **161,516 ops/s** |
| INCR existing ×5K | **400,249 ops/s** |
| EXPIRE ×5K | **448,293 ops/s** |
| GET with TTL ×5K | **1,326,084 ops/s** |
| keys_prefix('user:0:') ×1K | **1,099,808 ops/s** |
| keys_prefix_limit ×100 | **77,208 ops/s** |
| keys_match pattern ×50 | **1,433 ops/s** |

---

## 3. 向量引擎 (dim=128, HNSW)

| 操作 | **M202 基线** | **M205 优化后** | 提升 | 业界参考 | 评级 |
|------|------------|---------------|------|----------|------|
| INSERT 50K | 248 vecs/s | **357 vecs/s** | ⬆️ 44% | Qdrant 1-5K | ✅ |
| KNN k=10 (1K queries) | 845 qps | **1,082 qps** | ⬆️ 28% | Qdrant 1-10K | ✅ |
| Batch KNN ×10 | 107ms | **25ms** | ⬆️ 4.3x | — | ✅ |
| recall@10 | 100% | **100%** | — | — | ✅ |

---

## 4. 时序引擎

| 操作 | M90 (JSON) | **M91 (二进制)** | 提升 | 业界参考 | 评级 |
|------|-----------|-----------------|------|----------|------|
| INSERT | 37,160 | **555,859** | **15x** 🚀 | InfluxDB 100-500K | ✅ **达标** |
| QUERY (tag+time) | 595 | **1,518** | **2.6x** | InfluxDB 10-100K | ⚠️ 接近低端 |
| AGGREGATE (SUM) | 219 | **1,315** | **6x** 🚀 | InfluxDB 1-10K | ✅ **达标** |

---

## 5. 消息队列

| 操作 | **M90** | 业界参考 | 评级 |
|------|--------|----------|------|
| PUBLISH | **24,117** | Redis Streams 100K+ | ⚠️ (Redis 内存型) |
| POLL (100 msgs) | **22,892** | — | ⚠️ |

---

## 6. 运维操作

| 操作 | 吞吐量 |
|------|--------|
| EXPORT (5K rows) | **156,116 ops/s** |
| IMPORT (5K rows) | **569,506 ops/s** |
| database_stats | **944 ops/s** |
| health_check | **154 ops/s** |

---

## 7. Talon vs SQLite 直接对比 (10K ops)

| 操作 | Talon | SQLite | 倍数 |
|------|-------|--------|------|
| SQL INSERT (batch) | **266,305** | 61,381 (auto-commit) | ✅ **4.3x** |
| SQL SELECT by PK | **1,283,738** | 693,381 | ✅ **1.85x** |
| KV SET (single) | **113,209** | 43,123 | ✅ **2.6x** |
| KV SET (batch) | 674,756 | **1,466,473** | ⚠️ SQLite 2.2x |
| KV GET | **2,701,547** | 511,827 | ✅ **5.3x** |
| SQL INSERT (txn batch) | — | 2,001,551 | ⚠️ 差距大 |

---

## 8. P0 性能基准验证（百万行实测）

> 测试文件：`tests/bench_p0.rs`
> 测试条件：单节点嵌入模式，WAL=NORMAL，百万行表，P95 延迟
> 运行：`cargo test --test bench_p0 --release -- --nocapture`

| # | 指标 | 目标值 | **实测值** | 余量 | 结果 |
|---|------|--------|-----------|------|------|
| 1 | 点查询（主键） | P95 < 5ms | **P95 = 0.011ms** | **454x** | ✅ PASS |
| 2 | 范围查询（索引+LIMIT 100） | P95 < 50ms | **P95 = 23.6ms** | **2.1x** | ✅ PASS |
| 3 | 插入（单条） | > 10,000 QPS | **197,905 QPS** | **19.8x** | ✅ PASS |
| 4 | 插入（批量 1000行/txn） | > 100,000 行/s | **372,477 行/s** | **3.7x** | ✅ PASS |
| 5 | 更新（主键） | > 5,000 QPS | **121,570 QPS** | **24.3x** | ✅ PASS |
| 6 | 删除（主键） | > 5,000 QPS | **164,039 QPS** | **32.8x** | ✅ PASS |
| 7 | 聚合 COUNT (1M行) | P95 < 500ms | **P95 = 200ms** | **2.5x** | ✅ PASS |
| 7 | 聚合 SUM (1M行) | P95 < 500ms | **P95 = 277ms** | **1.8x** | ✅ PASS |
| 7 | 聚合 AVG (1M行) | P95 < 500ms | **P95 = 287ms** | **1.7x** | ✅ PASS |
| 8 | **JOIN 查询 (100K×2表)** | P95 < 200ms | **P95 = 121ms** | **1.6x** | ✅ PASS (M92) |

> **达标 8/8**，M92 实现 INNER JOIN + LEFT JOIN (Nested Loop + Index Lookup)。

### 8.2 M93 宽表压力测试（Node 71列, 100万行）

> 测试文件：`tests/bench_node.rs`
> 测试条件：71 列宽表（INT/TEXT 混合），原生批量 INSERT，含 2 个二级索引
> 优化技术：LZ4 压缩 / 列裁剪 / 零分配聚合 / 表级运行统计 / 原生批量 INSERT / LIMIT 下推

| # | 指标 | 目标值 | **实测值** | 余量 | 结果 |
|---|------|--------|-----------|------|------|
| 1 | 点查询 PK (1M行) | P95 < 5ms | **P95 = 0.147ms** | **34x** | ✅ PASS |
| 2 | 范围查询 索引+LIMIT | P95 < 50ms | **P95 = 0.392ms** | **128x** | ✅ PASS |
| 3 | 单条 INSERT (71列) | > 10K QPS | **33,388 QPS** | **3.3x** | ✅ PASS |
| 4 | 批量 INSERT (原生API) | > 100K 行/s | **124,375 行/s** | **1.2x** | ✅ PASS |
| 5 | COUNT(*) 1M行 | P95 < 500ms | **P95 = 244ms** | **2.0x** | ✅ PASS |
| 5 | SUM(col) 1M行 | P95 < 500ms | **P95 = 0.0ms** | **∞** | ✅ PASS |
| 5 | AVG(col) 1M行 | P95 < 500ms | **P95 = 0.0ms** | **∞** | ✅ PASS |
| 6 | JOIN 100K×100 | P95 < 200ms | **P95 = 544ms** | — | ❌ FAIL |

> **达标 6/7**，SUM/AVG 通过 O(1) 表级运行统计实现亚毫秒级聚合。
> JOIN 瓶颈：左表 100K×71列全行解码开销，标准 4 列表 JOIN P95=121ms 已达标。

#### M93 应用的业界优化技术

| 技术 | 来源 | 解决问题 | 效果 |
|------|------|----------|------|
| LZ4 块压缩 | RocksDB/LevelDB | I/O 数据量 | 全局生效 |
| 索引 keyspace COUNT | MySQL InnoDB | COUNT(*) 750→170ms | **4.4x** |
| 列裁剪 skip_value | DuckDB 投影下推 | SUM 2.9s→870ms | **3.4x** |
| 零分配字节级聚合 | ClickHouse 向量化 | SUM 870→668ms | **1.3x** |
| 表级运行统计 | MySQL InnoDB / Oracle MV | SUM/AVG→O(1) | **∞x** 🚀 |
| 索引扫描 LIMIT 下推 | 所有数据库标配 | 范围查询超时→0.4ms | **∞x** 🚀 |
| 原生批量 INSERT | Prepared Statement | 32K→124K rows/s | **3.9x** |

---

## 8.3 M93 全引擎百万级压力测试

> 测试文件：`tests/bench_engines_1m.rs`
> 测试条件：单节点嵌入模式，各引擎百万级数据规模
> 运行：`cargo test --test bench_engines_1m --release -- --nocapture`

### KV 引擎（100万 key, value=100B）

| # | 指标 | 目标 | **实测** | 结果 |
|---|------|------|---------|------|
| KV-1 | Batch SET 1M | > 500K ops/s | **1,611,606 ops/s** | ✅ PASS |
| KV-2 | Random GET 1M | > 1M ops/s | **867,360 ops/s** | ⚠️ 接近 |
| KV-3 | Prefix Scan LIMIT 100 | > 50K ops/s | **54,520 ops/s** | ✅ PASS |

### 时序引擎（100万数据点, 3 fields）

| # | 指标 | 目标 | **实测** | 结果 |
|---|------|------|---------|------|
| TS-1 | Batch INSERT 1M | > 200K pts/s | **773,338 pts/s** | ✅ PASS |
| TS-2 | Query (tag+time+LIMIT) | P95 < 50ms | **P95 = 0.5ms** | ✅ PASS (M94) |
| TS-3 | Aggregate SUM | P95 < 500ms | **P95 = 210ms** | ✅ PASS |

> TS-2 M94 优化：key 范围剪枝（tag_hash + time_start/end），从全表扫描缩小到精确时间窗口。
> 提升 **354x**（177ms → 0.5ms）。

### 向量引擎（10万向量, dim=128, HNSW）

| # | 指标 | 目标 | **实测** | 结果 |
|---|------|------|---------|------|
| VEC-1 | INSERT 100K | > 1K vec/s | **1,392 vec/s** | ✅ PASS |
| VEC-2 | KNN Search (k=10) | P95 < 50ms | **P95 = 0.2ms** | ✅ PASS |

### 消息队列（100万消息, payload=100B）

| # | 指标 | 目标 | **实测** | 结果 |
|---|------|------|---------|------|
| MQ-1 | Publish 1M | > 50K msg/s | **259,351 msg/s** | ✅ PASS |
| MQ-2 | Poll (100 msgs/batch) | P95 < 50ms | **P95 = 11.8ms** | ✅ PASS |

### 全引擎百万级汇总

| 引擎 | 达标 | 未达标 | 达标率 |
|------|------|--------|--------|
| **SQL** (§8.1 + §8.2) | 14/15 | JOIN 宽表 | 93% |
| **KV** | 2/3 | GET 接近目标 | 67% |
| **时序** | **3/3** | — | **100%** (M94) |
| **向量** | 2/2 | — | 100% |
| **消息队列** | 2/2 | — | 100% |
| **合计** | **23/25** | 2 项 | **92%** |

---

## 9. 优化历程 (M79 → M89)

| 里程碑 | 优化项 | 关键提升 |
|--------|--------|----------|
| **M79** | DML WriteBatch 单次提交 | DELETE/UPDATE 6-13x |
| **M80** | 流式聚合 + 双重查找消除 | COUNT(*) 10x, SUM/AVG 2-3x |
| **M81** | 二进制行编码替代 JSON | 聚合/扫描 1.5-2.4x |
| **M82** | 事务索引缓冲 | BEGIN+COMMIT 4.2x, UPSERT 16x |
| **M83** | 五轮审查修复 | encode_row 零克隆, scan_prefix_limit 修复, tx_get 零分配 |
| **M86** | 全量 N+1 消除 | keys_with_prefix→for_each_kv_prefix 流式迭代 |
| **M87** | 深度审查：正确性 bug + 残余 N+1 | R-TX-1 事务 DELETE 正确性修复, 索引扫描流式化 |
| **M88** | 已知改进项修复 | tx_get O(1) HashMap, DISTINCT hash 去重, HNSW 泛型重构 |
| **M89** | 缓存优化 + 兼容修复 | has_vec_indexes 缓存标志, 快速路径反引号表名 |
| **M90** | 流式扫描 + WriteBatch 合并 + fsync 配置 | WHERE 1.4-1.9x, Vector INSERT 1.6x, KNN 2.0x |
| **M91** | TS 二进制编码重构 + P0 基准验证 | TS INSERT **15x**, AGG **6x**, P0 百万行 7/8 达标 |

### 累计提升 (M79 前 → M89 后)

| 基准 | M79 前 | M83 | **M89** | 全程累计 |
|------|--------|-----|---------|----------|
| COUNT(*) 5K | 283 | 2,841 | **2,785** | **9.8x** |
| SUM(val) 5K | 250 | 1,311 | **1,349** | **5.4x** |
| AVG(val) 5K | 252 | 1,373 | **1,220** | **4.8x** |
| MIN/MAX 5K | 249 | 1,222 | **1,057** | **4.2x** |
| SQL INSERT | 22K | 99K | **266K** | **12.1x** |
| SELECT by PK | 906K | 1,063K | **1,284K** | **1.4x** |
| TX COMMIT (100 rows) | — | 49 | **272** | **5.6x** (vs M83) |
| DISTINCT | — | 302 | **475** | **1.57x** (vs M83) |
| DELETE by PK | — | 27.8K | **32.4K** | **1.17x** (vs M83) |
| DELETE AND (PK+filter) | — | 25.3K | **44.3K** | **1.75x** (vs M83) |

### M86-M89 关键修复对性能的影响

| 修复编号 | 内容 | 性能影响 |
|----------|------|----------|
| R-TX-1 | 事务 DELETE 后 TopN 正确性 | 正确性修复（无性能变化） |
| R-TX-2 | tx_get O(n)→O(1) HashMap | TX COMMIT **5.6x** |
| R-SEL-1 | 索引扫描 N+1 消除 (2处) | SELECT WHERE index **稳定 11K** |
| R-AGG-1 | 聚合索引扫描 N+1 消除 | 聚合查询稳定 |
| R-VIDX-1 | list_vec_indexes N+1 消除 | DML 向量同步优化 |
| R-KV-1 | keys_prefix_limit N+1 消除 | KV 扫描优化 |
| R-VIDX-2 | has_vec_indexes 缓存标志 | 无向量索引表 DML 零开销 |
| R-VIDX-3 | Text PK 向量同步支持 | 正确性修复 |
| R-HNSW-1 | search_layer 泛型重构 | 代码质量（-56行） |
| R-HELP-1 | DISTINCT hash 去重 | DISTINCT **1.57x** |
| R-FAST-2 | 快速路径反引号表名 | 兼容性修复 |

---

### M90 短板优化成果

| 短板 | M89 | **M90** | 变化 | 优化手段 |
|------|-----|---------|------|----------|
| 全表扫描 WHERE BETWEEN | 572 | **1,040-1,090** | ✅ **1.9x** | `tx_for_each_row` 流式扫描 |
| 全表扫描 WHERE LIKE | 443 | **795-838** | ✅ **1.9x** | 零中间 Vec 分配 |
| 全表扫描 WHERE OR | 483 | **607-771** | ✅ **1.4-1.6x** | 非事务直接流式 |
| COUNT WHERE | 567 | **614-797** | ✅ **1.2-1.4x** | 聚合流式扫描 |
| 向量 INSERT (HNSW) | 483 | **777** | ✅ **1.6x** | vec/quant 合入 WriteBatch |
| KNN search (k=10) | 5,359 | **10,549** | ✅ **2.0x** | 减少 journal writes |
| 事务批量写入 | 272 txn/s | 272 txn/s | — | `manual_journal_persist` 配置已就绪 |

## 10. M102 百万行基准（含 persist() 落盘校验 + 精准 close→reopen→verify）

> 日期：2026-02-24，所有写入测试含 `db.persist()` + close→reopen→逐条验证。

| # | 测试项 | 目标 | M100 | **M102** | 结果 |
|---|--------|------|------|----------|------|
| 1 | SQL 点查询 PK (1M行) | P95<5ms | 0.018ms | **0.018ms** | ✅ 278x |
| 2 | SQL 批量INSERT (1000行/txn) | >100K rows/s | 200K | **200K** | ✅ 2.0x |
| 3 | SQL 单条INSERT | >10K QPS | 115K | **115K** | ✅ 11.5x |
| 4 | SQL COUNT(*) (1M行) | P95<500ms | 288ms | **167ms** | ✅ |
| 5 | SQL SUM(score) (1M行) | P95<500ms | ~~677ms~~ | **138ms** | ✅ **4.9x** |
| 6 | SQL AVG(score) (1M行) | P95<500ms | 316ms | **143ms** | ✅ |
| 7 | KV Batch SET (1M keys) | >400K ops/s | 744K | **744K** | ✅ 1.9x |
| 8 | TS Batch INSERT (1M点) | >200K pts/s | 352K | **352K** | ✅ 1.8x |
| 9 | MQ Publish (1M消息, batch=1000) | >50K msg/s | ~~106K~~ | **1,611K** | ✅ **32x** |
| 10 | Node 71列原生批量INSERT | >100K rows/s | ~~61K~~ | **127K** | ✅ **2.1x** |

**达标 10/10 (100%)**。M102 优化：warmup 消除 LSM 尾部延迟 + encode 精确预分配 + buffer 复用。

---

## 11. M105 性能优化（百万级基准未达标项修复）

> 日期：2026-02-24，针对百万级基准未达标项逐一优化。

### 优化成果（最终稳态验证）

| # | 测试项 | 优化前 | **最终稳态** | 提升 | 目标 | 状态 |
|---|--------|--------|------------|------|------|------|
| 1 | MQ Poll P95 | 69.4ms | **0.1ms** | **694x** 🚀 | <50ms | ✅ **达标** |
| 2 | TS Aggregate SUM P95 | 1534ms | **8.7ms** | **176x** 🚀 | <500ms | ✅ **达标** |
| 3 | KV Random GET | 108K | **625K** | **5.8x** | >500K | ✅ **达标** |
| 4 | KV Batch SET | 214K | **620K** | **2.9x** | >400K | ✅ **达标** |
| 5 | TS Batch INSERT | 129K | **353K** | **2.7x** | >200K | ✅ **达标** |
| 6 | KV Prefix Scan | 9.7K | **13.2K** | **1.4x** | >25K | ⚠️ fjall seek 开销 |

> 注：稳态数据取隔离运行（排除冷启动 + 并发干扰），反映引擎真实上限。

### 优化技术

| 优化项 | 技术 | 根因 | 效果 |
|--------|------|------|------|
| MQ Poll 流式化 | `keys_from` → `for_each_kv_range` + HashSet pending | O(N) 全量 key 加载 + N+1 查询 | **174x** |
| TS Aggregate tag 索引 | tag 组合注册表 + 部分 tag 精确 prefix scan | 部分 tag 匹配退化全表扫描 | **28x** |
| KV GET 零分配 | TTL=0 快速路径 `drain` 复用 Vec | 每次 GET 额外 alloc+memcpy | **2x** |
| TS INSERT buffer 复用 | `make_key_into` + `seen_hashes` 预分配 | 每点分配 16B key Vec | **1.5x** |
| TS 聚合单次遍历 | `read_field_if_tags_match` 合并 field+tag 读取 | 两次遍历二进制数据 | 常数优化 |

### 剩余短板分析

| 短板 | 实测 | 目标 | 根因 | 可行方向 |
|------|------|------|------|----------|
| KV GET 500K | 212K | 500K | fjall LSM 层级深 + `get()` 返回 `Vec<u8>` 无法零拷贝 | 预热 compaction / bloom filter hint |
| KV Batch SET 400K | 394K | 400K | fjall journal write + `db.persist()` fsync | 接近极限，可调 fsync 策略 |
| KV Prefix Scan 25K | 13.2K | 25K | fjall prefix 迭代器多层合并 seek | bloom filter / iterator pool |
| TS INSERT 200K | 193K | 200K | encode_point alloc + tag 注册 contains_key | 接近极限 |

---

## 12. 已知性能短板与优化方向

| 短板 | 标准基准 | 稳态吞吐 | 业界目标 | 差距 | 优化方向 |
|------|---------|---------|----------|------|----------|
| TX 批量提交 | 250 txn/s | 4,540 txn/s | SQLite ~20K txn/s | 4.4x | prepared stmt 复用 |
| CREATE TABLE | 31 ops/s | 31 ops/s | SQLite ~100-300 | 3-10x | fjall keyspace 缓存 |
| KV Prefix Scan | 9.7K | **13.2K** | >25K | 1.9x | fjall seek 优化 |
| ~~TS 写入~~ | ~~129K~~ | **353K** | InfluxDB 100-500K | ✅ **达标** | M105 buffer 复用 + tag 注册 |
| ~~TS 聚合~~ | ~~1534ms~~ | **P95=8.7ms** | P95 < 500ms | ✅ **达标** | M105 tag 注册表索引 |
| ~~KV batch SET~~ | ~~214K~~ | **620K** | >400K | ✅ **达标** | 稳态性能提升 |
| ~~KV random GET~~ | ~~108K~~ | **625K** | >500K | ✅ **达标** | M105 零分配快速路径 |
| ~~MQ Poll~~ | ~~69ms~~ | **P95=0.1ms** | P95 < 50ms | ✅ **达标** | M105 流式 range scan |
| ~~MQ PUBLISH~~ | ~~24K~~ | **1,611K** | Redis 100K+ | ✅ **16x 领先** | M103 publish_batch |
| ~~JOIN 查询~~ | — | **P95=191ms** | P95 < 200ms | ✅ **达标** | INNER/LEFT JOIN |

---

## 11. 测试说明

- **标准基准**：每个测试使用 `tempfile::tempdir()` 全隔离冷启动，含 DB open + table setup
- **稳态吞吐**：DB 已打开、表已创建后的纯操作吞吐，排除一次性 setup 开销
- 数据规模：SQL 5K-10K 行 (P0 基准 1M 行), KV 10K-100K 条, Vector 500-1K 条 (dim=128)
- 业界参考值来自公开 benchmark 报告，仅供量级对比，非严格 apple-to-apple
- SQLite 对比使用 `rusqlite` crate，WAL 模式，同一机器同一测试
- 聚合数值受系统负载波动影响 ±15%，以趋势和量级为准
- **业界对比详见** `INDUSTRY_COMPARISON.md`

---

## 12. 海量数据全引擎基准（10万 / 50万 / 100万）

> 完整报告见 `docs/性能/` 目录，包含嵌入模式与网络模式两份独立报告。

### 12.1 嵌入模式海量基准汇总

> 测试文件：`tests/bench_embedded_full.rs`  
> 运行：`cargo test --test bench_embedded_full --release -- --nocapture`

#### SQL 引擎

| 规模 | INSERT | SELECT PK P95 | COUNT(*) P95 |
|------|--------|--------------|-------------|
| 10万 | ~200K rows/s | < 0.1ms | < 30ms |
| 50万 | ~180K rows/s | < 0.5ms | < 80ms |
| 100万 | ~165K rows/s | < 0.02ms | < 200ms |

#### KV 引擎

| 规模 | SET batch | GET random | MGET(100) |
|------|----------|-----------|----------|
| 10万 | > 400K ops/s | > 1M ops/s | — |
| 50万 | > 400K ops/s | > 800K ops/s | > 1M ops/s |
| 100万 | ~620K ops/s | ~625K ops/s | > 1M ops/s |

#### 时序引擎

| 规模 | INSERT | QUERY P95 | AGG SUM P95 |
|------|--------|----------|------------|
| 10万 | > 300K pts/s | < 2ms | < 50ms |
| 50万 | > 250K pts/s | — | < 200ms |
| 100万 | ~353K pts/s | < 1ms | < 15ms |

#### 向量引擎（dim=128, HNSW）

| 规模 | INSERT | KNN(k=10) P95 |
|------|--------|--------------|
| 1万 | > 1K vec/s | < 2ms |
| 5万 | > 1K vec/s | < 5ms |
| 10万 | ~1.4K vec/s | < 10ms |

#### 消息队列

| 规模 | PUBLISH | POLL P95 |
|------|---------|---------|
| 10万 | > 200K msg/s | < 1ms |
| 50万 | > 150K msg/s | < 5ms |
| 100万 | ~1.6M msg/s | < 0.5ms |

### 12.2 网络模式海量基准汇总

> 测试文件：`tests/bench_network_full.rs`  
> 运行：`cargo test --test bench_network_full --release -- --nocapture`

网络模式（HTTP API）引入 TCP + HTTP + JSON 序列化开销，单次请求延迟增加 ~1-3ms：

| 引擎 | 100K规模 | 1M规模 | 单请求延迟 | 嵌入模式倍数 |
|------|---------|--------|----------|-----------|
| SQL SELECT PK | < 3ms | < 5ms | +2ms HTTP | 100x 更慢 |
| KV GET | < 2ms | < 5ms | +1.5ms HTTP | 1000x 更慢 |
| TS QUERY | < 10ms | < 20ms | +7ms HTTP | 15x 更慢 |
| MQ POLL | < 10ms | < 15ms | +10ms HTTP | 100x 更慢 |
| 向量 KNN | < 5ms | < 20ms | +2ms HTTP | 5x 更慢 |

> **详细分析**：见 `docs/性能/嵌入模式性能测试报告.md` 和 `docs/性能/网络模式性能测试报告.md`。

---

## 13. 全引擎全方位基准测试（93 项指标）

> 日期：2026-02-27
> 测试文件：`tests/bench_kv_full.rs` / `bench_sql_full.rs` / `bench_ts_full.rs` / `bench_vector_full.rs` / `bench_fts_full.rs` / `bench_mq_full.rs` / `bench_综合.rs`
> 运行：`cargo test --test bench_*_full --release -- --nocapture`
> 环境：macOS Apple Silicon, `--release`, 全部含 `persist()` 落盘
> 指标维度：吞吐量 / 延迟(Avg/P50/P95/P99/Max) / 数据大小梯度 / 磁盘占用 / 内存 RSS

### 13.1 KV 引擎（15 项，对标 Redis / RocksDB）

| # | 指标 | 结果 |
|---|------|------|
| K1 | 单次 SET 100K (100B) | **636,569 ops/s** |
| K2 | 批量 SET 1M (batch=10K, 100B) | **1,464,537 ops/s** |
| K3 | 随机 GET 100K (from 1M) | **737,687 ops/s** |
| K3-L | GET 延迟分布 | Avg=1.3µs P50=1.0µs P95=2.5µs P99=3.8µs |
| K4 | MGET ×100 (10K rounds) | **12,498 ops/s** |
| K5 | EXISTS 100K (from 1M) | **830,228 ops/s** |
| K6 | DEL 100K (含 persist) | **624,221 ops/s** |
| K7 | Prefix Scan LIMIT 100 (10K) | **52,695 ops/s** |
| K8a | SET with TTL (100K) | **689,594 ops/s** |
| K8b | GET with TTL (100K) | **2,254,851 ops/s** |
| K9a | INCR 新 key (100K) | **715,126 ops/s** |
| K9b | INCR 已有 key (100K) | **520,676 ops/s** |
| K10 | key_count (1M) | count=900K, 10.7ms |
| K13 | 1M×100B 磁盘 | **70.1MB** (raw=103MB, 压缩比=1.5x) |
| K14 | 1M×100B RSS 增量 | **161,424KB** |

**Value 大小梯度 (10K each)**

| Value 大小 | 写入 ops/s | 读取 ops/s | 磁盘 |
|-----------|-----------|-----------|------|
| 100B | 1,118,527 | 2,656,072 | 64.0MB |
| 1KB | 470,005 | 2,350,499 | 64.0MB |
| 10KB | 192,513 | 1,285,168 | 64.6MB |
| 100KB | 18,184 | 134,569 | 66.9MB |

| K15 | 100K×1KB 磁盘 | **66.5MB** (raw=98.4MB, 压缩比=1.5x) |

### 13.2 SQL 引擎（25 项，对标 SQLite / DuckDB）

| # | 指标 | 结果 |
|---|------|------|
| S1 | 单条 INSERT (10K) | **304,738 rows/s** |
| S2 | 批量 INSERT (1000行/txn, 至 1M) | **308,416 rows/s** |
| S3 | PK 点查 (10K from 1M) | **251,702 ops/s** |
| S3-L | PK 点查延迟 | Avg=4.0µs P50=3.5µs P95=6.1µs P99=9.9µs |
| S4 | 索引查询 WHERE cat=? LIMIT 100 | Avg=219µs P95=289µs P99=362µs |
| S5 | 全表扫描 WHERE name=? (1M) | **273.4ms** |
| S6 | BETWEEN+ORDER BY+LIMIT 50 | Avg=248ms P95=262ms |
| S7 | COUNT(*) 1M 行 | P95=**50.2µs** (O(1) 快速路径) |
| S8 | SUM/AVG(score) 1M | SUM=**0.00ms** AVG=**0.00ms** (表级运行统计) |
| S9 | GROUP BY cat + COUNT + SUM (1M) | **318.3ms** (100组) |
| S10 | ORDER BY score DESC LIMIT 10 (1M) | Avg=260ms P95=264ms |
| S11 | INNER JOIN (1M×100) LIMIT 1000 | Avg=856µs P95=1.14ms |
| S12 | LEFT JOIN (1M×100) LIMIT 1000 | Avg=813µs P95=870µs |
| S13 | 子查询 WHERE IN (SELECT) LIMIT 100 | **2.9ms** |
| S14 | UNION ALL (500+500) | **492.6ms** |
| S15 | CTE WITH...AS + COUNT (1M) | **532.5ms** |
| S16 | ROW_NUMBER() OVER() LIMIT 100 | **1221.3ms** |
| S17 | UPDATE by PK (10K) | **87,115 ops/s** |
| S18 | DELETE by PK (10K) | **88,591 ops/s** |
| S19 | BEGIN+100INSERT+COMMIT (1K txn) | **2,304 txn/s** (230K rows/s) |
| S20 | EXPLAIN (10K) | **467,043 ops/s** |
| S21 | 宽表 INSERT (50列, 10K) | **114,603 rows/s** |
| S22 | 1M 行(4列) 磁盘 | **146.6MB** |
| S23 | 1M 行 RSS 增量 | **218,000KB** |

**行大小梯度 / TEXT 大小梯度**

| 类型 | 写入 rows/s | 磁盘 |
|------|-----------|------|
| 4列 INSERT 10K | 394,992 | 64.0MB |
| 20列 INSERT 10K | 240,748 | 64.0MB |
| 50列 INSERT 10K | 112,870 | 64.0MB |
| TEXT=50B 10K | 398,851 | 64.0MB |
| TEXT=1KB 10K | 147,250 | 64.0MB |
| TEXT=10KB 10K | 37,317 | 64.3MB |

### 13.3 TS 引擎（14 项，对标 InfluxDB / TDengine）

| # | 指标 | 结果 |
|---|------|------|
| T1 | 批量 INSERT 1M (3f, batch=10K) | **617,653 pts/s** |
| T2 | 单 tag 查询 (100 samples) | Avg=163ms P95=169ms |
| T3 | 多 tag 查询 (host+region) | Avg=61.6µs P95=68.8µs |
| T4 | 时间范围查询 (no tag) | Avg=166ms P95=171ms |
| T5 | AGG SUM/AVG/COUNT (f0, h_0) | SUM=1.87ms, AVG=1.36ms, COUNT=1.35ms |
| T6 | 时间桶聚合 (interval=10s) | **0.18ms** (10桶) |
| T7 | 降采样 (60s bucket → ds) | **2.21ms** (17桶) |
| T8 | 会话窗口聚合 (gap=500ms) | **0.06ms** (1桶) |
| T9 | 正则 tag 查询 (1s range) | Avg=494ms P95=500ms |
| T12 | 1M 点(3f) 磁盘 | **84.4MB** |
| T13 | 1M 点 RSS 增量 | **178,752KB** |

**字段数梯度 (100K each)**

| 字段数 | INSERT pts/s | 磁盘 |
|--------|-------------|------|
| 1 field | 946,811 | 64.0MB |
| 3 fields | 667,521 | 64.0MB |
| 5 fields | 517,799 | 64.0MB |
| 10 fields | 308,205 | 64.0MB |

### 13.4 Vector 引擎（12 项，对标 Qdrant / Milvus）

> M205 Sprint 1-6 优化：SegmentManager Arc 零拷贝 + 向量缓存 + 零拷贝反序列化 + HNSW 预分配

| # | 指标 | 结果 |
|---|------|------|
| V1 | INSERT 50K (dim=128, HNSW) | **357 vecs/s** |
| V2 | KNN k=10 cosine (1K queries) | **1,082 qps** |
| V2-L | KNN 延迟 | Avg=924µs P50=908µs P95=1.16ms P99=1.32ms |
| V3 | KNN k=100 (200 queries) | **571 qps** | Avg=1.75ms P95=2.22ms |
| V4 | KNN k=10 + filter (200 queries) | **305 qps** | Avg=3.28ms P95=3.68ms |
| V6 | batch_search ×10 (k=10) | **25.2ms** total |
| V7 | recall@10 (5K, dim=16, L2, ef=200) | **100.0%** |
| V9 | 50K×dim128 磁盘 | **170.6MB** |
| V10 | 50K×dim128 RSS 增量 | **208,560KB** |

**维度梯度 (10K each)**

| dim | INSERT vecs/s | KNN qps | 磁盘 |
|-----|-------------|---------|------|
| 64 | 988 | 2,336 | 106.3MB |
| 128 | 743 | 1,664 | 110.0MB |
| 256 | 507 | 1,043 | 117.4MB |
| 512 | 360 | 819 | 85.2MB |
| 768 | 266 | 611 | 92.4MB |

**ef_search 调参 (50K, k=10, 100 queries)**

| ef_search | qps |
|-----------|-----|
| 50 | 985 |
| 100 | 615 |
| 200 | 330 |
| 400 | 174 |

### 13.5 FTS 引擎（11 项，对标 Elasticsearch）

| # | 指标 | 结果 |
|---|------|------|
| F1 | 单文档索引 (10K, ~200B) | **20,247 docs/s** |
| F2 | 批量索引 (至 100K, batch=1000) | **17,892 docs/s** |
| F3 | BM25 搜索 (100K, 500 queries) | **303 qps** |
| F4 | 多词搜索 LIMIT 10 (500 queries) | **26 qps** |
| F5 | Fuzzy 搜索 (100K, 100 queries) | **10 qps** |
| F7 | get_mapping (1K calls) | **18,241 ops/s** |
| F8 | reindex 10K 文档 | **810ms** |
| F9 | 100K 文档磁盘 | **308.4MB** |
| F10 | 100K 文档 RSS 增量 | **223,072KB** |

**文档大小梯度 (10K each)**

| Body 大小 | 索引 docs/s | 磁盘 |
|----------|-----------|------|
| 100B | 27,876 | 64.0MB |
| 1KB | 12,810 | 64.0MB |
| 10KB | 3,017 | 214.7MB |

### 13.6 MQ 引擎（10 项，对标 Redis Streams / Kafka）

| # | 指标 | 结果 |
|---|------|------|
| M1 | 单条 PUBLISH (100K, 100B) | **540,825 msg/s** |
| M2 | 批量 PUBLISH (至 1M, batch=1000) | **1,565,594 msg/s** |
| M3 | POLL 100条/次 + ACK (1K rounds) | **2,307 rounds/s**, Avg=114µs P95=94µs |
| M5 | 多消费组 (2/4/8组, 100K) | 640ms / 664ms / 1463ms |
| M7 | 积压 1M 后 poll 100 | Avg=80µs P95=89µs |
| M8 | 1M×100B 磁盘 | **186.9MB** |
| M9 | 1M 消息 RSS 增量 | **173,760KB** |

**Payload 大小梯度 (10K each)**

| Payload | 写入 msg/s | 读取 msg/s | 磁盘 |
|---------|-----------|-----------|------|
| 100B | 1,114,004 | 342,685 | 64.0MB |
| 1KB | 500,288 | 296,609 | 64.0MB |
| 10KB | 149,377 | 81,976 | 65.1MB |

### 13.7 综合指标（6 项）

| # | 指标 | 结果 |
|---|------|------|
| X1 | 冷启动（空 DB open） | **234ms** |
| X2 | 热启动（已有数据 reopen） | **1,353ms** |
| X3 | 全引擎填充后磁盘总量 | **164.4MB** |
| X4 | 全引擎填充后 RSS | **406.7MB** |
| X5 | persist() 延迟 (10次) | Avg=**0.75ms** P95=**7.49ms** |
| X6 | close→reopen→verify 全引擎 | **174ms** ✅ 数据完整 |

### 13.8 GEO 引擎（12 项，对标 Redis GEO / PostGIS）

| # | 指标 | 结果 |
|---|------|------|
| G1 | 单条 GEOADD (10K) | **387,546 ops/s** |
| G2 | 批量 GEOADD (至 100K, batch=1000) | **514,636 ops/s** |
| G3 | GEOPOS (10K queries) | **549,171 ops/s**, Avg=1.8µs P95=3.5µs P99=5.8µs |
| G4 | GEODIST (10K pairs) | **463,990 ops/s**, Avg=2.1µs P95=2.8µs |
| G5 | GEOSEARCH r=1km LIMIT 100 (500) | **384 qps**, Avg=2.6ms P95=4.0ms |
| G7 | GEOSEARCH_BOX ±0.005° (500) | **752 qps**, Avg=1.3ms P95=1.8ms |
| G8 | GEOFENCE r=5km (10K checks) | **785,896 ops/s**, Avg=1.2µs P95=1.9µs |
| G9 | GEOHASH (10K) | **715,980 ops/s** |
| G10 | 100K 成员磁盘占用 | **64.0MB** |
| G11 | 100K 成员 RSS 增量 | **21,856KB** (21MB) |
| G12a | GEO_COUNT (100K) | **7.19ms** |
| G12b | GEODEL (10K) | **364,767 ops/s** |

**GEOSEARCH 半径梯度 (100K 成员，北京 0.5°×0.5° 区域)**

| 半径 | qps | Avg | P95 | 平均命中数 |
|------|-----|-----|-----|-----------|
| 100m | 105 | 9.5ms | 15.8ms | ~4 |
| 1km | 382 | 2.6ms | 4.4ms | ~62 |
| 5km | 31 | 32.5ms | 42.7ms | ~1000 |

**条件写入**

| 模式 | ops/s |
|------|-------|
| GEOADD NX (10K, 50% skip) | 1,020,812 |
| GEOADD XX (10K, all exist) | 304,298 |

### 13.9 性能优化成果（M200-M202）

> 日期：2026-02-27，针对基准测试中发现的未达标指标进行性能优化。

#### 已完成优化

| 引擎 | 指标 | 优化前 | **优化后** | 提升 | 优化手段 |
|------|------|--------|----------|------|----------|
| **TS** | 单 tag 查询 | 163ms | **352µs** | **455x** 🚀 | M200：部分 tag 通过 tag 注册表索引定位匹配 prefix，避免全表扫描 |
| **TS** | 正则 tag 查询 | 494ms | **244µs** | **2025x** 🚀 | M200：同上，正则查询也走 tag 注册表 prefix 扫描 |
| **TS** | 无 tag 时间范围查询 | 169ms | **1.07ms** | **158x** 🚀 | M202：获取所有 tag hash prefix，逐个做 time range scan 替代全表扫描 |
| **FTS** | BM25 搜索 | 27 qps | **303 qps** | **11.2x** 🚀 | M205: decode_inv_tf_dl 零堆分配 + BinaryHeap Top-K + SegmentManager Arc 零拷贝 |
| **Vector** | recall@10 | 0.2% | **100%** | **正确性修复** | M201/M202：prune_neighbors 局部向量缓存解决 load_vec None bug |

#### M201/M202：Vector HNSW recall 正确性修复

> **根因**：`prune_neighbors` 中 `load_vec(新节点id)` 返回 None（向量尚未持久化），
> 导致新节点被静默从邻居的反向连接中删除，HNSW 图退化为单向稀疏图。
>
> **修复**：`hnsw_insert` 内部维护局部向量缓存 `HashMap<u64, Vec<f32>>`，新向量预存入缓存。
> `search_layer_cached` 和 `prune_neighbors` 共享此缓存，零额外 I/O。

| 指标 | 修复前 | M202 修复后 | **M205 优化后** | 说明 |
|------|--------|-----------|---------------|------|
| **recall@10** | 0.2% | 100% | **100%** ✅ | 正确性 bug 彻底修复 |
| INSERT (50K, dim=128) | 2,376 vecs/s | 248 vecs/s | **357 vecs/s** | ⬆️ 44% vs M202 |
| KNN k=10 (1K queries) | 14,737 qps | 845 qps | **1,082 qps** | ⬆️ 28% vs M202 |
| batch_search ×10 | — | 107ms | **25ms** | ⬆️ 4.3x vs M202 |

**M205 Sprint 1-6 优化成果（保持 recall=100%，性能大幅恢复）：**

- recall 0.2% 是**正确性 bug**——修复前的"高性能"是图退化的假象（搜索秒退但结果全错）
- INSERT 230 vecs/s 在 AI 场景可接受——知识库构建是离线一次性操作
- KNN 795 qps 对嵌入式单用户够用——单 AI 应用 <100 qps，有 **8x 余量**

#### 竞品对比不足项（后续版本攻克）

| 优先级 | 引擎 | 指标 | Talon | 竞品 | 差距 | 优化方向 |
|--------|------|------|-------|------|------|----------|
| P1 | Vector | INSERT/KNN | 357/1,082 | Qdrant 1K-5K/5K-20K | 3-18x | mmap 向量 + HNSW 图内存驻留 |
| P2 | FTS | 搜索 qps | 303 | ES 1K-10K | 3-33x | Posting 合并 + 块式压缩 |
| P1 | SQL | 事务 txn/s | 2.3K | SQLite 20K | 8.7x | prepared statement |
| P2 | SQL | 全表扫描/排序 | 260ms | SQLite 50ms | 5x | 列式存储 |
| P2 | GEO | 大半径搜索 | 31 qps | Redis 10K+ | 300x | 内存型差异 |

### 13.10 全引擎汇总（含 M200-M202 优化）

| 引擎 | 指标数 | 核心写入吞吐 | 核心读取吞吐 | 磁盘 (100K-1M) | RSS 增量 |
|------|--------|-------------|-------------|---------------|---------|
| **KV** | 15 | 1.46M SET/s | 737K GET/s | 70MB | 158MB |
| **SQL** | 25 | 308K INSERT/s | 252K PK查/s | 147MB | 213MB |
| **TS** | 14 | 657K pts/s | **352µs/1.07ms** 查询 | 84MB | 176MB |
| **Vector** | 12 | 357 INSERT/s | 1,082 KNN/s, **recall=100%** | 171MB | 209MB |
| **FTS** | 11 | 17.9K docs/s | 303 search/s | 308MB | 218MB |
| **MQ** | 10 | 1.57M pub/s | 2.3K poll/s | 187MB | 170MB |
| **GEO** | 12 | 515K ADD/s | 550K POS/s | 64MB | 21MB |
| **综合** | 6 | — | — | 164MB 全量 | 407MB 全量 |
| **合计** | **105** | — | — | — | — |
