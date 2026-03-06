# 时序引擎

高性能时间序列存储，支持降采样聚合、保留策略和 540K pts/s 写入吞吐。

## 概述

时序引擎存储带时间戳的数据点和命名字段，支持范围查询、聚合（降采样）、保留策略和 InfluxDB 行协议导入。

## 快速开始

```rust
use talon::{Talon, TsSchema};

let db = Talon::open("./data")?;

let schema = TsSchema::new(vec!["cpu".into(), "mem".into()]);
let ts = db.create_timeseries("metrics", schema)?;

ts.insert(1700000000_000, &[0.85, 0.72])?;
ts.insert(1700000001_000, &[0.90, 0.68])?;

use talon::TsQuery;
let points = ts.query(&TsQuery {
    start: Some(1700000000_000),
    end: None,
    order_asc: true,
    limit: Some(100),
})?;
```

## API 参考

### `Talon::create_timeseries`

创建新的时序表。

```rust
pub fn create_timeseries(&self, name: &str, schema: TsSchema) -> Result<TsEngine, Error>
```

### `Talon::open_timeseries`

打开已有的时序表。

```rust
pub fn open_timeseries(&self, name: &str) -> Result<TsEngine, Error>
```

### `TsEngine::insert`

插入单个数据点。

```rust
pub fn insert(&self, point: &DataPoint) -> Result<(), Error>
```

### `TsEngine::insert_batch`

批量插入数据点（WriteBatch）。

```rust
pub fn insert_batch(&self, points: &[DataPoint]) -> Result<(), Error>
```

### `TsEngine::query`

查询数据点，支持时间范围、排序和限制。

```rust
pub fn query(&self, q: &TsQuery) -> Result<Vec<DataPoint>, Error>
```

**`TsQuery` 字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `start` | `Option<i64>` | 起始时间戳（包含） |
| `end` | `Option<i64>` | 结束时间戳（包含） |
| `order_asc` | `bool` | `true` = 升序 |
| `limit` | `Option<usize>` | 最大结果数 |

### `TsEngine::aggregate`

按时间桶降采样聚合。

```rust
pub fn aggregate(&self, q: &TsAggQuery) -> Result<Vec<AggBucket>, Error>
```

**`TsAggQuery` 字段：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `interval_ms` | `i64` | 桶大小（毫秒） |
| `func` | `AggFunc` | `Avg`, `Sum`, `Min`, `Max`, `Count`, `First`, `Last` |
| `field_index` | `usize` | 聚合哪个字段 |
| `fill` | `FillStrategy` | `None`, `Null`, `Previous`, `Linear`, `Value(f64)` |

### 访问器

```rust
pub fn name(&self) -> &str       // 时序表名
pub fn schema(&self) -> &TsSchema // 获取 schema
```

### 保留策略与清理

#### `set_retention` / `get_retention`
```rust
pub fn set_retention(&self, duration_ms: u64) -> Result<(), Error>
pub fn get_retention(&self) -> Result<Option<u64>, Error>
```
设置/获取数据保留策略。超过保留期的数据可被清理。

#### `purge_expired`
```rust
pub fn purge_expired(&self) -> Result<u64, Error>
```
删除超过保留策略的数据点。返回删除数量。

#### `purge_before`
```rust
pub fn purge_before(&self, cutoff_ms: i64) -> Result<u64, Error>
```
删除指定时间戳之前的所有数据。

#### `purge_by_tag`
```rust
pub fn purge_by_tag(&self, tag_filters: &[(String, String)]) -> Result<u64, Error>
```
按标签条件删除数据。

### 标签操作

```rust
pub fn tag_values(&self, tag_name: &str) -> Result<Vec<String>, Error>
pub fn all_tag_values(&self) -> Result<BTreeMap<String, Vec<String>>, Error>
```

### 管理函数

```rust
talon::list_timeseries(&store)?;                          // 列出所有时序表
talon::describe_timeseries(&store, "metrics")?;           // 描述（schema + 统计）
talon::drop_timeseries(&store, "metrics")?;               // 删除
talon::start_ts_retention_cleaner(&store, interval_secs); // 启动自动清理
```

### InfluxDB 行协议导入

```rust
use talon::parse_line_protocol;
let line = "cpu,host=server01 usage=0.85 1700000000000000000";
let points = parse_line_protocol(line)?;
```

## InfluxDB / TimescaleDB 兼容性

### InfluxDB 行协议

完整支持 InfluxDB Line Protocol 写入格式：

```
measurement,tag1=v1,tag2=v2 field1=1.0,field2="str",field3=42i timestamp_ns
```

- 纳秒时间戳自动转换为毫秒
- 字段类型：整数（`42i`）、浮点（`1.5`）、字符串（`"str"`）、布尔（`true`/`false`/`T`/`F`）
- `#` 注释行和空行自动跳过
- 支持多行输入

### 功能对比

| 功能 | InfluxDB | TimescaleDB | Talon TS |
|------|----------|-------------|----------|
| Line Protocol 写入 | ✅ | ❌ | ✅ |
| 标签过滤 | ✅ | ✅（索引） | ✅ |
| 保留策略 | ✅ | ✅ | ✅ |
| 降采样/聚合 | ✅ | ✅ | ✅（查询时） |
| `TIME_BUCKET` 函数 | ❌ | ✅ | ✅（SQL 引擎） |
| SQL 查询 | ❌（InfluxQL） | ✅ | ✅（跨引擎） |
| 嵌入式模式 | ❌ | ❌ | ✅ |
| 单二进制 | ❌ | ❌（PostgreSQL） | ✅ |
| 多模融合（KV+SQL+Vector） | ❌ | ❌ | ✅ |

### Talon 独有特性

- **多引擎融合** — TS 数据与 SQL 表、向量搜索、图遍历联合查询
- **嵌入式部署** — 无需外部数据库进程
- **AI 原生** — TIME_BUCKET 用于 LLM Token 用量统计、Agent 追踪时间线

## 性能

| 基准测试 | 结果 |
|----------|------|
| 批量写入（1M 点） | 540K pts/s |
| 范围查询（ASC + LIMIT） | P95 1.0ms |
