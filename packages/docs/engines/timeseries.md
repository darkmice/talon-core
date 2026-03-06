# TimeSeries Engine

High-performance time-series storage with downsampling, retention policies, and 540K pts/s ingestion.

## Overview

The TimeSeries Engine stores timestamped data points with named fields. It supports range queries, aggregation (downsampling), retention policies, and InfluxDB Line Protocol import.

## Quick Start

```rust
use talon::{Talon, TsSchema};

let db = Talon::open("./data")?;

// Create a time-series table with two fields
let schema = TsSchema::new(vec!["cpu".into(), "mem".into()]);
let ts = db.create_timeseries("metrics", schema)?;

// Write data points
ts.insert(1700000000_000, &[0.85, 0.72])?;
ts.insert(1700000001_000, &[0.90, 0.68])?;

// Query recent data
use talon::TsQuery;
let points = ts.query(&TsQuery {
    start: Some(1700000000_000),
    end: None,
    order_asc: true,
    limit: Some(100),
})?;
```

## API Reference

### `Talon::create_timeseries`

Create a new time-series table.

```rust
pub fn create_timeseries(&self, name: &str, schema: TsSchema) -> Result<TsEngine, Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `&str` | Time-series name |
| `schema` | `TsSchema` | Field definitions |

### `Talon::open_timeseries`

Open an existing time-series table.

```rust
pub fn open_timeseries(&self, name: &str) -> Result<TsEngine, Error>
```

### `TsEngine::insert`

Insert a single data point.

```rust
pub fn insert(&self, timestamp_ms: i64, values: &[f64]) -> Result<(), Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `timestamp_ms` | `i64` | Unix timestamp in milliseconds |
| `values` | `&[f64]` | Field values (must match schema length) |

### `TsEngine::insert_batch`

Batch insert multiple data points via WriteBatch.

```rust
pub fn insert_batch(&self, points: &[(i64, Vec<f64>)]) -> Result<(), Error>
```

### `TsEngine::query`

Query data points with time range, ordering, and limit.

```rust
pub fn query(&self, q: &TsQuery) -> Result<Vec<DataPoint>, Error>
```

**`TsQuery` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `start` | `Option<i64>` | Start timestamp (inclusive) |
| `end` | `Option<i64>` | End timestamp (inclusive) |
| `order_asc` | `bool` | `true` = ascending, `false` = descending |
| `limit` | `Option<usize>` | Max results |

### `TsEngine::aggregate`

Downsampling aggregation over time buckets.

```rust
pub fn aggregate(&self, q: &TsAggQuery) -> Result<Vec<AggBucket>, Error>
```

**`TsAggQuery` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `start` | `Option<i64>` | Start timestamp |
| `end` | `Option<i64>` | End timestamp |
| `interval_ms` | `i64` | Bucket size in milliseconds |
| `func` | `AggFunc` | `Avg`, `Sum`, `Min`, `Max`, `Count`, `First`, `Last` |
| `field_index` | `usize` | Which field to aggregate |
| `fill` | `FillStrategy` | `None`, `Null`, `Previous`, `Linear`, `Value(f64)` |

### Accessors

#### `name`
```rust
pub fn name(&self) -> &str
```
Get the time-series name.

#### `schema`
```rust
pub fn schema(&self) -> &TsSchema
```
Get the schema (field definitions).

### Retention & Purging

#### `set_retention`
```rust
pub fn set_retention(&self, duration_ms: u64) -> Result<(), Error>
```
Set data retention policy. Data older than `duration_ms` will be eligible for purging.

#### `get_retention`
```rust
pub fn get_retention(&self) -> Result<Option<u64>, Error>
```
Get current retention duration (milliseconds). `None` = no retention policy.

#### `purge_expired`
```rust
pub fn purge_expired(&self) -> Result<u64, Error>
```
Delete all data points that exceed the retention policy. Returns count deleted.

#### `purge_before`
```rust
pub fn purge_before(&self, cutoff_ms: i64) -> Result<u64, Error>
```
Delete all data points before the given timestamp. Returns count deleted.

#### `purge_by_tag`
```rust
pub fn purge_by_tag(&self, tag_filters: &[(String, String)]) -> Result<u64, Error>
```
Delete data points matching specific tag key-value pairs.

### Tag Operations

#### `tag_values`
```rust
pub fn tag_values(&self, tag_name: &str) -> Result<Vec<String>, Error>
```
List all distinct values for a given tag.

#### `all_tag_values`
```rust
pub fn all_tag_values(&self) -> Result<BTreeMap<String, Vec<String>>, Error>
```
List all tag names and their distinct values.

### Utility Functions

```rust
// List all time-series tables
let names = talon::list_timeseries(&store)?;

// Describe a time-series (schema + stats)
let info: TsInfo = talon::describe_timeseries(&store, "metrics")?;

// Drop a time-series
talon::drop_timeseries(&store, "metrics")?;

// Start retention cleaner (auto-delete old data)
let cleaner = talon::start_ts_retention_cleaner(&store, interval_secs);
```

### InfluxDB Line Protocol Import

```rust
use talon::parse_line_protocol;

let line = "cpu,host=server01 usage=0.85 1700000000000000000";
let points = parse_line_protocol(line)?;
```

## InfluxDB / TimescaleDB Compatibility

### InfluxDB Line Protocol

Full support for InfluxDB Line Protocol write format:

```
measurement,tag1=v1,tag2=v2 field1=1.0,field2="str",field3=42i timestamp_ns
```

- Nanosecond timestamps auto-converted to milliseconds
- Field types: integer (`42i`), float (`1.5`), string (`"str"`), boolean (`true`/`false`/`T`/`F`)
- `#` comment lines and empty lines auto-skipped
- Multi-line input supported

### Feature Comparison

| Feature | InfluxDB | TimescaleDB | Talon TS |
|---------|----------|-------------|----------|
| Line Protocol write | ✅ | ❌ | ✅ |
| Tag-based filtering | ✅ | ✅ (index) | ✅ |
| Retention policies | ✅ | ✅ | ✅ |
| Downsampling / aggregation | ✅ | ✅ | ✅ (query-time) |
| `TIME_BUCKET` function | ❌ | ✅ | ✅ (SQL engine) |
| Flux query language | ✅ | ❌ | ❌ |
| SQL queries | ❌ (InfluxQL) | ✅ | ✅ (cross-engine) |
| Continuous queries | ✅ | ✅ | ❌ |
| Purge by tag / time | ✅ | ✅ | ✅ |
| Embedded mode | ❌ | ❌ | ✅ |
| Single binary | ❌ | ❌ (PostgreSQL) | ✅ |
| Multi-model (KV+SQL+Vector) | ❌ | ❌ | ✅ |

### Talon-Only Features

- **Multi-engine fusion** — join TS data with SQL tables, vector search, graph traversal
- **Embedded deployment** — no external database process needed
- **AI-native** — TIME_BUCKET for LLM token usage tracking, agent trace timelines

## Performance

| Benchmark | Result |
|-----------|--------|
| Batch INSERT (1M points) | 540K pts/s |
| Range query (ASC + LIMIT) | P95 1.0ms |
