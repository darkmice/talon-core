# GEO 地理引擎

基于 Geohash 的空间索引，Redis GEO 命令兼容，支持 GEOSEARCHSTORE 和地理围栏。

## 概述

GEO 引擎使用 52 位 Geohash 编码（~0.6m 精度）在 LSM-Tree 前缀扫描上实现空间索引。兼容 Redis GEO 命令语义，支持圆形/矩形范围搜索、地理围栏、批量操作和条件写入（NX/XX/CH 模式）。

## 快速开始

```rust
let db = Talon::open("./data")?;

db.geo()?.create("places")?;
db.geo()?.geo_add("places", "office", 116.4074, 39.9042)?;
db.geo()?.geo_add("places", "cafe", 116.4084, 39.9052)?;

let nearby = db.geo()?.geo_search("places", 116.4074, 39.9042, 500.0, GeoUnit::Meters, 10)?;
let dist = db.geo()?.geo_dist("places", "office", "cafe", GeoUnit::Meters)?;
```

## API 参考

### 命名空间管理

```rust
pub fn create(&self, name: &str) -> Result<(), Error>
```

### 写操作

```rust
pub fn geo_add(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<(), Error>
pub fn geo_add_batch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<(), Error>
pub fn geo_add_nx(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>  // 仅不存在时写入
pub fn geo_add_xx(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>  // 仅存在时更新
pub fn geo_add_ch(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>  // 返回是否有变更
pub fn geo_add_batch_ch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<u64, Error>    // 批量+变更计数
pub fn geo_del(&self, name: &str, member_key: &str) -> Result<bool, Error>
```

坐标范围：经度 `[-180, 180]`，纬度 `[-85.05, 85.05]`。

### 读操作

```rust
pub fn geo_pos(&self, name: &str, member_key: &str) -> Result<Option<GeoPoint>, Error>
pub fn geo_dist(&self, name: &str, key1: &str, key2: &str, unit: GeoUnit) -> Result<Option<f64>, Error>
pub fn geo_hash(&self, name: &str, member_key: &str) -> Result<Option<String>, Error>  // 11 字符 base32
pub fn geo_members(&self, name: &str) -> Result<Vec<String>, Error>
pub fn geo_count(&self, name: &str) -> Result<u64, Error>
```

### 搜索操作

#### 圆形搜索
```rust
pub fn geo_search(&self, name: &str, lng: f64, lat: f64, radius: f64, unit: GeoUnit, limit: usize) -> Result<Vec<GeoMember>, Error>
```

#### 矩形搜索
```rust
pub fn geo_search_box(&self, name: &str, lng: f64, lat: f64, width: f64, height: f64, unit: GeoUnit, limit: usize) -> Result<Vec<GeoMember>, Error>
```

#### 搜索并存储
```rust
pub fn geo_search_store(&self, name: &str, dest: &str, lng: f64, lat: f64, radius: f64, unit: GeoUnit, limit: usize) -> Result<u64, Error>
pub fn geo_search_box_store(&self, name: &str, dest: &str, lng: f64, lat: f64, width: f64, height: f64, unit: GeoUnit, limit: usize) -> Result<u64, Error>
```

#### 地理围栏
```rust
pub fn geo_fence(&self, name: &str, lng: f64, lat: f64, radius: f64, unit: GeoUnit) -> Result<Vec<GeoMember>, Error>
```
检查当前在给定圆内的所有成员。用于监控/告警。

### 跨引擎查询

```rust
use talon::{geo_vector_search, GeoVectorQuery};

// GEO + 向量：地理过滤 → 向量相似度排序
let hits = geo_vector_search(&db.store_ref(), &GeoVectorQuery {
    geo_name: "places",
    vec_name: "embeddings",
    lng: 116.4074,
    lat: 39.9042,
    radius_m: 1000.0,
    query_vec: &embedding,
    k: 10,
})?;
```

### 数据类型

```rust
pub struct GeoPoint { pub lng: f64, pub lat: f64 }
pub struct GeoMember { pub key: String, pub point: GeoPoint, pub dist: Option<f64> }
pub enum GeoUnit { Meters, Kilometers, Miles }
```

## Redis GEO / PostGIS 兼容性

### Redis GEO 命令映射

| Redis 命令 | Talon 等效 |
|-----------|-----------|
| `GEOADD key lng lat member` | `geo_add(ns, key, point, NxXxMode)` |
| `GEOADD NX` / `XX` / `CH` | `geo_add` + `NxXxMode` 参数 |
| `GEODIST key m1 m2 [unit]` | `geo_dist(ns, key1, key2, unit)` |
| `GEOPOS key member` | `geo_get(ns, key)` |
| `GEOSEARCH BYRADIUS` | `geo_radius(ns, center, radius, unit, limit)` |
| `GEOSEARCH BYBOX` | `geo_bbox(ns, min, max, limit)` |
| `ZREM key member` | `geo_remove(ns, key)` |
| `ZCARD key` | `geo_count(ns)` |

### 功能对比

| 功能 | Redis GEO | PostGIS | Talon GEO |
|------|-----------|---------|-----------|
| 点存储 | ✅ | ✅ | ✅ |
| 半径搜索 | ✅ | ✅ | ✅ |
| 矩形搜索 | ✅ | ✅ | ✅ |
| 距离计算 | ✅ | ✅ | ✅ |
| KNN（最近 N 个） | ✅ 6.2+ | ✅ | ✅ |
| NX/XX/CH 模式 | ✅ | ❌ | ✅ |
| 命名空间隔离 | ❌ | ✅（schema） | ✅ |
| 多边形/线段 | ❌ | ✅ | ❌ |
| SQL 集成 | ❌ | ✅ | ✅（`ST_DISTANCE`/`ST_WITHIN`） |
| 嵌入式模式 | ❌ | ❌ | ✅ |
| 多模融合 | ❌ | ❌ | ✅ |

### Talon 独有特性

- **SQL 原生地理查询** — `SELECT ST_DISTANCE(location, GEOPOINT(39.9, 116.4))`
- **跨引擎融合** — 地理 + 向量 + FTS 位置感知语义搜索
- **命名空间隔离** — 单数据库内多个独立地理索引
