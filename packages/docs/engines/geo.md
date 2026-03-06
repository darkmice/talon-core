# GEO Engine

Geohash-based spatial indexing with Redis GEO command compatibility and GEOSEARCHSTORE.

## Overview

The GEO Engine provides spatial indexing using 52-bit Geohash encoding (~0.6m precision) over LSM-Tree prefix scans. It is compatible with Redis GEO command semantics and supports circle/rectangle range searches, geofencing, batch operations, and conditional writes (NX/XX/CH modes).

## Quick Start

```rust
let db = Talon::open("./data")?;

db.geo()?.create("places")?;
db.geo()?.geo_add("places", "office", 116.4074, 39.9042)?;
db.geo()?.geo_add("places", "cafe", 116.4084, 39.9052)?;

// Circle search within 500 meters
let nearby = db.geo()?.geo_search("places", 116.4074, 39.9042, 500.0, GeoUnit::Meters, 10)?;

// Distance between two members
let dist = db.geo()?.geo_dist("places", "office", "cafe", GeoUnit::Meters)?;
```

## API Reference

### Namespace Management

#### `create`
```rust
pub fn create(&self, name: &str) -> Result<(), Error>
```
Create a GEO namespace.

### Write Operations

#### `geo_add`
```rust
pub fn geo_add(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<(), Error>
```
Add or update a member's coordinates. Coordinates: lng `[-180, 180]`, lat `[-85.05, 85.05]`.

#### `geo_add_batch`
```rust
pub fn geo_add_batch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<(), Error>
```
Batch add members via WriteBatch.

#### `geo_add_nx`
```rust
pub fn geo_add_nx(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>
```
Add only if member does not exist. Returns `false` if already exists. (Redis `GEOADD NX`)

#### `geo_add_xx`
```rust
pub fn geo_add_xx(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>
```
Update only if member exists. Returns `false` if not found. (Redis `GEOADD XX`)

#### `geo_add_ch`
```rust
pub fn geo_add_ch(&self, name: &str, member_key: &str, lng: f64, lat: f64) -> Result<bool, Error>
```
Returns `true` if there was a change (new member or coordinates changed). (Redis `GEOADD CH`)

#### `geo_add_batch_ch`
```rust
pub fn geo_add_batch_ch(&self, name: &str, members: &[(&str, f64, f64)]) -> Result<u64, Error>
```
Batch add with change tracking. Returns number of changes.

#### `geo_del`
```rust
pub fn geo_del(&self, name: &str, member_key: &str) -> Result<bool, Error>
```
Delete a member. Returns `true` if member existed.

### Read Operations

#### `geo_pos`
```rust
pub fn geo_pos(&self, name: &str, member_key: &str) -> Result<Option<GeoPoint>, Error>
```
Get member coordinates. Returns `None` if not found.

#### `geo_dist`
```rust
pub fn geo_dist(&self, name: &str, key1: &str, key2: &str, unit: GeoUnit) -> Result<Option<f64>, Error>
```
Distance between two members. Returns `None` if either member not found.

**GeoUnit:** `Meters`, `Kilometers`, `Miles`

#### `geo_hash`
```rust
pub fn geo_hash(&self, name: &str, member_key: &str) -> Result<Option<String>, Error>
```
Get 11-character base32 geohash string for a member.

#### `geo_members`
```rust
pub fn geo_members(&self, name: &str) -> Result<Vec<String>, Error>
```
List all member keys in a namespace.

#### `geo_count`
```rust
pub fn geo_count(&self, name: &str) -> Result<u64, Error>
```
Count members in a namespace. (Redis `ZCARD`)

### Search Operations

#### `geo_search` (Circle)
```rust
pub fn geo_search(
    &self, name: &str,
    lng: f64, lat: f64,
    radius: f64, unit: GeoUnit,
    limit: usize,
) -> Result<Vec<GeoMember>, Error>
```
Search within a circle, sorted by distance ascending.

#### `geo_search_box` (Rectangle)
```rust
pub fn geo_search_box(
    &self, name: &str,
    lng: f64, lat: f64,
    width: f64, height: f64, unit: GeoUnit,
    limit: usize,
) -> Result<Vec<GeoMember>, Error>
```
Search within a bounding box.

#### `geo_search_store`
```rust
pub fn geo_search_store(
    &self, name: &str, dest: &str,
    lng: f64, lat: f64,
    radius: f64, unit: GeoUnit,
    limit: usize,
) -> Result<u64, Error>
```
Search and store results in destination namespace. Returns count stored. (Redis 7.0 `GEOSEARCHSTORE`)

#### `geo_search_box_store`
```rust
pub fn geo_search_box_store(
    &self, name: &str, dest: &str,
    lng: f64, lat: f64,
    width: f64, height: f64, unit: GeoUnit,
    limit: usize,
) -> Result<u64, Error>
```
Box search and store results. Combines `geo_search_box` + store.

#### `geo_fence`
```rust
pub fn geo_fence(
    &self, name: &str,
    lng: f64, lat: f64,
    radius: f64, unit: GeoUnit,
) -> Result<Vec<GeoMember>, Error>
```
Geofence check — returns all members currently inside the given circle. Useful for monitoring/alerting.

### Cross-Engine Queries

```rust
use talon::{geo_vector_search, GeoVectorQuery};

// GEO + Vector: geographic filter → vector similarity ranking
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

### Data Types

```rust
pub struct GeoPoint {
    pub lng: f64,  // Longitude [-180, 180]
    pub lat: f64,  // Latitude [-85.05, 85.05]
}

pub struct GeoMember {
    pub key: String,          // Member identifier
    pub point: GeoPoint,      // Coordinates
    pub dist: Option<f64>,    // Distance from search center (meters)
}

pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
}
```

## Redis GEO / PostGIS Compatibility

### Redis GEO Command Mapping

| Redis Command | Talon Equivalent |
|---------------|------------------|
| `GEOADD key lng lat member` | `geo_add(ns, key, point, NxXxMode)` |
| `GEOADD NX` / `XX` / `CH` | `geo_add` with `NxXxMode` param |
| `GEODIST key m1 m2 [unit]` | `geo_dist(ns, key1, key2, unit)` |
| `GEOPOS key member` | `geo_get(ns, key)` |
| `GEOSEARCH key FROMLONLAT lng lat BYRADIUS r` | `geo_radius(ns, center, radius, unit, limit)` |
| `GEOSEARCH BYBOX` | `geo_bbox(ns, min, max, limit)` |
| `GEOSEARCHSTORE` | `geo_radius` + `geo_add` |
| `ZREM key member` | `geo_remove(ns, key)` |
| `ZCARD key` | `geo_count(ns)` |

### Feature Comparison

| Feature | Redis GEO | PostGIS | Talon GEO |
|---------|-----------|---------|-----------|
| Point storage | ✅ | ✅ | ✅ |
| Radius search | ✅ | ✅ | ✅ |
| Bounding box search | ✅ | ✅ | ✅ |
| Distance calculation | ✅ | ✅ | ✅ |
| KNN (nearest N) | ✅ 6.2+ | ✅ | ✅ |
| Haversine formula | ✅ | ✅ | ✅ |
| NX/XX/CH modes | ✅ | ❌ | ✅ |
| Namespace isolation | ❌ | ✅ (schema) | ✅ |
| Polygon / LineString | ❌ | ✅ | ❌ |
| Spatial index (R-tree) | ❌ (sorted set) | ✅ | ❌ (geohash) |
| SQL integration | ❌ | ✅ | ✅ (`ST_DISTANCE`, `ST_WITHIN`) |
| Embedded mode | ❌ | ❌ | ✅ |
| Multi-model | ❌ | ❌ | ✅ |

### Talon-Only Features

- **SQL-native geo queries** — `SELECT ST_DISTANCE(location, GEOPOINT(39.9, 116.4))` in standard SQL
- **Cross-engine fusion** — geo + vector + FTS for location-aware semantic search
- **Namespace isolation** — multiple independent geo indexes in one database
