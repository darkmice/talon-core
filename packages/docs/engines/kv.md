# KV Engine

Redis-compatible key-value store with TTL, batch operations, snapshots, and 744K ops/s throughput.

## Overview

The KV Engine provides a high-performance key-value store with TTL support, atomic operations, glob pattern matching, and snapshot reads. Values are stored as raw bytes with an 8-byte TTL header. Maximum value size is 16 MB.

## Quick Start

```rust
let db = Talon::open("./data")?;

// Basic operations
db.kv()?.set(b"user:1", b"Alice", None)?;           // No TTL
db.kv()?.set(b"session:abc", b"token", Some(3600))?; // 1 hour TTL

let val = db.kv_read()?.get(b"user:1")?;             // Read (concurrent safe)
db.kv()?.del(b"user:1")?;                            // Delete
```

## API Reference

### Write Operations

#### `set`
```rust
pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), Error>
```
Write a key-value pair. `ttl_secs = None` means no expiration. Max value size: 16 MB.

#### `setnx`
```rust
pub fn setnx(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<bool, Error>
```
SET if Not eXists. Returns `true` if key was set, `false` if it already exists.

#### `mset`
```rust
pub fn mset(&self, keys: &[&[u8]], values: &[&[u8]]) -> Result<(), Error>
```
Batch set multiple key-value pairs (no TTL). Uses WriteBatch for single journal write.

#### `set_batch`
```rust
pub fn set_batch(&self, batch: &mut Batch, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), Error>
```
Add a set operation to a WriteBatch for manual batch control.

#### `append`
```rust
pub fn append(&self, key: &[u8], value: &[u8]) -> Result<usize, Error>
```
Append bytes to existing value. Returns total length after append. Key not found = create new.

#### `setrange`
```rust
pub fn setrange(&self, key: &[u8], offset: usize, value: &[u8]) -> Result<usize, Error>
```
Overwrite value at offset. Zero-fills gaps. Returns final length.

#### `getset`
```rust
pub fn getset(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Error>
```
Atomically set new value, return old value. Clears TTL on new value.

#### `rename`
```rust
pub fn rename(&self, src: &[u8], dst: &[u8]) -> Result<(), Error>
```
Rename key. TTL is preserved. Source must exist. Destination is overwritten.

### Read Operations

#### `get`
```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>
```
Read value. Lazily deletes expired keys. Returns payload without TTL header.

#### `mget`
```rust
pub fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Error>
```
Batch get multiple keys. Returns values in same order as keys.

#### `exists`
```rust
pub fn exists(&self, key: &[u8]) -> Result<bool, Error>
```
Check if key exists (with TTL check). No payload copy overhead.

#### `strlen`
```rust
pub fn strlen(&self, key: &[u8]) -> Result<Option<usize>, Error>
```
Get value byte length (excluding TTL header).

#### `getrange`
```rust
pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>, Error>
```
Get byte substring `[start, end]` (closed interval). Negative indexes from end.

#### `key_type`
```rust
pub fn key_type(&self, key: &[u8]) -> Result<&'static str, Error>
```
Returns `"string"` (valid UTF-8), `"bytes"` (binary), or `"none"` (not found/expired).

#### `random_key`
```rust
pub fn random_key(&self) -> Result<Option<Vec<u8>>, Error>
```
Return one unexpired key (prefix scan, not truly random).

### Delete Operations

#### `del`
```rust
pub fn del(&self, key: &[u8]) -> Result<(), Error>
```
Delete a single key (tombstone write, no existence check).

#### `mdel`
```rust
pub fn mdel(&self, keys: &[&[u8]]) -> Result<(), Error>
```
Batch delete via WriteBatch.

#### `del_prefix`
```rust
pub fn del_prefix(&self, prefix: &[u8]) -> Result<u64, Error>
```
Delete all keys with given prefix. Returns count deleted. Atomic (WriteBatch).

### TTL Operations

#### `expire`
```rust
pub fn expire(&self, key: &[u8], secs: u64) -> Result<(), Error>
```
Set TTL on existing key.

#### `pexpire`
```rust
pub fn pexpire(&self, key: &[u8], millis: u64) -> Result<(), Error>
```
Set TTL in milliseconds (rounded up to seconds internally).

#### `ttl`
```rust
pub fn ttl(&self, key: &[u8]) -> Result<Option<u64>, Error>
```
Remaining TTL in seconds. `None` = no TTL or expired.

#### `pttl`
```rust
pub fn pttl(&self, key: &[u8]) -> Result<Option<u64>, Error>
```
Remaining TTL in milliseconds.

#### `persist`
```rust
pub fn persist(&self, key: &[u8]) -> Result<bool, Error>
```
Remove TTL, make key permanent. Returns `true` if TTL was removed.

#### `expire_at`
```rust
pub fn expire_at(&self, key: &[u8], timestamp: u64) -> Result<bool, Error>
```
Set expiration at Unix timestamp (seconds). `timestamp = 0` = persist.

#### `expire_time`
```rust
pub fn expire_time(&self, key: &[u8]) -> Result<Option<u64>, Error>
```
Get expiration Unix timestamp. `Some(0)` = no TTL.

### Counter Operations

#### `incr` / `decr`
```rust
pub fn incr(&self, key: &[u8]) -> Result<i64, Error>
pub fn decr(&self, key: &[u8]) -> Result<i64, Error>
```
Atomic increment/decrement by 1. Key not found = start from 0.

#### `incrby` / `decrby`
```rust
pub fn incrby(&self, key: &[u8], delta: i64) -> Result<i64, Error>
pub fn decrby(&self, key: &[u8], delta: i64) -> Result<i64, Error>
```
Atomic increment/decrement by delta.

#### `incrbyfloat`
```rust
pub fn incrbyfloat(&self, key: &[u8], delta: f64) -> Result<f64, Error>
```
Atomic float increment. Returns error on NaN/Infinity. Do not mix with `incrby` on same key.

### Scan & Enumeration

#### `keys_prefix`
```rust
pub fn keys_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error>
```
List all keys with given prefix (TTL filtered).

#### `keys_match`
```rust
pub fn keys_match(&self, pattern: &[u8]) -> Result<Vec<Vec<u8>>, Error>
```
List keys matching glob pattern (`*` and `?`). Uses prefix optimization.

#### `keys_prefix_limit`
```rust
pub fn keys_prefix_limit(&self, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<Vec<u8>>, Error>
```
Paginated key listing. O(offset + limit) time, O(limit) memory. Billion-key safe.

#### `scan_prefix_limit`
```rust
pub fn scan_prefix_limit(&self, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<KvPair>, Error>
```
Paginated key-value scan with TTL filtering.

#### `key_count`
```rust
pub fn key_count(&self) -> Result<u64, Error>
```
Total key count. O(1) memory streaming scan.

#### `disk_space`
```rust
pub fn disk_space(&self) -> u64
```
Disk usage in bytes (based on SST statistics).

### Snapshot Reads

#### `snapshot_get`
```rust
pub fn snapshot_get(&self, snap: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>, Error>
```
Read from a point-in-time snapshot. Does not perform lazy deletion.

#### `snapshot_scan_prefix_limit`
```rust
pub fn snapshot_scan_prefix_limit(&self, snap: &Snapshot, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<KvPair>, Error>
```
Paginated scan from snapshot.

### Background TTL Cleanup

#### `start_ttl_cleaner`
```rust
pub fn start_ttl_cleaner(&self, interval_secs: u64) -> TtlCleaner
```
Start background thread that periodically purges expired keys. Drop the returned handle to stop.

```rust
let cleaner = db.kv()?.start_ttl_cleaner(60); // Scan every 60 seconds
// cleaner auto-stops when dropped
```

## Redis Compatibility

Talon KV implements Redis-compatible commands via RESP protocol (port 6380). Any Redis client library works.

### Supported Commands

| Category | Commands |
|----------|----------|
| **String** | `SET` (EX), `GET`, `DEL`, `MSET`, `MGET`, `SETNX`, `GETSET`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT` |
| **Key** | `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `EXPIREAT`, `EXPIRETIME`, `RENAME`, `TYPE`, `RANDOMKEY`, `KEYS`, `DBSIZE` |
| **Server** | `PING`, `INFO`, `COMMAND COUNT` |

### Not Supported (vs Redis)

| Feature | Redis | Talon | Reason |
|---------|-------|-------|--------|
| List (`LPUSH`, `RPOP`, ...) | ✅ | ❌ | Use MQ engine instead |
| Set (`SADD`, `SMEMBERS`, ...) | ✅ | ❌ | Use SQL engine |
| Sorted Set (`ZADD`, `ZRANGE`, ...) | ✅ | ❌ | Use SQL + index |
| Hash (`HSET`, `HGET`, ...) | ✅ | ❌ | Use JSON in KV value |
| Pub/Sub | ✅ | ❌ | Use MQ engine |
| Streams | ✅ | ❌ | Use MQ engine |
| Lua scripting | ✅ | ❌ | Use Rust API |
| Cluster | ✅ | ⚠️ | Primary-Replica only |

### Talon-Only Features (beyond Redis)

| Feature | Description |
|---------|-------------|
| `del_prefix(prefix)` | Delete all keys with prefix — O(scan) |
| `keys_prefix_limit(prefix, offset, limit)` | Paginated prefix scan |
| `scan_prefix_limit(prefix, offset, limit)` | Paginated key+value scan |
| `snapshot_get` / `snapshot_scan` | Point-in-time consistent reads |
| `key_count()` | O(1) total key count |
| `disk_space()` | Storage usage in bytes |
| Batch API (`set_batch`, `mset_batch`) | WriteBatch for atomicity |

## Performance

| Benchmark | Result |
|-----------|--------|
| Batch SET (1M keys) | 744K ops/s |
| GET | ~900K ops/s |
| INCR | ~800K ops/s |
