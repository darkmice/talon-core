# KV 引擎

Redis 兼容键值存储，支持 TTL、批量操作、快照读和 744K ops/s 吞吐。

## 概述

KV 引擎提供高性能键值存储，支持 TTL 过期、原子操作、glob 模式匹配和快照读。值以原始字节存储，带 8 字节 TTL 头。最大值大小 16 MB。

## 快速开始

```rust
let db = Talon::open("./data")?;

db.kv()?.set(b"user:1", b"Alice", None)?;           // 无 TTL
db.kv()?.set(b"session:abc", b"token", Some(3600))?; // 1 小时 TTL

let val = db.kv_read()?.get(b"user:1")?;             // 读取（并发安全）
db.kv()?.del(b"user:1")?;                            // 删除
```

## API 参考

### 写操作

#### `set`
```rust
pub fn set(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<(), Error>
```
写入键值对。`ttl_secs = None` 表示永不过期。最大值大小 16 MB。

#### `setnx`
```rust
pub fn setnx(&self, key: &[u8], value: &[u8], ttl_secs: Option<u64>) -> Result<bool, Error>
```
仅在 key 不存在时写入。返回 `true` 表示写入成功。

#### `mset`
```rust
pub fn mset(&self, keys: &[&[u8]], values: &[&[u8]]) -> Result<(), Error>
```
批量写入（无 TTL）。通过 WriteBatch 实现单次日志写入。

#### `append`
```rust
pub fn append(&self, key: &[u8], value: &[u8]) -> Result<usize, Error>
```
追加字节到已有值末尾。返回追加后总长度。key 不存在则创建。

#### `setrange`
```rust
pub fn setrange(&self, key: &[u8], offset: usize, value: &[u8]) -> Result<usize, Error>
```
在偏移位置覆写。间隙用零填充。返回最终长度。

#### `getset`
```rust
pub fn getset(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, Error>
```
原子性写入新值，返回旧值。新值不带 TTL。

#### `rename`
```rust
pub fn rename(&self, src: &[u8], dst: &[u8]) -> Result<(), Error>
```
重命名 key。保留 TTL。源必须存在，目标会被覆盖。

### 读操作

#### `get`
```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>
```
读取值。过期 key 惰性删除。返回不含 TTL 头的净荷。

#### `mget`
```rust
pub fn mget(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Error>
```
批量读取。返回顺序与输入 keys 一致。

#### `exists`
```rust
pub fn exists(&self, key: &[u8]) -> Result<bool, Error>
```
检查 key 是否存在（含 TTL 检查）。无数据拷贝开销。

#### `strlen`
```rust
pub fn strlen(&self, key: &[u8]) -> Result<Option<usize>, Error>
```
获取值字节长度（不含 TTL 头）。

#### `getrange`
```rust
pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>, Error>
```
获取字节子串 `[start, end]`（闭区间）。负数从末尾计算。

#### `key_type`
```rust
pub fn key_type(&self, key: &[u8]) -> Result<&'static str, Error>
```
返回 `"string"`（有效 UTF-8）、`"bytes"`（二进制）或 `"none"`（不存在/已过期）。

#### `random_key`
```rust
pub fn random_key(&self) -> Result<Option<Vec<u8>>, Error>
```
返回一个未过期的 key（前缀扫描实现，非真随机）。

### 删除操作

#### `del`
```rust
pub fn del(&self, key: &[u8]) -> Result<(), Error>
```
删除单个 key（墓碑写入，不检查存在性）。

#### `mdel`
```rust
pub fn mdel(&self, keys: &[&[u8]]) -> Result<(), Error>
```
批量删除（WriteBatch）。

#### `del_prefix`
```rust
pub fn del_prefix(&self, prefix: &[u8]) -> Result<u64, Error>
```
删除所有指定前缀的 key。返回删除数量。原子操作。

### TTL 操作

#### `expire` / `pexpire`
```rust
pub fn expire(&self, key: &[u8], secs: u64) -> Result<(), Error>
pub fn pexpire(&self, key: &[u8], millis: u64) -> Result<(), Error>
```
设置 TTL（秒/毫秒）。

#### `ttl` / `pttl`
```rust
pub fn ttl(&self, key: &[u8]) -> Result<Option<u64>, Error>
pub fn pttl(&self, key: &[u8]) -> Result<Option<u64>, Error>
```
剩余 TTL（秒/毫秒）。`None` = 无 TTL 或已过期。

#### `persist`
```rust
pub fn persist(&self, key: &[u8]) -> Result<bool, Error>
```
移除 TTL，使 key 永久化。返回 `true` 表示 TTL 被移除。

#### `expire_at` / `expire_time`
```rust
pub fn expire_at(&self, key: &[u8], timestamp: u64) -> Result<bool, Error>
pub fn expire_time(&self, key: &[u8]) -> Result<Option<u64>, Error>
```
设置/获取过期 Unix 时间戳。

### 计数器操作

```rust
pub fn incr(&self, key: &[u8]) -> Result<i64, Error>     // +1
pub fn decr(&self, key: &[u8]) -> Result<i64, Error>     // -1
pub fn incrby(&self, key: &[u8], delta: i64) -> Result<i64, Error>
pub fn decrby(&self, key: &[u8], delta: i64) -> Result<i64, Error>
pub fn incrbyfloat(&self, key: &[u8], delta: f64) -> Result<f64, Error>
```
原子计数。key 不存在时从 0 开始。

### 扫描与枚举

#### `keys_prefix`
```rust
pub fn keys_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, Error>
```
列出指定前缀的所有 key（TTL 过滤）。

#### `keys_match`
```rust
pub fn keys_match(&self, pattern: &[u8]) -> Result<Vec<Vec<u8>>, Error>
```
按 glob 模式匹配 key（`*` 和 `?`）。使用前缀优化。

#### `keys_prefix_limit`
```rust
pub fn keys_prefix_limit(&self, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<Vec<u8>>, Error>
```
分页列出 key。亿级 key 安全。

#### `scan_prefix_limit`
```rust
pub fn scan_prefix_limit(&self, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<KvPair>, Error>
```
分页扫描键值对。

#### `key_count`
```rust
pub fn key_count(&self) -> Result<u64, Error>
```
总 key 数量。O(1) 内存流式扫描。

#### `disk_space`
```rust
pub fn disk_space(&self) -> u64
```
磁盘使用量（字节）。

### 快照读

```rust
pub fn snapshot_get(&self, snap: &Snapshot, key: &[u8]) -> Result<Option<Vec<u8>>, Error>
pub fn snapshot_scan_prefix_limit(&self, snap: &Snapshot, prefix: &[u8], offset: u64, limit: u64) -> Result<Vec<KvPair>, Error>
```
从时间点快照读取，不执行惰性删除。

### 后台 TTL 清理

```rust
pub fn start_ttl_cleaner(&self, interval_secs: u64) -> TtlCleaner
```
启动后台线程定期清理过期 key。Drop 返回的句柄即停止。

## Redis 兼容性

Talon KV 通过 RESP 协议（端口 6380）实现 Redis 兼容命令，任何 Redis 客户端库可直接使用。

### 支持的命令

| 类别 | 命令 |
|------|------|
| **字符串** | `SET` (EX), `GET`, `DEL`, `MSET`, `MGET`, `SETNX`, `GETSET`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT` |
| **Key** | `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `EXPIREAT`, `EXPIRETIME`, `RENAME`, `TYPE`, `RANDOMKEY`, `KEYS`, `DBSIZE` |
| **服务器** | `PING`, `INFO`, `COMMAND COUNT` |

### 不支持的功能（对比 Redis）

| 功能 | Redis | Talon | 替代方案 |
|------|-------|-------|----------|
| List（`LPUSH`/`RPOP`…） | ✅ | ❌ | 使用 MQ 引擎 |
| Set（`SADD`/`SMEMBERS`…） | ✅ | ❌ | 使用 SQL 引擎 |
| Sorted Set（`ZADD`/`ZRANGE`…） | ✅ | ❌ | 使用 SQL + 索引 |
| Hash（`HSET`/`HGET`…） | ✅ | ❌ | 使用 JSON 值存储 |
| Pub/Sub | ✅ | ❌ | 使用 MQ 引擎 |
| Streams | ✅ | ❌ | 使用 MQ 引擎 |
| Lua 脚本 | ✅ | ❌ | 使用 Rust API |
| 集群 | ✅ | ⚠️ | Primary-Replica 模式 |

### Talon 独有特性（超越 Redis）

| 特性 | 说明 |
|------|------|
| `del_prefix(prefix)` | 按前缀批量删除 |
| `keys_prefix_limit(prefix, offset, limit)` | 分页前缀扫描 |
| `scan_prefix_limit(prefix, offset, limit)` | 分页 key+value 扫描 |
| `snapshot_get` / `snapshot_scan` | 时间点一致性读取 |
| `key_count()` | O(1) key 总数 |
| `disk_space()` | 存储用量（字节） |
| 批量 API（`set_batch`/`mset_batch`） | WriteBatch 原子写入 |

## 性能

| 基准测试 | 结果 |
|----------|------|
| 批量 SET（1M key） | 744K ops/s |
| GET | ~900K ops/s |
| INCR | ~800K ops/s |
