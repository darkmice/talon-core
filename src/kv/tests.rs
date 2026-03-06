/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! KV 引擎单元测试（从 mod.rs 拆分，保持 ≤500 行）。

use super::*;
use crate::storage::Store;

#[test]
fn kv_set_get_del_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k1", b"v1", None).unwrap();
    assert_eq!(kv.get(b"k1").unwrap().as_deref(), Some(b"v1" as &[u8]));
    kv.del(b"k1").unwrap();
    assert!(kv.get(b"k1").unwrap().is_none());
}

#[test]
fn kv_mset_mget() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.mset(
        &[b"a".as_slice(), b"b".as_slice()],
        &[b"1".as_slice(), b"2".as_slice()],
    )
    .unwrap();
    let out = kv
        .mget(&[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()])
        .unwrap();
    assert_eq!(out[0].as_deref(), Some(b"1" as &[u8]));
    assert_eq!(out[1].as_deref(), Some(b"2" as &[u8]));
    assert!(out[2].is_none());
}

#[test]
fn kv_ttl_expire() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"t", b"v", None).unwrap();
    assert!(kv.ttl(b"t").unwrap().is_none());
    kv.set(b"expired", b"x", Some(0)).unwrap(); // 0 秒即过期
    assert!(kv.get(b"expired").unwrap().is_none());
    kv.set(b"e", b"y", Some(3600)).unwrap();
    kv.expire(b"e", 0).unwrap(); // 设为已过期
    assert!(kv.get(b"e").unwrap().is_none());
}

#[test]
fn kv_exists_incr_keys_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert!(!kv.exists(b"x").unwrap());
    kv.set(b"x", b"0", None).unwrap();
    assert!(kv.exists(b"x").unwrap());
    assert_eq!(kv.incr(b"n").unwrap(), 1);
    assert_eq!(kv.incr(b"n").unwrap(), 2);
    kv.set(b"user:1", b"a", None).unwrap();
    kv.set(b"user:2", b"b", None).unwrap();
    let keys = kv.keys_prefix(b"user:").unwrap();
    assert_eq!(keys.len(), 2);
}

#[test]
fn kv_ttl_background_purge() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    // 写入一个已过期的 key（TTL=0 → 立即过期）
    kv.set(b"gone", b"val", Some(0)).unwrap();
    // 写入一个不过期的 key
    kv.set(b"stay", b"val", None).unwrap();
    // 手动调用 purge
    let purged = purge_expired_keys(&kv.keyspace).unwrap();
    assert_eq!(purged, 1);
    // gone 已被清理
    assert!(kv.keyspace.get(b"gone").unwrap().is_none());
    // stay 仍在
    assert!(kv.get(b"stay").unwrap().is_some());
}

#[test]
fn kv_keys_match_glob() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"user:1", b"a", None).unwrap();
    kv.set(b"user:2", b"b", None).unwrap();
    kv.set(b"user:10", b"c", None).unwrap();
    kv.set(b"admin:1", b"d", None).unwrap();
    // * 通配
    let keys = kv.keys_match(b"user:*").unwrap();
    assert_eq!(keys.len(), 3);
    // ? 单字符
    let keys = kv.keys_match(b"user:?").unwrap();
    assert_eq!(keys.len(), 2); // user:1, user:2
                               // 全通配
    let keys = kv.keys_match(b"*").unwrap();
    assert_eq!(keys.len(), 4);
    // 精确匹配
    let keys = kv.keys_match(b"admin:1").unwrap();
    assert_eq!(keys.len(), 1);
}

#[test]
fn glob_match_cases() {
    assert!(super::glob_match(b"*", b"anything"));
    assert!(super::glob_match(b"a*c", b"abc"));
    assert!(super::glob_match(b"a*c", b"aXYZc"));
    assert!(super::glob_match(b"a?c", b"abc"));
    assert!(!super::glob_match(b"a?c", b"aXYc"));
    assert!(super::glob_match(b"*:*", b"user:1"));
    assert!(!super::glob_match(b"x*", b"abc"));
}

// ── M95 快照读测试 ─────────────────────────────────

#[test]
fn kv_snapshot_get_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k1", b"v1", None).unwrap();
    let snap = store.snapshot();

    // 快照后修改
    kv.set(b"k1", b"v1_new", None).unwrap();
    kv.set(b"k2", b"v2", None).unwrap();

    // 快照读：看到旧值
    assert_eq!(
        kv.snapshot_get(&snap, b"k1").unwrap().as_deref(),
        Some(b"v1" as &[u8])
    );
    assert_eq!(kv.snapshot_get(&snap, b"k2").unwrap(), None);

    // 当前读：看到新值
    assert_eq!(kv.get(b"k1").unwrap().as_deref(), Some(b"v1_new" as &[u8]));
    assert_eq!(kv.get(b"k2").unwrap().as_deref(), Some(b"v2" as &[u8]));
}

#[test]
fn kv_snapshot_scan_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"user:1", b"alice", None).unwrap();
    kv.set(b"user:2", b"bob", None).unwrap();

    let snap = store.snapshot();
    kv.set(b"user:3", b"charlie", None).unwrap();

    // 快照扫描：只看到 2 个
    let snap_results = kv
        .snapshot_scan_prefix_limit(&snap, b"user:", 0, 100)
        .unwrap();
    assert_eq!(snap_results.len(), 2);

    // 当前扫描：看到 3 个
    let current = kv.scan_prefix_limit(b"user:", 0, 100).unwrap();
    assert_eq!(current.len(), 3);
}

#[test]
fn kv_snapshot_get_expired_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // 设置一个立即过期的 key（TTL=0 实际上设为 now+0=now，立即过期）
    kv.set(b"expiring", b"val", Some(0)).unwrap();
    let snap = store.snapshot();

    // 快照读也应检查 TTL（不执行惰性删除，但返回 None）
    // TTL=0 → expiry=now → now >= expiry → expired
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert_eq!(kv.snapshot_get(&snap, b"expiring").unwrap(), None);
}

#[test]
fn kv_incrby_decrby() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert_eq!(kv.incrby(b"c", 10).unwrap(), 10);
    assert_eq!(kv.incrby(b"c", 5).unwrap(), 15);
    assert_eq!(kv.decrby(b"c", 3).unwrap(), 12);
    assert_eq!(kv.decrby(b"c", 20).unwrap(), -8);
}

#[test]
fn kv_setnx() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert!(kv.setnx(b"k", b"v1", None).unwrap());
    assert!(!kv.setnx(b"k", b"v2", None).unwrap());
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"v1");
}

#[test]
fn kv_setnx_with_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert!(kv.setnx(b"t", b"val", Some(1)).unwrap());
    assert!(!kv.setnx(b"t", b"val2", None).unwrap());
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(kv.setnx(b"t", b"val3", None).unwrap());
    assert_eq!(kv.get(b"t").unwrap().unwrap(), b"val3");
}

#[test]
fn kv_del_nonexistent_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    // 删除不存在的 key 应该成功（幂等）
    kv.del(b"no_such_key").unwrap();
}

#[test]
fn kv_mdel_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"a", b"1", None).unwrap();
    kv.set(b"b", b"2", None).unwrap();
    kv.set(b"c", b"3", None).unwrap();
    // 批量删除 a, b
    kv.mdel(&[b"a", b"b"]).unwrap();
    assert!(kv.get(b"a").unwrap().is_none());
    assert!(kv.get(b"b").unwrap().is_none());
    assert!(kv.get(b"c").unwrap().is_some());
    // 空数组不报错
    kv.mdel(&[]).unwrap();
    // 删除不存在的 key 不报错
    kv.mdel(&[b"no_such"]).unwrap();
}

// ── M80: PERSIST 测试 ──

#[test]
fn kv_persist_removes_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // 设置带 TTL 的 key
    kv.set(b"cache_key", b"value", Some(300)).unwrap();
    assert!(kv.ttl(b"cache_key").unwrap().is_some());

    // persist 移除 TTL
    let ok = kv.persist(b"cache_key").unwrap();
    assert!(ok);

    // TTL 应为 None（永久）
    assert!(kv.ttl(b"cache_key").unwrap().is_none());

    // 值仍然可读
    assert_eq!(kv.get(b"cache_key").unwrap().unwrap(), b"value");
}

#[test]
fn kv_persist_no_ttl_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // 无 TTL 的 key
    kv.set(b"perm", b"data", None).unwrap();
    let ok = kv.persist(b"perm").unwrap();
    assert!(!ok);
}

#[test]
fn kv_persist_nonexistent_key_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    let ok = kv.persist(b"no_such").unwrap();
    assert!(!ok);
}

#[test]
fn kv_persist_expired_key_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // 设置 1 秒 TTL
    kv.set(b"short", b"val", Some(1)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // 已过期，persist 应返回 false
    let ok = kv.persist(b"short").unwrap();
    assert!(!ok);
    // key 也不可读
    assert!(kv.get(b"short").unwrap().is_none());
}

// ── M83: strlen 测试 ──

#[test]
fn kv_strlen_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k1", b"hello", None).unwrap();
    assert_eq!(kv.strlen(b"k1").unwrap(), Some(5));

    kv.set(b"k2", b"", None).unwrap();
    assert_eq!(kv.strlen(b"k2").unwrap(), Some(0));
}

#[test]
fn kv_strlen_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    assert_eq!(kv.strlen(b"no_such").unwrap(), None);
}

#[test]
fn kv_strlen_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // TTL=1 秒，写入后立即检查应有值
    kv.set(b"k1", b"data", Some(1)).unwrap();
    assert!(kv.strlen(b"k1").unwrap().is_some());

    // 等待过期
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(kv.strlen(b"k1").unwrap(), None);
}

#[test]
fn kv_strlen_with_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k1", b"12345678", Some(3600)).unwrap();
    assert_eq!(kv.strlen(b"k1").unwrap(), Some(8));
}

#[test]
fn kv_incrbyfloat_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    let v = kv.incrbyfloat(b"score", 1.5).unwrap();
    assert!((v - 1.5).abs() < f64::EPSILON);
}

#[test]
fn kv_incrbyfloat_accumulate() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.incrbyfloat(b"cost", 0.1).unwrap();
    kv.incrbyfloat(b"cost", 0.2).unwrap();
    let v = kv.incrbyfloat(b"cost", 0.3).unwrap();
    assert!((v - 0.6).abs() < 1e-10);
}

#[test]
fn kv_incrbyfloat_negative() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.incrbyfloat(b"bal", 10.0).unwrap();
    let v = kv.incrbyfloat(b"bal", -3.5).unwrap();
    assert!((v - 6.5).abs() < f64::EPSILON);
}

#[test]
fn kv_incrbyfloat_nan_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    assert!(kv.incrbyfloat(b"x", f64::NAN).is_err());
}

#[test]
fn kv_incrbyfloat_inf_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.incrbyfloat(b"x", f64::MAX).unwrap();
    assert!(kv.incrbyfloat(b"x", f64::MAX).is_err());
}

#[test]
fn kv_append_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    let len = kv.append(b"k", b"hello").unwrap();
    assert_eq!(len, 5);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"hello");
}

#[test]
fn kv_append_existing_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"hello", None).unwrap();
    let len = kv.append(b"k", b" world").unwrap();
    assert_eq!(len, 11);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"hello world");
}

#[test]
fn kv_append_preserves_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"hi", Some(3600)).unwrap();
    let ttl_before = kv.ttl(b"k").unwrap().unwrap();
    let len = kv.append(b"k", b"!").unwrap();
    assert_eq!(len, 3);
    let ttl_after = kv.ttl(b"k").unwrap().unwrap();
    // TTL 应保留（允许 1 秒误差）
    assert!(ttl_after >= ttl_before - 1);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"hi!");
}

#[test]
fn kv_append_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"abc", None).unwrap();
    let len = kv.append(b"k", b"").unwrap();
    assert_eq!(len, 3);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"abc");
}

#[test]
fn kv_append_expired_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    // 设置一个已过期的 key（TTL=0 秒 → 立即过期不行，手动写过期数据）
    kv.set(b"k", b"old", Some(1)).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    let len = kv.append(b"k", b"new").unwrap();
    assert_eq!(len, 3);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"new");
    // 新创建的 key 无 TTL
    assert!(kv.ttl(b"k").unwrap().is_none());
}

#[test]
fn kv_getrange_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"hello world", None).unwrap();
    assert_eq!(kv.getrange(b"k", 0, 4).unwrap(), b"hello");
    assert_eq!(kv.getrange(b"k", 6, 10).unwrap(), b"world");
}

#[test]
fn kv_getrange_negative_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"hello world", None).unwrap();
    // 最后 5 字节
    assert_eq!(kv.getrange(b"k", -5, -1).unwrap(), b"world");
    // 从头到倒数第 7 个
    assert_eq!(kv.getrange(b"k", 0, -7).unwrap(), b"hello");
}

#[test]
fn kv_getrange_out_of_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"abc", None).unwrap();
    // end 超出范围，自动截断
    assert_eq!(kv.getrange(b"k", 0, 100).unwrap(), b"abc");
    // start > end → 空
    assert_eq!(kv.getrange(b"k", 5, 2).unwrap(), b"");
}

#[test]
fn kv_getrange_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    assert_eq!(kv.getrange(b"nope", 0, 10).unwrap(), b"");
}

#[test]
fn kv_getrange_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"data", Some(1)).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(kv.getrange(b"k", 0, 3).unwrap(), b"");
}

#[test]
fn kv_rename_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"old", b"value", None).unwrap();
    kv.rename(b"old", b"new").unwrap();
    assert!(kv.get(b"old").unwrap().is_none());
    assert_eq!(kv.get(b"new").unwrap().unwrap(), b"value");
}

#[test]
fn kv_rename_preserves_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"src", b"data", Some(3600)).unwrap();
    let ttl_before = kv.ttl(b"src").unwrap().unwrap();
    kv.rename(b"src", b"dst").unwrap();
    let ttl_after = kv.ttl(b"dst").unwrap().unwrap();
    assert!(ttl_after >= ttl_before - 1);
}

#[test]
fn kv_rename_overwrites_dst() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"a", b"new_val", None).unwrap();
    kv.set(b"b", b"old_val", None).unwrap();
    kv.rename(b"a", b"b").unwrap();
    assert_eq!(kv.get(b"b").unwrap().unwrap(), b"new_val");
    assert!(kv.get(b"a").unwrap().is_none());
}

#[test]
fn kv_rename_nonexistent_src() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    assert!(kv.rename(b"nope", b"dst").is_err());
}

#[test]
fn kv_rename_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();

    kv.set(b"k", b"v", None).unwrap();
    kv.rename(b"k", b"k").unwrap();
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"v");
}

// ── M91: SETRANGE ──

#[test]
fn kv_setrange_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"Hello World", None).unwrap();
    let len = kv.setrange(b"k", 6, b"Redis").unwrap();
    assert_eq!(len, 11);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"Hello Redis");
}

#[test]
fn kv_setrange_extend() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"Hi", None).unwrap();
    // offset 超出当前长度，中间零填充
    let len = kv.setrange(b"k", 5, b"XY").unwrap();
    assert_eq!(len, 7);
    let val = kv.get(b"k").unwrap().unwrap();
    assert_eq!(&val[..2], b"Hi");
    assert_eq!(&val[2..5], &[0, 0, 0]);
    assert_eq!(&val[5..], b"XY");
}

#[test]
fn kv_setrange_new_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    // key 不存在，创建零填充值
    let len = kv.setrange(b"k", 3, b"AB").unwrap();
    assert_eq!(len, 5);
    let val = kv.get(b"k").unwrap().unwrap();
    assert_eq!(&val[..3], &[0, 0, 0]);
    assert_eq!(&val[3..], b"AB");
}

#[test]
fn kv_setrange_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"hello", None).unwrap();
    // 空 value 不修改，返回当前长度
    let len = kv.setrange(b"k", 0, b"").unwrap();
    assert_eq!(len, 5);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"hello");
    // key 不存在 + 空 value → 返回 0
    assert_eq!(kv.setrange(b"nokey", 10, b"").unwrap(), 0);
}

#[test]
fn kv_setrange_preserves_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"abcdef", Some(3600)).unwrap();
    kv.setrange(b"k", 0, b"XY").unwrap();
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"XYcdef");
    let ttl = kv.ttl(b"k").unwrap();
    assert!(ttl.is_some() && ttl.unwrap() > 3500);
}

// ── M92: GETSET ──

#[test]
fn kv_getset_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"old", None).unwrap();
    let old = kv.getset(b"k", b"new").unwrap();
    assert_eq!(old, Some(b"old".to_vec()));
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"new");
}

#[test]
fn kv_getset_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    let old = kv.getset(b"k", b"val").unwrap();
    assert_eq!(old, None);
    assert_eq!(kv.get(b"k").unwrap().unwrap(), b"val");
}

#[test]
fn kv_getset_clears_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", Some(3600)).unwrap();
    assert!(kv.ttl(b"k").unwrap().is_some());
    kv.getset(b"k", b"new").unwrap();
    // GETSET 清除 TTL
    let ttl = kv.ttl(b"k").unwrap();
    assert!(ttl.is_none() || ttl == Some(0));
}

// ── M93: EXPIREAT ──

#[test]
fn kv_expire_at_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", None).unwrap();
    // 设置未来时间戳
    let future = super::now_secs() + 3600;
    assert!(kv.expire_at(b"k", future).unwrap());
    // TTL 应该接近 3600
    let ttl = kv.ttl(b"k").unwrap().unwrap();
    assert!(ttl > 3500 && ttl <= 3600);
}

#[test]
fn kv_expire_at_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert!(!kv.expire_at(b"nokey", 9999999999).unwrap());
}

#[test]
fn kv_expire_at_zero_clears_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", Some(60)).unwrap();
    assert!(kv.ttl(b"k").unwrap().is_some());
    // timestamp=0 等同于 persist
    assert!(kv.expire_at(b"k", 0).unwrap());
    let ttl = kv.ttl(b"k").unwrap();
    assert!(ttl.is_none() || ttl == Some(0));
}

#[test]
fn kv_expire_at_past_makes_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", None).unwrap();
    // 设置过去时间戳 → key 立即过期
    assert!(kv.expire_at(b"k", 1).unwrap());
    assert!(kv.get(b"k").unwrap().is_none());
}

// ── M94: EXPIRETIME ──

#[test]
fn kv_expire_time_with_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", Some(3600)).unwrap();
    let ts = kv.expire_time(b"k").unwrap().unwrap();
    let now = super::now_secs();
    assert!(ts >= now + 3599 && ts <= now + 3601);
}

#[test]
fn kv_expire_time_no_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k", b"v", None).unwrap();
    assert_eq!(kv.expire_time(b"k").unwrap(), Some(0));
}

#[test]
fn kv_expire_time_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert_eq!(kv.expire_time(b"nokey").unwrap(), None);
}

// ── M96: DECR ──

#[test]
fn kv_decr_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    // 新 key → 0 - 1 = -1
    assert_eq!(kv.decr(b"cnt").unwrap(), -1);
    assert_eq!(kv.decr(b"cnt").unwrap(), -2);
    // 先 incr 再 decr
    kv.incr(b"x").unwrap();
    kv.incr(b"x").unwrap();
    assert_eq!(kv.decr(b"x").unwrap(), 1);
}

// ── M128: del_prefix ──

#[test]
fn kv_del_prefix_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"session:1:ctx", b"a", None).unwrap();
    kv.set(b"session:1:mem", b"b", None).unwrap();
    kv.set(b"session:2:ctx", b"c", None).unwrap();
    kv.set(b"other:key", b"d", None).unwrap();

    let deleted = kv.del_prefix(b"session:1:").unwrap();
    assert_eq!(deleted, 2);
    // session:1:* 已删除
    assert!(kv.get(b"session:1:ctx").unwrap().is_none());
    assert!(kv.get(b"session:1:mem").unwrap().is_none());
    // 其他 key 不受影响
    assert!(kv.get(b"session:2:ctx").unwrap().is_some());
    assert!(kv.get(b"other:key").unwrap().is_some());
}

#[test]
fn kv_del_prefix_empty_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"a", b"1", None).unwrap();
    kv.set(b"b", b"2", None).unwrap();
    // 空前缀删除所有
    let deleted = kv.del_prefix(b"").unwrap();
    assert_eq!(deleted, 2);
    assert!(kv.get(b"a").unwrap().is_none());
    assert!(kv.get(b"b").unwrap().is_none());
}

#[test]
fn kv_del_prefix_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"foo", b"1", None).unwrap();
    let deleted = kv.del_prefix(b"bar:").unwrap();
    assert_eq!(deleted, 0);
    // 原 key 不受影响
    assert!(kv.get(b"foo").unwrap().is_some());
}

#[test]
fn kv_del_prefix_with_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"cache:1", b"v1", Some(3600)).unwrap();
    kv.set(b"cache:2", b"v2", Some(3600)).unwrap();
    kv.set(b"cache:3", b"v3", None).unwrap();
    // 带 TTL 的 key 也应被删除
    let deleted = kv.del_prefix(b"cache:").unwrap();
    assert_eq!(deleted, 3);
    assert!(kv.get(b"cache:1").unwrap().is_none());
    assert!(kv.get(b"cache:2").unwrap().is_none());
    assert!(kv.get(b"cache:3").unwrap().is_none());
}

#[test]
fn kv_pexpire_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k1", b"v1", None).unwrap();
    // 设置 2500ms → 向上取整为 3 秒
    kv.pexpire(b"k1", 2500).unwrap();
    let ttl_ms = kv.pttl(b"k1").unwrap().unwrap();
    // 内部精度为秒，pttl 返回 remaining_secs * 1000
    assert!(ttl_ms >= 2000 && ttl_ms <= 3000);
}

#[test]
fn kv_pttl_no_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"k1", b"v1", None).unwrap();
    assert_eq!(kv.pttl(b"k1").unwrap(), None);
}

#[test]
fn kv_pttl_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert_eq!(kv.pttl(b"no_key").unwrap(), None);
}

#[test]
fn kv_key_type_string() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"hello", b"world", None).unwrap();
    assert_eq!(kv.key_type(b"hello").unwrap(), "string");
}

#[test]
fn kv_key_type_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    // 0xFF 0xFE 不是合法 UTF-8
    kv.set(b"bin", &[0xFF, 0xFE, 0x00, 0x80], None).unwrap();
    assert_eq!(kv.key_type(b"bin").unwrap(), "bytes");
}

#[test]
fn kv_key_type_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert_eq!(kv.key_type(b"missing").unwrap(), "none");
}

#[test]
fn kv_key_type_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"tmp", b"val", Some(1)).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(kv.key_type(b"tmp").unwrap(), "none");
}

#[test]
fn kv_random_key_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"a", b"1", None).unwrap();
    kv.set(b"b", b"2", None).unwrap();
    let key = kv.random_key().unwrap();
    assert!(key.is_some());
}

#[test]
fn kv_random_key_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    assert!(kv.random_key().unwrap().is_none());
}

#[test]
fn kv_random_key_skips_expired() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    kv.set(b"exp1", b"v1", Some(1)).unwrap();
    kv.set(b"exp2", b"v2", Some(1)).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    // 所有 key 都过期了
    assert!(kv.random_key().unwrap().is_none());
}
