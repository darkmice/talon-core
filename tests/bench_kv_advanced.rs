/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! KV 引擎高级功能基准测试：TTL、INCR、MSET/MGET、EXPIRE、KEYS 模式匹配。
//! 运行：cargo test --test bench_kv_advanced --release -- --nocapture

use std::time::Instant;
use talon::*;

fn bench<F: FnOnce() -> u64>(label: &str, f: F) -> f64 {
    let start = Instant::now();
    let ops = f();
    let elapsed = start.elapsed();
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    println!(
        "  {}: {} ops in {:.2?} ({:.0} ops/s)",
        label, ops, elapsed, ops_per_sec
    );
    ops_per_sec
}

#[test]
fn bench_kv_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 5_000u64;
    println!("\n=== KV TTL Benchmark ===");

    // SET with TTL
    bench("SET with TTL ×5K", || {
        for i in 0..n {
            kv.set(format!("ttl:{}", i).as_bytes(), b"value", Some(3600))
                .unwrap();
        }
        n
    });

    // TTL query
    bench("TTL query ×5K", || {
        for i in 0..n {
            let t = kv.ttl(format!("ttl:{}", i).as_bytes()).unwrap();
            assert!(t.is_some());
        }
        n
    });

    // EXPIRE (update TTL)
    bench("EXPIRE ×5K", || {
        for i in 0..n {
            kv.expire(format!("ttl:{}", i).as_bytes(), 7200).unwrap();
        }
        n
    });

    // GET with TTL (should still be alive)
    bench("GET with TTL ×5K", || {
        for i in 0..n {
            let v = kv.get(format!("ttl:{}", i).as_bytes()).unwrap();
            assert!(v.is_some());
        }
        n
    });

    // SET with very short TTL (1s) — verify expiry semantics
    kv.set(b"expire_soon", b"data", Some(1)).unwrap();
    let ttl_val = kv.ttl(b"expire_soon").unwrap();
    assert!(ttl_val.is_some(), "TTL should be set");
}

#[test]
fn bench_kv_incr() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 5_000u64;
    println!("\n=== KV INCR Benchmark ===");

    // INCR on new keys
    bench("INCR new keys ×5K", || {
        for i in 0..n {
            let v = kv.incr(format!("cnt:{}", i).as_bytes()).unwrap();
            assert_eq!(v, 1);
        }
        n
    });

    // INCR on existing keys (accumulate)
    bench("INCR existing ×5K (10 rounds)", || {
        for _ in 0..10u64 {
            for i in 0..500u64 {
                kv.incr(format!("cnt:{}", i).as_bytes()).unwrap();
            }
        }
        5000
    });

    // verify counter value
    let v = kv.incr(b"cnt:0").unwrap();
    assert_eq!(v, 12, "cnt:0 should be 1 (new) + 10 (rounds) + 1 = 12");
}

#[test]
fn bench_kv_mset_mget() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 1_000u64;
    println!("\n=== KV MSET/MGET Benchmark ===");

    // MSET
    let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("mk:{}", i).into_bytes()).collect();
    let vals: Vec<Vec<u8>> = (0..n).map(|i| format!("val_{}", i).into_bytes()).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let val_refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();

    bench("MSET ×1K", || {
        kv.mset(&key_refs, &val_refs).unwrap();
        n
    });

    // MGET
    bench("MGET ×1K", || {
        let results = kv.mget(&key_refs).unwrap();
        assert_eq!(results.len(), n as usize);
        for r in &results {
            assert!(r.is_some());
        }
        n
    });

    // mset_batch (high-performance batch)
    let batch_keys: Vec<Vec<u8>> = (0..n).map(|i| format!("bm:{}", i).into_bytes()).collect();
    let batch_key_refs: Vec<&[u8]> = batch_keys.iter().map(|k| k.as_slice()).collect();
    bench("MSET_BATCH ×1K", || {
        let mut batch = db.batch();
        kv.mset_batch(&mut batch, &batch_key_refs, &val_refs)
            .unwrap();
        batch.commit().unwrap();
        n
    });
}

#[test]
fn bench_kv_keys_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    println!("\n=== KV KEYS Pattern Benchmark ===");

    // Insert structured keys
    for i in 0..2000u64 {
        kv.set(format!("user:{}:profile", i).as_bytes(), b"data", None)
            .unwrap();
        kv.set(format!("user:{}:settings", i).as_bytes(), b"data", None)
            .unwrap();
    }

    // keys_prefix
    bench("keys_prefix('user:0:') ×1K", || {
        for _ in 0..1000u64 {
            let keys = kv.keys_prefix(b"user:0:").unwrap();
            assert_eq!(keys.len(), 2);
        }
        1000
    });

    // keys_prefix_limit (pagination)
    bench("keys_prefix_limit('user:', 0, 100) ×100", || {
        for _ in 0..100u64 {
            let keys = kv.keys_prefix_limit(b"user:", 0, 100).unwrap();
            assert_eq!(keys.len(), 100);
        }
        100
    });

    // keys_match (glob pattern)
    bench("keys_match('user:1*:profile') ×50", || {
        for _ in 0..50u64 {
            let keys = kv.keys_match(b"user:1*:profile").unwrap();
            assert!(!keys.is_empty());
        }
        50
    });

    // EXISTS check
    bench("EXISTS ×2K", || {
        for i in 0..2000u64 {
            let e = kv.exists(format!("user:{}:profile", i).as_bytes()).unwrap();
            assert!(e);
        }
        2000
    });
}
