/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! KV 引擎全方位基准（15 项，对标 Redis / RocksDB）
//! cargo test --test bench_kv_full --release -- --nocapture

use std::path::Path;
use std::time::Instant;
use talon::Talon;

fn rss_kb() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}
fn dir_size(p: &Path) -> u64 {
    let mut t = 0u64;
    if let Ok(es) = std::fs::read_dir(p) {
        for e in es.flatten() {
            let pp = e.path();
            if pp.is_dir() {
                t += dir_size(&pp);
            } else if let Ok(m) = pp.metadata() {
                t += m.len();
            }
        }
    }
    t
}
fn hb(b: u64) -> String {
    if b >= 1_048_576 {
        format!("{:.1}MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1}KB", b as f64 / 1024.0)
    } else {
        format!("{}B", b)
    }
}
fn pct(l: &mut [f64]) -> (f64, f64, f64, f64, f64) {
    let n = l.len();
    if n == 0 {
        return (0., 0., 0., 0., 0.);
    }
    l.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let a = l.iter().sum::<f64>() / n as f64;
    (
        a,
        l[(n as f64 * 0.5) as usize],
        l[((n as f64 * 0.95) as usize).min(n - 1)],
        l[((n as f64 * 0.99) as usize).min(n - 1)],
        l[n - 1],
    )
}
fn fms(us: f64) -> String {
    if us < 1000.0 {
        format!("{:.1}us", us)
    } else if us < 1e6 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{:.2}s", us / 1e6)
    }
}

#[test]
fn kv_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         KV 引擎全方位基准（15 项指标）                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let n: u64 = 1_000_000;
    let bs: u64 = 10_000;

    // === K1: 单次 SET ===
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let cnt = 100_000u64;
    let v100 = vec![0x42u8; 100];
    let t0 = Instant::now();
    for i in 0..cnt {
        kv.set(format!("k:{:08}", i).as_bytes(), &v100, None)
            .unwrap();
    }
    db.persist().unwrap();
    println!(
        "K1  | 单次 SET 100K (100B)                | {:>12.0} ops/s",
        cnt as f64 / t0.elapsed().as_secs_f64()
    );
    drop(kv);
    drop(db);

    // === K2: 批量 SET 1M + K13/K14 ===
    let dir2 = tempfile::tempdir().unwrap();
    let db = Talon::open(dir2.path()).unwrap();
    let kv = db.kv().unwrap();
    let rss0 = rss_kb();
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + bs).min(n);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(&mut batch, format!("k:{:08}", j).as_bytes(), &v100, None)
                .unwrap();
        }
        batch.commit().unwrap();
        i = end;
    }
    db.persist().unwrap();
    let k2_ops = n as f64 / t0.elapsed().as_secs_f64();
    let rss1 = rss_kb();
    let disk = dir_size(dir2.path());
    let raw = n * 108;
    println!(
        "K2  | 批量 SET 1M (batch=10K, 100B)       | {:>12.0} ops/s",
        k2_ops
    );
    println!(
        "K13 | 磁盘: {} (raw={}, 压缩比={:.1}x)",
        hb(disk),
        hb(raw),
        raw as f64 / disk as f64
    );
    println!(
        "K14 | RSS 增量: {}KB (写入 1M 后)",
        rss1 as i64 - rss0 as i64
    );

    // === K3: 随机 GET + 延迟分布 ===
    let s = 100_000u64;
    let mut lat = Vec::with_capacity(s as usize);
    let t0 = Instant::now();
    for i in 0..s {
        let t = Instant::now();
        let _ = kv
            .get(format!("k:{:08}", (i * 7 + 13) % n).as_bytes())
            .unwrap();
        lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
    }
    let (avg, p50, p95, p99, max) = pct(&mut lat);
    println!(
        "K3  | 随机 GET 100K (from 1M)             | {:>12.0} ops/s",
        s as f64 / t0.elapsed().as_secs_f64()
    );
    println!(
        "      Avg={} P50={} P95={} P99={} Max={}",
        fms(avg),
        fms(p50),
        fms(p95),
        fms(p99),
        fms(max)
    );

    // === K4: MGET ===
    let rounds = 10_000u64;
    let t0 = Instant::now();
    for i in 0..rounds {
        let ks: Vec<Vec<u8>> = (0..100)
            .map(|j| format!("k:{:08}", (i * 100 + j) % n).into_bytes())
            .collect();
        let rs: Vec<&[u8]> = ks.iter().map(|k| k.as_slice()).collect();
        let _ = kv.mget(&rs).unwrap();
    }
    println!(
        "K4  | MGET x100 (10K rounds)              | {:>12.0} ops/s",
        rounds as f64 / t0.elapsed().as_secs_f64()
    );

    // === K5: EXISTS ===
    let t0 = Instant::now();
    for i in 0..s {
        let _ = kv
            .exists(format!("k:{:08}", (i * 11 + 7) % n).as_bytes())
            .unwrap();
    }
    println!(
        "K5  | EXISTS 100K (from 1M)               | {:>12.0} ops/s",
        s as f64 / t0.elapsed().as_secs_f64()
    );

    // === K6: DEL ===
    let dn = 100_000u64;
    let t0 = Instant::now();
    for i in 0..dn {
        kv.del(format!("k:{:08}", i).as_bytes()).unwrap();
    }
    db.persist().unwrap();
    println!(
        "K6  | DEL 100K (含 persist)               | {:>12.0} ops/s",
        dn as f64 / t0.elapsed().as_secs_f64()
    );

    // === K7: Prefix Scan ===
    let sc = 10_000u64;
    let t0 = Instant::now();
    for _ in 0..sc {
        let _ = kv.scan_prefix_limit(b"k:0050", 0, 100).unwrap();
    }
    println!(
        "K7  | Prefix Scan LIMIT 100 (10K rounds)  | {:>12.0} ops/s",
        sc as f64 / t0.elapsed().as_secs_f64()
    );

    // === K8: TTL SET + GET ===
    drop(kv);
    drop(db);
    let dir3 = tempfile::tempdir().unwrap();
    let db = Talon::open(dir3.path()).unwrap();
    let kv = db.kv().unwrap();
    let tn = 100_000u64;
    let t0 = Instant::now();
    for i in 0..tn {
        kv.set(format!("t:{:08}", i).as_bytes(), &v100, Some(3600))
            .unwrap();
    }
    db.persist().unwrap();
    println!(
        "K8a | SET with TTL (100K)                 | {:>12.0} ops/s",
        tn as f64 / t0.elapsed().as_secs_f64()
    );
    let t0 = Instant::now();
    for i in 0..tn {
        let _ = kv.get(format!("t:{:08}", i).as_bytes()).unwrap();
    }
    println!(
        "K8b | GET with TTL (100K)                 | {:>12.0} ops/s",
        tn as f64 / t0.elapsed().as_secs_f64()
    );

    // === K9: INCR ===
    drop(kv);
    drop(db);
    let dir4 = tempfile::tempdir().unwrap();
    let db = Talon::open(dir4.path()).unwrap();
    let kv = db.kv().unwrap();
    let t0 = Instant::now();
    for i in 0..tn {
        kv.incr(format!("c:{:08}", i).as_bytes()).unwrap();
    }
    println!(
        "K9a | INCR 新 key (100K)                  | {:>12.0} ops/s",
        tn as f64 / t0.elapsed().as_secs_f64()
    );
    let t0 = Instant::now();
    for i in 0..tn {
        kv.incr(format!("c:{:08}", i).as_bytes()).unwrap();
    }
    println!(
        "K9b | INCR 已有 key (100K)                | {:>12.0} ops/s",
        tn as f64 / t0.elapsed().as_secs_f64()
    );

    // === K10: key_count ===
    let t0 = Instant::now();
    let cnt = kv.key_count().unwrap();
    println!(
        "K10 | key_count                           | count={} {:.1}ms",
        cnt,
        t0.elapsed().as_secs_f64() * 1000.0
    );
    drop(kv);
    drop(db);

    // === K11/K12: Value 大小梯度 ===
    println!("\n--- Value 大小梯度 (10K each, batch SET + random GET) ---");
    for &vs in &[100usize, 1024, 10240, 102400] {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let kv = db.kv().unwrap();
        let cnt = 10_000u64;
        let val = vec![0x42u8; vs];
        let t0 = Instant::now();
        let mut batch = db.batch();
        for i in 0..cnt {
            kv.set_batch(&mut batch, format!("v:{:08}", i).as_bytes(), &val, None)
                .unwrap();
        }
        batch.commit().unwrap();
        db.persist().unwrap();
        let wops = cnt as f64 / t0.elapsed().as_secs_f64();
        let t0 = Instant::now();
        for i in 0..cnt {
            let _ = kv.get(format!("v:{:08}", i).as_bytes()).unwrap();
        }
        let rops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(dir.path());
        println!(
            "    val={:>6} | W: {:>9.0} ops/s | R: {:>9.0} ops/s | Disk={}",
            hb(vs as u64),
            wops,
            rops,
            hb(disk)
        );
    }

    // === K15: 100K×1KB 磁盘 ===
    let dir5 = tempfile::tempdir().unwrap();
    let db = Talon::open(dir5.path()).unwrap();
    let kv = db.kv().unwrap();
    let cnt = 100_000u64;
    let v1k = vec![0x42u8; 1024];
    let mut i = 0u64;
    while i < cnt {
        let end = (i + 10_000).min(cnt);
        let mut batch = db.batch();
        for j in i..end {
            kv.set_batch(&mut batch, format!("l:{:08}", j).as_bytes(), &v1k, None)
                .unwrap();
        }
        batch.commit().unwrap();
        i = end;
    }
    db.persist().unwrap();
    let disk = dir_size(dir5.path());
    let raw = cnt * 1032;
    println!(
        "K15 | 100K x 1KB 磁盘: {} (raw={}, 压缩比={:.1}x)",
        hb(disk),
        hb(raw),
        raw as f64 / disk as f64
    );

    println!("\n✅ KV 引擎 15 项基准完成");
}
