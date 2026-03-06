/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M93 百万级基准测试：KV / 时序 / 向量 / 消息队列
//! 运行：cargo test --test bench_engines_1m --release -- --nocapture

use std::collections::BTreeMap;
use std::time::Instant;
use talon::Talon;

fn p95_us(latencies: &mut [f64]) -> f64 {
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((latencies.len() as f64) * 0.95) as usize;
    latencies[idx.min(latencies.len() - 1)]
}

// ══════════════════════════════════════════════════════
// 1. KV 引擎 — 100万条
// ══════════════════════════════════════════════════════

#[test]
fn kv_1m_set_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let kv = db.kv().unwrap();
    let n = 1_000_000u64;
    let batch_size = 10_000u64;
    println!("\n=== KV-1: Batch SET 1M keys (value=100B) ===");
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        let mut batch = db.batch();
        for j in i..end {
            let key = format!("key:{:08}", j);
            let val = vec![0x42u8; 100];
            kv.set_batch(&mut batch, key.as_bytes(), &val, None)
                .unwrap();
        }
        batch.commit().unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t0.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {} keys in {:.2?}, OPS: {:.0}", n, elapsed, ops);
    println!(
        "  Target: > 400K ops/s  =>  {}",
        if ops > 400_000.0 { "PASS" } else { "FAIL" }
    );
}

#[test]
fn kv_1m_get_random() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 1_000_000u64;
    // fill
    println!("\n=== KV-2: Random GET from 1M keys ===");
    println!("  Filling 1M keys...");
    {
        let kv = db.kv().unwrap();
        let batch_size = 10_000u64;
        let mut i = 0u64;
        while i < n {
            let end = (i + batch_size).min(n);
            let mut batch = db.batch();
            for j in i..end {
                let key = format!("key:{:08}", j);
                let val = vec![0x42u8; 100];
                kv.set_batch(&mut batch, key.as_bytes(), &val, None)
                    .unwrap();
            }
            batch.commit().unwrap();
            i = end;
        }
        db.persist().unwrap(); // 落盘校验
    }
    // random get
    let samples = 100_000u64;
    let kv = db.kv().unwrap();
    let t0 = Instant::now();
    for i in 0..samples {
        let idx = (i * 7 + 13) % n; // pseudo-random
        let key = format!("key:{:08}", idx);
        let _ = kv.get(key.as_bytes()).unwrap();
    }
    let elapsed = t0.elapsed();
    let ops = samples as f64 / elapsed.as_secs_f64();
    println!("  {} GETs in {:.2?}, OPS: {:.0}", samples, elapsed, ops);
    println!(
        "  Target: > 500K ops/s  =>  {}",
        if ops > 500_000.0 { "PASS" } else { "FAIL" }
    );
}

#[test]
fn kv_1m_scan_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let n = 1_000_000u64;
    println!("\n=== KV-3: Prefix Scan LIMIT 100 from 1M keys ===");
    println!("  Filling 1M keys...");
    {
        let kv = db.kv().unwrap();
        let batch_size = 10_000u64;
        let mut i = 0u64;
        while i < n {
            let end = (i + batch_size).min(n);
            let mut batch = db.batch();
            for j in i..end {
                let key = format!("key:{:08}", j);
                kv.set_batch(&mut batch, key.as_bytes(), &[0x42; 100], None)
                    .unwrap();
            }
            batch.commit().unwrap();
            i = end;
        }
        db.persist().unwrap(); // 落盘校验
    }
    let kv = db.kv().unwrap();
    let samples = 10_000;
    let t0 = Instant::now();
    for _ in 0..samples {
        let _ = kv.scan_prefix_limit(b"key:0050", 0, 100).unwrap();
    }
    let elapsed = t0.elapsed();
    let ops = samples as f64 / elapsed.as_secs_f64();
    println!("  {} scans in {:.2?}, OPS: {:.0}", samples, elapsed, ops);
    println!(
        "  Target: > 25K ops/s  =>  {}",
        if ops > 25_000.0 { "PASS" } else { "FAIL" }
    );
}

// ══════════════════════════════════════════════════════
// 2. 时序引擎 — 100万数据点
// ══════════════════════════════════════════════════════

fn make_ts_point(i: u64) -> talon::DataPoint {
    let mut tags = BTreeMap::new();
    tags.insert("host".to_string(), format!("host_{}", i % 100));
    tags.insert("region".to_string(), format!("region_{}", i % 10));
    let mut fields = BTreeMap::new();
    fields.insert("cpu".to_string(), format!("{:.2}", 10.0 + (i % 90) as f64));
    fields.insert("mem".to_string(), format!("{}", 1024 + (i % 8192)));
    fields.insert("disk".to_string(), format!("{}", 50000 + (i % 50000)));
    talon::DataPoint {
        timestamp: 1700000000000 + i as i64,
        tags,
        fields,
    }
}

#[test]
fn ts_1m_insert_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = talon::TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into(), "disk".into()],
    };
    let ts = db.create_timeseries("metrics", schema).unwrap();
    let n = 1_000_000u64;
    let batch_size = 10_000u64;
    println!("\n=== TS-1: Batch INSERT 1M data points (3 fields) ===");
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        let points: Vec<_> = (i..end).map(make_ts_point).collect();
        ts.insert_batch(&points).unwrap();
        i = end;
        if i % 200_000 == 0 {
            println!("    {}K points...", i / 1000);
        }
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t0.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {} points in {:.2?}, OPS: {:.0}", n, elapsed, ops);
    println!(
        "  Target: > 200K pts/s  =>  {}",
        if ops > 200_000.0 { "PASS" } else { "FAIL" }
    );
}

#[test]
fn ts_1m_query() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = talon::TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into(), "disk".into()],
    };
    let ts = db.create_timeseries("metrics", schema).unwrap();
    let n = 1_000_000u64;
    println!("\n=== TS-2: Query (tag+time range) from 1M points ===");
    println!("  Filling 1M points...");
    let batch_size = 10_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        let points: Vec<_> = (i..end).map(make_ts_point).collect();
        ts.insert_batch(&points).unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    println!("  Fill done.");
    // query: specific host+region (complete tag match) + time range + limit
    let samples = 100;
    let mut latencies = Vec::with_capacity(samples);
    for s in 0..samples {
        let q = talon::TsQuery {
            tag_filters: vec![
                ("host".into(), format!("host_{}", s % 100)),
                ("region".into(), format!("region_{}", s % 10)),
            ],
            time_start: Some(1700000000000 + (s as i64) * 1000),
            time_end: Some(1700000000000 + (s as i64) * 1000 + 50000),
            desc: false,
            limit: Some(100),
        };
        let t = Instant::now();
        let _ = ts.query(&q).unwrap();
        latencies.push(t.elapsed().as_micros() as f64);
    }
    let p95 = p95_us(&mut latencies);
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "  Samples: {}, Avg: {:.1}ms, P95: {:.1}ms",
        samples,
        avg / 1000.0,
        p95 / 1000.0
    );
    let pass = if p95 / 1000.0 < 50.0 { "PASS" } else { "FAIL" };
    println!("  Target: P95 < 50ms  =>  {}", pass);
}

#[test]
fn ts_1m_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let schema = talon::TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["cpu".into(), "mem".into(), "disk".into()],
    };
    let ts = db.create_timeseries("metrics", schema).unwrap();
    let n = 1_000_000u64;
    println!("\n=== TS-3: Aggregate SUM(cpu) from 1M points ===");
    println!("  Filling 1M points...");
    let batch_size = 10_000u64;
    let mut i = 0u64;
    while i < n {
        let end = (i + batch_size).min(n);
        let points: Vec<_> = (i..end).map(make_ts_point).collect();
        ts.insert_batch(&points).unwrap();
        i = end;
    }
    db.persist().unwrap(); // 落盘校验
    println!("  Fill done.");
    // aggregate: SUM(cpu) for a specific host (10K points)
    let samples = 20;
    let mut latencies = Vec::with_capacity(samples);
    for s in 0..samples {
        let q = talon::TsAggQuery {
            tag_filters: vec![("host".into(), format!("host_{}", s % 100))],
            time_start: None,
            time_end: None,
            field: "cpu".into(),
            func: talon::AggFunc::Sum,
            interval_ms: None,
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        };
        let t = Instant::now();
        let _ = ts.aggregate(&q).unwrap();
        latencies.push(t.elapsed().as_micros() as f64);
    }
    let p95 = p95_us(&mut latencies);
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "  Samples: {}, Avg: {:.1}ms, P95: {:.1}ms",
        samples,
        avg / 1000.0,
        p95 / 1000.0
    );
    let pass = if p95 / 1000.0 < 500.0 { "PASS" } else { "FAIL" };
    println!("  Target: P95 < 500ms  =>  {}", pass);
}

// ══════════════════════════════════════════════════════
// 3. 向量引擎 — 10万条 (dim=128, HNSW)
// ══════════════════════════════════════════════════════

fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed;
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.push((s >> 33) as f32 / (u32::MAX as f32) - 0.5);
    }
    v
}

#[test]
fn vec_100k_insert() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench128").unwrap();
    let n = 100_000u64;
    let dim = 128;
    println!(
        "\n=== VEC-1: Insert {}K vectors (dim={}) ===",
        n / 1000,
        dim
    );
    let t0 = Instant::now();
    for i in 0..n {
        ve.insert(i, &random_vec(dim, i)).unwrap();
        if i > 0 && i % 10_000 == 0 {
            let elapsed = t0.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("    {}K vectors, {:.0} vec/s", i / 1000, rate);
        }
    }
    db.persist().unwrap(); // 落盘校验
    let elapsed = t0.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {} vectors in {:.2?}, OPS: {:.0}", n, elapsed, ops);
    println!(
        "  Target: > 1K vec/s  =>  {}",
        if ops > 1000.0 { "PASS" } else { "FAIL" }
    );
}

#[test]
fn vec_100k_knn_search() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let ve = db.vector("bench128").unwrap();
    let n = 100_000u64;
    let dim = 128;
    println!(
        "\n=== VEC-2: KNN Search (k=10) from {}K vectors (dim={}) ===",
        n / 1000,
        dim
    );
    println!("  Filling {}K vectors...", n / 1000);
    for i in 0..n {
        ve.insert(i, &random_vec(dim, i)).unwrap();
        if i > 0 && i % 20_000 == 0 {
            println!("    {}K...", i / 1000);
        }
    }
    db.persist().unwrap(); // 落盘校验
    println!("  Fill done.");
    // KNN search
    let samples = 100;
    let mut latencies = Vec::with_capacity(samples);
    for s in 0..samples {
        let query = random_vec(dim, 999_999 + s as u64);
        let t = Instant::now();
        let _ = ve.search(&query, 10, "cosine").unwrap();
        latencies.push(t.elapsed().as_micros() as f64);
    }
    let p95 = p95_us(&mut latencies);
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    println!(
        "  Samples: {}, Avg: {:.1}ms, P95: {:.1}ms",
        samples,
        avg / 1000.0,
        p95 / 1000.0
    );
    let pass = if p95 / 1000.0 < 50.0 { "PASS" } else { "FAIL" };
    println!("  Target: P95 < 50ms  =>  {}", pass);
}

// ══════════════════════════════════════════════════════
// 4. 消息队列 — 100万消息
// ══════════════════════════════════════════════════════

#[test]
fn mq_1m_publish() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    mq.create_topic("bench", 0).unwrap();
    let n = 1_000_000u64;
    let batch_size = 1000usize;
    println!(
        "\n=== MQ-1: Publish 1M messages (payload=100B, batch={}) ===",
        batch_size
    );
    let payload = vec![0x42u8; 100];
    let payload_ref: &[u8] = &payload;
    let batch_payloads: Vec<&[u8]> = vec![payload_ref; batch_size];
    let t0 = Instant::now();
    let mut published = 0u64;
    while published < n {
        mq.publish_batch("bench", &batch_payloads).unwrap();
        published += batch_size as u64;
        if published % 200_000 == 0 {
            println!("    {}K msgs...", published / 1000);
        }
    }
    drop(mq);
    db.persist().unwrap(); // 落盘校验
    let elapsed = t0.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("  {} msgs in {:.2?}, OPS: {:.0}", n, elapsed, ops);
    println!(
        "  Target: > 50K msg/s  =>  {}",
        if ops > 50_000.0 { "PASS" } else { "FAIL" }
    );
}

#[test]
fn mq_1m_poll() {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let mq = db.mq().unwrap();
    mq.create_topic("bench", 0).unwrap();
    let n = 100_000u64; // 10万消息，poll 100条/次 = 1000次 poll
    println!(
        "\n=== MQ-2: Poll (100 msgs/batch) from {}K messages ===",
        n / 1000
    );
    println!("  Publishing {}K messages...", n / 1000);
    let payload = vec![0x42u8; 100];
    for _ in 0..n {
        mq.publish("bench", &payload).unwrap();
    }
    db.persist().unwrap(); // 落盘校验
    mq.subscribe("bench", "grp1").unwrap();
    let batch = 100;
    let samples = 500;
    let mut latencies = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t = Instant::now();
        let msgs = mq.poll("bench", "grp1", "c1", batch).unwrap();
        latencies.push(t.elapsed().as_micros() as f64);
        // ack
        for m in &msgs {
            mq.ack("bench", "grp1", "c1", m.id).unwrap();
        }
        if msgs.is_empty() {
            break;
        }
    }
    let p95 = p95_us(&mut latencies);
    let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;
    let total_polled = latencies.len() * batch;
    println!(
        "  {} polls ({} msgs), Avg: {:.1}ms, P95: {:.1}ms",
        latencies.len(),
        total_polled,
        avg / 1000.0,
        p95 / 1000.0
    );
    let pass = if p95 / 1000.0 < 50.0 { "PASS" } else { "FAIL" };
    println!("  Target: P95 < 50ms  =>  {}", pass);
}

// ══════════════════════════════════════════════════════
// 5. 精准落盘校验：close→reopen→逐条验证
// ══════════════════════════════════════════════════════

#[test]
fn durability_kv_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let n = 100_000u64;
    println!(
        "\n=== DUR-KV: Durability Verify ({}K keys, close→reopen→verify) ===",
        n / 1000
    );
    {
        let db = Talon::open(&path).unwrap();
        let kv = db.kv().unwrap();
        let batch_size = 10_000u64;
        let mut i = 0u64;
        while i < n {
            let end = (i + batch_size).min(n);
            let mut batch = db.batch();
            for j in i..end {
                let key = format!("key:{:08}", j);
                let val = format!("val:{:08}", j);
                kv.set_batch(&mut batch, key.as_bytes(), val.as_bytes(), None)
                    .unwrap();
            }
            batch.commit().unwrap();
            i = end;
        }
        db.persist().unwrap();
        drop(kv);
        drop(db);
    }
    {
        let db = Talon::open(&path).unwrap();
        let kv = db.kv().unwrap();
        let sample = 1000u64;
        let step = n / sample;
        for s in 0..sample {
            let idx = s * step;
            let key = format!("key:{:08}", idx);
            let expected = format!("val:{:08}", idx);
            let got = kv.get(key.as_bytes()).unwrap();
            assert_eq!(got, Some(expected.into_bytes()), "KV key {} 值不匹配", idx);
        }
        println!("  ✅ {} key 抽样验证通过", sample);
    }
}

#[test]
fn durability_ts_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let n = 50_000u64;
    println!(
        "\n=== DUR-TS: Durability Verify ({}K points, close→reopen→verify) ===",
        n / 1000
    );
    {
        let db = Talon::open(&path).unwrap();
        let schema = talon::TsSchema {
            tags: vec!["host".into()],
            fields: vec!["cpu".into()],
        };
        let ts = db.create_timeseries("dur_metrics", schema).unwrap();
        let batch_size = 5_000u64;
        let mut i = 0u64;
        while i < n {
            let end = (i + batch_size).min(n);
            let points: Vec<_> = (i..end)
                .map(|j| {
                    let mut tags = BTreeMap::new();
                    tags.insert("host".to_string(), format!("h_{}", j % 10));
                    let mut fields = BTreeMap::new();
                    fields.insert("cpu".to_string(), format!("{}", j));
                    talon::DataPoint {
                        timestamp: 1700000000000 + j as i64,
                        tags,
                        fields,
                    }
                })
                .collect();
            ts.insert_batch(&points).unwrap();
            i = end;
        }
        db.persist().unwrap();
        drop(ts);
        drop(db);
    }
    {
        let db = Talon::open(&path).unwrap();
        let ts = db.open_timeseries("dur_metrics").unwrap();
        let q = talon::TsQuery {
            tag_filters: vec![("host".into(), "h_0".into())],
            time_start: None,
            time_end: None,
            desc: false,
            limit: Some(10),
        };
        let results = ts.query(&q).unwrap();
        assert!(!results.is_empty(), "TS 数据丢失");
        println!("  ✅ TS 查询返回 {} 条，数据存活", results.len());
    }
}

#[test]
fn durability_vec_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let n = 10_000u64;
    let dim = 128;
    println!(
        "\n=== DUR-VEC: Durability Verify ({}K vectors, close→reopen→verify) ===",
        n / 1000
    );
    {
        let db = Talon::open(&path).unwrap();
        let ve = db.vector("dur_vec").unwrap();
        for i in 0..n {
            ve.insert(i, &random_vec(dim, i)).unwrap();
        }
        db.persist().unwrap();
        drop(ve);
        drop(db);
    }
    {
        let db = Talon::open(&path).unwrap();
        let ve = db.vector("dur_vec").unwrap();
        let query = random_vec(dim, 42);
        let results = ve.search(&query, 10, "cosine").unwrap();
        assert_eq!(results.len(), 10, "向量搜索结果数不对");
        assert_eq!(ve.count().unwrap(), n, "向量总数不匹配: 期望 {}", n);
        println!("  ✅ {} 向量存活，KNN 搜索返回 {} 条", n, results.len());
    }
}
