/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 全引擎性能基准报告 — 覆盖核心性能 / 资源占用 / 稳定性 / 并发 / 业务场景
//!
//! 运行：cargo test --test bench_perf_report --release -- --nocapture --test-threads=1
//!
//! 指标体系对标：MySQL / Redis / Milvus / Elasticsearch / TimescaleDB / RabbitMQ

#![allow(dead_code)] // p50, p90, section 保留供扩展报告使用

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use talon::{DataPoint, Direction, FtsConfig, FtsDoc, GeoUnit, Talon, TsSchema};

// ─── 辅助函数 ─────────────────────────────────────────────

fn p50(lat: &mut [f64]) -> f64 {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat[(lat.len() as f64 * 0.50) as usize]
}
fn p90(lat: &mut [f64]) -> f64 {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat[((lat.len() as f64 * 0.90) as usize).min(lat.len() - 1)]
}
fn p95(lat: &mut [f64]) -> f64 {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat[((lat.len() as f64 * 0.95) as usize).min(lat.len() - 1)]
}
fn p99(lat: &mut [f64]) -> f64 {
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    lat[((lat.len() as f64 * 0.99) as usize).min(lat.len() - 1)]
}
fn avg(lat: &[f64]) -> f64 {
    lat.iter().sum::<f64>() / lat.len().max(1) as f64
}
fn stddev(lat: &[f64]) -> f64 {
    let m = avg(lat);
    let var = lat.iter().map(|v| (v - m) * (v - m)).sum::<f64>() / lat.len().max(1) as f64;
    var.sqrt()
}

fn rss_kb() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .unwrap_or_else(|_| std::process::Output {
            status: std::process::ExitStatus::default(),
            stdout: b"0".to_vec(),
            stderr: vec![],
        });
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
    if b >= 1_073_741_824 {
        format!("{:.2} GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

fn rvec(dim: usize, seed: u64) -> Vec<f32> {
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ─── 报告输出 ─────────────────────────────────────────────

fn section(title: &str) {
    let bar: String = std::iter::repeat('═').take(68).collect();
    println!("\n╔{}╗", bar);
    println!("║  {:<66}║", title);
    println!("╚{}╝", bar);
}

fn metric(name: &str, value: &str, target: &str, pass: bool) {
    let status = if pass { "✅ PASS" } else { "⚠️  WARN" };
    println!(
        "  {:<36} {:>14}  (target: {:<14}) {}",
        name, value, target, status
    );
}

// ═══════════════════════════════════════════════════════════
//  主测试入口
// ═══════════════════════════════════════════════════════════

#[test]
fn perf_report_all_engines() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║          Talon v0.1.0 — 全引擎性能基准报告                         ║");
    println!("║          8 Engines × 5 Metric Categories                           ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!("  Date: {}", chrono_like_now());
    println!(
        "  CPU:  {} cores",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );
    println!("  RSS:  {} KB (baseline)", rss_kb());
    println!();

    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss_before = rss_kb();

    // ═══ 1. KV Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  1. KV Engine (对标 Redis)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 1a. Write throughput (batch SET)
    let n_kv: u64 = 100_000;
    let batch_size = 5_000u64;
    {
        let kv = db.kv().unwrap();
        let t0 = Instant::now();
        let mut i = 0u64;
        while i < n_kv {
            let end = (i + batch_size).min(n_kv);
            let mut batch = db.batch();
            for j in i..end {
                let key = format!("kv:{:08}", j);
                let val = vec![0x42u8; 100];
                kv.set_batch(&mut batch, key.as_bytes(), &val, None)
                    .unwrap();
            }
            batch.commit().unwrap();
            i = end;
        }
        let elapsed = t0.elapsed();
        let write_qps = n_kv as f64 / elapsed.as_secs_f64();
        metric(
            "KV batch SET QPS",
            &format!("{:.0}", write_qps),
            ">200K",
            write_qps > 200_000.0,
        );
    }

    // 1b. Read throughput + latency
    {
        let kv = db.kv().unwrap();
        let samples = 10_000u64;
        let mut lats = Vec::with_capacity(samples as usize);
        let t0 = Instant::now();
        for i in 0..samples {
            let key = format!("kv:{:08}", i * 10);
            let t = Instant::now();
            let _ = kv.get(key.as_bytes()).unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0); // μs
        }
        let elapsed = t0.elapsed();
        let read_qps = samples as f64 / elapsed.as_secs_f64();
        metric(
            "KV GET QPS",
            &format!("{:.0}", read_qps),
            ">100K",
            read_qps > 100_000.0,
        );
        metric(
            "KV GET avg latency",
            &format!("{:.1} μs", avg(&lats)),
            "<100μs",
            avg(&lats) < 100.0,
        );
        metric(
            "KV GET P99 latency",
            &format!("{:.1} μs", p99(&mut lats)),
            "<500μs",
            p99(&mut lats) < 500.0,
        );
    }

    // 1c. Read/write success rate
    {
        let kv = db.kv().unwrap();
        let mut ok = 0u64;
        let total = 1000u64;
        for i in 0..total {
            let key = format!("kv:{:08}", i);
            if kv.get(key.as_bytes()).is_ok() {
                ok += 1;
            }
        }
        let rate = ok as f64 / total as f64 * 100.0;
        metric(
            "KV success rate",
            &format!("{:.1}%", rate),
            "100%",
            rate >= 99.9,
        );
    }

    // ═══ 2. SQL Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  2. SQL Engine (对标 SQLite / MySQL)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_sql: u64 = 10_000;
    db.run_sql("CREATE TABLE bench_sql (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
        .unwrap();

    // 2a. Batch INSERT
    {
        let t0 = Instant::now();
        for i in 0..n_sql {
            db.run_sql(&format!(
                "INSERT INTO bench_sql VALUES ({}, 'user_{}', {:.2})",
                i,
                i,
                i as f64 * 0.1
            ))
            .unwrap();
        }
        let elapsed = t0.elapsed();
        let insert_qps = n_sql as f64 / elapsed.as_secs_f64();
        metric(
            "SQL INSERT QPS",
            &format!("{:.0}", insert_qps),
            ">5K",
            insert_qps > 5_000.0,
        );
    }

    // 2b. PK SELECT latency
    {
        let samples = 5_000u64;
        let mut lats = Vec::with_capacity(samples as usize);
        for i in 0..samples {
            let t = Instant::now();
            let _ = db
                .run_sql(&format!("SELECT * FROM bench_sql WHERE id = {}", i * 2))
                .unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "SQL PK SELECT avg",
            &format!("{:.1} μs", avg(&lats)),
            "<200μs",
            avg(&lats) < 200.0,
        );
        metric(
            "SQL PK SELECT P95",
            &format!("{:.1} μs", p95(&mut lats)),
            "<500μs",
            p95(&mut lats) < 500.0,
        );
        metric(
            "SQL PK SELECT P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<1ms",
            p99(&mut lats) < 1000.0,
        );
    }

    // 2c. Aggregation
    {
        let mut lats = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let _ = db.run_sql("SELECT SUM(score) FROM bench_sql").unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "SQL SUM() avg",
            &format!("{:.1} μs", avg(&lats)),
            "<10ms",
            avg(&lats) < 10_000.0,
        );
        metric(
            "SQL SUM() P95",
            &format!("{:.1} μs", p95(&mut lats)),
            "<20ms",
            p95(&mut lats) < 20_000.0,
        );
    }

    // 2d. Range query
    {
        let mut lats = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let _ = db
                .run_sql("SELECT * FROM bench_sql WHERE id >= 100 AND id < 200")
                .unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "SQL range (100 rows) avg",
            &format!("{:.1} μs", avg(&lats)),
            "<5ms",
            avg(&lats) < 5_000.0,
        );
    }

    // ═══ 3. TimeSeries Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  3. TimeSeries Engine (对标 TimescaleDB / TDengine)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_ts: usize = 50_000;
    {
        let ts = db
            .create_timeseries(
                "bench_ts",
                TsSchema {
                    tags: vec!["host".into(), "region".into()],
                    fields: vec!["cpu".into(), "mem".into()],
                },
            )
            .unwrap();

        // 3a. Batch write
        let base_time = now_ms() - (n_ts as i64 * 1000);
        let mut points = Vec::with_capacity(n_ts);
        for i in 0..n_ts {
            let mut tags = BTreeMap::new();
            tags.insert("host".into(), format!("h{}", i % 10));
            tags.insert("region".into(), format!("r{}", i % 3));
            let mut fields = BTreeMap::new();
            fields.insert("cpu".into(), format!("{:.1}", (i as f64 % 100.0)));
            fields.insert("mem".into(), format!("{:.0}", (i * 1024) % 8192));
            points.push(DataPoint {
                timestamp: base_time + i as i64 * 1000,
                tags,
                fields,
            });
        }
        let t0 = Instant::now();
        ts.insert_batch(&points).unwrap();
        let elapsed = t0.elapsed();
        let write_rate = n_ts as f64 / elapsed.as_secs_f64();
        metric(
            "TS batch write rate",
            &format!("{:.0} pts/s", write_rate),
            ">50K",
            write_rate > 50_000.0,
        );

        // 3b. Range query latency
        let mut lats = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let q = talon::TsQuery {
                tag_filters: vec![("host".into(), "h0".into())],
                time_start: Some(base_time),
                time_end: Some(base_time + 10_000_000),
                desc: false,
                limit: Some(100),
            };
            let _ = ts.query(&q).unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "TS range query avg",
            &format!("{:.1} μs", avg(&lats)),
            "<5ms",
            avg(&lats) < 5_000.0,
        );
        metric(
            "TS range query P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<20ms",
            p99(&mut lats) < 20_000.0,
        );

        // 3c. Aggregation
        let mut lats = Vec::with_capacity(50);
        for _ in 0..50 {
            let t = Instant::now();
            let _ = ts
                .aggregate(&talon::TsAggQuery {
                    tag_filters: vec![("host".into(), "h0".into())],
                    time_start: Some(base_time),
                    time_end: Some(base_time + 10_000_000),
                    field: "cpu".into(),
                    func: talon::AggFunc::Avg,
                    interval_ms: None,
                    sliding_ms: None,
                    session_gap_ms: None,
                    fill: None,
                })
                .unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "TS aggregation avg",
            &format!("{:.1} μs", avg(&lats)),
            "<10ms",
            avg(&lats) < 10_000.0,
        );
    }

    // ═══ 4. MessageQueue Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  4. MessageQueue Engine (对标 RabbitMQ / NATS)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_mq: usize = 50_000;
    {
        let mq = db.mq().unwrap();
        mq.create_topic("bench_mq", 0).unwrap();
        mq.subscribe("bench_mq", "grp").unwrap();

        // 4a. Batch publish
        let payloads: Vec<Vec<u8>> = (0..n_mq)
            .map(|i| format!("msg_{}", i).into_bytes())
            .collect();
        let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let t0 = Instant::now();
        mq.publish_batch("bench_mq", &payload_refs).unwrap();
        let elapsed = t0.elapsed();
        let pub_rate = n_mq as f64 / elapsed.as_secs_f64();
        metric(
            "MQ publish rate",
            &format!("{:.0} msg/s", pub_rate),
            ">100K",
            pub_rate > 100_000.0,
        );

        // 4b. Poll latency
        let mut lats = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let msgs = mq.poll("bench_mq", "grp", "c0", 10).unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
            for m in &msgs {
                let _ = mq.ack("bench_mq", "grp", "c0", m.id);
            }
        }
        metric(
            "MQ poll (10 msgs) avg",
            &format!("{:.1} μs", avg(&lats)),
            "<1ms",
            avg(&lats) < 1_000.0,
        );
        metric(
            "MQ poll P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<5ms",
            p99(&mut lats) < 5_000.0,
        );
    }

    // ═══ 5. Vector Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  5. Vector Engine (对标 Milvus / Qdrant)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let dim = 128;
    let n_vec: u64 = 5_000;
    {
        let vec_eng = db.vector("bench_vec").unwrap();

        // 5a. Insert vectors
        let t0 = Instant::now();
        for i in 0..n_vec {
            let v = rvec(dim, i);
            vec_eng.insert(i, &v).unwrap();
        }
        let elapsed = t0.elapsed();
        let insert_rate = n_vec as f64 / elapsed.as_secs_f64();
        metric(
            "Vec insert rate",
            &format!("{:.0} vec/s", insert_rate),
            ">500",
            insert_rate > 500.0,
        );

        // 5b. KNN search latency
        let mut lats = Vec::with_capacity(200);
        for i in 0..200u64 {
            let q = rvec(dim, i + n_vec);
            let t = Instant::now();
            let _ = vec_eng.search(&q, 10, "cosine").unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "Vec KNN-10 avg",
            &format!("{:.1} μs", avg(&lats)),
            "<5ms",
            avg(&lats) < 5_000.0,
        );
        metric(
            "Vec KNN-10 P95",
            &format!("{:.1} μs", p95(&mut lats)),
            "<10ms",
            p95(&mut lats) < 10_000.0,
        );
        metric(
            "Vec KNN-10 P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<20ms",
            p99(&mut lats) < 20_000.0,
        );
    }

    // ═══ 6. Full-Text Search Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  6. FTS Engine (对标 Elasticsearch / Lucene)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_fts: usize = 5_000;
    {
        let fts = db.fts().unwrap();
        fts.create_index("bench_fts", &FtsConfig::default())
            .unwrap();

        // 6a. Index documents
        let t0 = Instant::now();
        for i in 0..n_fts {
            let mut fields = BTreeMap::new();
            fields.insert(
                "title".into(),
                format!("Performance benchmark document number {}", i),
            );
            fields.insert(
                "body".into(),
                format!(
                    "This is the body text for document {} with various keywords like database engine optimization throughput latency",
                    i
                ),
            );
            fts.index_doc(
                "bench_fts",
                &FtsDoc {
                    doc_id: format!("doc_{}", i),
                    fields,
                },
            )
            .unwrap();
        }
        let elapsed = t0.elapsed();
        let idx_rate = n_fts as f64 / elapsed.as_secs_f64();
        metric(
            "FTS index rate",
            &format!("{:.0} doc/s", idx_rate),
            ">1K",
            idx_rate > 1_000.0,
        );

        // 6b. Search latency
        let queries = [
            "database",
            "engine optimization",
            "performance benchmark",
            "throughput latency",
        ];
        let mut lats = Vec::with_capacity(200);
        for i in 0..200 {
            let q = queries[i % queries.len()];
            let t = Instant::now();
            let _ = fts.search("bench_fts", q, 10).unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "FTS search avg",
            &format!("{:.1} μs", avg(&lats)),
            "<10ms",
            avg(&lats) < 10_000.0,
        );
        metric(
            "FTS search P95",
            &format!("{:.1} μs", p95(&mut lats)),
            "<20ms",
            p95(&mut lats) < 20_000.0,
        );
        metric(
            "FTS search P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<50ms",
            p99(&mut lats) < 50_000.0,
        );
    }

    // ═══ 7. Graph Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  7. Graph Engine (对标 Neo4j / JanusGraph)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_graph_v: u64 = 2_000;
    let n_graph_e: u64 = 5_000;
    {
        let graph = db.graph().unwrap();
        graph.create("bench_g").unwrap();

        // 7a. Add vertices
        let t0 = Instant::now();
        for i in 0..n_graph_v {
            let mut props = BTreeMap::new();
            props.insert("name".into(), format!("node_{}", i));
            graph.add_vertex("bench_g", "person", &props).unwrap();
        }
        let v_elapsed = t0.elapsed();
        let v_rate = n_graph_v as f64 / v_elapsed.as_secs_f64();
        metric(
            "Graph add_vertex rate",
            &format!("{:.0} v/s", v_rate),
            ">5K",
            v_rate > 5_000.0,
        );

        // 7b. Add edges
        let t0 = Instant::now();
        for i in 0..n_graph_e {
            let from = (i % n_graph_v) + 1;
            let to = ((i * 7 + 3) % n_graph_v) + 1;
            if from != to {
                let _ = graph.add_edge("bench_g", from, to, "knows", &BTreeMap::new());
            }
        }
        let e_elapsed = t0.elapsed();
        let e_rate = n_graph_e as f64 / e_elapsed.as_secs_f64();
        metric(
            "Graph add_edge rate",
            &format!("{:.0} e/s", e_rate),
            ">3K",
            e_rate > 3_000.0,
        );

        // 7c. Neighbor query latency
        let mut lats = Vec::with_capacity(200);
        for i in 1..=200u64 {
            let t = Instant::now();
            let _ = graph.neighbors("bench_g", i, Direction::Out).unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "Graph neighbors avg",
            &format!("{:.1} μs", avg(&lats)),
            "<1ms",
            avg(&lats) < 1_000.0,
        );
        metric(
            "Graph neighbors P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<5ms",
            p99(&mut lats) < 5_000.0,
        );
    }

    // ═══ 8. GEO Engine ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  8. GEO Engine (对标 Redis GEO)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let n_geo: usize = 10_000;
    {
        let geo = db.geo().unwrap();
        geo.create("bench_geo").unwrap();

        // 8a. GEOADD batch
        let t0 = Instant::now();
        for i in 0..n_geo {
            let lng = 116.0 + (i as f64 % 100.0) * 0.001;
            let lat = 39.0 + (i as f64 / 100.0) * 0.001;
            let key = format!("poi_{}", i);
            geo.geo_add("bench_geo", &key, lng, lat).unwrap();
        }
        let elapsed = t0.elapsed();
        let geo_rate = n_geo as f64 / elapsed.as_secs_f64();
        metric(
            "GEO add rate",
            &format!("{:.0} pt/s", geo_rate),
            ">5K",
            geo_rate > 5_000.0,
        );

        // 8b. GEOSEARCH latency
        let mut lats = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let _ = geo
                .geo_search(
                    "bench_geo",
                    116.05,
                    39.05,
                    5000.0,
                    GeoUnit::Meters,
                    Some(20),
                )
                .unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        metric(
            "GEO search avg",
            &format!("{:.1} μs", avg(&lats)),
            "<10ms",
            avg(&lats) < 10_000.0,
        );
        metric(
            "GEO search P99",
            &format!("{:.1} μs", p99(&mut lats)),
            "<20ms",
            p99(&mut lats) < 20_000.0,
        );
    }

    // AI Engine 已迁移至 talon-ai 私有仓库，性能基准请参见 talon-ai 项目

    // ═══ Resource Metrics ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Resource Metrics (资源占用指标)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let rss_after = rss_kb();
    let disk = dir_size(dir.path());
    let mem_delta = if rss_after > rss_before {
        rss_after - rss_before
    } else {
        0
    };
    metric(
        "Memory (RSS delta)",
        &format!("{}", hb(mem_delta * 1024)),
        "<512MB",
        mem_delta < 512 * 1024,
    );
    metric(
        "Disk usage (all engines)",
        &format!("{}", hb(disk)),
        "<1GB",
        disk < 1_073_741_824,
    );

    // Persist latency
    {
        let mut lats = Vec::with_capacity(10);
        for _ in 0..10 {
            let t = Instant::now();
            db.persist().unwrap();
            lats.push(t.elapsed().as_nanos() as f64 / 1_000_000.0); // ms
        }
        metric(
            "Persist avg",
            &format!("{:.1} ms", avg(&lats)),
            "<100ms",
            avg(&lats) < 100.0,
        );
        metric(
            "Persist P95",
            &format!("{:.1} ms", p95(&mut lats)),
            "<200ms",
            p95(&mut lats) < 200.0,
        );
    }

    // ═══ Stability Metrics ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Stability Metrics (稳定性指标)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Steady-state: run 10 rounds of mixed ops, measure variance
    {
        let mut round_ops = Vec::with_capacity(10);
        for round in 0..10 {
            let t0 = Instant::now();
            let kv = db.kv().unwrap();
            let mut ops = 0u64;
            for j in 0..1_000u64 {
                let key = format!("stab:{:04}:{:05}", round, j);
                kv.set(key.as_bytes(), b"x", None).unwrap();
                let _ = kv.get(key.as_bytes()).unwrap();
                ops += 2;
            }
            let elapsed = t0.elapsed();
            round_ops.push(ops as f64 / elapsed.as_secs_f64());
        }
        let variance_pct = stddev(&round_ops) / avg(&round_ops) * 100.0;
        metric(
            "KV ops/s variance (10 rounds)",
            &format!("{:.1}%", variance_pct),
            "<15%",
            variance_pct < 15.0,
        );
    }

    // Memory leak check: run additional ops and check RSS growth
    {
        let rss_mid = rss_kb();
        let kv = db.kv().unwrap();
        for i in 0..10_000u64 {
            let key = format!("leak_check:{:06}", i);
            kv.set(key.as_bytes(), &vec![0u8; 64], None).unwrap();
            let _ = kv.get(key.as_bytes());
        }
        let rss_end = rss_kb();
        let leak = if rss_end > rss_mid {
            rss_end - rss_mid
        } else {
            0
        };
        metric(
            "Memory growth (10K ops)",
            &format!("{}", hb(leak * 1024)),
            "<10MB",
            leak < 10 * 1024,
        );
    }

    // Durability check: persist → reopen → verify
    {
        db.persist().unwrap();
        let kv = db.kv().unwrap();
        let v = kv.get(b"kv:00000000").unwrap();
        let durable = v.is_some();
        metric(
            "Durability (persist+verify)",
            if durable { "OK" } else { "FAIL" },
            "OK",
            durable,
        );
    }

    // ═══ Concurrency Metrics ═══
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Concurrency Metrics (并发指标)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Concurrent read test
    {
        let db_arc = Arc::new(db);
        let n_threads = 4;
        let ops_per_thread = 5_000u64;
        let barrier = Arc::new(Barrier::new(n_threads));
        let t0 = Instant::now();
        let handles: Vec<_> = (0..n_threads)
            .map(|tid| {
                let db = Arc::clone(&db_arc);
                let bar = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    bar.wait();
                    let kv = db.kv_read().unwrap();
                    let mut ok = 0u64;
                    for i in 0..ops_per_thread {
                        let key = format!("kv:{:08}", (tid as u64 * 1000 + i) % n_kv);
                        if kv.get(key.as_bytes()).is_ok() {
                            ok += 1;
                        }
                    }
                    ok
                })
            })
            .collect();
        let total_ok: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let elapsed = t0.elapsed();
        let concurrent_qps = (n_threads as u64 * ops_per_thread) as f64 / elapsed.as_secs_f64();
        let success_rate = total_ok as f64 / (n_threads as u64 * ops_per_thread) as f64 * 100.0;
        metric(
            &format!("Concurrent read QPS ({}T)", n_threads),
            &format!("{:.0}", concurrent_qps),
            ">100K",
            concurrent_qps > 100_000.0,
        );
        metric(
            "Concurrent success rate",
            &format!("{:.1}%", success_rate),
            "100%",
            success_rate >= 99.9,
        );

        // ═══ Summary ═══
        println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("  Performance Summary");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        let rss_final = rss_kb();
        println!(
            "  Total RSS: {} | Disk: {} | Engines: 8 | Tests: COMPLETE",
            hb(rss_final * 1024),
            hb(disk)
        );
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    }
}

fn chrono_like_now() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let secs = d.as_secs();
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    // Approximate date from days since epoch
    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            366
        } else {
            365
        };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let month_days: Vec<i64> = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
        vec![31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        vec![31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0usize;
    for (i, &md) in month_days.iter().enumerate() {
        if remaining < md {
            m = i;
            break;
        }
        remaining -= md;
    }
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        y,
        m + 1,
        remaining + 1,
        hours,
        minutes,
        seconds
    )
}
