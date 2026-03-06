/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 综合指标基准（6 项：冷热启动 / 总磁盘 / 总内存 / persist / database_stats）
//! cargo test --test bench_综合 --release -- --nocapture

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;
use talon::{Analyzer, DataPoint, FtsConfig, FtsDoc, Talon, TsSchema};

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
    if b >= 1_073_741_824 {
        format!("{:.2}GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1}MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1}KB", b as f64 / 1024.0)
    } else {
        format!("{}B", b)
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

#[test]
fn comprehensive() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         综合指标基准（6 项）                                ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // X1: 冷启动时间（空 DB）
    {
        let dir = tempfile::tempdir().unwrap();
        let t = Instant::now();
        let _db = Talon::open(dir.path()).unwrap();
        println!(
            "X1  | 冷启动（空 DB open）                | {:.2}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // 填充数据用于后续测试
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();

    // KV: 100K
    {
        let kv = db.kv().unwrap();
        let val = vec![0x42u8; 100];
        let mut i = 0u64;
        while i < 100_000 {
            let end = (i + 10_000).min(100_000);
            let mut batch = db.batch();
            for j in i..end {
                kv.set_batch(&mut batch, format!("k:{:08}", j).as_bytes(), &val, None)
                    .unwrap();
            }
            batch.commit().unwrap();
            i = end;
        }
    }

    // SQL: 100K
    {
        db.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .unwrap();
        db.run_sql("BEGIN").unwrap();
        for i in 0..100_000u64 {
            db.run_sql(&format!(
                "INSERT INTO t VALUES ({}, 'u{}', {})",
                i,
                i,
                i % 1000
            ))
            .unwrap();
        }
        db.run_sql("COMMIT").unwrap();
    }

    // TS: 100K
    {
        let schema = TsSchema {
            tags: vec!["host".into()],
            fields: vec!["v".into()],
        };
        let ts = db.create_timeseries("m", schema).unwrap();
        let pts: Vec<_> = (0..100_000u64)
            .map(|i| {
                let mut tags = BTreeMap::new();
                tags.insert("host".into(), format!("h{}", i % 10));
                let mut fields = BTreeMap::new();
                fields.insert("v".into(), format!("{}", i));
                DataPoint {
                    timestamp: 1700000000000 + i as i64,
                    tags,
                    fields,
                }
            })
            .collect();
        let mut i = 0usize;
        while i < pts.len() {
            let end = (i + 10_000).min(pts.len());
            ts.insert_batch(&pts[i..end]).unwrap();
            i = end;
        }
    }

    // Vector: 10K
    {
        let ve = db.vector("v").unwrap();
        for i in 0..10_000u64 {
            ve.insert(i, &rvec(128, i)).unwrap();
        }
    }

    // FTS: 10K
    {
        let fts = db.fts().unwrap();
        fts.create_index(
            "d",
            &FtsConfig {
                analyzer: Analyzer::Standard,
            },
        )
        .unwrap();
        for i in 0..10_000u64 {
            let mut fields = BTreeMap::new();
            fields.insert(
                "body".into(),
                format!("the quick brown fox jumps over doc {}", i),
            );
            fts.index_doc(
                "d",
                &FtsDoc {
                    doc_id: format!("d{}", i),
                    fields,
                },
            )
            .unwrap();
        }
    }

    // MQ: 100K
    {
        let payload = vec![0x42u8; 100];
        let mq = db.mq().unwrap();
        mq.create_topic("q", 0).unwrap();
        let mut published = 0u64;
        while published < 100_000 {
            let payloads: Vec<&[u8]> = (0..1000).map(|_| payload.as_slice()).collect();
            mq.publish_batch("q", &payloads).unwrap();
            published += 1000;
        }
    }

    // X5: persist() 延迟
    {
        let samples = 10usize;
        let mut lats = Vec::with_capacity(samples);
        for _ in 0..samples {
            let t = Instant::now();
            db.persist().unwrap();
            lats.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let avg = lats.iter().sum::<f64>() / samples as f64;
        println!(
            "X5  | persist() 延迟 (全引擎, 10次)      | Avg={:.2}ms P95={:.2}ms",
            avg,
            lats[(samples as f64 * 0.95) as usize]
        );
    }

    db.persist().unwrap();
    let disk = dir_size(dir.path());
    let rss = rss_kb();

    // X3: DB 总磁盘
    println!("X3  | 全引擎填充后磁盘总量              | {}", hb(disk));

    // X4: 内存 RSS
    println!(
        "X4  | 全引擎填充后 RSS                   | {}KB ({:.1}MB)",
        rss,
        rss as f64 / 1024.0
    );

    // 关闭 DB，测热启动
    let db_path = dir.path().to_path_buf();
    drop(db);

    // X2: 热启动时间
    {
        let t = Instant::now();
        let _db = Talon::open(&db_path).unwrap();
        println!(
            "X2  | 热启动（已有数据 DB reopen）        | {:.2}ms",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // X6: close→reopen→verify
    {
        let db2 = Talon::open(&db_path).unwrap();
        let t = Instant::now();
        // 验证 KV
        let kv = db2.kv().unwrap();
        assert!(kv.get(b"k:00000042").unwrap().is_some());
        // 验证 SQL
        let rows = db2.run_sql("SELECT COUNT(*) FROM t").unwrap();
        assert!(!rows.is_empty());
        // 验证 TS
        let ts = db2.open_timeseries("m").unwrap();
        let q = talon::TsQuery {
            tag_filters: vec![("host".into(), "h0".into())],
            time_start: None,
            time_end: None,
            desc: false,
            limit: Some(1),
        };
        assert!(!ts.query(&q).unwrap().is_empty());
        // 验证 Vector
        let ve = db2.vector_read("v").unwrap();
        assert!(ve.count().unwrap() > 0);
        // 验证 FTS
        let fts = db2.fts().unwrap();
        let hits = fts.search("d", "fox", 1).unwrap();
        assert!(!hits.is_empty());
        // 验证 MQ
        let mq = db2.mq().unwrap();
        assert!(mq.len("q").unwrap() > 0);

        println!(
            "X6  | close→reopen→verify 全引擎         | {:.2}ms ✅ 数据完整",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    println!("\n✅ 综合指标 6 项完成");
}
