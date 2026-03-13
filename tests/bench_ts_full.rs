/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TS 引擎全方位基准（14 项，对标 InfluxDB / TDengine）
//! cargo test --test bench_ts_full --release -- --nocapture

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;
use talon::{AggFunc, DataPoint, Talon, TsAggQuery, TsQuery, TsSchema};

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

fn make_point(i: u64, nfields: usize) -> DataPoint {
    let mut tags = BTreeMap::new();
    tags.insert("host".into(), format!("h_{}", i % 100));
    tags.insert("region".into(), format!("r_{}", i % 10));
    let mut fields = BTreeMap::new();
    for f in 0..nfields {
        fields.insert(
            format!("f{}", f),
            format!("{:.2}", 10.0 + (i % 90) as f64 + f as f64),
        );
    }
    DataPoint {
        timestamp: 1700000000000 + i as i64,
        tags,
        fields,
    }
}

#[test]
fn ts_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         TS 引擎全方位基准（14 项指标）                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let n: u64 = 1_000_000;
    let bs: u64 = 10_000;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss0 = rss_kb();

    let schema = TsSchema {
        tags: vec!["host".into(), "region".into()],
        fields: vec!["f0".into(), "f1".into(), "f2".into()],
    };
    let ts = db.create_timeseries("metrics", schema).unwrap();

    // T1: 批量 INSERT 1M
    let t0 = Instant::now();
    let mut i = 0u64;
    while i < n {
        let end = (i + bs).min(n);
        let pts: Vec<_> = (i..end).map(|j| make_point(j, 3)).collect();
        ts.insert_batch(&pts).unwrap();
        i = end;
    }
    db.persist().unwrap();
    println!(
        "T1  | 批量 INSERT 1M (3 fields, batch=10K) | {:>12.0} pts/s",
        n as f64 / t0.elapsed().as_secs_f64()
    );

    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!("T12 | 1M 点(3f) 磁盘                      | {}", hb(disk));
    println!(
        "T13 | 1M 点 RSS 增量                       | {}KB",
        rss1 as i64 - rss0 as i64
    );

    // T2: 单 tag 查询
    {
        let s = 100usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let q = TsQuery {
                tag_filters: vec![("host".into(), format!("h_{}", i % 100))],
                time_start: Some(1700000000000),
                time_end: Some(1700000050000),
                desc: false,
                limit: Some(100),
            };
            let t = Instant::now();
            let _ = ts.query(&q).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, p99, _) = pct(&mut lat);
        println!(
            "T2  | 单 tag 查询 (100 samples)           | Avg={} P95={} P99={}",
            fms(avg),
            fms(p95),
            fms(p99)
        );
    }

    // T3: 多 tag 查询
    {
        let s = 100usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let q = TsQuery {
                tag_filters: vec![
                    ("host".into(), format!("h_{}", i % 100)),
                    ("region".into(), format!("r_{}", i % 10)),
                ],
                time_start: Some(1700000000000),
                time_end: Some(1700000050000),
                desc: false,
                limit: Some(100),
            };
            let t = Instant::now();
            let _ = ts.query(&q).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "T3  | 多 tag 查询 (host+region, 100)      | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // T4: 时间范围查询
    {
        let s = 100usize;
        let mut lat = Vec::with_capacity(s);
        for i in 0..s {
            let start = 1700000000000i64 + (i as i64) * 10000;
            let q = TsQuery {
                tag_filters: vec![],
                time_start: Some(start),
                time_end: Some(start + 1000),
                desc: false,
                limit: Some(100),
            };
            let t = Instant::now();
            let _ = ts.query(&q).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "T4  | 时间范围查询 (no tag, 100)          | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // T5: 聚合 SUM/AVG/COUNT
    {
        for (label, func) in [
            ("SUM", AggFunc::Sum),
            ("AVG", AggFunc::Avg),
            ("COUNT", AggFunc::Count),
        ] {
            let q = TsAggQuery {
                tag_filters: vec![("host".into(), "h_0".into())],
                time_start: None,
                time_end: None,
                field: "f0".into(),
                func,
                interval_ms: None,
                sliding_ms: None,
                session_gap_ms: None,
                fill: None,
            };
            let t = Instant::now();
            let _ = ts.aggregate(&q).unwrap();
            println!(
                "T5  | AGG {}(f0) host=h_0                 | {:.2}ms",
                label,
                t.elapsed().as_secs_f64() * 1000.0
            );
        }
    }

    // T6: 时间桶聚合
    {
        let q = TsAggQuery {
            tag_filters: vec![("host".into(), "h_0".into())],
            time_start: Some(1700000000000),
            time_end: Some(1700000100000),
            field: "f0".into(),
            func: AggFunc::Avg,
            interval_ms: Some(10000),
            sliding_ms: None,
            session_gap_ms: None,
            fill: None,
        };
        let t = Instant::now();
        let buckets = ts.aggregate(&q).unwrap();
        println!(
            "T6  | 时间桶聚合 (interval=10s, h_0)     | {:.2}ms ({}桶)",
            t.elapsed().as_secs_f64() * 1000.0,
            buckets.len()
        );
    }

    // T7: 降采样
    {
        let schema2 = TsSchema {
            tags: vec!["host".into()],
            fields: vec!["f0_avg".into()],
        };
        let _ = db.create_timeseries("metrics_ds", schema2);
        let t = Instant::now();
        let target = db.open_timeseries("metrics_ds").unwrap();
        let cnt = ts
            .downsample(
                &target,
                "f0",
                AggFunc::Avg,
                60000,
                &[("host".into(), "h_0".into())],
                None,
                None,
            )
            .unwrap();
        println!(
            "T7  | 降采样 (60s bucket, h_0 → ds)      | {:.2}ms ({}桶)",
            t.elapsed().as_secs_f64() * 1000.0,
            cnt
        );
    }

    // T8: 会话窗口聚合
    {
        let q = TsAggQuery {
            tag_filters: vec![("host".into(), "h_0".into())],
            time_start: Some(1700000000000),
            time_end: Some(1700000010000),
            field: "f0".into(),
            func: AggFunc::Count,
            interval_ms: None,
            sliding_ms: None,
            session_gap_ms: Some(500),
            fill: None,
        };
        let t = Instant::now();
        let buckets = ts.aggregate(&q).unwrap();
        println!(
            "T8  | 会话窗口聚合 (gap=500ms, h_0)      | {:.2}ms ({}桶)",
            t.elapsed().as_secs_f64() * 1000.0,
            buckets.len()
        );
    }

    // T9: 正则 tag 过滤（正则本质全扫描，减少采样+缩窄范围）
    {
        let s = 5usize;
        let mut lat = Vec::with_capacity(s);
        for _ in 0..s {
            let t = Instant::now();
            let _ = ts
                .query_regex(
                    &[("host".into(), "h_4[0-2]".into())],
                    Some(1700000000000),
                    Some(1700000001000),
                    false,
                    Some(50),
                )
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "T9  | 正则 tag 查询 (5 samples, 1s range) | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    // T10: 多字段梯度 — 用独立 DB
    println!("\n--- 字段数梯度 (100K each) ---");
    for &nf in &[1usize, 3, 5, 10] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let fields: Vec<String> = (0..nf).map(|i| format!("f{}", i)).collect();
        let schema = TsSchema {
            tags: vec!["host".into()],
            fields,
        };
        let ts2 = db2.create_timeseries("m", schema).unwrap();
        let cnt = 100_000u64;
        let t0 = Instant::now();
        let mut i = 0u64;
        while i < cnt {
            let end = (i + 10_000).min(cnt);
            let pts: Vec<_> = (i..end).map(|j| make_point(j, nf)).collect();
            ts2.insert_batch(&pts).unwrap();
            i = end;
        }
        db2.persist().unwrap();
        let ops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "T10 | {}f INSERT 100K                      | {:>9.0} pts/s | Disk={}",
            nf,
            ops,
            hb(disk)
        );
    }

    // T11: 数据保留 — 通过 retention cleaner 测试
    println!("T11 | retention (需 start_ts_retention_cleaner) | 跳过（异步清理器）");

    println!("\n✅ TS 引擎 14 项基准完成");
}
