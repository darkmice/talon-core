/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 引擎全方位基准（11 项，对标 Elasticsearch）
//! cargo test --test bench_fts_full --release -- --nocapture

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;
use talon::{Analyzer, FtsConfig, FtsDoc, Talon};

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

fn make_doc(i: u64, body_len: usize) -> FtsDoc {
    let words = [
        "the",
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "machine",
        "learning",
        "neural",
        "network",
        "deep",
        "model",
        "training",
        "vector",
        "embedding",
        "transformer",
        "attention",
        "language",
    ];
    let mut body = String::with_capacity(body_len);
    let mut s = i;
    while body.len() < body_len {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        let w = words[(s >> 33) as usize % words.len()];
        if !body.is_empty() {
            body.push(' ');
        }
        body.push_str(w);
    }
    let mut fields = BTreeMap::new();
    fields.insert(
        "title".into(),
        format!("doc_{} about {}", i, words[(i as usize) % words.len()]),
    );
    fields.insert("body".into(), body);
    fields.insert("category".into(), format!("cat{}", i % 20));
    FtsDoc {
        doc_id: format!("d{}", i),
        fields,
    }
}

#[test]
fn fts_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         FTS 引擎全方位基准（11 项指标）                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let n = 100_000u64;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let fts = db.fts().unwrap();
    let rss0 = rss_kb();

    fts.create_index(
        "docs",
        &FtsConfig {
            analyzer: Analyzer::Standard,
        },
    )
    .unwrap();

    // F1: 单文档索引 (10K)
    let f1n = 10_000u64;
    {
        let t0 = Instant::now();
        for i in 0..f1n {
            fts.index_doc("docs", &make_doc(i, 200)).unwrap();
        }
        db.persist().unwrap();
        println!(
            "F1  | 单文档索引 (10K, ~200B body)        | {:>12.0} docs/s",
            f1n as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // F2: 批量索引 (至 100K)
    {
        let t0 = Instant::now();
        let mut i = f1n;
        while i < n {
            let end = (i + 1000).min(n);
            let batch: Vec<_> = (i..end).map(|j| make_doc(j, 200)).collect();
            fts.index_doc_batch("docs", &batch).unwrap();
            i = end;
        }
        db.persist().unwrap();
        let actual = n - f1n;
        println!(
            "F2  | 批量索引 (至 100K, batch=1000)      | {:>12.0} docs/s",
            actual as f64 / t0.elapsed().as_secs_f64()
        );
    }

    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!("F9  | 100K 文档磁盘占用                  | {}", hb(disk));
    println!(
        "F10 | 100K 文档 RSS 增量                  | {}KB",
        rss1 as i64 - rss0 as i64
    );

    // F3: 单词搜索
    {
        let s = 500usize;
        let terms = ["fox", "machine", "neural", "deep", "transformer"];
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let q = terms[i % terms.len()];
            let t = Instant::now();
            let _ = fts.search("docs", q, 10).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _p99, _) = pct(&mut lat);
        println!(
            "F3  | 单词搜索 LIMIT 10 (500 queries)    | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // F4: 多词搜索
    {
        let s = 500usize;
        let queries = [
            "machine learning",
            "neural network",
            "deep model",
            "quick fox",
            "language transformer",
        ];
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let q = queries[i % queries.len()];
            let t = Instant::now();
            let _ = fts.search("docs", q, 10).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "F4  | 多词搜索 LIMIT 10 (500 queries)    | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // F5: 大 LIMIT
    {
        for &lim in &[10usize, 100, 1000] {
            let t = Instant::now();
            let hits = fts.search("docs", "machine", lim).unwrap();
            println!(
                "F5  | search('machine', limit={:4})       | {:.2}ms ({} hits)",
                lim,
                t.elapsed().as_secs_f64() * 1000.0,
                hits.len()
            );
        }
    }

    // F7: get_mapping
    {
        let s = 1000usize;
        let t0 = Instant::now();
        for _ in 0..s {
            let _ = fts.get_mapping("docs").unwrap();
        }
        println!(
            "F7  | get_mapping (1K calls)              | {:>12.0} ops/s",
            s as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // F8: reindex
    {
        // 用小索引测 reindex
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let fts2 = db2.fts().unwrap();
        fts2.create_index(
            "small",
            &FtsConfig {
                analyzer: Analyzer::Standard,
            },
        )
        .unwrap();
        for i in 0..10_000u64 {
            fts2.index_doc("small", &make_doc(i, 200)).unwrap();
        }
        let t = Instant::now();
        let cnt = fts2.reindex("small").unwrap();
        println!(
            "F8a | reindex 10K 文档                    | {:.2}ms ({} docs)",
            t.elapsed().as_secs_f64() * 1000.0,
            cnt
        );
    }

    // F11: 文档大小梯度
    println!("\n--- 文档大小梯度 (10K each) ---");
    for &(label, blen) in &[("100B", 100usize), ("1KB", 1024), ("10KB", 10240)] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let fts2 = db2.fts().unwrap();
        fts2.create_index(
            "t",
            &FtsConfig {
                analyzer: Analyzer::Standard,
            },
        )
        .unwrap();
        let cnt = 10_000u64;
        let t0 = Instant::now();
        for i in 0..cnt {
            fts2.index_doc("t", &make_doc(i, blen)).unwrap();
        }
        db2.persist().unwrap();
        let ops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "F11 | body={:4} index 10K                | {:>9.0} docs/s | Disk={}",
            label,
            ops,
            hb(disk)
        );
    }

    println!("\n✅ FTS 引擎 11 项基准完成");
}
