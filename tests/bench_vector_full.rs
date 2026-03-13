/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Vector 引擎全方位基准（12 项，对标 Qdrant / Milvus）
//! cargo test --test bench_vector_full --release -- --nocapture

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use talon::{MetaFilter, MetaFilterOp, MetaValue, Talon};

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
fn rvec(dim: usize, seed: u64) -> Vec<f32> {
    let mut v = Vec::with_capacity(dim);
    let mut s = seed.wrapping_add(1); // avoid seed=0 degeneracy
    for _ in 0..dim {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // 使用高 32 位，映射到 [-1.0, 1.0] 全范围
        v.push((s >> 32) as i32 as f32 / (i32::MAX as f32));
    }
    // L2 归一化 — cosine 搜索标准做法
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 1e-10 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}


#[test]
fn vector_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         Vector 引擎全方位基准（12 项指标）                  ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let dim = 128usize;
    let n = 50_000u64;

    // V1: INSERT + HNSW 构建
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss0 = rss_kb();

    let ve = db.vector("bench").unwrap();
    let t0 = Instant::now();
    for i in 0..n {
        ve.insert(i, &rvec(dim, i)).unwrap();
    }
    db.persist().unwrap();
    let insert_ops = n as f64 / t0.elapsed().as_secs_f64();
    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!(
        "V1  | INSERT 50K (dim={})                | {:>9.0} vecs/s",
        dim, insert_ops
    );
    println!(
        "V9  | 50K×dim{} 磁盘                     | {}",
        dim,
        hb(disk)
    );
    println!(
        "V10 | 50K×dim{} RSS 增量                  | {}KB",
        dim,
        rss1 as i64 - rss0 as i64
    );

    // V2: KNN k=10
    {
        let s = 1000usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let q = rvec(dim, n + i as u64);
            let t = Instant::now();
            let _ = ve.search(&q, 10, "cosine").unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, p50, p95, p99, max) = pct(&mut lat);
        println!(
            "V2  | KNN k=10 cosine (1K queries)        | {:>9.0} qps",
            ops
        );
        println!(
            "      Avg={} P50={} P95={} P99={} Max={}",
            fms(avg),
            fms(p50),
            fms(p95),
            fms(p99),
            fms(max)
        );
    }

    // V3: KNN k=100
    {
        let s = 200usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let q = rvec(dim, n + 10000 + i as u64);
            let t = Instant::now();
            let _ = ve.search(&q, 100, "cosine").unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "V3  | KNN k=100 cosine (200 queries)      | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // V4: 带过滤搜索
    {
        // 先给前 50K 设置 metadata
        for i in 0..1000u64 {
            let mut meta = HashMap::new();
            meta.insert("cat".into(), MetaValue::String(format!("c{}", i % 10)));
            ve.set_metadata(i, &meta).unwrap();
        }
        let s = 200usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let q = rvec(dim, n + 20000 + i as u64);
            let filter = vec![MetaFilter {
                field: "cat".into(),
                op: MetaFilterOp::Eq(MetaValue::String("c5".into())),
            }];
            let t = Instant::now();
            let _ = ve.search_with_filter(&q, 10, "cosine", &filter).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "V4  | KNN k=10 + filter (200 queries)    | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // V5: 维度梯度
    println!("\n--- 维度梯度 (10K each, INSERT + KNN k=10) ---");
    for &d in &[64usize, 128, 256, 512, 768] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let ve2 = db2.vector("v").unwrap();
        let cnt = 10_000u64;
        let t0 = Instant::now();
        for i in 0..cnt {
            ve2.insert(i, &rvec(d, i)).unwrap();
        }
        db2.persist().unwrap();
        let iops = cnt as f64 / t0.elapsed().as_secs_f64();
        // search
        let qs = 100usize;
        let t0 = Instant::now();
        for i in 0..qs {
            let q = rvec(d, cnt + i as u64);
            let _ = ve2.search(&q, 10, "cosine").unwrap();
        }
        let sops = qs as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "V5  | dim={:4} | I: {:>7.0} vecs/s | S: {:>7.0} qps | Disk={}",
            d,
            iops,
            sops,
            hb(disk)
        );
    }

    // V6: 批量搜索
    {
        let s = 10usize;
        let queries: Vec<Vec<f32>> = (0..s).map(|i| rvec(dim, n + 30000 + i as u64)).collect();
        let refs: Vec<&[f32]> = queries.iter().map(|q| q.as_slice()).collect();
        let t = Instant::now();
        let _ = ve.batch_search(&refs, 10, "cosine").unwrap();
        println!(
            "V6  | batch_search ×{} (k=10)             | {:.2}ms total",
            s,
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // V7: recall@10 (vs 暴力搜索) — L2 距离 + 低维（16D）减少维度诅咒
    {
        let recall_dim = 16usize; // 低维让 L2 距离分布更有区分度
        let recall_n = 5_000u64;
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let ve2 = db2.vector("r").unwrap();
        let vecs: Vec<Vec<f32>> = (0..recall_n).map(|i| rvec(recall_dim, i)).collect();
        for i in 0..recall_n {
            ve2.insert(i, &vecs[i as usize]).unwrap();
        }
        ve2.set_ef_search(200).unwrap();
        let test_n = 100usize;
        let mut total_recall = 0.0f64;
        for qi in 0..test_n {
            let q = rvec(recall_dim, recall_n + qi as u64);
            // L2 暴力搜索 top 10
            let mut dists: Vec<(u64, f32)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let d2: f32 = q.iter().zip(v.iter()).map(|(a, b)| (a - b).powi(2)).sum();
                    (i as u64, d2.sqrt())
                })
                .collect();
            dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let brute_top: Vec<u64> = dists.iter().take(10).map(|(id, _)| *id).collect();
            // HNSW L2 搜索
            let hnsw_res = ve2.search(&q, 10, "l2").unwrap();
            let hnsw_top: Vec<u64> = hnsw_res.iter().map(|(id, _)| *id).collect();
            let hit = brute_top.iter().filter(|id| hnsw_top.contains(id)).count();
            total_recall += hit as f64 / 10.0;
        }
        let recall = total_recall / test_n as f64;
        println!(
            "V7  | recall@10 (5K, dim=16, L2, ef=200) | {:.1}%",
            recall * 100.0
        );
    }

    // V8: ef_search 调参
    println!("\n--- ef_search 调参 (50K, k=10, 100 queries) ---");
    for &ef in &[50usize, 100, 200, 400] {
        ve.set_ef_search(ef).unwrap();
        let qs = 100usize;
        let t0 = Instant::now();
        for i in 0..qs {
            let q = rvec(dim, n + 40000 + i as u64);
            let _ = ve.search(&q, 10, "cosine").unwrap();
        }
        let qps = qs as f64 / t0.elapsed().as_secs_f64();
        println!("V8  | ef_search={:3} | {:>7.0} qps", ef, qps);
    }

    // V11: 维度 vs 磁盘
    println!("\n--- 维度 vs 磁盘 (10K vecs each) ---");
    for &d in &[64usize, 128, 256, 512, 768] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let ve2 = db2.vector("d").unwrap();
        for i in 0..10_000u64 {
            ve2.insert(i, &rvec(d, i)).unwrap();
        }
        db2.persist().unwrap();
        let disk = dir_size(d2.path());
        let raw = 10_000u64 * (d as u64) * 4;
        println!(
            "V11 | dim={:4} | Disk={} | Raw={} | Ratio={:.1}x",
            d,
            hb(disk),
            hb(raw),
            disk as f64 / raw as f64
        );
    }

    println!("\n✅ Vector 引擎 12 项基准完成");
}
