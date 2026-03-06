/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO 引擎全方位基准（12 项，对标 Redis GEO / PostGIS）
//! cargo test --test bench_geo_full --release -- --nocapture

use std::path::Path;
use std::time::Instant;
use talon::{GeoUnit, Talon};

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

/// 生成模拟坐标（北京周边散布）
fn make_coord(i: u64) -> (f64, f64) {
    let mut s = i
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let lng = 116.0 + (s >> 33) as f64 / (u32::MAX as f64) * 0.5; // 116.0 ~ 116.5
    s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
    let lat = 39.5 + (s >> 33) as f64 / (u32::MAX as f64) * 0.5; // 39.5 ~ 40.0
    (lng, lat)
}

#[test]
fn geo_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         GEO 引擎全方位基准（12 项指标）                     ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let n = 100_000u64;
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss0 = rss_kb();

    let geo = db.geo().unwrap();
    geo.create("places").unwrap();

    // G1: 单条 GEOADD
    {
        let cnt = 10_000u64;
        let t0 = Instant::now();
        for i in 0..cnt {
            let (lng, lat) = make_coord(i);
            geo.geo_add("places", &format!("p{}", i), lng, lat).unwrap();
        }
        db.persist().unwrap();
        println!(
            "G1  | 单条 GEOADD (10K)                   | {:>12.0} ops/s",
            cnt as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // G2: 批量 GEOADD (至 100K)
    {
        let t0 = Instant::now();
        let mut i = 10_000u64;
        while i < n {
            let end = (i + 1000).min(n);
            let batch: Vec<(&str, f64, f64)> = Vec::new();
            // 需要 owned strings，改用循环
            let members: Vec<(String, f64, f64)> = (i..end)
                .map(|j| {
                    let (lng, lat) = make_coord(j);
                    (format!("p{}", j), lng, lat)
                })
                .collect();
            let refs: Vec<(&str, f64, f64)> = members
                .iter()
                .map(|(k, lng, lat)| (k.as_str(), *lng, *lat))
                .collect();
            geo.geo_add_batch("places", &refs).unwrap();
            i = end;
        }
        db.persist().unwrap();
        let actual = n - 10_000;
        println!(
            "G2  | 批量 GEOADD (至 100K, batch=1000)   | {:>12.0} ops/s",
            actual as f64 / t0.elapsed().as_secs_f64()
        );
    }

    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!("G10 | 100K 成员磁盘占用                  | {}", hb(disk));
    println!(
        "G11 | 100K 成员 RSS 增量                  | {}KB",
        rss1 as i64 - rss0 as i64
    );

    // G3: GEOPOS 查询
    {
        let s = 10_000usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let t = Instant::now();
            let _ = geo.geo_pos("places", &format!("p{}", i)).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, p99, _) = pct(&mut lat);
        println!(
            "G3  | GEOPOS (10K queries)                | {:>12.0} ops/s | Avg={} P95={} P99={}",
            ops,
            fms(avg),
            fms(p95),
            fms(p99)
        );
    }

    // G4: GEODIST 两点距离
    {
        let s = 10_000usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let t = Instant::now();
            let _ = geo
                .geo_dist(
                    "places",
                    &format!("p{}", i),
                    &format!("p{}", (i + 1) % n as usize),
                    GeoUnit::Meters,
                )
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "G4  | GEODIST (10K pairs)                 | {:>12.0} ops/s | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // G5: GEOSEARCH 圆形范围（半径 1km）
    {
        let s = 500usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let (lng, la) = make_coord(i as u64);
            let t = Instant::now();
            let hits = geo
                .geo_search("places", lng, la, 1000.0, GeoUnit::Meters, Some(100))
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
            let _ = hits.len();
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "G5  | GEOSEARCH r=1km LIMIT 100 (500)     | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // G6: GEOSEARCH 不同半径
    println!("\n--- GEOSEARCH 半径梯度 ---");
    for &(label, radius) in &[
        ("100m", 100.0f64),
        ("1km", 1000.0),
        ("5km", 5000.0),
        ("50km", 50000.0),
    ] {
        let s = 100usize;
        let mut lat = Vec::with_capacity(s);
        let mut total_hits = 0usize;
        let t0 = Instant::now();
        for i in 0..s {
            let (lng, la) = make_coord(i as u64);
            let t = Instant::now();
            let hits = geo
                .geo_search("places", lng, la, radius, GeoUnit::Meters, Some(1000))
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
            total_hits += hits.len();
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        let avg_hits = total_hits / s;
        println!(
            "G6  | r={:>4} | {:>7.0} qps | Avg={} P95={} | ~{}hits",
            label,
            ops,
            fms(avg),
            fms(p95),
            avg_hits
        );
    }

    // G7: GEOSEARCH_BOX 矩形搜索
    {
        let s = 500usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let (lng, la) = make_coord(i as u64);
            let t = Instant::now();
            let hits = geo
                .geo_search_box(
                    "places",
                    lng - 0.005,
                    la - 0.005,
                    lng + 0.005,
                    la + 0.005,
                    Some(100),
                )
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
            let _ = hits.len();
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "\nG7  | GEOSEARCH_BOX ±0.005° (500)        | {:>9.0} qps | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // G8: GEOFENCE 围栏检测
    {
        let s = 10_000usize;
        let mut lat = Vec::with_capacity(s);
        let t0 = Instant::now();
        for i in 0..s {
            let t = Instant::now();
            let _ = geo
                .geo_fence(
                    "places",
                    &format!("p{}", i),
                    116.25,
                    39.75,
                    5000.0,
                    GeoUnit::Meters,
                )
                .unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
        }
        let ops = s as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "G8  | GEOFENCE r=5km (10K checks)         | {:>9.0} ops/s | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // G9: GEOHASH
    {
        let s = 10_000usize;
        let t0 = Instant::now();
        for i in 0..s {
            let _ = geo.geo_hash("places", &format!("p{}", i)).unwrap();
        }
        println!(
            "G9  | GEOHASH (10K)                       | {:>12.0} ops/s",
            s as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // G12: GEODIST + GEO_COUNT + GEO_MEMBERS
    {
        let t = Instant::now();
        let cnt = geo.geo_count("places").unwrap();
        println!(
            "G12a| GEO_COUNT                            | count={} | {:.2}ms",
            cnt,
            t.elapsed().as_secs_f64() * 1000.0
        );
    }

    // G_DEL: 删除
    {
        let cnt = 10_000u64;
        let t0 = Instant::now();
        for i in 0..cnt {
            geo.geo_del("places", &format!("p{}", i)).unwrap();
        }
        db.persist().unwrap();
        println!(
            "G12b| GEODEL (10K)                         | {:>12.0} ops/s",
            cnt as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // G_NX/XX: 条件写入
    {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        let geo2 = db2.geo().unwrap();
        geo2.create("cond").unwrap();
        // 预填 5K
        for i in 0..5_000u64 {
            let (lng, lat) = make_coord(i);
            geo2.geo_add("cond", &format!("c{}", i), lng, lat).unwrap();
        }
        // NX: 5K 已存在(skip) + 5K 新增
        let cnt = 10_000u64;
        let t0 = Instant::now();
        let mut added = 0u64;
        for i in 0..cnt {
            let (lng, lat) = make_coord(i + 100_000);
            if geo2
                .geo_add_nx("cond", &format!("c{}", i), lng, lat)
                .unwrap()
            {
                added += 1;
            }
        }
        println!(
            "G12c| GEOADD NX (10K, {}added)            | {:>9.0} ops/s",
            added,
            cnt as f64 / t0.elapsed().as_secs_f64()
        );

        // XX: 10K 全部存在(update)
        let t0 = Instant::now();
        for i in 0..cnt {
            let (lng, lat) = make_coord(i + 200_000);
            geo2.geo_add_xx("cond", &format!("c{}", i), lng, lat)
                .unwrap();
        }
        println!(
            "G12d| GEOADD XX (10K)                     | {:>9.0} ops/s",
            cnt as f64 / t0.elapsed().as_secs_f64()
        );
    }

    println!("\n✅ GEO 引擎 12 项基准完成");
}
