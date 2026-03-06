/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! MQ 引擎全方位基准（10 项，对标 Redis Streams / Kafka）
//! cargo test --test bench_mq_full --release -- --nocapture

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
fn mq_full() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         MQ 引擎全方位基准（10 项指标）                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let n: u64 = 1_000_000;
    let payload_100 = vec![0x42u8; 100];
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    let rss0 = rss_kb();

    // 创建 topic
    {
        let mq = db.mq().unwrap();
        mq.create_topic("bench", 0).unwrap();
    }

    // M1: 单条 PUBLISH
    {
        let cnt = 100_000u64;
        let t0 = Instant::now();
        for _ in 0..cnt {
            let mq = db.mq().unwrap();
            mq.publish("bench", &payload_100).unwrap();
        }
        db.persist().unwrap();
        println!(
            "M1  | 单条 PUBLISH (100K, 100B)           | {:>12.0} msg/s",
            cnt as f64 / t0.elapsed().as_secs_f64()
        );
    }

    // M2: 批量 PUBLISH (至 1M)
    {
        let t0 = Instant::now();
        let mut published = 100_000u64;
        while published < n {
            let batch_size = 1000usize;
            let payloads: Vec<&[u8]> = (0..batch_size).map(|_| payload_100.as_slice()).collect();
            let mq = db.mq().unwrap();
            mq.publish_batch("bench", &payloads).unwrap();
            published += batch_size as u64;
        }
        db.persist().unwrap();
        let actual = n - 100_000;
        println!(
            "M2  | 批量 PUBLISH (至 1M, batch=1000)    | {:>12.0} msg/s",
            actual as f64 / t0.elapsed().as_secs_f64()
        );
    }

    let rss1 = rss_kb();
    let disk = dir_size(dir.path());
    println!("M8  | 1M×100B 磁盘占用                  | {}", hb(disk));
    println!(
        "M9  | 1M 消息 RSS 增量                   | {}KB",
        rss1 as i64 - rss0 as i64
    );

    // subscribe + poll
    {
        let mq = db.mq().unwrap();
        mq.subscribe("bench", "g1").unwrap();
    }

    // M3: POLL
    {
        let rounds = 1000usize;
        let mut lat = Vec::with_capacity(rounds);
        let t0 = Instant::now();
        for _ in 0..rounds {
            let mq = db.mq().unwrap();
            let t = Instant::now();
            let msgs = mq.poll("bench", "g1", "c1", 100).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
            // ack
            for msg in &msgs {
                mq.ack("bench", "g1", "c1", msg.id).unwrap();
            }
        }
        let ops = rounds as f64 / t0.elapsed().as_secs_f64();
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "M3  | POLL 100条/次 (1K rounds, with ACK) | {:>9.0} rounds/s | Avg={} P95={}",
            ops,
            fms(avg),
            fms(p95)
        );
    }

    // M4: ACK (already measured in M3)
    println!("M4  | ACK (included in M3 poll+ack loop) | —");

    // M5: 多消费组并发（串行模拟）
    {
        let dir2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(dir2.path()).unwrap();
        {
            let mq = db2.mq().unwrap();
            mq.create_topic("multi", 0).unwrap();
        }
        // 发布 100K 消息
        {
            let mut published = 0u64;
            while published < 100_000 {
                let payloads: Vec<&[u8]> = (0..1000).map(|_| payload_100.as_slice()).collect();
                let mq = db2.mq().unwrap();
                mq.publish_batch("multi", &payloads).unwrap();
                published += 1000;
            }
        }
        for &ng in &[2usize, 4, 8] {
            // subscribe groups
            for g in 0..ng {
                let mq = db2.mq().unwrap();
                let _ = mq.subscribe("multi", &format!("g{}", g));
            }
            let t0 = Instant::now();
            for g in 0..ng {
                let group = format!("g{}", g);
                let consumer = format!("c{}", g);
                let mq = db2.mq().unwrap();
                let mut total = 0usize;
                loop {
                    let msgs = mq.poll("multi", &group, &consumer, 100).unwrap();
                    if msgs.is_empty() {
                        break;
                    }
                    for msg in &msgs {
                        mq.ack("multi", &group, &consumer, msg.id).unwrap();
                    }
                    total += msgs.len();
                }
            }
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            println!(
                "M5  | {}组消费 100K (串行)                | {:.1}ms",
                ng, ms
            );
        }
    }

    // M6: 消息大小梯度
    println!("\n--- Payload 大小梯度 (10K each) ---");
    for &ps in &[100usize, 1024, 10240] {
        let d2 = tempfile::tempdir().unwrap();
        let db2 = Talon::open(d2.path()).unwrap();
        {
            let mq = db2.mq().unwrap();
            mq.create_topic("sz", 0).unwrap();
        }
        let payload = vec![0x42u8; ps];
        let cnt = 10_000u64;
        let t0 = Instant::now();
        {
            let mut published = 0u64;
            while published < cnt {
                let batch_size = 1000usize.min((cnt - published) as usize);
                let payloads: Vec<&[u8]> = (0..batch_size).map(|_| payload.as_slice()).collect();
                let mq = db2.mq().unwrap();
                mq.publish_batch("sz", &payloads).unwrap();
                published += batch_size as u64;
            }
        }
        db2.persist().unwrap();
        let wops = cnt as f64 / t0.elapsed().as_secs_f64();
        // read
        {
            let mq = db2.mq().unwrap();
            mq.subscribe("sz", "g").unwrap();
        }
        let t0 = Instant::now();
        {
            let mq = db2.mq().unwrap();
            let mut total = 0u64;
            loop {
                let msgs = mq.poll("sz", "g", "c", 100).unwrap();
                if msgs.is_empty() {
                    break;
                }
                for msg in &msgs {
                    mq.ack("sz", "g", "c", msg.id).unwrap();
                }
                total += msgs.len() as u64;
            }
        }
        let rops = cnt as f64 / t0.elapsed().as_secs_f64();
        let disk = dir_size(d2.path());
        println!(
            "M6  | payload={:>6} | W: {:>9.0} msg/s | R: {:>9.0} msg/s | Disk={}",
            hb(ps as u64),
            wops,
            rops,
            hb(disk)
        );
    }

    // M7: 队列深度（积压 1M 后 poll 延迟）
    {
        let mq = db.mq().unwrap();
        // g1 已消费部分，创建新 group 从头消费
        mq.subscribe("bench", "g_deep").unwrap();
        let mut lat = Vec::with_capacity(100);
        for _ in 0..100 {
            let t = Instant::now();
            let msgs = mq.poll("bench", "g_deep", "c_deep", 100).unwrap();
            lat.push(t.elapsed().as_nanos() as f64 / 1000.0);
            for msg in &msgs {
                mq.ack("bench", "g_deep", "c_deep", msg.id).unwrap();
            }
        }
        let (avg, _, p95, _, _) = pct(&mut lat);
        println!(
            "M7  | 积压 1M 后 poll 100 (100 rounds)   | Avg={} P95={}",
            fms(avg),
            fms(p95)
        );
    }

    println!("\n✅ MQ 引擎 10 项基准完成");
}
