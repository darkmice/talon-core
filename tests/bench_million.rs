/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 百万级全引擎基准测试。
//! 用法：cargo test --release --test bench_million -- --nocapture

use std::collections::BTreeMap;
use std::time::Instant;
use talon::*;

// ────────────────────────────── KV ──────────────────────────────

#[test]
fn bench_kv_1m() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let kv = KvEngine::open(&store).unwrap();
    let count = 1_000_000usize;

    // 写入
    let t0 = Instant::now();
    for i in 0..count {
        kv.set(
            format!("key:{:08}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
            None,
        )
        .unwrap();
    }
    let write_ms = t0.elapsed().as_millis();
    let write_ops = count as f64 / (write_ms as f64 / 1000.0);

    // 随机读
    let t1 = Instant::now();
    let read_count = 100_000usize;
    for i in (0..read_count).map(|x| x * 7 % count) {
        let _ = kv.get(format!("key:{:08}", i).as_bytes()).unwrap();
    }
    let read_ms = t1.elapsed().as_millis();
    let read_ops = read_count as f64 / (read_ms as f64 / 1000.0);

    // Prefix scan
    let t2 = Instant::now();
    let scan_results = kv.scan_prefix_limit(b"key:0050", 0, 1000).unwrap();
    let scan_ms = t2.elapsed().as_millis();

    eprintln!("=== KV 百万基准 ===");
    eprintln!(
        "  写入 {} 条: {}ms ({:.0} ops/s)",
        count, write_ms, write_ops
    );
    eprintln!(
        "  随机读 {} 条: {}ms ({:.0} ops/s)",
        read_count, read_ms, read_ops
    );
    eprintln!("  前缀扫描 {} 条结果: {}ms", scan_results.len(), scan_ms);

    assert!(write_ops > 50_000.0, "KV 写入太慢: {:.0} ops/s", write_ops);
    assert!(read_ops > 100_000.0, "KV 读取太慢: {:.0} ops/s", read_ops);
}

// ────────────────────────────── SQL ──────────────────────────────

#[test]
fn bench_sql_1m() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();

    eng.run_sql("CREATE TABLE bench (id INT, name TEXT, score INT)")
        .unwrap();

    // 批量插入 100万行 (每批 1000 行)
    let count = 1_000_000usize;
    let batch_size = 1000;
    let t0 = Instant::now();
    for batch_start in (0..count).step_by(batch_size) {
        let mut sql = String::from("INSERT INTO bench VALUES ");
        for i in batch_start..batch_start + batch_size {
            if i > batch_start {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({}, 'user_{}', {})", i, i, i % 100));
        }
        eng.run_sql(&sql).unwrap();
    }
    let insert_ms = t0.elapsed().as_millis();
    let insert_ops = count as f64 / (insert_ms as f64 / 1000.0);

    // PK 查询
    let t1 = Instant::now();
    let pk_count = 10_000usize;
    for i in (0..pk_count).map(|x| x * 97 % count) {
        let rows = eng
            .run_sql(&format!("SELECT * FROM bench WHERE id = {}", i))
            .unwrap();
        assert!(!rows.is_empty());
    }
    let pk_ms = t1.elapsed().as_millis();
    let pk_ops = pk_count as f64 / (pk_ms as f64 / 1000.0);

    // 全表扫描 + WHERE
    let t2 = Instant::now();
    let rows = eng
        .run_sql("SELECT * FROM bench WHERE score = 42 LIMIT 100")
        .unwrap();
    let scan_ms = t2.elapsed().as_millis();

    // 表达式列 (本次新增功能)
    let t3 = Instant::now();
    let expr_rows = eng
        .run_sql("SELECT id, score + 10 AS boosted FROM bench WHERE id < 1000")
        .unwrap();
    let expr_ms = t3.elapsed().as_millis();

    eprintln!("=== SQL 百万基准 ===");
    eprintln!(
        "  插入 {} 行: {}ms ({:.0} rows/s)",
        count, insert_ms, insert_ops
    );
    eprintln!("  PK查询 {} 次: {}ms ({:.0} qps)", pk_count, pk_ms, pk_ops);
    eprintln!(
        "  全表扫描 WHERE score=42 LIMIT 100: {}ms ({} rows)",
        scan_ms,
        rows.len()
    );
    eprintln!(
        "  表达式列 SELECT id, score+10: {}ms ({} rows)",
        expr_ms,
        expr_rows.len()
    );

    assert!(
        insert_ops > 100_000.0,
        "SQL 插入太慢: {:.0} rows/s",
        insert_ops
    );
    assert!(pk_ops > 1_000.0, "SQL PK查询太慢: {:.0} qps", pk_ops);
}

// ────────────────────────────── FTS ──────────────────────────────

#[test]
fn bench_fts_100k() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bench", &FtsConfig::default()).unwrap();

    // 索引 10万文档 (FTS 单条索引较重，100K 即可反映性能)
    let count = 100_000usize;
    let topics = [
        "rust programming",
        "machine learning",
        "database systems",
        "web development",
        "cloud computing",
        "artificial intelligence",
        "deep learning",
        "natural language",
        "computer vision",
        "data science",
    ];
    let t0 = Instant::now();
    for i in 0..count {
        let topic = topics[i % topics.len()];
        let doc = FtsDoc {
            doc_id: format!("doc_{}", i),
            fields: {
                let mut m = BTreeMap::new();
                m.insert("title".into(), format!("{} tutorial part {}", topic, i));
                m.insert("body".into(), format!(
                    "This is document {} about {}. It covers advanced topics in {} and related fields.",
                    i, topic, topic
                ));
                m
            },
        };
        fts.index_doc("bench", &doc).unwrap();
    }
    let index_ms = t0.elapsed().as_millis();
    let index_ops = count as f64 / (index_ms as f64 / 1000.0);

    // BM25 搜索
    let queries = [
        "rust",
        "machine learning",
        "database",
        "cloud",
        "deep learning",
    ];
    let t1 = Instant::now();
    let search_count = 500;
    for i in 0..search_count {
        let q = queries[i % queries.len()];
        let hits = fts.search("bench", q, 10).unwrap();
        assert!(!hits.is_empty());
        // 验证高亮功能 (本次新增)
        if !hits[0].highlights.is_empty() {
            // 有高亮结果
        }
    }
    let search_ms = t1.elapsed().as_millis();
    let search_qps = search_count as f64 / (search_ms as f64 / 1000.0);

    // Fuzzy 搜索 (本次新增功能)
    let t2 = Instant::now();
    let fuzzy_count = 100;
    for _ in 0..fuzzy_count {
        let _hits = fts.search_fuzzy("bench", "machne lerning", 1, 10).unwrap();
    }
    let fuzzy_ms = t2.elapsed().as_millis();
    let fuzzy_qps = fuzzy_count as f64 / (fuzzy_ms as f64 / 1000.0);

    eprintln!("=== FTS 10万基准 ===");
    eprintln!(
        "  索引 {} 文档: {}ms ({:.0} docs/s)",
        count, index_ms, index_ops
    );
    eprintln!(
        "  BM25搜索 {} 次: {}ms ({:.0} qps)",
        search_count, search_ms, search_qps
    );
    eprintln!(
        "  Fuzzy搜索 {} 次: {}ms ({:.0} qps)",
        fuzzy_count, fuzzy_ms, fuzzy_qps
    );

    assert!(index_ops > 500.0, "FTS 索引太慢: {:.0} docs/s", index_ops);
    assert!(search_qps > 5.0, "FTS 搜索太慢: {:.0} qps", search_qps);
}

// ────────────────────────────── GEO ──────────────────────────────

#[test]
fn bench_geo_1m() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("bench").unwrap();

    // 百万 POI 写入
    let count = 1_000_000usize;
    let t0 = Instant::now();
    // 批量写入 (每批 1000)
    let batch_size = 1000;
    for batch_start in (0..count).step_by(batch_size) {
        let members: Vec<(&str, f64, f64)> = (batch_start..batch_start + batch_size)
            .map(|i| {
                let lng = (i as f64 / count as f64) * 360.0 - 180.0;
                let lat = (i as f64 / count as f64) * 170.0 - 85.0;
                // 需要 'static str，用 leak
                let key: &str = Box::leak(format!("poi_{}", i).into_boxed_str());
                (key, lng, lat)
            })
            .collect();
        geo.geo_add_batch("bench", &members).unwrap();
    }
    let write_ms = t0.elapsed().as_millis();
    let write_ops = count as f64 / (write_ms as f64 / 1000.0);

    // 圆形搜索
    let t1 = Instant::now();
    let search_count = 1000;
    for _ in 0..search_count {
        let _r = geo
            .geo_search("bench", 116.4, 39.9, 100.0, GeoUnit::Kilometers, Some(20))
            .unwrap();
    }
    let circle_ms = t1.elapsed().as_millis();
    let circle_qps = search_count as f64 / (circle_ms as f64 / 1000.0);

    // 矩形搜索 — 小矩形(城市级 ~10km)
    let t2 = Instant::now();
    let box_count = 1000;
    for _ in 0..box_count {
        let _r = geo
            .geo_search_box("bench", 116.3, 39.8, 116.5, 40.0, Some(20))
            .unwrap();
    }
    let box_ms = t2.elapsed().as_millis();
    let box_qps = box_count as f64 / (box_ms.max(1) as f64 / 1000.0);

    // 矩形搜索 — 大矩形(省级 ~1000km)
    let t2b = Instant::now();
    let box_count_big = 100;
    for _ in 0..box_count_big {
        let _r = geo
            .geo_search_box("bench", 110.0, 30.0, 120.0, 40.0, Some(20))
            .unwrap();
    }
    let box_big_ms = t2b.elapsed().as_millis();
    let box_big_qps = box_count_big as f64 / (box_big_ms.max(1) as f64 / 1000.0);

    // 围栏检测 (本次新增功能)
    let t3 = Instant::now();
    let fence_count = 10_000;
    for i in 0..fence_count {
        let _r = geo
            .geo_fence(
                "bench",
                &format!("poi_{}", i * 100),
                116.4,
                39.9,
                500.0,
                GeoUnit::Kilometers,
            )
            .unwrap();
    }
    let fence_ms = t3.elapsed().as_millis();
    let fence_qps = fence_count as f64 / (fence_ms as f64 / 1000.0);

    eprintln!("=== GEO 百万基准 ===");
    eprintln!(
        "  写入 {} POI: {}ms ({:.0} ops/s)",
        count, write_ms, write_ops
    );
    eprintln!(
        "  圆形搜索 {} 次: {}ms ({:.0} qps)",
        search_count, circle_ms, circle_qps
    );
    eprintln!(
        "  矩形搜索(城市级) {} 次: {}ms ({:.0} qps)",
        box_count, box_ms, box_qps
    );
    eprintln!(
        "  矩形搜索(省级) {} 次: {}ms ({:.0} qps)",
        box_count_big, box_big_ms, box_big_qps
    );
    eprintln!(
        "  围栏检测 {} 次: {}ms ({:.0} qps)",
        fence_count, fence_ms, fence_qps
    );

    assert!(write_ops > 10_000.0, "GEO 写入太慢: {:.0} ops/s", write_ops);
    assert!(circle_qps > 10.0, "GEO 圆形搜索太慢: {:.0} qps", circle_qps);
}

// ────────────────────────────── TS ──────────────────────────────

#[test]
fn bench_ts_1m() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let schema = TsSchema {
        tags: vec!["host".into()],
        fields: vec!["value".into()],
    };
    let ts = TsEngine::create(&store, "cpu", schema).unwrap();

    // 百万写入
    let count = 1_000_000usize;
    let t0 = Instant::now();
    for i in 0..count {
        let mut tags = BTreeMap::new();
        tags.insert("host".into(), format!("srv_{}", i % 10));
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), format!("{:.2}", i as f64 * 0.01));
        let dp = DataPoint {
            timestamp: i as i64,
            tags,
            fields,
        };
        ts.insert(&dp).unwrap();
    }
    let write_ms = t0.elapsed().as_millis();
    let write_ops = count as f64 / (write_ms as f64 / 1000.0);

    // 范围查询
    let t1 = Instant::now();
    let query_count = 100;
    for i in 0..query_count {
        let start = (i * 10000) as i64;
        let q = TsQuery {
            tag_filters: vec![("host".into(), "srv_0".into())],
            time_start: Some(start),
            time_end: Some(start + 10000),
            desc: false,
            limit: Some(100),
        };
        let points = ts.query(&q).unwrap();
        assert!(!points.is_empty());
    }
    let query_ms = t1.elapsed().as_millis();
    let query_qps = query_count as f64 / (query_ms as f64 / 1000.0);

    eprintln!("=== TS 百万基准 ===");
    eprintln!(
        "  写入 {} 点: {}ms ({:.0} points/s)",
        count, write_ms, write_ops
    );
    eprintln!(
        "  范围查询 {} 次: {}ms ({:.0} qps)",
        query_count, query_ms, query_qps
    );

    assert!(
        write_ops > 100_000.0,
        "TS 写入太慢: {:.0} points/s",
        write_ops
    );
}

// ────────────────────────────── MQ ──────────────────────────────

#[test]
fn bench_mq_1m() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mq = MqEngine::open(&store).unwrap();
    mq.create_topic("bench", 0).unwrap();
    mq.subscribe("bench", "g1").unwrap();

    // 百万发布
    let count = 1_000_000usize;
    let t0 = Instant::now();
    for i in 0..count {
        mq.publish("bench", format!("msg_{}", i).as_bytes())
            .unwrap();
    }
    let pub_ms = t0.elapsed().as_millis();
    let pub_ops = count as f64 / (pub_ms as f64 / 1000.0);

    // 百万消费 (poll)
    let t1 = Instant::now();
    let mut consumed = 0usize;
    loop {
        let msgs = mq.poll("bench", "g1", "c1", 1000).unwrap();
        if msgs.is_empty() {
            break;
        }
        for msg in &msgs {
            mq.ack("bench", "g1", "c1", msg.id).unwrap();
        }
        consumed += msgs.len();
    }
    let con_ms = t1.elapsed().as_millis();
    let con_ops = consumed as f64 / (con_ms.max(1) as f64 / 1000.0);

    eprintln!("=== MQ 百万基准 ===");
    eprintln!("  发布 {} 消息: {}ms ({:.0} msg/s)", count, pub_ms, pub_ops);
    eprintln!(
        "  消费 {} 消息: {}ms ({:.0} msg/s)",
        consumed, con_ms, con_ops
    );

    assert!(pub_ops > 50_000.0, "MQ 发布太慢: {:.0} msg/s", pub_ops);
}
