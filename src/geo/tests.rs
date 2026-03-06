/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO 引擎集成测试。

use super::*;

#[test]
fn geo_add_pos_del_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    geo.geo_add("places", "beijing", 116.4074, 39.9042).unwrap();
    let pos = geo.geo_pos("places", "beijing").unwrap().unwrap();
    assert!((pos.lng - 116.4074).abs() < 0.001);
    assert!((pos.lat - 39.9042).abs() < 0.001);
    assert!(geo.geo_del("places", "beijing").unwrap());
    assert!(geo.geo_pos("places", "beijing").unwrap().is_none());
}

#[test]
fn geo_dist_km() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("cities").unwrap();
    geo.geo_add("cities", "beijing", 116.4074, 39.9042).unwrap();
    geo.geo_add("cities", "shanghai", 121.4737, 31.2304)
        .unwrap();
    let dist = geo
        .geo_dist("cities", "beijing", "shanghai", GeoUnit::Kilometers)
        .unwrap()
        .unwrap();
    assert!(dist > 1000.0 && dist < 1200.0, "dist={}", dist);
}

#[test]
fn geo_search_radius() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("pois").unwrap();
    geo.geo_add("pois", "tiananmen", 116.3912, 39.9087).unwrap();
    geo.geo_add("pois", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    geo.geo_add("pois", "temple_of_heaven", 116.4107, 39.8822)
        .unwrap();
    geo.geo_add("pois", "great_wall", 116.5704, 40.4319)
        .unwrap();
    let results = geo
        .geo_search("pois", 116.3912, 39.9087, 5.0, GeoUnit::Kilometers, None)
        .unwrap();
    let keys: Vec<&str> = results.iter().map(|m| m.key.as_str()).collect();
    assert!(keys.contains(&"tiananmen"), "should contain tiananmen");
    assert!(
        keys.contains(&"forbidden_city"),
        "should contain forbidden_city"
    );
    assert!(
        keys.contains(&"temple_of_heaven"),
        "should contain temple_of_heaven"
    );
    assert!(
        !keys.contains(&"great_wall"),
        "should not contain great_wall"
    );
    assert!(results[0].dist.unwrap() <= results[1].dist.unwrap());
}

#[test]
fn geo_members_list() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("test").unwrap();
    geo.geo_add("test", "a", 0.0, 0.0).unwrap();
    geo.geo_add("test", "b", 1.0, 1.0).unwrap();
    let members = geo.geo_members("test").unwrap();
    assert_eq!(members.len(), 2);
}

#[test]
fn geo_update_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("upd").unwrap();
    geo.geo_add("upd", "x", 10.0, 20.0).unwrap();
    geo.geo_add("upd", "x", 30.0, 40.0).unwrap();
    let pos = geo.geo_pos("upd", "x").unwrap().unwrap();
    assert!((pos.lng - 30.0).abs() < 0.001);
    assert!((pos.lat - 40.0).abs() < 0.001);
    assert_eq!(geo.geo_members("upd").unwrap().len(), 1);
}

#[test]
fn geo_batch_add() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("batch").unwrap();
    geo.geo_add_batch(
        "batch",
        &[("a", 0.0, 0.0), ("b", 1.0, 1.0), ("c", 2.0, 2.0)],
    )
    .unwrap();
    assert_eq!(geo.geo_members("batch").unwrap().len(), 3);
}

#[test]
fn haversine_basic() {
    let d = haversine_distance(39.9042, 116.4074, 39.9042, 116.4074);
    assert!(d < 1.0);
}

#[test]
fn validate_coords_rejects_invalid() {
    assert!(validate_coords(181.0, 0.0).is_err());
    assert!(validate_coords(0.0, 86.0).is_err());
    assert!(validate_coords(180.0, 85.0).is_ok());
}

#[test]
fn geo_search_box_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("box").unwrap();
    // 北京区域 POI
    geo.geo_add("box", "tiananmen", 116.3912, 39.9087).unwrap();
    geo.geo_add("box", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    geo.geo_add("box", "temple_of_heaven", 116.4107, 39.8822)
        .unwrap();
    // 上海（远处）
    geo.geo_add("box", "oriental_pearl", 121.4999, 31.2397)
        .unwrap();
    // 搜索北京区域矩形
    let results = geo
        .geo_search_box("box", 116.3, 39.8, 116.5, 40.0, None)
        .unwrap();
    let keys: Vec<&str> = results.iter().map(|m| m.key.as_str()).collect();
    assert!(keys.contains(&"tiananmen"));
    assert!(keys.contains(&"forbidden_city"));
    assert!(keys.contains(&"temple_of_heaven"));
    assert!(!keys.contains(&"oriental_pearl"), "上海不在北京矩形内");
}

#[test]
fn geo_search_box_with_count() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("boxc").unwrap();
    // 北京区域三个 POI
    geo.geo_add("boxc", "a", 116.39, 39.90).unwrap();
    geo.geo_add("boxc", "b", 116.40, 39.91).unwrap();
    geo.geo_add("boxc", "c", 116.41, 39.92).unwrap();
    let results = geo
        .geo_search_box("boxc", 116.38, 39.89, 116.42, 39.93, Some(2))
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn geo_fence_inside() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("fence").unwrap();
    geo.geo_add("fence", "me", 116.3912, 39.9087).unwrap();
    // 天安门 1km 范围内
    let inside = geo
        .geo_fence("fence", "me", 116.3912, 39.9087, 1.0, GeoUnit::Kilometers)
        .unwrap()
        .unwrap();
    assert!(inside);
}

#[test]
fn geo_fence_outside() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("fence2").unwrap();
    geo.geo_add("fence2", "far", 121.4737, 31.2304).unwrap();
    // 上海不在北京天安门 10km 范围内
    let inside = geo
        .geo_fence(
            "fence2",
            "far",
            116.3912,
            39.9087,
            10.0,
            GeoUnit::Kilometers,
        )
        .unwrap()
        .unwrap();
    assert!(!inside);
}

#[test]
fn geo_bench_10k_pois() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("bench").unwrap();
    let count = 10_000usize;
    let t0 = Instant::now();
    for i in 0..count {
        let lng = (i as f64 / count as f64) * 360.0 - 180.0;
        let lat = (i as f64 / count as f64) * 170.0 - 85.0;
        geo.geo_add("bench", &format!("poi_{}", i), lng, lat)
            .unwrap();
    }
    let add_ms = t0.elapsed().as_millis();
    let add_ops = count as f64 / (add_ms as f64 / 1000.0);
    eprintln!(
        "[GEO bench] added {} POIs in {}ms ({:.0} ops/s)",
        count, add_ms, add_ops
    );
    // 圆形搜索性能
    let t1 = Instant::now();
    let search_count = 100;
    for _ in 0..search_count {
        let _r = geo
            .geo_search("bench", 116.4, 39.9, 500.0, GeoUnit::Kilometers, Some(20))
            .unwrap();
    }
    let search_ms = t1.elapsed().as_millis();
    let search_qps = search_count as f64 / (search_ms as f64 / 1000.0);
    eprintln!(
        "[GEO bench] {} radius searches in {}ms ({:.0} qps)",
        search_count, search_ms, search_qps
    );
    // 矩形搜索性能
    let t2 = Instant::now();
    for _ in 0..search_count {
        let _r = geo
            .geo_search_box("bench", 100.0, 20.0, 130.0, 50.0, Some(20))
            .unwrap();
    }
    let box_ms = t2.elapsed().as_millis();
    let box_qps = search_count as f64 / (box_ms as f64 / 1000.0);
    eprintln!(
        "[GEO bench] {} box searches in {}ms ({:.0} qps)",
        search_count, box_ms, box_qps
    );
    assert!(add_ops > 100.0, "add too slow: {:.0} ops/s", add_ops);
}

#[test]
fn geo_fence_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("fence3").unwrap();
    let result = geo
        .geo_fence("fence3", "nobody", 0.0, 0.0, 1.0, GeoUnit::Meters)
        .unwrap();
    assert!(result.is_none());
}

// ── M81: GEO COUNT 测试 ──

#[test]
fn geo_count_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();

    geo.create("cnt").unwrap();
    assert_eq!(geo.geo_count("cnt").unwrap(), 0);

    geo.geo_add("cnt", "a", 1.0, 1.0).unwrap();
    geo.geo_add("cnt", "b", 2.0, 2.0).unwrap();
    geo.geo_add("cnt", "c", 3.0, 3.0).unwrap();
    assert_eq!(geo.geo_count("cnt").unwrap(), 3);

    // 删除一个
    geo.geo_del("cnt", "b").unwrap();
    assert_eq!(geo.geo_count("cnt").unwrap(), 2);
}

#[test]
fn geo_count_update_same_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();

    geo.create("upd_cnt").unwrap();
    geo.geo_add("upd_cnt", "x", 1.0, 1.0).unwrap();
    geo.geo_add("upd_cnt", "x", 2.0, 2.0).unwrap(); // 更新同一成员
    assert_eq!(geo.geo_count("upd_cnt").unwrap(), 1);
}

// ── M95: GEOADD NX/XX ──

#[test]
fn geo_add_nx_new_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    // 不存在 → 添加成功
    assert!(geo.geo_add_nx("places", "a", 116.0, 39.0).unwrap());
    let pos = geo.geo_pos("places", "a").unwrap().unwrap();
    assert!((pos.lng - 116.0).abs() < 0.001);
}

#[test]
fn geo_add_nx_existing_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    geo.geo_add("places", "a", 116.0, 39.0).unwrap();
    // 已存在 → 跳过，返回 false
    assert!(!geo.geo_add_nx("places", "a", 117.0, 40.0).unwrap());
    // 坐标不变
    let pos = geo.geo_pos("places", "a").unwrap().unwrap();
    assert!((pos.lng - 116.0).abs() < 0.001);
}

#[test]
fn geo_add_xx_existing_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    geo.geo_add("places", "a", 116.0, 39.0).unwrap();
    // 已存在 → 更新成功
    assert!(geo.geo_add_xx("places", "a", 117.0, 40.0).unwrap());
    let pos = geo.geo_pos("places", "a").unwrap().unwrap();
    assert!((pos.lng - 117.0).abs() < 0.001);
}

#[test]
fn geo_add_xx_nonexistent_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    // 不存在 → 跳过，返回 false
    assert!(!geo.geo_add_xx("places", "a", 116.0, 39.0).unwrap());
    assert!(geo.geo_pos("places", "a").unwrap().is_none());
}

// ── M97: GEOHASH ──

#[test]
fn geo_hash_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    geo.geo_add("places", "beijing", 116.4074, 39.9042).unwrap();
    let hash = geo.geo_hash("places", "beijing").unwrap().unwrap();
    assert_eq!(hash.len(), 11, "geohash 应为 11 字符");
    // geohash 只包含 base32 字符
    assert!(hash
        .chars()
        .all(|c| "0123456789bcdefghjkmnpqrstuvwxyz".contains(c)));
    // 注意：Talon 使用 Web Mercator 纬度范围 [-85.05, 85.05]，
    // 编码结果与标准 geohash（[-90, 90]）略有差异，但内部一致性正确。
    assert!(
        hash.starts_with("wx"),
        "北京 geohash 应以 wx 开头，实际: {}",
        hash
    );
}

#[test]
fn geo_hash_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("places").unwrap();
    assert!(geo.geo_hash("places", "nobody").unwrap().is_none());
}

#[test]
fn geo_hash_nearby_share_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("pois").unwrap();
    // 天安门和故宫相距约 1km，应共享较长 geohash 前缀
    geo.geo_add("pois", "tiananmen", 116.3912, 39.9087).unwrap();
    geo.geo_add("pois", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    let h1 = geo.geo_hash("pois", "tiananmen").unwrap().unwrap();
    let h2 = geo.geo_hash("pois", "forbidden_city").unwrap().unwrap();
    // 至少前 4 个字符相同（约 20km 精度）
    assert_eq!(
        &h1[..4],
        &h2[..4],
        "相近点应共享 geohash 前缀: {} vs {}",
        h1,
        h2
    );
}

#[test]
fn geo_add_ch_new_member() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("ch").unwrap();
    // 新成员 → 变更
    let changed = geo.geo_add_ch("ch", "beijing", 116.4074, 39.9042).unwrap();
    assert!(changed, "新成员应返回 true");
    let pos = geo.geo_pos("ch", "beijing").unwrap().unwrap();
    assert!((pos.lng - 116.4074).abs() < 0.001);
}

#[test]
fn geo_add_ch_same_coords_no_change() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("ch").unwrap();
    geo.geo_add("ch", "beijing", 116.4074, 39.9042).unwrap();
    // 相同坐标 → 无变更
    let changed = geo.geo_add_ch("ch", "beijing", 116.4074, 39.9042).unwrap();
    assert!(!changed, "坐标相同应返回 false");
}

#[test]
fn geo_add_ch_different_coords_changed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("ch").unwrap();
    geo.geo_add("ch", "beijing", 116.4074, 39.9042).unwrap();
    // 不同坐标 → 变更
    let changed = geo.geo_add_ch("ch", "beijing", 117.0, 40.0).unwrap();
    assert!(changed, "坐标变化应返回 true");
    let pos = geo.geo_pos("ch", "beijing").unwrap().unwrap();
    assert!((pos.lng - 117.0).abs() < 0.001);
}

#[test]
fn geo_add_batch_ch_mixed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("ch").unwrap();
    geo.geo_add("ch", "beijing", 116.4074, 39.9042).unwrap();
    // 批量：beijing 坐标不变(0)，shanghai 新增(1)，shenzhen 新增(1) → 2
    let count = geo
        .geo_add_batch_ch(
            "ch",
            &[
                ("beijing", 116.4074, 39.9042),
                ("shanghai", 121.4737, 31.2304),
                ("shenzhen", 114.0579, 22.5431),
            ],
        )
        .unwrap();
    assert_eq!(count, 2, "应有 2 个变更（1 个未变 + 2 个新增）");
    assert_eq!(geo.geo_count("ch").unwrap(), 3);
}

#[test]
fn geo_search_store_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("src").unwrap();
    // 北京附近三个点 + 一个远处的上海
    geo.geo_add("src", "tiananmen", 116.3912, 39.9087).unwrap();
    geo.geo_add("src", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    geo.geo_add("src", "temple_of_heaven", 116.4107, 39.8822)
        .unwrap();
    geo.geo_add("src", "shanghai", 121.4737, 31.2304).unwrap();
    // 5km 范围搜索 → 应包含 3 个北京点，不含上海
    let n = geo
        .geo_search_store(
            "src",
            "nearby",
            116.3912,
            39.9087,
            5.0,
            GeoUnit::Kilometers,
            None,
        )
        .unwrap();
    assert_eq!(n, 3);
    assert_eq!(geo.geo_count("nearby").unwrap(), 3);
    // dest 中的成员可以正常查询
    let pos = geo.geo_pos("nearby", "tiananmen").unwrap();
    assert!(pos.is_some());
    assert!(geo.geo_pos("nearby", "shanghai").unwrap().is_none());
}

#[test]
fn geo_search_store_overwrites_dest() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("src").unwrap();
    geo.geo_add("src", "tiananmen", 116.3912, 39.9087).unwrap();
    // 先存一次 → 1 个结果
    geo.geo_search_store(
        "src",
        "dest",
        116.3912,
        39.9087,
        5.0,
        GeoUnit::Kilometers,
        None,
    )
    .unwrap();
    assert_eq!(geo.geo_count("dest").unwrap(), 1);
    // 源增加近处成员后再存 → dest 应被覆盖为 2
    geo.geo_add("src", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    let n = geo
        .geo_search_store(
            "src",
            "dest",
            116.3912,
            39.9087,
            5.0,
            GeoUnit::Kilometers,
            None,
        )
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(geo.geo_count("dest").unwrap(), 2);
}

#[test]
fn geo_search_box_store_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let geo = GeoEngine::open(&store).unwrap();
    geo.create("pois").unwrap();
    // 北京区域内两个点 + 一个远处的上海
    geo.geo_add("pois", "tiananmen", 116.3912, 39.9087).unwrap();
    geo.geo_add("pois", "forbidden_city", 116.3972, 39.9169)
        .unwrap();
    geo.geo_add("pois", "shanghai", 121.4737, 31.2304).unwrap();
    // 矩形框住北京区域
    let n = geo
        .geo_search_box_store("pois", "box_result", 116.3, 39.85, 116.5, 39.95, None)
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(geo.geo_count("box_result").unwrap(), 2);
    assert!(geo.geo_pos("box_result", "shanghai").unwrap().is_none());
}
