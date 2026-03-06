/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M94 GeoPoint 集成测试：CREATE TABLE + INSERT + SELECT + ST_WITHIN + ST_DISTANCE

use talon::{Talon, Value};

fn db() -> (tempfile::TempDir, Talon) {
    let dir = tempfile::tempdir().unwrap();
    let db = Talon::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn geopoint_create_insert_select() {
    let (_dir, db) = db();
    db.run_sql("CREATE TABLE places (id INTEGER, name TEXT, loc GEOPOINT)")
        .unwrap();
    db.run_sql("INSERT INTO places VALUES (1, 'Beijing', GEOPOINT(39.9042, 116.4074))")
        .unwrap();
    db.run_sql("INSERT INTO places VALUES (2, 'Shanghai', GEOPOINT(31.2397, 121.4998))")
        .unwrap();
    db.run_sql("INSERT INTO places VALUES (3, 'Guangzhou', GEOPOINT(23.1291, 113.2644))")
        .unwrap();

    let rows = db.run_sql("SELECT * FROM places").unwrap();
    assert_eq!(rows.len(), 3);
    // 找到 Beijing 行（不假设行顺序）
    let beijing = rows
        .iter()
        .find(|r| r[1] == Value::Text("Beijing".into()))
        .unwrap();
    match &beijing[2] {
        Value::GeoPoint(lat, lng) => {
            assert!((lat - 39.9042).abs() < 0.001, "lat mismatch: {}", lat);
            assert!((lng - 116.4074).abs() < 0.001, "lng mismatch: {}", lng);
        }
        other => panic!("Expected GeoPoint, got: {:?}", other),
    }
}

#[test]
fn geopoint_st_within() {
    let (_dir, db) = db();
    db.run_sql("CREATE TABLE devices (id INTEGER, name TEXT, loc GEOPOINT)")
        .unwrap();
    // 天安门 39.9042, 116.4074
    db.run_sql("INSERT INTO devices VALUES (1, 'sensor_a', GEOPOINT(39.9042, 116.4074))")
        .unwrap();
    // 故宫 39.9163, 116.3972 (~1.5km from 天安门)
    db.run_sql("INSERT INTO devices VALUES (2, 'sensor_b', GEOPOINT(39.9163, 116.3972))")
        .unwrap();
    // 上海 31.2397, 121.4998 (~1064km from 天安门)
    db.run_sql("INSERT INTO devices VALUES (3, 'sensor_c', GEOPOINT(31.2397, 121.4998))")
        .unwrap();

    // 5km 范围内 → 应返回 sensor_a 和 sensor_b
    let rows = db
        .run_sql("SELECT name FROM devices WHERE ST_WITHIN(loc, 39.9042, 116.4074, 5000)")
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "Expected 2 devices within 5km, got: {:?}",
        rows
    );

    // 1km 范围内 → 只有 sensor_a
    let rows = db
        .run_sql("SELECT name FROM devices WHERE ST_WITHIN(loc, 39.9042, 116.4074, 1000)")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("sensor_a".into()));

    // 2000km 范围内 → 所有 3 个
    let rows = db
        .run_sql("SELECT name FROM devices WHERE ST_WITHIN(loc, 39.9042, 116.4074, 2000000)")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn geopoint_st_distance() {
    let (_dir, db) = db();
    db.run_sql("CREATE TABLE cities (id INTEGER, name TEXT, loc GEOPOINT)")
        .unwrap();
    db.run_sql("INSERT INTO cities VALUES (1, 'Beijing', GEOPOINT(39.9042, 116.4074))")
        .unwrap();
    db.run_sql("INSERT INTO cities VALUES (2, 'Shanghai', GEOPOINT(31.2397, 121.4998))")
        .unwrap();
    db.run_sql("INSERT INTO cities VALUES (3, 'Guangzhou', GEOPOINT(23.1291, 113.2644))")
        .unwrap();

    // ST_DISTANCE 追加距离列
    let rows = db.run_sql(
        "SELECT name, ST_DISTANCE(loc, GEOPOINT(39.9042, 116.4074)) AS dist FROM cities ORDER BY dist LIMIT 2"
    ).unwrap();
    println!("ST_DISTANCE rows: {:?}", rows);
    assert_eq!(rows.len(), 2);
    // 第一行应该是 Beijing（距离 ≈ 0）
    assert_eq!(rows[0][0], Value::Text("Beijing".into()));
    if let Value::Float(d) = &rows[0][1] {
        assert!(*d < 100.0, "Beijing distance should be ~0, got: {}", d);
    }
    // 第二行应该是 Shanghai
    assert_eq!(rows[1][0], Value::Text("Shanghai".into()));
    if let Value::Float(d) = &rows[1][1] {
        assert!(
            *d > 1_000_000.0 && *d < 1_200_000.0,
            "Shanghai distance: {}",
            d
        );
    }
}

#[test]
fn geopoint_st_within_and_distance_combined() {
    let (_dir, db) = db();
    db.run_sql("CREATE TABLE nodes (id INTEGER, name TEXT, pos GEOPOINT)")
        .unwrap();
    for i in 0..20 {
        let lat = 39.9 + (i as f64) * 0.01;
        let lng = 116.4 + (i as f64) * 0.005;
        db.run_sql(&format!(
            "INSERT INTO nodes VALUES ({}, 'node_{}', GEOPOINT({}, {}))",
            i, i, lat, lng
        ))
        .unwrap();
    }

    // ST_WITHIN + ST_DISTANCE ORDER BY dist LIMIT 5
    let rows = db
        .run_sql(
            "SELECT name, ST_DISTANCE(pos, GEOPOINT(39.9, 116.4)) AS dist FROM nodes \
         WHERE ST_WITHIN(pos, 39.9, 116.4, 50000) ORDER BY dist LIMIT 5",
        )
        .unwrap();
    assert!(rows.len() <= 5);
    assert_eq!(rows[0][0], Value::Text("node_0".into()));
    // 验证距离递增
    for i in 1..rows.len() {
        let d_prev = match &rows[i - 1][1] {
            Value::Float(f) => *f,
            _ => panic!(),
        };
        let d_curr = match &rows[i][1] {
            Value::Float(f) => *f,
            _ => panic!(),
        };
        assert!(d_curr >= d_prev, "Distances should be increasing");
    }
}

#[test]
fn geopoint_validate_out_of_range() {
    let (_dir, db) = db();
    db.run_sql("CREATE TABLE t (id INTEGER, loc GEOPOINT)")
        .unwrap();
    // lat 超范围
    let err = db.run_sql("INSERT INTO t VALUES (1, GEOPOINT(91.0, 0.0))");
    assert!(err.is_err(), "lat=91 should fail validation");
    // lng 超范围
    let err = db.run_sql("INSERT INTO t VALUES (1, GEOPOINT(0.0, 181.0))");
    assert!(err.is_err(), "lng=181 should fail validation");
}
