/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO FFI 执行函数：create / add / pos / del / dist / search / search_box / fence / members。
//!
//! 从 ffi_exec/mod.rs 拆分，保持单文件行数限制。

use crate::geo::GeoUnit;
use crate::Talon;

use super::{err_json, ok_json, str_p};

/// GEO 模块入口。
pub(crate) fn exec_geo(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let name = str_p(params, "name");
    match action {
        "create" => match db.geo() {
            Ok(g) => match g.create(name) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        "add" => {
            let key = str_p(params, "key");
            let lng = f64_p(params, "lng");
            let lat = f64_p(params, "lat");
            match db.geo() {
                Ok(g) => match g.geo_add(name, key, lng, lat) {
                    Ok(()) => ok_json(serde_json::json!({})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "add_batch" => {
            let members_raw = params
                .get("members")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let parsed: Vec<(String, f64, f64)> = members_raw
                .iter()
                .filter_map(|m| {
                    let k = m.get("key").and_then(|v| v.as_str())?.to_string();
                    let lng = m.get("lng").and_then(|v| v.as_f64())?;
                    let lat = m.get("lat").and_then(|v| v.as_f64())?;
                    Some((k, lng, lat))
                })
                .collect();
            let refs: Vec<(&str, f64, f64)> = parsed
                .iter()
                .map(|(k, lng, lat)| (k.as_str(), *lng, *lat))
                .collect();
            match db.geo() {
                Ok(g) => match g.geo_add_batch(name, &refs) {
                    Ok(()) => ok_json(serde_json::json!({"count": refs.len()})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "pos" => {
            let key = str_p(params, "key");
            match db.geo_read() {
                Ok(g) => match g.geo_pos(name, key) {
                    Ok(Some(p)) => ok_json(serde_json::json!({"lng": p.lng, "lat": p.lat})),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "del" => {
            let key = str_p(params, "key");
            match db.geo() {
                Ok(g) => match g.geo_del(name, key) {
                    Ok(deleted) => ok_json(serde_json::json!({"deleted": deleted})),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "dist" => {
            let key1 = str_p(params, "key1");
            let key2 = str_p(params, "key2");
            let unit = parse_unit(params);
            match db.geo_read() {
                Ok(g) => match g.geo_dist(name, key1, key2, unit) {
                    Ok(Some(d)) => ok_json(serde_json::json!({"dist": d})),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "search" => {
            let lng = f64_p(params, "lng");
            let lat = f64_p(params, "lat");
            let radius = f64_p(params, "radius");
            let unit = parse_unit(params);
            let count = params
                .get("count")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            match db.geo_read() {
                Ok(g) => match g.geo_search(name, lng, lat, radius, unit, count) {
                    Ok(members) => {
                        let arr: Vec<serde_json::Value> = members.iter().map(|m| serde_json::json!({
                            "key": m.key, "lng": m.point.lng, "lat": m.point.lat, "dist": m.dist
                        })).collect();
                        ok_json(serde_json::json!({"members": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "search_box" => {
            let min_lng = f64_p(params, "min_lng");
            let min_lat = f64_p(params, "min_lat");
            let max_lng = f64_p(params, "max_lng");
            let max_lat = f64_p(params, "max_lat");
            let count = params
                .get("count")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            match db.geo_read() {
                Ok(g) => match g.geo_search_box(name, min_lng, min_lat, max_lng, max_lat, count) {
                    Ok(members) => {
                        let arr: Vec<serde_json::Value> = members
                            .iter()
                            .map(|m| {
                                serde_json::json!({
                                    "key": m.key, "lng": m.point.lng, "lat": m.point.lat
                                })
                            })
                            .collect();
                        ok_json(serde_json::json!({"members": arr}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "fence" => {
            let key = str_p(params, "key");
            let lng = f64_p(params, "center_lng");
            let lat = f64_p(params, "center_lat");
            let radius = f64_p(params, "radius");
            let unit = parse_unit(params);
            match db.geo_read() {
                Ok(g) => match g.geo_fence(name, key, lng, lat, radius, unit) {
                    Ok(Some(inside)) => ok_json(serde_json::json!({"inside": inside})),
                    Ok(None) => ok_json(serde_json::json!(null)),
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&e.to_string()),
            }
        }
        "members" => match db.geo_read() {
            Ok(g) => match g.geo_members(name) {
                Ok(members) => ok_json(serde_json::json!({"members": members})),
                Err(e) => err_json(&e.to_string()),
            },
            Err(e) => err_json(&e.to_string()),
        },
        _ => err_json(&format!("未知 geo action: {}", action)),
    }
}

fn f64_p(params: &serde_json::Value, key: &str) -> f64 {
    params.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

fn parse_unit(params: &serde_json::Value) -> GeoUnit {
    match params.get("unit").and_then(|v| v.as_str()).unwrap_or("m") {
        "km" | "kilometers" => GeoUnit::Kilometers,
        "mi" | "miles" => GeoUnit::Miles,
        _ => GeoUnit::Meters,
    }
}
