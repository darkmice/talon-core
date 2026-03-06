/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! GEO HTTP 路由处理函数：geo_add / geo_pos / geo_del / geo_dist / geo_search / geo_members。

use std::net::TcpStream;

use crate::error::Error;
use crate::Talon;

use super::handlers::{parse_request, write_response};
use super::protocol::Response;

/// GEO 路由：create / add / pos / del / dist / search / members。
pub(super) fn handle_geo(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let name = req
        .params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let resp = match req.action.as_str() {
        "create" => match db.geo()?.create(name) {
            Ok(()) => Response::ok_empty(),
            Err(e) => Response::err(e.to_string()),
        },
        "add" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            let lng = req
                .params
                .get("lng")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let lat = req
                .params
                .get("lat")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            match db.geo()?.geo_add(name, key, lng, lat) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "add_batch" => {
            let members_raw = req
                .params
                .get("members")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let mut members: Vec<(&str, f64, f64)> = Vec::new();
            let member_strs: Vec<String> = members_raw
                .iter()
                .filter_map(|m| m.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();
            for (i, m) in members_raw.iter().enumerate() {
                let lng = m.get("lng").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let lat = m.get("lat").and_then(|v| v.as_f64()).unwrap_or(0.0);
                if i < member_strs.len() {
                    members.push((&member_strs[i], lng, lat));
                }
            }
            match db.geo()?.geo_add_batch(name, &members) {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "pos" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            match db.geo_read()?.geo_pos(name, key) {
                Ok(Some(p)) => Response::ok(serde_json::json!({"lng": p.lng, "lat": p.lat})),
                Ok(None) => Response::ok(serde_json::json!(null)),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "del" => {
            let key = req.params.get("key").and_then(|v| v.as_str()).unwrap_or("");
            match db.geo()?.geo_del(name, key) {
                Ok(deleted) => Response::ok(serde_json::json!({"deleted": deleted})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "dist" => {
            let key1 = req
                .params
                .get("key1")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let key2 = req
                .params
                .get("key2")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let unit = parse_unit(&req.params);
            match db.geo_read()?.geo_dist(name, key1, key2, unit) {
                Ok(Some(d)) => Response::ok(serde_json::json!({"dist": d})),
                Ok(None) => Response::ok(serde_json::json!(null)),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "search" => {
            let lng = req
                .params
                .get("lng")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let lat = req
                .params
                .get("lat")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let radius = req
                .params
                .get("radius")
                .and_then(|v| v.as_f64())
                .unwrap_or(1000.0);
            let unit = parse_unit(&req.params);
            let count = req
                .params
                .get("count")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            match db
                .geo_read()?
                .geo_search(name, lng, lat, radius, unit, count)
            {
                Ok(members) => {
                    let results: Vec<serde_json::Value> = members
                        .iter()
                        .map(|m| {
                            serde_json::json!({
                                "key": m.key,
                                "lng": m.point.lng,
                                "lat": m.point.lat,
                                "dist": m.dist,
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"members": results}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "members" => match db.geo_read()?.geo_members(name) {
            Ok(members) => Response::ok(serde_json::json!({"members": members})),
            Err(e) => Response::err(e.to_string()),
        },
        _ => Response::err(format!("未知 GEO 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}

/// 解析距离单位参数。
fn parse_unit(params: &serde_json::Value) -> crate::GeoUnit {
    match params.get("unit").and_then(|v| v.as_str()).unwrap_or("m") {
        "km" | "kilometers" => crate::GeoUnit::Kilometers,
        "mi" | "miles" => crate::GeoUnit::Miles,
        _ => crate::GeoUnit::Meters,
    }
}
