/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 时序引擎 HTTP 路由处理函数。

use std::net::TcpStream;

use crate::error::Error;
use crate::Talon;

use super::handlers::{parse_request, write_response};
use super::protocol::Response;

/// 时序路由：create/insert/query/aggregate/set_retention/purge_expired/purge_by_tag。
pub(super) fn handle_ts(db: &Talon, body: &[u8], stream: &mut TcpStream) -> Result<(), Error> {
    let req = parse_request(body)?;
    let resp = match req.action.as_str() {
        "create" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tags: Vec<String> = req
                .params
                .get("tags")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let fields: Vec<String> = req
                .params
                .get("fields")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let schema = crate::TsSchema { tags, fields };
            match db.create_timeseries(name, schema) {
                Ok(_) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "insert" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let point: Result<crate::DataPoint, _> =
                serde_json::from_value(req.params.get("point").cloned().unwrap_or_default());
            match point {
                Ok(p) => match db.open_timeseries(name).and_then(|ts| ts.insert(&p)) {
                    Ok(()) => Response::ok_empty(),
                    Err(e) => Response::err(e.to_string()),
                },
                Err(e) => Response::err(format!("数据点解析失败: {}", e)),
            }
        }
        "query" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tag_filters: Vec<(String, String)> = req
                .params
                .get("tag_filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let time_start = req.params.get("time_start").and_then(|v| v.as_i64());
            let time_end = req.params.get("time_end").and_then(|v| v.as_i64());
            let desc = req
                .params
                .get("desc")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let limit = req
                .params
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| n as usize);
            let q = crate::TsQuery {
                tag_filters,
                time_start,
                time_end,
                desc,
                limit,
            };
            match db.open_timeseries(name).and_then(|ts| ts.query(&q)) {
                Ok(points) => Response::ok(serde_json::json!({"points": points})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "aggregate" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tag_filters: Vec<(String, String)> = req
                .params
                .get("tag_filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let time_start = req.params.get("time_start").and_then(|v| v.as_i64());
            let time_end = req.params.get("time_end").and_then(|v| v.as_i64());
            let field = req
                .params
                .get("field")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let func_str = req
                .params
                .get("func")
                .and_then(|v| v.as_str())
                .unwrap_or("count");
            let func = match func_str.to_lowercase().as_str() {
                "sum" => crate::AggFunc::Sum,
                "avg" => crate::AggFunc::Avg,
                "min" => crate::AggFunc::Min,
                "max" => crate::AggFunc::Max,
                "first" => crate::AggFunc::First,
                "last" => crate::AggFunc::Last,
                "stddev" | "std" => crate::AggFunc::Stddev,
                _ => crate::AggFunc::Count,
            };
            let interval_ms = req.params.get("interval_ms").and_then(|v| v.as_i64());
            let q = crate::TsAggQuery {
                tag_filters,
                time_start,
                time_end,
                field,
                func,
                interval_ms,
                sliding_ms: req.params.get("sliding_ms").and_then(|v| v.as_i64()),
                session_gap_ms: req.params.get("session_gap_ms").and_then(|v| v.as_i64()),
                fill: None,
            };
            match db.open_timeseries(name).and_then(|ts| ts.aggregate(&q)) {
                Ok(buckets) => {
                    let items: Vec<serde_json::Value> = buckets
                        .iter()
                        .map(|b| {
                            serde_json::json!({
                                "bucket_start": b.bucket_start,
                                "value": b.value,
                                "count": b.count,
                            })
                        })
                        .collect();
                    Response::ok(serde_json::json!({"buckets": items}))
                }
                Err(e) => Response::err(e.to_string()),
            }
        }
        "set_retention" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let retention_ms = req
                .params
                .get("retention_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            match db
                .open_timeseries(name)
                .and_then(|ts| ts.set_retention(retention_ms))
            {
                Ok(()) => Response::ok_empty(),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "purge_expired" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match db.open_timeseries(name).and_then(|ts| ts.purge_expired()) {
                Ok(count) => Response::ok(serde_json::json!({"purged": count})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        "purge_by_tag" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tag_filters: Vec<(String, String)> = req
                .params
                .get("tag_filters")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            match db
                .open_timeseries(name)
                .and_then(|ts| ts.purge_by_tag(&tag_filters))
            {
                Ok(count) => Response::ok(serde_json::json!({"purged": count})),
                Err(e) => Response::err(e.to_string()),
            }
        }
        _ => Response::err(format!("未知 TS 操作: {}", req.action)),
    };
    write_response(stream, 200, &resp)
}
