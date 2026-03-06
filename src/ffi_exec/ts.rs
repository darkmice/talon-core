/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FFI 时序引擎命令路由。

use super::{err_json, ok_json, str_p, u64_p};
use crate::Talon;

pub(super) fn exec_ts(db: &Talon, action: &str, params: &serde_json::Value) -> String {
    let name = str_p(params, "name");
    match action {
        "create" => {
            let tags: Vec<String> = params
                .get("tags")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let fields: Vec<String> = params
                .get("fields")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let schema = crate::TsSchema { tags, fields };
            let schema_data = serde_json::to_vec(&schema).unwrap_or_default();
            match db.create_timeseries(name, schema) {
                Ok(_) => {
                    let _ = db.append_oplog(crate::Operation::TsCreate {
                        series: name.to_string(),
                        schema_data,
                    });
                    ok_json(serde_json::json!({}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "insert" => {
            let point: Result<crate::DataPoint, _> =
                serde_json::from_value(params.get("point").cloned().unwrap_or_default());
            match point {
                Ok(p) => match db.open_timeseries(name).and_then(|ts| ts.insert(&p)) {
                    Ok(()) => {
                        let point_data = serde_json::to_vec(&p).unwrap_or_default();
                        let _ = db.append_oplog(crate::Operation::TsInsert {
                            series: name.to_string(),
                            point_data,
                        });
                        ok_json(serde_json::json!({}))
                    }
                    Err(e) => err_json(&e.to_string()),
                },
                Err(e) => err_json(&format!("数据点解析失败: {}", e)),
            }
        }
        "query" => {
            let q = parse_ts_query(params);
            match db.open_timeseries(name).and_then(|ts| ts.query(&q)) {
                Ok(points) => ok_json(serde_json::json!({"points": points})),
                Err(e) => err_json(&e.to_string()),
            }
        }
        "aggregate" => {
            let tag_filters = parse_tag_filters(params);
            let field = str_p(params, "field").to_string();
            let func = parse_agg_func(str_p(params, "func"));
            let q = crate::TsAggQuery {
                tag_filters,
                time_start: params.get("time_start").and_then(|v| v.as_i64()),
                time_end: params.get("time_end").and_then(|v| v.as_i64()),
                field,
                func,
                interval_ms: params.get("interval_ms").and_then(|v| v.as_i64()),
                sliding_ms: params.get("sliding_ms").and_then(|v| v.as_i64()),
                session_gap_ms: params.get("session_gap_ms").and_then(|v| v.as_i64()),
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
                    ok_json(serde_json::json!({"buckets": items}))
                }
                Err(e) => err_json(&e.to_string()),
            }
        }
        "set_retention" => {
            let ms = u64_p(params, "retention_ms");
            match db.open_timeseries(name).and_then(|ts| ts.set_retention(ms)) {
                Ok(()) => ok_json(serde_json::json!({})),
                Err(e) => err_json(&e.to_string()),
            }
        }
        "purge_expired" => match db.open_timeseries(name).and_then(|ts| ts.purge_expired()) {
            Ok(c) => ok_json(serde_json::json!({"purged": c})),
            Err(e) => err_json(&e.to_string()),
        },
        "purge_by_tag" => {
            let filters = parse_tag_filters(params);
            match db
                .open_timeseries(name)
                .and_then(|ts| ts.purge_by_tag(&filters))
            {
                Ok(c) => ok_json(serde_json::json!({"purged": c})),
                Err(e) => err_json(&e.to_string()),
            }
        }
        _ => err_json(&format!("未知 TS 操作: {}", action)),
    }
}

fn parse_tag_filters(params: &serde_json::Value) -> Vec<(String, String)> {
    params
        .get("tag_filters")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default()
}

fn parse_ts_query(params: &serde_json::Value) -> crate::TsQuery {
    crate::TsQuery {
        tag_filters: parse_tag_filters(params),
        time_start: params.get("time_start").and_then(|v| v.as_i64()),
        time_end: params.get("time_end").and_then(|v| v.as_i64()),
        desc: params
            .get("desc")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        limit: params
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize),
    }
}

fn parse_agg_func(s: &str) -> crate::AggFunc {
    match s.to_lowercase().as_str() {
        "sum" => crate::AggFunc::Sum,
        "avg" => crate::AggFunc::Avg,
        "min" => crate::AggFunc::Min,
        "max" => crate::AggFunc::Max,
        "first" => crate::AggFunc::First,
        "last" => crate::AggFunc::Last,
        "stddev" | "std" => crate::AggFunc::Stddev,
        _ => crate::AggFunc::Count,
    }
}
