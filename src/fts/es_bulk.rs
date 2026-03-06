/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Elasticsearch `_bulk` API NDJSON 解析器。
//!
//! 格式（每两行一对）：
//! ```json
//! {"index":{"_index":"docs","_id":"1"}}
//! {"title":"hello","body":"world"}
//! ```
//!
//! 支持的 action：`index`（索引/覆盖）。
//! `_id` 缺失时自动生成 UUID。
//! 非字符串字段值自动转为字符串。

use std::collections::BTreeMap;

use crate::error::Error;
use crate::fts::FtsDoc;

/// ES Bulk 解析结果：索引名 + 文档。
#[derive(Debug, Clone)]
pub struct EsBulkItem {
    /// 目标索引名。
    pub index: String,
    /// 解析后的 FTS 文档。
    pub doc: FtsDoc,
}

/// 解析 ES `_bulk` NDJSON 格式。
/// 返回 `(index_name, FtsDoc)` 列表。
/// 空行自动跳过，不支持的 action（delete/update）跳过并忽略。
pub fn parse_es_bulk(input: &str) -> Result<Vec<EsBulkItem>, Error> {
    let lines: Vec<&str> = input.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut results = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let action_line = lines[i].trim();
        // 解析 action 行
        let action_obj: serde_json::Value = serde_json::from_str(action_line)
            .map_err(|e| bulk_err(&format!("invalid action JSON at line {}: {}", i + 1, e)))?;

        // 提取 action 类型和元数据
        let (action_type, meta) = parse_action(&action_obj)?;

        match action_type.as_str() {
            "index" | "create" => {
                // 下一行是文档
                i += 1;
                if i >= lines.len() {
                    return Err(bulk_err("missing document body after index action"));
                }
                let doc_line = lines[i].trim();
                let doc_obj: serde_json::Value = serde_json::from_str(doc_line)
                    .map_err(|e| bulk_err(&format!("invalid doc JSON at line {}: {}", i + 1, e)))?;

                let index_name = meta.index.unwrap_or_else(|| "default".to_string());
                let doc_id = meta.id.unwrap_or_else(generate_id);
                let fields = json_to_fields(&doc_obj);

                results.push(EsBulkItem {
                    index: index_name,
                    doc: FtsDoc { doc_id, fields },
                });
            }
            "delete" => {
                // delete 没有文档行，跳过
            }
            "update" => {
                // update 有文档行，跳过
                i += 1;
            }
            _ => {
                // 未知 action，尝试跳过文档行
                i += 1;
            }
        }
        i += 1;
    }

    Ok(results)
}

/// Action 元数据。
struct ActionMeta {
    index: Option<String>,
    id: Option<String>,
}

/// 解析 action 行，提取 action 类型和元数据。
fn parse_action(obj: &serde_json::Value) -> Result<(String, ActionMeta), Error> {
    let map = obj
        .as_object()
        .ok_or_else(|| bulk_err("action must be a JSON object"))?;

    // action 对象只有一个顶级 key：index/create/delete/update
    let (action_type, meta_val) = map
        .iter()
        .next()
        .ok_or_else(|| bulk_err("empty action object"))?;

    let index = meta_val
        .get("_index")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let id = meta_val.get("_id").and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    });

    Ok((action_type.clone(), ActionMeta { index, id }))
}

/// 将 JSON 对象的所有字段转为 BTreeMap<String, String>。
/// 非字符串值自动 to_string。嵌套对象序列化为 JSON 字符串。
fn json_to_fields(obj: &serde_json::Value) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    if let Some(map) = obj.as_object() {
        for (k, v) in map {
            let val = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                _ => v.to_string(),
            };
            fields.insert(k.clone(), val);
        }
    }
    fields
}

/// 简易 ID 生成（基于时间戳+计数器）。
fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}", ts, seq)
}

/// 构造 ES Bulk 解析错误。
fn bulk_err(msg: &str) -> Error {
    Error::Serialization(format!("es bulk: {}", msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_index() {
        let input = r#"{"index":{"_index":"docs","_id":"1"}}
{"title":"hello","body":"world"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].index, "docs");
        assert_eq!(items[0].doc.doc_id, "1");
        assert_eq!(items[0].doc.fields["title"], "hello");
        assert_eq!(items[0].doc.fields["body"], "world");
    }

    #[test]
    fn parse_multiple_docs() {
        let input = r#"{"index":{"_index":"posts","_id":"a"}}
{"title":"first post"}
{"index":{"_index":"posts","_id":"b"}}
{"title":"second post"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].doc.doc_id, "a");
        assert_eq!(items[1].doc.doc_id, "b");
    }

    #[test]
    fn parse_auto_id() {
        let input = r#"{"index":{"_index":"logs"}}
{"msg":"no id given"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
        assert!(!items[0].doc.doc_id.is_empty());
    }

    #[test]
    fn parse_numeric_id() {
        let input = r#"{"index":{"_index":"docs","_id":42}}
{"text":"numeric id"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items[0].doc.doc_id, "42");
    }

    #[test]
    fn parse_non_string_fields() {
        let input = r#"{"index":{"_index":"m","_id":"1"}}
{"name":"test","count":42,"active":true,"score":3.14}
"#;
        let items = parse_es_bulk(input).unwrap();
        let f = &items[0].doc.fields;
        assert_eq!(f["name"], "test");
        assert_eq!(f["count"], "42");
        assert_eq!(f["active"], "true");
        assert_eq!(f["score"], "3.14");
    }

    #[test]
    fn parse_create_action() {
        let input = r#"{"create":{"_index":"docs","_id":"c1"}}
{"text":"created"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].doc.doc_id, "c1");
    }

    #[test]
    fn parse_skip_delete() {
        let input = r#"{"delete":{"_index":"docs","_id":"1"}}
{"index":{"_index":"docs","_id":"2"}}
{"text":"after delete"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].doc.doc_id, "2");
    }

    #[test]
    fn parse_skip_update() {
        let input = r#"{"update":{"_index":"docs","_id":"1"}}
{"doc":{"text":"updated"}}
{"index":{"_index":"docs","_id":"3"}}
{"text":"after update"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].doc.doc_id, "3");
    }

    #[test]
    fn parse_empty_lines() {
        let input = "\n\n{\"index\":{\"_index\":\"a\",\"_id\":\"1\"}}\n{\"x\":\"y\"}\n\n";
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn parse_default_index() {
        let input = r#"{"index":{"_id":"1"}}
{"text":"no index name"}
"#;
        let items = parse_es_bulk(input).unwrap();
        assert_eq!(items[0].index, "default");
    }

    #[test]
    fn parse_error_invalid_json() {
        let result = parse_es_bulk("not json\n{\"x\":\"y\"}");
        assert!(result.is_err());
    }
}
