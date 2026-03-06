/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 通用数据导入器：CSV → SQL 表，JSONL → FTS 全文索引。
//!
//! CSV 导入：自动推断 schema（首行为列名），批量 `batch_insert_rows`。
//! JSONL 导入：每行一个 JSON 对象，`doc_id` 字段必须存在，其余字段索引。

use crate::error::Error;
use crate::fts::{FtsConfig, FtsDoc, FtsEngine};
use crate::types::{ColumnType, Value};
use crate::Talon;
use std::collections::BTreeMap;
use std::io::BufRead;

/// CSV 导入批量大小。
const CSV_BATCH_SIZE: usize = 1000;

/// CSV 导入结果统计。
#[derive(Debug, Default)]
pub struct CsvImportStats {
    /// 成功插入的行数。
    pub rows_inserted: u64,
    /// 跳过的行数（解析失败）。
    pub rows_skipped: u64,
    /// 列名列表。
    pub columns: Vec<String>,
}

/// JSONL 导入结果统计。
#[derive(Debug, Default)]
pub struct JsonlImportStats {
    /// 成功索引的文档数。
    pub docs_indexed: u64,
    /// 跳过的行数（解析失败或缺少 doc_id）。
    pub rows_skipped: u64,
}

/// CSV 导入到 SQL 表。
///
/// 首行为列名（逗号分隔），自动推断列类型，创建表并批量导入。
/// 如果表已存在，直接插入（列名必须匹配）。
///
/// 类型推断规则：尝试 Integer → Float → 默认 Text。
pub fn import_csv(
    db: &Talon,
    table: &str,
    reader: impl BufRead,
    create_table: bool,
) -> Result<CsvImportStats, Error> {
    let mut stats = CsvImportStats::default();
    let mut lines = reader.lines();

    // 读取首行作为列名
    let header_line = match lines.next() {
        Some(Ok(line)) => line,
        Some(Err(e)) => return Err(Error::Io(e)),
        None => return Ok(stats),
    };
    // 跳过 BOM
    let header_line = header_line.strip_prefix('\u{feff}').unwrap_or(&header_line);
    let columns: Vec<String> = header_line
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    if columns.is_empty() {
        return Ok(stats);
    }
    stats.columns = columns.clone();

    // 采样前 100 行用于类型推断（保留原始字段，后续直接插入）
    let mut sample_fields: Vec<Vec<String>> = Vec::new();
    let mut remaining_lines = Vec::new(); // 仅在采样阶段暂存
    let mut sampling = true;
    for line_result in &mut lines {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if sampling && sample_fields.len() < 100 {
            sample_fields.push(parse_csv_line(trimmed));
            if sample_fields.len() >= 100 {
                sampling = false;
            }
        } else {
            // 采样结束后不再存储，直接中断采样循环
            remaining_lines.push(line);
            break;
        }
    }

    // 类型推断
    let col_types = infer_column_types(&columns, &sample_fields);

    // 创建表（如果需要）— 列名加反引号防止特殊字符
    if create_table {
        let col_defs: Vec<String> = columns
            .iter()
            .zip(col_types.iter())
            .enumerate()
            .map(|(i, (name, ct))| {
                let type_str = match ct {
                    ColumnType::Integer => "INTEGER",
                    ColumnType::Float => "FLOAT",
                    _ => "TEXT",
                };
                if i == 0 {
                    format!("`{}` {} PRIMARY KEY", name, type_str)
                } else {
                    format!("`{}` {}", name, type_str)
                }
            })
            .collect();
        let create_sql = format!("CREATE TABLE `{}` ({})", table, col_defs.join(", "));
        db.run_sql(&create_sql)?;
    }

    // 批量插入辅助闭包
    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(CSV_BATCH_SIZE);

    let flush_batch =
        |batch: &mut Vec<Vec<Value>>, stats: &mut CsvImportStats| -> Result<(), Error> {
            if !batch.is_empty() {
                let n = batch.len() as u64;
                db.batch_insert_rows(table, &col_refs, std::mem::take(batch))?;
                stats.rows_inserted += n;
                *batch = Vec::with_capacity(CSV_BATCH_SIZE);
            }
            Ok(())
        };

    // 插入采样行
    for fields in &sample_fields {
        match convert_row(fields, &col_types) {
            Some(row) => batch.push(row),
            None => stats.rows_skipped += 1,
        }
        if batch.len() >= CSV_BATCH_SIZE {
            flush_batch(&mut batch, &mut stats)?;
        }
    }
    drop(sample_fields); // 释放采样数据

    // 流式处理剩余行（先处理 break 时暂存的那一行）
    for line in remaining_lines {
        let fields = parse_csv_line(line.trim());
        match convert_row(&fields, &col_types) {
            Some(row) => batch.push(row),
            None => stats.rows_skipped += 1,
        }
        if batch.len() >= CSV_BATCH_SIZE {
            flush_batch(&mut batch, &mut stats)?;
        }
    }
    // 继续从 iterator 流式读取
    for line_result in lines {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let fields = parse_csv_line(trimmed);
        match convert_row(&fields, &col_types) {
            Some(row) => batch.push(row),
            None => stats.rows_skipped += 1,
        }
        if batch.len() >= CSV_BATCH_SIZE {
            flush_batch(&mut batch, &mut stats)?;
        }
    }

    // 刷出剩余
    flush_batch(&mut batch, &mut stats)?;

    Ok(stats)
}

/// JSONL 导入到 FTS 全文索引。
///
/// 每行一个 JSON 对象。必须包含 `doc_id` 字段（或 `id`/`_id`）。
/// 其余字符串字段自动索引。
pub fn import_jsonl(
    db: &Talon,
    index_name: &str,
    reader: impl BufRead,
    create_index: bool,
) -> Result<JsonlImportStats, Error> {
    let mut stats = JsonlImportStats::default();

    let store = db.store_ref();
    let fts = FtsEngine::open(store)?;

    if create_index {
        let config = FtsConfig {
            analyzer: crate::fts::Analyzer::Standard,
        };
        // 忽略"已存在"错误
        let _ = fts.create_index(index_name, &config);
    }

    let mut batch: Vec<FtsDoc> = Vec::with_capacity(100);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => {
                stats.rows_skipped += 1;
                continue;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // 解析 JSON
        let obj: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                stats.rows_skipped += 1;
                continue;
            }
        };

        let map = match obj.as_object() {
            Some(m) => m,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };

        // 提取 doc_id
        let doc_id = map
            .get("doc_id")
            .or_else(|| map.get("id"))
            .or_else(|| map.get("_id"))
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            });

        let doc_id = match doc_id {
            Some(id) => id,
            None => {
                stats.rows_skipped += 1;
                continue;
            }
        };

        // 收集字符串字段
        let mut fields = BTreeMap::new();
        for (key, val) in map {
            if key == "doc_id" || key == "id" || key == "_id" {
                continue;
            }
            if let serde_json::Value::String(s) = val {
                fields.insert(key.clone(), s.clone());
            }
        }

        batch.push(FtsDoc { doc_id, fields });

        if batch.len() >= 100 {
            let n = batch.len() as u64;
            fts.index_doc_batch(index_name, &batch)?;
            stats.docs_indexed += n;
            batch.clear();
        }
    }

    // 刷出剩余
    if !batch.is_empty() {
        let n = batch.len() as u64;
        fts.index_doc_batch(index_name, &batch)?;
        stats.docs_indexed += n;
    }

    Ok(stats)
}

/// 简易 CSV 行解析（支持双引号字段，正确处理 UTF-8 多字节字符）。
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next(); // 消耗转义的第二个引号
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == ',' {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// 从样本数据推断列类型。
fn infer_column_types(columns: &[String], samples: &[Vec<String>]) -> Vec<ColumnType> {
    let mut types = vec![ColumnType::Integer; columns.len()];

    for row in samples {
        for (i, val) in row.iter().enumerate() {
            if i >= types.len() {
                break;
            }
            if val.is_empty() {
                continue;
            }
            match types[i] {
                ColumnType::Integer => {
                    if val.parse::<i64>().is_err() {
                        if val.parse::<f64>().is_ok() {
                            types[i] = ColumnType::Float;
                        } else {
                            types[i] = ColumnType::Text;
                        }
                    }
                }
                ColumnType::Float => {
                    if val.parse::<f64>().is_err() {
                        types[i] = ColumnType::Text;
                    }
                }
                _ => {}
            }
        }
    }
    types
}

/// 将字符串字段列表转为 Value 行。
fn convert_row(fields: &[String], col_types: &[ColumnType]) -> Option<Vec<Value>> {
    let mut row = Vec::with_capacity(col_types.len());
    for (i, ct) in col_types.iter().enumerate() {
        let val = fields.get(i).map(|s| s.as_str()).unwrap_or("");
        if val.is_empty() {
            row.push(Value::Null);
            continue;
        }
        let v = match ct {
            ColumnType::Integer => val
                .parse::<i64>()
                .ok()
                .map(Value::Integer)
                .unwrap_or(Value::Null),
            ColumnType::Float => val
                .parse::<f64>()
                .ok()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            _ => Value::Text(val.to_string()),
        };
        row.push(v);
    }
    Some(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn csv_import_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let csv = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,28\n";
        let reader = BufReader::new(csv.as_bytes());
        let stats = import_csv(&db, "users", reader, true).unwrap();
        assert_eq!(stats.rows_inserted, 3);
        assert_eq!(stats.columns.len(), 3);

        let rows = db.run_sql("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn csv_import_type_inference() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let csv = "id,score,label\n1,3.15,hello\n2,2.71,world\n";
        let reader = BufReader::new(csv.as_bytes());
        let stats = import_csv(&db, "data", reader, true).unwrap();
        assert_eq!(stats.rows_inserted, 2);

        let rows = db.run_sql("SELECT score FROM data WHERE id = 1").unwrap();
        assert_eq!(rows[0][0], Value::Float(3.15));
    }

    #[test]
    fn csv_import_quoted_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let csv = "id,name,bio\n1,\"Alice\",\"Hello, World\"\n2,\"Bob\",\"He said \"\"hi\"\"\"\n";
        let reader = BufReader::new(csv.as_bytes());
        let stats = import_csv(&db, "people", reader, true).unwrap();
        assert_eq!(stats.rows_inserted, 2);

        let rows = db.run_sql("SELECT bio FROM people WHERE id = 1").unwrap();
        assert_eq!(rows[0][0], Value::Text("Hello, World".into()));
    }

    #[test]
    fn jsonl_import_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let jsonl = r#"{"doc_id":"1","title":"Hello","content":"World"}
{"doc_id":"2","title":"Foo","content":"Bar"}
"#;
        let reader = BufReader::new(jsonl.as_bytes());
        let stats = import_jsonl(&db, "docs", reader, true).unwrap();
        assert_eq!(stats.docs_indexed, 2);
        assert_eq!(stats.rows_skipped, 0);
    }

    #[test]
    fn jsonl_import_missing_id_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        let jsonl = r#"{"doc_id":"1","text":"ok"}
{"text":"no id"}
{"id":"3","text":"alt id"}
"#;
        let reader = BufReader::new(jsonl.as_bytes());
        let stats = import_jsonl(&db, "idx", reader, true).unwrap();
        assert_eq!(stats.docs_indexed, 2);
        assert_eq!(stats.rows_skipped, 1);
    }
}
