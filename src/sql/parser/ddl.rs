/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! DDL 解析：ALTER TABLE / CREATE VECTOR INDEX / TRUNCATE / DROP INDEX / ON CONFLICT。
//! 从 parser/mod.rs 拆分，保持单文件 ≤500 行。

use super::types::*;
use super::utils::*;
use crate::types::Value;

/// 解析 TRUNCATE [TABLE] name。
pub(super) fn parse_truncate(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[8..].trim_start(); // skip "TRUNCATE"
    let rest = if rest.to_uppercase().starts_with("TABLE") {
        rest[5..].trim_start()
    } else {
        rest
    };
    let table = rest
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("");
    if table.is_empty() {
        return Err(crate::Error::SqlParse("TRUNCATE missing table name".into()));
    }
    Ok(Stmt::Truncate {
        table: unquote_ident(table),
    })
}

/// 解析 DROP INDEX [IF EXISTS] idx_name。
pub(super) fn parse_drop_index(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[10..].trim_start(); // skip "DROP INDEX"
    let upper = rest.to_uppercase();
    let (if_exists, name_part) = if upper.starts_with("IF EXISTS") {
        (true, rest[9..].trim_start())
    } else {
        (false, rest)
    };
    let index_name = name_part
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("");
    if index_name.is_empty() {
        return Err(crate::Error::SqlParse(
            "DROP INDEX missing index name".into(),
        ));
    }
    Ok(Stmt::DropIndex {
        index_name: unquote_ident(index_name),
        if_exists,
    })
}

pub(super) fn parse_alter_table(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[11..].trim_start();
    let (table, remainder) = extract_table_name(rest);
    let table = unquote_ident(&table);
    let remainder = remainder.trim();
    let rem_upper = remainder.to_uppercase();
    if rem_upper.starts_with("ADD") {
        let after_add = remainder[3..].trim_start();
        let after_add_upper = after_add.to_uppercase();
        // M166：ADD CONSTRAINT name UNIQUE(col1, col2)
        if after_add_upper.starts_with("CONSTRAINT") {
            let rest = after_add[10..].trim_start();
            let tokens: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
            if tokens.len() < 2 {
                return Err(crate::Error::SqlParse(
                    "ALTER TABLE ADD CONSTRAINT 缺少约束名或类型".into(),
                ));
            }
            let constraint_name = unquote_ident(tokens[0]);
            let type_rest = tokens[1].trim();
            let type_upper = type_rest.to_uppercase();
            if type_upper.starts_with("UNIQUE") {
                let paren = type_rest[6..].trim_start();
                if !paren.starts_with('(') || !paren.contains(')') {
                    return Err(crate::Error::SqlParse(
                        "UNIQUE 约束需要 (col1, col2, ...) 格式".into(),
                    ));
                }
                let inner = &paren[1..paren.find(')').unwrap()];
                let cols: Vec<String> = inner.split(',').map(|s| unquote_ident(s.trim())).collect();
                if cols.is_empty() {
                    return Err(crate::Error::SqlParse("UNIQUE 约束列列表为空".into()));
                }
                return Ok(Stmt::CreateIndex {
                    index_name: constraint_name,
                    table,
                    columns: cols,
                    unique: true,
                });
            }
            return Err(crate::Error::SqlParse(format!(
                "不支持的约束类型: {}",
                type_rest
            )));
        }
        let after_add = if after_add_upper.starts_with("COLUMN") {
            after_add[6..].trim_start()
        } else {
            after_add
        };
        let tokens: Vec<&str> = after_add.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(crate::Error::SqlParse(
                "ALTER TABLE ADD COLUMN missing name or type".into(),
            ));
        }
        let col_name = unquote_ident(tokens[0]);
        let col_type_str = if tokens[1].contains('(') && !tokens[1].contains(')') {
            format!("{}{}", tokens[1], tokens.get(2).unwrap_or(&""))
        } else {
            tokens[1].to_string()
        };
        let col_type = parse_column_type(&col_type_str)
            .ok_or_else(|| crate::Error::SqlParse(format!("unknown type: {}", col_type_str)))?;
        let default = if let Some(pos) = find_keyword(after_add, "DEFAULT") {
            let val_str = after_add[pos + 7..].trim();
            Some(parse_value(val_str).ok_or_else(|| {
                crate::Error::SqlParse(format!("DEFAULT value parse failed: {}", val_str))
            })?)
        } else {
            None
        };
        return Ok(Stmt::AlterTable {
            table,
            action: AlterAction::AddColumn {
                name: col_name,
                col_type,
                default,
            },
        });
    }
    if rem_upper.starts_with("RENAME") {
        let after_rename = remainder[6..].trim_start();
        let after_rename_upper = after_rename.to_uppercase();
        // M103: RENAME TO new_table — 重命名表
        if after_rename_upper.starts_with("TO ") {
            let new_name = unquote_ident(
                after_rename[2..]
                    .trim_start()
                    .split_whitespace()
                    .next()
                    .unwrap_or(""),
            );
            if new_name.is_empty() {
                return Err(crate::Error::SqlParse(
                    "ALTER TABLE RENAME TO 缺少新表名".into(),
                ));
            }
            return Ok(Stmt::AlterTable {
                table,
                action: AlterAction::RenameTo { new_name },
            });
        }
        // RENAME COLUMN old TO new
        let after_rename = if after_rename_upper.starts_with("COLUMN") {
            after_rename[6..].trim_start()
        } else {
            after_rename
        };
        let tokens: Vec<&str> = after_rename.split_whitespace().collect();
        // 格式: old_name TO new_name
        if tokens.len() < 3 || !tokens[1].eq_ignore_ascii_case("TO") {
            return Err(crate::Error::SqlParse(
                "ALTER TABLE RENAME COLUMN 格式: RENAME [COLUMN] old TO new".into(),
            ));
        }
        let old_name = unquote_ident(tokens[0]);
        let new_name = unquote_ident(tokens[2]);
        return Ok(Stmt::AlterTable {
            table,
            action: AlterAction::RenameColumn { old_name, new_name },
        });
    }
    if rem_upper.starts_with("DROP") {
        let after_drop = remainder[4..].trim_start();
        let after_drop = if after_drop.to_uppercase().starts_with("COLUMN") {
            after_drop[6..].trim_start()
        } else {
            after_drop
        };
        let col_name = after_drop
            .split(|c: char| c.is_whitespace() || c == ';')
            .next()
            .unwrap_or("");
        if col_name.is_empty() {
            return Err(crate::Error::SqlParse(
                "ALTER TABLE DROP COLUMN missing column name".into(),
            ));
        }
        return Ok(Stmt::AlterTable {
            table,
            action: AlterAction::DropColumn {
                name: unquote_ident(col_name),
            },
        });
    }
    // M165：ALTER TABLE t ALTER COLUMN col SET DEFAULT val / DROP DEFAULT
    if rem_upper.starts_with("ALTER") {
        let after_alter = remainder[5..].trim_start();
        let after_alter = if after_alter.to_uppercase().starts_with("COLUMN") {
            after_alter[6..].trim_start()
        } else {
            after_alter
        };
        let tokens: Vec<&str> = after_alter.splitn(2, char::is_whitespace).collect();
        if tokens.len() < 2 {
            return Err(crate::Error::SqlParse(
                "ALTER TABLE ALTER COLUMN 缺少列名或操作".into(),
            ));
        }
        let col_name = unquote_ident(tokens[0]);
        let action_str = tokens[1].trim();
        let action_upper = action_str.to_uppercase();
        if action_upper.starts_with("SET DEFAULT") {
            let val_str = action_str[11..].trim();
            let val = parse_value(val_str).ok_or_else(|| {
                crate::Error::SqlParse(format!("DEFAULT 值解析失败: {}", val_str))
            })?;
            return Ok(Stmt::AlterTable {
                table,
                action: AlterAction::SetDefault {
                    column: col_name,
                    value: val,
                },
            });
        }
        if action_upper.starts_with("DROP DEFAULT") {
            return Ok(Stmt::AlterTable {
                table,
                action: AlterAction::DropDefault { column: col_name },
            });
        }
        // M169：ALTER COLUMN col TYPE new_type（PostgreSQL 语法）
        if action_upper.starts_with("TYPE ") || action_upper.starts_with("TYPE\t") {
            let type_str = action_str[4..]
                .trim()
                .split(';')
                .next()
                .unwrap_or("")
                .trim();
            let new_type = parse_column_type(type_str)
                .ok_or_else(|| crate::Error::SqlParse(format!("未知列类型: {}", type_str)))?;
            return Ok(Stmt::AlterTable {
                table,
                action: AlterAction::AlterType {
                    column: col_name,
                    new_type,
                },
            });
        }
        return Err(crate::Error::SqlParse(format!(
            "不支持的 ALTER COLUMN 操作: {}",
            action_str
        )));
    }
    // M169：ALTER TABLE t MODIFY [COLUMN] col new_type（MySQL 语法）
    if rem_upper.starts_with("MODIFY") {
        let after_modify = remainder[6..].trim_start();
        let after_modify = if after_modify.to_uppercase().starts_with("COLUMN") {
            after_modify[6..].trim_start()
        } else {
            after_modify
        };
        let tokens: Vec<&str> = after_modify.splitn(2, char::is_whitespace).collect();
        if tokens.len() < 2 {
            return Err(crate::Error::SqlParse(
                "ALTER TABLE MODIFY 缺少列名或类型".into(),
            ));
        }
        let col_name = unquote_ident(tokens[0]);
        let type_str = tokens[1].trim().split(';').next().unwrap_or("").trim();
        let new_type = parse_column_type(type_str)
            .ok_or_else(|| crate::Error::SqlParse(format!("未知列类型: {}", type_str)))?;
        return Ok(Stmt::AlterTable {
            table,
            action: AlterAction::AlterType {
                column: col_name,
                new_type,
            },
        });
    }
    Err(crate::Error::SqlParse(format!(
        "不支持的 ALTER TABLE 操作: {}",
        remainder
    )))
}

/// 解析 CREATE VECTOR INDEX idx ON table(col) [USING HNSW]
/// [WITH (metric='cosine', m=16, ef_construction=200)]。
pub(super) fn parse_create_vector_index(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[19..].trim_start();
    let (index_name, rest) = rest
        .split_once(|c: char| c.is_whitespace())
        .ok_or_else(|| crate::Error::SqlParse("invalid CREATE VECTOR INDEX syntax".into()))?;
    let index_name = unquote_ident(index_name);
    let rest = rest.trim();
    if !rest.to_uppercase().starts_with("ON") {
        return Err(crate::Error::SqlParse(
            "CREATE VECTOR INDEX missing ON".into(),
        ));
    }
    let rest = rest[2..].trim_start();
    let paren_pos = rest
        .find('(')
        .ok_or_else(|| crate::Error::SqlParse("CREATE VECTOR INDEX missing (".into()))?;
    let table = unquote_ident(rest[..paren_pos].trim());
    let close_pos = rest
        .find(')')
        .ok_or_else(|| crate::Error::SqlParse("CREATE VECTOR INDEX missing )".into()))?;
    let column = unquote_ident(rest[paren_pos + 1..close_pos].trim());
    let rest = rest[close_pos + 1..].trim();

    let mut metric = "cosine".to_string();
    let mut m: usize = 16;
    let mut ef_construction: usize = 200;

    let rest_upper = rest.to_uppercase();
    let params_str = if let Some(pos) = find_keyword(&rest_upper, "WITH") {
        &rest[pos + 4..]
    } else {
        ""
    };
    if !params_str.is_empty() {
        let params_str = params_str.trim();
        let inner = if params_str.starts_with('(') {
            let close = params_str.rfind(')').unwrap_or(params_str.len());
            &params_str[1..close]
        } else {
            params_str
        };
        for part in inner.split(',') {
            let part = part.trim();
            if let Some((k, v)) = part.split_once('=') {
                let k = k.trim().to_lowercase();
                let v = v.trim().trim_matches('\'').trim_matches('"');
                match k.as_str() {
                    "metric" => metric = v.to_string(),
                    "m" => m = v.parse().unwrap_or(16),
                    "ef_construction" => ef_construction = v.parse().unwrap_or(200),
                    _ => {}
                }
            }
        }
    }

    Ok(Stmt::CreateVectorIndex {
        index_name,
        table,
        column,
        metric,
        m,
        ef_construction,
    })
}

/// 解析 INSERT 语句末尾的 ON CONFLICT 子句。
/// 语法：`ON CONFLICT (col) DO UPDATE SET col1 = EXCLUDED.col1, ...`
pub(super) fn parse_on_conflict(sql: &str) -> Result<Option<OnConflict>, crate::Error> {
    let upper = sql.to_uppercase();
    let pos = match find_keyword(&upper, "ON CONFLICT") {
        Some(p) => p,
        None => return Ok(None),
    };
    let rest = sql[pos + 11..].trim_start();
    if !rest.starts_with('(') {
        return Err(crate::Error::SqlParse(
            "ON CONFLICT missing (column)".into(),
        ));
    }
    let close = rest
        .find(')')
        .ok_or_else(|| crate::Error::SqlParse("ON CONFLICT unmatched parentheses".into()))?;
    let cols_str = rest[1..close].trim();
    let conflict_columns: Vec<String> = cols_str
        .split(',')
        .map(|s| unquote_ident(s.trim()))
        .filter(|s| !s.is_empty())
        .collect();
    if conflict_columns.is_empty() {
        return Err(crate::Error::SqlParse(
            "ON CONFLICT missing conflict column".into(),
        ));
    }
    let rest = rest[close + 1..].trim_start();
    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("DO UPDATE") {
        return Err(crate::Error::SqlParse(
            "ON CONFLICT 仅支持 DO UPDATE SET".into(),
        ));
    }
    let rest = rest[9..].trim_start();
    let rest_upper2 = rest.to_uppercase();
    if !rest_upper2.starts_with("SET") {
        return Err(crate::Error::SqlParse(
            "ON CONFLICT DO UPDATE missing SET".into(),
        ));
    }
    let set_str = rest[3..].trim_start();
    let mut assignments: Vec<(String, OnConflictValue)> = Vec::new();
    for part in split_respecting_quotes(set_str) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (col, val_str) = part.split_once('=').ok_or_else(|| {
            crate::Error::SqlParse(format!("ON CONFLICT SET invalid assignment: {}", part))
        })?;
        let col = unquote_ident(col.trim());
        let val_str = val_str.trim();
        let value = if val_str.to_uppercase().starts_with("EXCLUDED.") {
            OnConflictValue::Excluded(unquote_ident(&val_str[9..]))
        } else {
            let v: Value = parse_value(val_str).ok_or_else(|| {
                crate::Error::SqlParse(format!("ON CONFLICT SET value parse failed: {}", val_str))
            })?;
            OnConflictValue::Literal(v)
        };
        assignments.push((col, value));
    }
    if assignments.is_empty() {
        return Err(crate::Error::SqlParse(
            "ON CONFLICT DO UPDATE SET missing assignments".into(),
        ));
    }
    Ok(Some(OnConflict {
        conflict_columns,
        assignments,
    }))
}

/// 截断 VALUES 部分中尾部的 ON CONFLICT 子句，使 split_value_rows 只看到行数据。
pub(super) fn truncate_on_conflict(s: &str) -> &str {
    let upper = s.to_uppercase();
    if let Some(pos) = find_keyword(&upper, "ON CONFLICT") {
        s[..pos].trim_end()
    } else {
        s
    }
}

/// 从列定义原始字符串中提取 DEFAULT 值。
pub(super) fn parse_default_from_part(part: &str) -> Option<crate::types::Value> {
    let upper = part.to_uppercase();
    let pos = upper.find("DEFAULT")?;
    let after = part[pos + 7..].trim();
    if after.is_empty() {
        return None;
    }
    let end_upper = after.to_uppercase();
    let end = ["NOT NULL", "PRIMARY KEY", "UNIQUE", "CHECK", "REFERENCES"]
        .iter()
        .filter_map(|kw| end_upper.find(kw))
        .min()
        .unwrap_or(after.len());
    let val_str = after[..end].trim();
    if val_str.eq_ignore_ascii_case("NOW()") || val_str.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        return Some(crate::types::Value::Timestamp(i64::MIN));
    }
    parse_value(val_str)
}
