/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 解析：兼容主流 SQL 写法的子集解析器。
//!
//! 支持：CREATE TABLE / DROP TABLE / DROP INDEX / INSERT / SELECT / UPDATE / DELETE / CREATE INDEX。

mod ddl;
mod geo_search;
mod insert;
mod join;
mod select;
mod types;
mod update;
mod utils;
mod vec_search;
pub(crate) mod where_clause;
pub(crate) mod window;

pub use types::{
    AlterAction, ArithOp, ColumnDef, CteClause, GeoSearchExpr, JoinClause, JoinType, OnConflict,
    OnConflictValue, SetExpr, SetOpKind, Stmt, VecSearchExpr, WhereCondition, WhereExpr, WhereOp,
    WindowExpr, WindowFuncKind,
};
use utils::*;
pub(crate) use utils::{parse_row_single_pass, parse_value, unquote_ident};

/// 解析一条 SQL；兼容主流写法。
pub fn parse(sql: &str) -> Result<Stmt, crate::Error> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Err(crate::Error::SqlParse("empty SQL".to_string()));
    }
    // M118：只取前缀做关键字检测，避免对整个 SQL 做 to_uppercase
    let short = if sql.len() > 24 { &sql[..24] } else { sql };
    let upper = short.to_uppercase();

    // BEGIN / COMMIT / ROLLBACK（短语句，直接用 sql 长度判断）
    if sql.len() <= 20 {
        let full_upper = sql.to_uppercase();
        if full_upper == "BEGIN"
            || full_upper == "BEGIN TRANSACTION"
            || full_upper == "START TRANSACTION"
        {
            return Ok(Stmt::Begin);
        }
        if full_upper == "COMMIT" || full_upper == "END" || full_upper == "END TRANSACTION" {
            return Ok(Stmt::Commit);
        }
        if full_upper == "ROLLBACK" || full_upper == "ABORT" {
            return Ok(Stmt::Rollback);
        }
    }

    if upper.starts_with("EXPLAIN ") {
        return Ok(Stmt::Explain {
            inner: Box::new(parse(sql[7..].trim())?),
        });
    }
    // M110：SAVEPOINT name
    if upper.starts_with("SAVEPOINT ") {
        let name = unquote_ident(sql[9..].trim());
        if name.is_empty() {
            return Err(crate::Error::SqlParse("SAVEPOINT missing name".into()));
        }
        return Ok(Stmt::Savepoint { name });
    }
    // M110：RELEASE [SAVEPOINT] name
    if upper.starts_with("RELEASE ") {
        let rest = sql[7..].trim();
        let rest = if rest.len() > 10 && rest[..10].eq_ignore_ascii_case("SAVEPOINT ") {
            rest[9..].trim()
        } else {
            rest
        };
        let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
        if name.is_empty() {
            return Err(crate::Error::SqlParse(
                "RELEASE missing savepoint name".into(),
            ));
        }
        return Ok(Stmt::Release { name });
    }
    // M110：ROLLBACK TO [SAVEPOINT] name（长度 > 20，不会被上面的短路径捕获）
    if upper.starts_with("ROLLBACK TO ") {
        let rest = sql[11..].trim();
        let rest = if rest.len() > 10 && rest[..10].eq_ignore_ascii_case("SAVEPOINT ") {
            rest[9..].trim()
        } else {
            rest
        };
        let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
        if name.is_empty() {
            return Err(crate::Error::SqlParse(
                "ROLLBACK TO missing savepoint name".into(),
            ));
        }
        return Ok(Stmt::RollbackTo { name });
    }
    if upper.starts_with("DROP VECTOR INDEX") {
        return parse_drop_vector_index(sql);
    }
    // M125：DROP VIEW [IF EXISTS] name
    if upper.starts_with("DROP VIEW") {
        return parse_drop_view(sql);
    }
    if upper.starts_with("DROP INDEX") {
        return ddl::parse_drop_index(sql);
    }
    if upper.starts_with("DROP TABLE") {
        return parse_drop_table(sql);
    }
    if upper.starts_with("TRUNCATE") {
        return ddl::parse_truncate(sql);
    }
    if upper.starts_with("CREATE TABLE") {
        return parse_create_table(sql, false);
    }
    // M126：CREATE TEMP TABLE / CREATE TEMPORARY TABLE
    if upper.starts_with("CREATE TEMP ") || upper.starts_with("CREATE TEMPORARY ") {
        return parse_create_table(sql, true);
    }
    if upper.starts_with("CREATE UNIQUE I") || upper.starts_with("CREATE INDEX") {
        return parse_create_index(sql);
    }
    if upper.starts_with("CREATE VECTOR INDEX") {
        return ddl::parse_create_vector_index(sql);
    }
    // M125：CREATE VIEW [IF NOT EXISTS] name AS SELECT ...
    if upper.starts_with("CREATE VIEW") {
        return parse_create_view(sql);
    }
    if upper.starts_with("ALTER TABLE") {
        return ddl::parse_alter_table(sql);
    }
    if upper.starts_with("SHOW TABLES") || upper == "SHOW TABLES" {
        return Ok(Stmt::ShowTables);
    }
    if upper.starts_with("SHOW INDEX") {
        // SHOW INDEXES / SHOW INDEXES ON table / SHOW INDEX ON table
        let rest = sql[10..].trim();
        let rest = if rest.to_uppercase().starts_with("ES") {
            rest[2..].trim() // skip "ES" in "INDEXES"
        } else {
            rest
        };
        let table = if rest.to_uppercase().starts_with("ON ") {
            Some(unquote_ident(
                rest[3..].split_whitespace().next().unwrap_or(""),
            ))
        } else {
            None
        };
        return Ok(Stmt::ShowIndexes { table });
    }
    if upper.starts_with("DESCRIBE") || upper.starts_with("DESC ") {
        let rest = if upper.starts_with("DESCRIBE") {
            sql[8..].trim()
        } else {
            sql[4..].trim()
        };
        let table = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
        if table.is_empty() {
            return Err(crate::Error::SqlParse(
                "DESCRIBE missing table name".to_string(),
            ));
        }
        return Ok(Stmt::Describe { table });
    }
    if upper.starts_with("INSERT") {
        return insert::parse_insert(sql);
    }
    // MySQL: REPLACE INTO → rewrite as INSERT OR REPLACE INTO
    if upper.starts_with("REPLACE") {
        let rest = sql[7..].trim_start();
        if rest.len() >= 4 && rest[..4].eq_ignore_ascii_case("INTO") {
            let rewritten = format!("INSERT OR REPLACE INTO {}", &rest[4..]);
            return insert::parse_insert(&rewritten);
        }
    }
    // M113：WITH ... AS (...) SELECT ... — CTE 解析
    if upper.starts_with("WITH ") {
        return parse_with_cte(sql);
    }
    if upper.starts_with("SELECT") {
        // 检测集合操作：UNION/INTERSECT/EXCEPT（在顶层非括号内查找）
        let full_upper = sql.to_uppercase();
        if let Some((set_pos, op_kind, kw_len)) = find_top_level_set_op(&full_upper) {
            let left_sql = sql[..set_pos].trim();
            let after_kw = sql[set_pos + kw_len..].trim();
            let after_kw_upper = after_kw.to_uppercase();
            let (all, right_sql) = if after_kw_upper.len() >= 4
                && &after_kw_upper.as_bytes()[..3] == b"ALL"
                && after_kw_upper.as_bytes()[3].is_ascii_whitespace()
            {
                (true, after_kw[3..].trim())
            } else {
                (false, after_kw)
            };
            let left = parse(left_sql)?;
            let right = parse(right_sql)?;
            return Ok(Stmt::Union {
                left: Box::new(left),
                right: Box::new(right),
                all,
                op: op_kind,
            });
        }
        return select::parse_select(sql);
    }
    if upper.starts_with("DELETE") {
        return update::parse_delete(sql);
    }
    if upper.starts_with("UPDATE") {
        return update::parse_update(sql);
    }
    // M164：COMMENT ON TABLE/COLUMN
    if upper.starts_with("COMMENT ON ") {
        return parse_comment(sql);
    }

    Err(crate::Error::SqlParse(
        "unsupported SQL statement".to_string(),
    ))
}

/// 在顶层（非括号/引号内）查找集合操作关键字（UNION/INTERSECT/EXCEPT）。
/// 返回 (位置, 操作类型, 关键字长度)。
fn find_top_level_set_op(upper: &str) -> Option<(usize, SetOpKind, usize)> {
    let bytes = upper.as_bytes();
    let mut depth = 0i32;
    let mut in_q = false;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_q = !in_q;
        } else if !in_q {
            if bytes[i] == b'(' {
                depth += 1;
            } else if bytes[i] == b')' {
                depth -= 1;
            } else if depth == 0 {
                let before_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
                // 按长度从长到短匹配，避免 INTERSECT 被部分匹配
                if before_ok {
                    for &(kw, op, kw_len) in &[
                        (b"INTERSECT" as &[u8], SetOpKind::Intersect, 9usize),
                        (b"EXCEPT", SetOpKind::Except, 6),
                        (b"UNION", SetOpKind::Union, 5),
                    ] {
                        if i + kw_len <= bytes.len()
                            && &bytes[i..i + kw_len] == kw
                            && (i + kw_len >= bytes.len()
                                || bytes[i + kw_len].is_ascii_whitespace())
                        {
                            return Some((i, op, kw_len));
                        }
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn parse_drop_table(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[10..].trim_start();
    let (if_exists, rest) = if rest.to_uppercase().starts_with("IF EXISTS") {
        (true, rest[9..].trim_start())
    } else {
        (false, rest)
    };
    let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
    if name.is_empty() {
        return Err(crate::Error::SqlParse(
            "DROP TABLE missing table name".to_string(),
        ));
    }
    Ok(Stmt::DropTable { name, if_exists })
}

fn parse_create_table(sql: &str, temporary: bool) -> Result<Stmt, crate::Error> {
    // M126：跳过 CREATE [TEMP|TEMPORARY] TABLE 前缀
    let rest = if temporary {
        let upper_full = sql.to_uppercase();
        let table_pos = upper_full.find("TABLE").unwrap_or(0);
        sql[table_pos + 5..].trim_start()
    } else {
        sql[12..].trim_start() // skip "CREATE TABLE"
    };
    let (if_not_exists, rest) = if rest.to_uppercase().starts_with("IF NOT EXISTS") {
        (true, rest[13..].trim_start())
    } else {
        (false, rest)
    };
    let (name, cols) = rest
        .split_once('(')
        .ok_or_else(|| crate::Error::SqlParse("CREATE TABLE missing (".to_string()))?;
    let name = unquote_ident(name.trim());
    let cols = cols.trim();
    let cols = if let Some(pos) = cols.rfind(')') {
        &cols[..pos]
    } else {
        return Err(crate::Error::SqlParse("CREATE TABLE missing )".to_string()));
    };
    let mut columns = Vec::new();
    let mut unique_constraints: Vec<Vec<String>> = Vec::new();
    let mut check_constraints: Vec<String> = Vec::new();
    let mut foreign_keys: Vec<(String, String, String)> = Vec::new();
    for part in split_respecting_quotes(cols) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let part_upper = part.to_uppercase();
        // 提取 UNIQUE(col1, col2) 约束
        if part_upper.starts_with("UNIQUE(") || part_upper.starts_with("UNIQUE (") {
            if let Some(uc) = parse_unique_constraint(part) {
                unique_constraints.push(uc);
            }
            continue;
        }
        // M118：提取表级 CHECK 约束
        if part_upper.starts_with("CHECK") {
            if let Some(chk) = extract_check_expr(part) {
                check_constraints.push(chk);
            }
            continue;
        }
        // 跳过其他表级约束定义
        if part_upper.starts_with("PRIMARY KEY")
            || part_upper.starts_with("UNIQUE KEY")
            || part_upper.starts_with("UNIQUE INDEX")
            || part_upper.starts_with("KEY ")
            || part_upper.starts_with("INDEX ")
            || part_upper.starts_with("CONSTRAINT")
        {
            continue;
        }
        // M127：表级 FOREIGN KEY (col) REFERENCES parent(col)
        if part_upper.starts_with("FOREIGN KEY") {
            if let Some(fk) = parse_table_foreign_key(part) {
                foreign_keys.push(fk);
            }
            continue;
        }
        // 移除 COMMENT 'xxx' 子句（MySQL），避免干扰后续解析
        let part = strip_column_comment(part);
        let part_upper = part.to_uppercase();
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(crate::Error::SqlParse(format!(
                "invalid column definition: {}",
                part
            )));
        }
        let cname = unquote_ident(tokens[0]);
        // 构造类型字符串：处理 ENUM('a','b') 或 VARCHAR(255) 等跨 token 的类型
        let ctype_str = build_type_str(&tokens[1..]);
        let col_type = parse_column_type(&ctype_str)
            .ok_or_else(|| crate::Error::SqlParse(format!("unknown type: {}", ctype_str)))?;
        // 解析约束：NOT NULL / DEFAULT / UNIQUE（列级 UNIQUE 静默跳过）
        let nullable = !part_upper.contains("NOT NULL");
        let default_value = ddl::parse_default_from_part(&part);
        let auto_increment =
            part_upper.contains("AUTOINCREMENT") || part_upper.contains("AUTO_INCREMENT");
        // M118：列级 CHECK 约束提取
        if let Some(chk) = extract_column_check(&part, &cname) {
            check_constraints.push(chk);
        }
        // M127：列级 REFERENCES parent(col) 外键
        let fk = parse_column_references(&part);
        if let Some((ref_table, ref_col)) = &fk {
            foreign_keys.push((cname.clone(), ref_table.clone(), ref_col.clone()));
        }
        columns.push(ColumnDef {
            name: cname,
            col_type,
            nullable,
            default_value,
            auto_increment,
            foreign_key: fk,
        });
    }
    if columns.is_empty() {
        return Err(crate::Error::SqlParse(
            "CREATE TABLE has no column definitions".to_string(),
        ));
    }
    Ok(Stmt::CreateTable {
        name,
        columns,
        if_not_exists,
        unique_constraints,
        check_constraints,
        temporary,
        foreign_keys,
    })
}

/// 从 `UNIQUE(col1, col2)` 提取列名列表。
fn parse_unique_constraint(s: &str) -> Option<Vec<String>> {
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = &s[open + 1..close];
    let cols: Vec<String> = inner
        .split(',')
        .map(|c| unquote_ident(c.trim()))
        .filter(|c| !c.is_empty())
        .collect();
    if cols.len() >= 2 {
        Some(cols)
    } else {
        None // 单列 UNIQUE 不需要特殊处理
    }
}

/// 移除列定义中的 COMMENT 'xxx' 子句（MySQL 兼容）。
/// 正确处理引号内转义（`''`）。
fn strip_column_comment(s: &str) -> String {
    let upper = s.to_uppercase();
    let Some(pos) = find_keyword(&upper, "COMMENT") else {
        return s.to_string();
    };
    let before = s[..pos].trim_end();
    let after_kw = s[pos + 7..].trim_start();
    // 跳过引号内容
    if after_kw.starts_with('\'') {
        let bytes = after_kw.as_bytes();
        let mut i = 1; // skip opening quote
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2; // escaped quote
                    continue;
                }
                // closing quote found
                let remainder = after_kw[i + 1..].trim_start();
                return format!("{} {}", before, remainder);
            }
            i += 1;
        }
    }
    before.to_string()
}

/// 从 token 列表中提取类型字符串（只取类型部分，后续修饰符自然忽略）。
/// 处理跨 token 的类型如 `ENUM('a','b','c')` 或 `VARCHAR(255)`。
fn build_type_str(tokens: &[&str]) -> String {
    if tokens.is_empty() {
        return String::new();
    }
    let first = tokens[0];
    let first_upper = first.to_uppercase();
    // 类型包含左括号但没有右括号 → 合并后续 token 直到找到 ')'
    if first.contains('(') && !first.contains(')') {
        let mut result = first.to_string();
        for &t in &tokens[1..] {
            result.push_str(t);
            if t.contains(')') {
                break;
            }
        }
        return result;
    }
    // ENUM 类型（可能后面跟括号作为独立 token）
    if first_upper == "ENUM" && tokens.len() > 1 && tokens[1].starts_with('(') {
        let mut result = first.to_string();
        for &t in &tokens[1..] {
            result.push_str(t);
            if t.contains(')') {
                break;
            }
        }
        return result;
    }
    first.to_string()
}

fn parse_create_index(sql: &str) -> Result<Stmt, crate::Error> {
    // M112：CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON table(col1, col2, ...)
    let rest_after_create = sql[6..].trim_start(); // skip "CREATE"
    let upper_rest = rest_after_create.to_uppercase();
    let (unique, rest) = if upper_rest.starts_with("UNIQUE ") {
        (true, rest_after_create[6..].trim_start())
    } else {
        (false, rest_after_create)
    };
    // skip "INDEX"
    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("INDEX") {
        return Err(crate::Error::SqlParse("expected INDEX keyword".into()));
    }
    let mut rest = rest[5..].trim_start();
    // Skip optional IF NOT EXISTS
    if rest.to_uppercase().starts_with("IF NOT EXISTS") {
        rest = rest[13..].trim_start();
    }
    let (index_name, rest) = rest
        .split_once(|c: char| c.is_whitespace())
        .ok_or_else(|| crate::Error::SqlParse("invalid CREATE INDEX syntax".to_string()))?;
    let index_name = unquote_ident(index_name);
    let rest = rest.trim();
    if !rest.to_uppercase().starts_with("ON") {
        return Err(crate::Error::SqlParse(
            "CREATE INDEX missing ON".to_string(),
        ));
    }
    let rest = rest[2..].trim_start();
    let (table_col, _) = rest
        .split_once(')')
        .ok_or_else(|| crate::Error::SqlParse("CREATE INDEX missing )".to_string()))?;
    let (table, cols_str) = table_col
        .split_once('(')
        .ok_or_else(|| crate::Error::SqlParse("CREATE INDEX missing (".to_string()))?;
    let columns: Vec<String> = cols_str
        .split(',')
        .map(|c| unquote_ident(c.trim()))
        .collect();
    if columns.is_empty() || columns.iter().any(|c| c.is_empty()) {
        return Err(crate::Error::SqlParse("CREATE INDEX 列名不能为空".into()));
    }
    if columns.len() > 8 {
        return Err(crate::Error::SqlParse("复合索引最多支持 8 列".into()));
    }
    Ok(Stmt::CreateIndex {
        index_name,
        table: unquote_ident(table.trim()),
        columns,
        unique,
    })
}

fn parse_drop_vector_index(sql: &str) -> Result<Stmt, crate::Error> {
    // DROP VECTOR INDEX [IF EXISTS] idx_name
    let rest = sql[17..].trim_start(); // skip "DROP VECTOR INDEX"
    let upper = rest.to_uppercase();
    let (if_exists, name_part) = if upper.starts_with("IF EXISTS") {
        (true, rest[9..].trim_start())
    } else {
        (false, rest)
    };
    let index_name = name_part
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(';');
    if index_name.is_empty() {
        return Err(crate::Error::SqlParse(
            "DROP VECTOR INDEX 缺少索引名".into(),
        ));
    }
    Ok(Stmt::DropVectorIndex {
        index_name: unquote_ident(index_name),
        if_exists,
    })
}

/// M113：解析 `WITH name AS (SELECT ...) [, name2 AS (...)] SELECT ...`。
/// 在顶层括号匹配中提取每个 CTE 子句，最后解析主 SELECT 并注入 ctes。
fn parse_with_cte(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[4..].trim_start(); // skip "WITH"
    let mut ctes = Vec::new();
    let mut remaining = rest;
    loop {
        // 提取 CTE 名称
        let name_end = remaining
            .find(|c: char| c.is_whitespace())
            .ok_or_else(|| crate::Error::SqlParse("WITH 子句缺少 CTE 名称".into()))?;
        let cte_name = unquote_ident(&remaining[..name_end]);
        remaining = remaining[name_end..].trim_start();
        // 跳过 AS
        let ru = remaining.to_uppercase();
        if !ru.starts_with("AS") {
            return Err(crate::Error::SqlParse(format!(
                "CTE '{}' 缺少 AS 关键字",
                cte_name
            )));
        }
        remaining = remaining[2..].trim_start();
        // 匹配括号内的子查询
        if !remaining.starts_with('(') {
            return Err(crate::Error::SqlParse(format!(
                "CTE '{}' 缺少左括号",
                cte_name
            )));
        }
        let close = find_matching_paren(remaining)?;
        let inner_sql = &remaining[1..close];
        let cte_query = parse(inner_sql)?;
        ctes.push(types::CteClause {
            name: cte_name,
            query: Box::new(cte_query),
        });
        remaining = remaining[close + 1..].trim_start();
        // 逗号分隔多个 CTE，否则进入主查询
        if remaining.starts_with(',') {
            remaining = remaining[1..].trim_start();
        } else {
            break;
        }
    }
    if ctes.is_empty() {
        return Err(crate::Error::SqlParse("WITH 子句至少需要一个 CTE".into()));
    }
    // 解析主查询（必须是 SELECT）
    let mut main_stmt = parse(remaining)?;
    match &mut main_stmt {
        Stmt::Select {
            ctes: ref mut c, ..
        } => {
            *c = ctes;
        }
        _ => {
            return Err(crate::Error::SqlParse(
                "WITH 子句后必须跟 SELECT 语句".into(),
            ));
        }
    }
    Ok(main_stmt)
}

/// 找到与第一个 `(` 匹配的 `)` 位置（跳过嵌套括号和引号）。
fn find_matching_paren(s: &str) -> Result<usize, crate::Error> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_q = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' {
            in_q = !in_q;
        } else if !in_q {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
        }
    }
    Err(crate::Error::SqlParse("CTE 子查询括号不匹配".into()))
}
/// M118：从表级 `CHECK (expr)` 定义中提取表达式字符串。
/// 例如 `CHECK (age >= 0)` → `Some("age >= 0")`。
fn extract_check_expr(part: &str) -> Option<String> {
    let open = part.find('(')?;
    let close = part.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = part[open + 1..close].trim();
    if inner.is_empty() {
        return None;
    }
    Some(inner.to_string())
}

/// M118：从列定义中提取列级 CHECK 约束。
/// 例如 `age INTEGER CHECK (age >= 0)` → `Some("age >= 0")`。
/// 列名参数用于在表达式中引用（但不强制替换）。
fn extract_column_check(part: &str, _col_name: &str) -> Option<String> {
    let upper = part.to_uppercase();
    let pos = find_keyword(&upper, "CHECK")?;
    let rest = &part[pos + 5..];
    let open = rest.find('(')?;
    let close = rest.rfind(')')?;
    if close <= open {
        return None;
    }
    let inner = rest[open + 1..close].trim();
    if inner.is_empty() {
        return None;
    }
    Some(inner.to_string())
}

/// M125：解析 `CREATE VIEW [IF NOT EXISTS] name AS SELECT ...`。
fn parse_create_view(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[11..].trim_start(); // skip "CREATE VIEW"
    let (if_not_exists, rest) = if rest.to_uppercase().starts_with("IF NOT EXISTS") {
        (true, rest[13..].trim_start())
    } else {
        (false, rest)
    };
    // 提取视图名（到 AS 关键字之前）
    let rest_upper = rest.to_uppercase();
    let as_pos = find_keyword(&rest_upper, "AS")
        .ok_or_else(|| crate::Error::SqlParse("CREATE VIEW 缺少 AS 关键字".into()))?;
    let name = unquote_ident(rest[..as_pos].trim());
    if name.is_empty() {
        return Err(crate::Error::SqlParse("CREATE VIEW 缺少视图名".into()));
    }
    let view_sql = rest[as_pos + 2..].trim();
    if view_sql.is_empty() {
        return Err(crate::Error::SqlParse(
            "CREATE VIEW 缺少 SELECT 语句".into(),
        ));
    }
    // 验证视图 SQL 是合法的 SELECT
    let view_upper = if view_sql.len() > 10 {
        view_sql[..10].to_uppercase()
    } else {
        view_sql.to_uppercase()
    };
    if !view_upper.starts_with("SELECT") && !view_upper.starts_with("WITH") {
        return Err(crate::Error::SqlParse(
            "CREATE VIEW 的 AS 后必须是 SELECT 语句".into(),
        ));
    }
    // 尝试解析以验证语法正确性
    let _ = parse(view_sql)?;
    Ok(Stmt::CreateView {
        name,
        if_not_exists,
        sql: view_sql.to_string(),
    })
}

/// M125：解析 `DROP VIEW [IF EXISTS] name`。
fn parse_drop_view(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[9..].trim_start(); // skip "DROP VIEW"
    let (if_exists, rest) = if rest.to_uppercase().starts_with("IF EXISTS") {
        (true, rest[9..].trim_start())
    } else {
        (false, rest)
    };
    let name = unquote_ident(rest.split_whitespace().next().unwrap_or(""));
    if name.is_empty() {
        return Err(crate::Error::SqlParse("DROP VIEW 缺少视图名".into()));
    }
    Ok(Stmt::DropView { name, if_exists })
}

/// M127：从列定义中提取列级 `REFERENCES parent(col)` 外键。
/// 例如 `doc_id INTEGER REFERENCES documents(id)` → `Some(("documents", "id"))`。
fn parse_column_references(part: &str) -> Option<(String, String)> {
    let upper = part.to_uppercase();
    let pos = find_keyword(&upper, "REFERENCES")?;
    let rest = part[pos + 10..].trim_start();
    // rest = "parent(col) ..."
    let open = rest.find('(')?;
    let close = rest.find(')')?;
    if close <= open {
        return None;
    }
    let ref_table = unquote_ident(rest[..open].trim());
    let ref_col = unquote_ident(rest[open + 1..close].trim());
    if ref_table.is_empty() || ref_col.is_empty() {
        return None;
    }
    Some((ref_table, ref_col))
}

/// M127：解析表级 `FOREIGN KEY (col) REFERENCES parent(col)` 约束。
/// 返回 (子列, 父表, 父列)。
fn parse_table_foreign_key(part: &str) -> Option<(String, String, String)> {
    let upper = part.to_uppercase();
    // 提取 FOREIGN KEY (col)
    let fk_pos = upper.find("FOREIGN KEY")?;
    let rest = &part[fk_pos + 11..];
    let open1 = rest.find('(')?;
    let close1 = rest.find(')')?;
    if close1 <= open1 {
        return None;
    }
    let child_col = unquote_ident(rest[open1 + 1..close1].trim());
    // 提取 REFERENCES parent(col)
    let after = &rest[close1 + 1..];
    let after_upper = after.to_uppercase();
    let ref_pos = after_upper.find("REFERENCES")?;
    let ref_rest = after[ref_pos + 10..].trim_start();
    let open2 = ref_rest.find('(')?;
    let close2 = ref_rest.find(')')?;
    if close2 <= open2 {
        return None;
    }
    let ref_table = unquote_ident(ref_rest[..open2].trim());
    let ref_col = unquote_ident(ref_rest[open2 + 1..close2].trim());
    if child_col.is_empty() || ref_table.is_empty() || ref_col.is_empty() {
        return None;
    }
    Some((child_col, ref_table, ref_col))
}

/// M164：解析 COMMENT ON TABLE/COLUMN 语句。
///
/// 语法：
/// - `COMMENT ON TABLE table_name IS 'text'`
/// - `COMMENT ON COLUMN table_name.column_name IS 'text'`
fn parse_comment(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[10..].trim_start(); // skip "COMMENT ON"
    let upper = rest.to_uppercase();
    if upper.starts_with("TABLE ") {
        let after = rest[6..].trim_start();
        let is_pos = utils::find_keyword(after, "IS")
            .ok_or_else(|| crate::Error::SqlParse("COMMENT ON TABLE 缺少 IS".into()))?;
        let table = utils::unquote_ident(after[..is_pos].trim());
        let text = extract_comment_text(after[is_pos + 2..].trim())?;
        Ok(Stmt::Comment {
            table,
            column: None,
            text,
        })
    } else if upper.starts_with("COLUMN ") {
        let after = rest[7..].trim_start();
        let is_pos = utils::find_keyword(after, "IS")
            .ok_or_else(|| crate::Error::SqlParse("COMMENT ON COLUMN 缺少 IS".into()))?;
        let target = after[..is_pos].trim();
        let dot = target.find('.').ok_or_else(|| {
            crate::Error::SqlParse("COMMENT ON COLUMN 需要 table.column 格式".into())
        })?;
        let table = utils::unquote_ident(target[..dot].trim());
        let column = utils::unquote_ident(target[dot + 1..].trim());
        let text = extract_comment_text(after[is_pos + 2..].trim())?;
        Ok(Stmt::Comment {
            table,
            column: Some(column),
            text,
        })
    } else {
        Err(crate::Error::SqlParse(
            "COMMENT ON 仅支持 TABLE 或 COLUMN".into(),
        ))
    }
}

/// 提取 IS 后面的引号字符串。
fn extract_comment_text(s: &str) -> Result<String, crate::Error> {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(crate::Error::SqlParse(
            "COMMENT ON ... IS 后需要引号字符串".into(),
        ))
    }
}
