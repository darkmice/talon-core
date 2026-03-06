/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! UPDATE / DELETE 语句解析。
//! 从 parser/mod.rs 拆分，保持单文件 ≤500 行。

use super::insert;
use super::types::*;
use super::utils::*;
use super::where_clause::parse_where;

/// M110：解析 SET 赋值表达式。支持字面量和 `col +/- expr` 模式。
pub(super) fn parse_set_expr(val_str: &str, col_name: &str) -> Result<SetExpr, crate::Error> {
    let col_lower = col_name.to_lowercase();
    // 尝试匹配 col_name +/- literal 模式
    for (op_char, op) in [
        ('+', ArithOp::Add),
        ('-', ArithOp::Sub),
        ('*', ArithOp::Mul),
        ('/', ArithOp::Div),
    ] {
        if let Some(pos) = val_str.find(op_char) {
            let lhs = val_str[..pos].trim();
            let rhs = val_str[pos + 1..].trim();
            if unquote_ident(lhs).to_lowercase() == col_lower {
                if let Some(val) = parse_value(rhs) {
                    return Ok(SetExpr::ColumnArith(col_name.to_string(), op, val));
                }
            }
        }
    }
    // 回退到字面量赋值
    let val = parse_value(val_str)
        .ok_or_else(|| crate::Error::SqlParse(format!("SET value parse failed: {}", val_str)))?;
    Ok(SetExpr::Literal(val))
}

/// 解析 UPDATE 语句。
/// 支持 M116: UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id
/// 支持 M167: UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.col = t2.col WHERE ...
pub(super) fn parse_update(sql: &str) -> Result<Stmt, crate::Error> {
    let rest = sql[6..].trim_start();
    let (table, remainder) = extract_table_name(rest);
    let table = unquote_ident(&table);
    let remainder = remainder.trim();

    // M167: 检测 MySQL 风格 JOIN（UPDATE t1 JOIN t2 ON ... SET ...）
    let upper_rem = remainder.to_uppercase();
    if upper_rem.starts_with("JOIN")
        || upper_rem.starts_with("INNER JOIN")
        || upper_rem.starts_with("LEFT JOIN")
    {
        return parse_update_join(sql, &table, remainder);
    }

    if !upper_rem.starts_with("SET") {
        return Err(crate::Error::SqlParse("UPDATE missing SET".to_string()));
    }
    let after_set = remainder[3..].trim_start();
    // M116: 检测 FROM 子句（在 WHERE 之前）
    // M153: 使用 top_level 避免匹配 EXISTS 子查询内的 FROM
    let from_pos = find_keyword_top_level(after_set, "FROM");
    let where_pos = find_keyword_top_level(after_set, "WHERE");
    // M117: 检测 ORDER BY 和 LIMIT
    let order_pos = find_keyword_top_level(after_set, "ORDER");
    let limit_pos = find_keyword_top_level(after_set, "LIMIT");
    // SET 部分截止到 FROM / WHERE / ORDER / LIMIT（取最早者）
    let set_end = [from_pos, where_pos, order_pos, limit_pos]
        .iter()
        .filter_map(|p| *p)
        .min()
        .unwrap_or(after_set.len());
    let set_str = after_set[..set_end].trim();
    // 解析 FROM 子句
    let from_table = if let Some(fp) = from_pos {
        let after_from = after_set[fp + 4..].trim_start();
        // FROM table_name 截止到 WHERE 或末尾
        let ft_end = if let Some(wp) = where_pos {
            // where_pos 是相对 after_set 的偏移，需要减去 from 之后的起始
            let from_start = fp + 4;
            if wp > from_start {
                wp - from_start
            } else {
                after_from.len()
            }
        } else {
            after_from.len()
        };
        let (ft, _) = extract_table_name(after_from[..ft_end].trim());
        let ft = unquote_ident(&ft);
        if ft == table {
            return Err(crate::Error::SqlParse(
                "UPDATE ... FROM 源表不能与目标表同名".into(),
            ));
        }
        Some(ft)
    } else {
        None
    };
    let mut assignments: Vec<(String, SetExpr)> = Vec::new();
    for part in split_respecting_quotes(set_str) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (col, val_str) = part
            .split_once('=')
            .ok_or_else(|| crate::Error::SqlParse(format!("SET 赋值格式错误: {}", part)))?;
        let col_name = unquote_ident(col.trim());
        let val_str = val_str.trim();
        // M116: 检测 table.col 引用（如 t2.score）
        let expr = if let Some(dot_pos) = val_str.find('.') {
            let ref_table = val_str[..dot_pos].trim();
            let ref_col = val_str[dot_pos + 1..].trim();
            // 只有当 FROM 存在且引用表匹配时才视为列引用
            if from_table.as_deref() == Some(ref_table) && !ref_col.is_empty() {
                SetExpr::ColumnRef(ref_table.to_string(), unquote_ident(ref_col))
            } else {
                parse_set_expr(val_str, &col_name)?
            }
        } else {
            parse_set_expr(val_str, &col_name)?
        };
        assignments.push((col_name, expr));
    }
    let mut where_clause = None;
    if let Some(pos) = where_pos {
        let where_str = after_set[pos + 5..].trim();
        let where_str = insert::truncate_returning(where_str);
        // M117: 截断 ORDER BY / LIMIT，避免 WHERE 解析器误读
        let end = min_keyword_pos(where_str, &["ORDER", "LIMIT", "OFFSET", "FETCH"]);
        let wstr = where_str[..end].trim();
        if !wstr.is_empty() {
            where_clause = Some(parse_where(wstr)?);
        }
    }
    // M117: 解析 ORDER BY 和 LIMIT（在整个 sql 上查找，位于 WHERE 之后）
    let tail = if let Some(wp) = where_pos {
        &after_set[wp + 5..]
    } else if let Some(fp) = from_pos {
        // FROM 之后
        let after_from = after_set[fp + 4..].trim_start();
        let ft_end = after_from
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after_from.len());
        &after_from[ft_end..]
    } else {
        &after_set[set_end..]
    };
    let mut order_by = None;
    let mut limit = None;
    if let Some(op) = find_keyword(tail, "ORDER") {
        let after_order = tail[op + 5..].trim_start();
        let after_by = if after_order.to_uppercase().starts_with("BY") {
            after_order[2..].trim_start()
        } else {
            after_order
        };
        let end = min_keyword_pos(after_by, &["LIMIT", "OFFSET", "FETCH", "RETURNING"]);
        let ob_str = after_by[..end].trim();
        if !ob_str.is_empty() {
            let mut cols = Vec::new();
            for part in split_respecting_quotes(ob_str) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                // 解析 NULLS FIRST / NULLS LAST 后缀
                let (part_no_nulls, nulls_first) = {
                    let u = part.to_uppercase();
                    if u.ends_with("NULLS FIRST") {
                        (&part[..part.len() - 11], Some(true))
                    } else if u.ends_with("NULLS LAST") {
                        (&part[..part.len() - 10], Some(false))
                    } else {
                        (part, None)
                    }
                };
                let part_trimmed = part_no_nulls.trim();
                let (col, desc) = if let Some(s) = part_trimmed
                    .strip_suffix("DESC")
                    .or_else(|| part_trimmed.strip_suffix("desc"))
                {
                    (unquote_ident(s.trim()), true)
                } else if let Some(s) = part_trimmed
                    .strip_suffix("ASC")
                    .or_else(|| part_trimmed.strip_suffix("asc"))
                {
                    (unquote_ident(s.trim()), false)
                } else {
                    (unquote_ident(part_trimmed), false)
                };
                if !col.is_empty() {
                    cols.push((col, desc, nulls_first));
                }
            }
            if !cols.is_empty() {
                order_by = Some(cols);
            }
        }
    }
    if let Some(lp) = find_keyword(tail, "LIMIT") {
        let after_limit = tail[lp + 5..].trim_start();
        let token = after_limit.split_whitespace().next().unwrap_or("");
        limit = if token == "?" {
            Some(u64::MAX) // 占位符哨兵，由 bind_params 替换
        } else {
            token.parse().ok()
        };
    }
    // ORDER BY / LIMIT 不能与 FROM 跨表更新同时使用
    if from_table.is_some() && (order_by.is_some() || limit.is_some()) {
        return Err(crate::Error::SqlParse(
            "UPDATE ... FROM 不支持 ORDER BY / LIMIT".into(),
        ));
    }
    Ok(Stmt::Update {
        table,
        assignments,
        where_clause,
        returning: insert::parse_returning(sql),
        from_table,
        order_by,
        limit,
    })
}

/// 解析 DELETE 语句（支持 RETURNING / USING / MySQL 多表 JOIN 子句）。
pub(super) fn parse_delete(sql: &str) -> Result<Stmt, crate::Error> {
    // M168: 检测 MySQL 风格 DELETE t1 FROM t1 JOIN t2 ON ...
    // 判断：DELETE 后第一个 token 不是 FROM → MySQL 多表模式
    let after_delete = sql[6..].trim_start();
    let first_token_upper = after_delete
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase();
    if first_token_upper != "FROM" && !after_delete.is_empty() {
        // MySQL 风格：DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...
        return parse_delete_join(sql, after_delete);
    }

    let from_pos = find_keyword(sql, "FROM")
        .ok_or_else(|| crate::Error::SqlParse("DELETE missing FROM".to_string()))?;
    let after_from = sql[from_pos + 4..].trim_start();
    let (table, remainder) = extract_table_name(after_from);
    let table = unquote_ident(&table);
    let remainder = remainder.trim();

    // M163: 检测 USING source_table
    let (using_table, remainder) = if let Some(up) = find_keyword(remainder, "USING") {
        let after_using = remainder[up + 5..].trim_start();
        let (src, rest) = extract_table_name(after_using);
        let src = unquote_ident(&src);
        if src.is_empty() {
            return Err(crate::Error::SqlParse("DELETE USING 缺少源表名".into()));
        }
        if src == table {
            return Err(crate::Error::SqlParse(
                "DELETE ... USING 源表不能与目标表同名".into(),
            ));
        }
        (Some(src), rest.trim().to_string())
    } else {
        (None, remainder.to_string())
    };

    let mut where_clause = None;
    if let Some(pos) = find_keyword(&remainder, "WHERE") {
        let where_str = remainder[pos + 5..].trim();
        // 截断 RETURNING 子句，避免 WHERE 解析器把它当作条件
        let where_str = insert::truncate_returning(where_str);
        if !where_str.is_empty() {
            where_clause = Some(parse_where(where_str)?);
        }
    }
    Ok(Stmt::Delete {
        table,
        where_clause,
        returning: insert::parse_returning(sql),
        using_table,
    })
}

/// M167: 解析 MySQL 风格多表 UPDATE（`UPDATE t1 JOIN t2 ON ... SET ... WHERE ...`）。
///
/// 转换为已有的 `Stmt::Update { from_table }` 结构，复用 `exec_update_from`。
/// ON 条件合并到 WHERE（AND 连接），SET 中的表前缀被剥离。
fn parse_update_join(sql: &str, target: &str, remainder: &str) -> Result<Stmt, crate::Error> {
    // 跳过 JOIN / INNER JOIN / LEFT JOIN 关键字
    let upper = remainder.to_uppercase();
    let after_join = if upper.starts_with("INNER JOIN") {
        remainder[10..].trim_start()
    } else if upper.starts_with("LEFT JOIN") {
        remainder[9..].trim_start()
    } else {
        // "JOIN"
        remainder[4..].trim_start()
    };
    // 提取右表名
    let (source, after_source) = extract_table_name(after_join);
    let source = unquote_ident(&source);
    if source.is_empty() {
        return Err(crate::Error::SqlParse("UPDATE JOIN 缺少源表名".into()));
    }
    if source == target {
        return Err(crate::Error::SqlParse(
            "UPDATE JOIN 源表不能与目标表同名".into(),
        ));
    }
    let after_source = after_source.trim();
    // 解析 ON 条件
    let on_pos = find_keyword(after_source, "ON")
        .ok_or_else(|| crate::Error::SqlParse("UPDATE JOIN 缺少 ON 子句".into()))?;
    let after_on = after_source[on_pos + 2..].trim_start();
    // ON 条件截止到 SET
    let set_pos = find_keyword(after_on, "SET")
        .ok_or_else(|| crate::Error::SqlParse("UPDATE JOIN 缺少 SET 子句".into()))?;
    let on_str = after_on[..set_pos].trim();
    if on_str.is_empty() {
        return Err(crate::Error::SqlParse("UPDATE JOIN ON 条件为空".into()));
    }
    let after_set = after_on[set_pos + 3..].trim_start();
    // 解析 SET 赋值，截止到 WHERE
    let where_pos = find_keyword_top_level(after_set, "WHERE");
    let set_end = where_pos.unwrap_or(after_set.len());
    let set_str = after_set[..set_end].trim();
    // 解析赋值列表，处理表前缀
    let mut assignments: Vec<(String, SetExpr)> = Vec::new();
    for part in split_respecting_quotes(set_str) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (col_part, val_str) = part
            .split_once('=')
            .ok_or_else(|| crate::Error::SqlParse(format!("SET 赋值格式错误: {}", part)))?;
        // 去掉目标表前缀：t1.name → name
        let col_name = strip_table_prefix(col_part.trim(), target);
        let val_str = val_str.trim();
        // 检测 table.col 引用
        let expr = if let Some(dot_pos) = val_str.find('.') {
            let ref_table = val_str[..dot_pos].trim();
            let ref_col = val_str[dot_pos + 1..].trim();
            if ref_table.eq_ignore_ascii_case(&source) && !ref_col.is_empty() {
                SetExpr::ColumnRef(ref_table.to_string(), unquote_ident(ref_col))
            } else {
                parse_set_expr(val_str, &col_name)?
            }
        } else {
            parse_set_expr(val_str, &col_name)?
        };
        assignments.push((col_name, expr));
    }
    // 构建 WHERE：ON 条件 AND 原始 WHERE
    let on_where = parse_where(on_str)?;
    let where_clause = if let Some(wp) = where_pos {
        let orig_where_str = after_set[wp + 5..].trim();
        let orig_where_str = insert::truncate_returning(orig_where_str);
        if orig_where_str.is_empty() {
            Some(on_where)
        } else {
            let orig_where = parse_where(orig_where_str)?;
            Some(WhereExpr::And(vec![on_where, orig_where]))
        }
    } else {
        Some(on_where)
    };
    Ok(Stmt::Update {
        table: target.to_string(),
        assignments,
        where_clause,
        returning: insert::parse_returning(sql),
        from_table: Some(source),
        order_by: None,
        limit: None,
    })
}

/// M168: 解析 MySQL 风格多表 DELETE（`DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...`）。
///
/// 转换为已有的 `Stmt::Delete { using_table }` 结构，复用 `exec_delete_using`。
/// ON 条件合并到 WHERE（AND 连接）。
fn parse_delete_join(sql: &str, after_delete: &str) -> Result<Stmt, crate::Error> {
    // 提取目标表名（DELETE 后的表名）
    let (target, remainder) = extract_table_name(after_delete);
    let target = unquote_ident(&target);
    let remainder = remainder.trim();
    // 期望 FROM
    if !remainder.to_uppercase().starts_with("FROM") {
        return Err(crate::Error::SqlParse(
            "DELETE 多表语法需要 FROM 子句".into(),
        ));
    }
    let after_from = remainder[4..].trim_start();
    // FROM 后应该是 t1 JOIN t2 ON ...，先跳过 t1（应与 target 同名）
    let (from_table, after_ft) = extract_table_name(after_from);
    let from_table = unquote_ident(&from_table);
    if !from_table.eq_ignore_ascii_case(&target) {
        return Err(crate::Error::SqlParse(format!(
            "DELETE 多表语法：FROM 后的表名 '{}' 应与目标表 '{}' 一致",
            from_table, target
        )));
    }
    let after_ft = after_ft.trim();
    // 检测 JOIN 关键字
    let upper_aft = after_ft.to_uppercase();
    let after_join = if upper_aft.starts_with("INNER JOIN") {
        after_ft[10..].trim_start()
    } else if upper_aft.starts_with("LEFT JOIN") {
        after_ft[9..].trim_start()
    } else if upper_aft.starts_with("JOIN") {
        after_ft[4..].trim_start()
    } else {
        return Err(crate::Error::SqlParse(
            "DELETE 多表语法需要 JOIN 子句".into(),
        ));
    };
    // 提取源表名
    let (source, after_source) = extract_table_name(after_join);
    let source = unquote_ident(&source);
    if source.is_empty() {
        return Err(crate::Error::SqlParse("DELETE JOIN 缺少源表名".into()));
    }
    if source == target {
        return Err(crate::Error::SqlParse(
            "DELETE JOIN 源表不能与目标表同名".into(),
        ));
    }
    let after_source = after_source.trim();
    // 解析 ON 条件
    let on_pos = find_keyword(after_source, "ON")
        .ok_or_else(|| crate::Error::SqlParse("DELETE JOIN 缺少 ON 子句".into()))?;
    let after_on = after_source[on_pos + 2..].trim_start();
    // ON 条件截止到 WHERE 或末尾
    let where_pos = find_keyword_top_level(after_on, "WHERE");
    let on_end = where_pos.unwrap_or(after_on.len());
    let on_str = after_on[..on_end].trim();
    if on_str.is_empty() {
        return Err(crate::Error::SqlParse("DELETE JOIN ON 条件为空".into()));
    }
    // 构建 WHERE：ON 条件 AND 原始 WHERE
    let on_where = parse_where(on_str)?;
    let where_clause = if let Some(wp) = where_pos {
        let orig_where_str = after_on[wp + 5..].trim();
        let orig_where_str = insert::truncate_returning(orig_where_str);
        if orig_where_str.is_empty() {
            Some(on_where)
        } else {
            let orig_where = parse_where(orig_where_str)?;
            Some(WhereExpr::And(vec![on_where, orig_where]))
        }
    } else {
        Some(on_where)
    };
    Ok(Stmt::Delete {
        table: target,
        where_clause,
        returning: insert::parse_returning(sql),
        using_table: Some(source),
    })
}

/// 去掉列名的表前缀（`t1.name` → `name`，`name` → `name`）。
fn strip_table_prefix(col: &str, table: &str) -> String {
    if let Some(dot_pos) = col.find('.') {
        let prefix = col[..dot_pos].trim();
        if prefix.eq_ignore_ascii_case(table) {
            return unquote_ident(col[dot_pos + 1..].trim());
        }
    }
    unquote_ident(col)
}
