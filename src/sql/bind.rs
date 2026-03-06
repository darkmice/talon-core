/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 参数化查询绑定：将 `?` 占位符替换为实际值。
//!
//! 解析阶段 `parse_value("?")` 生成 `Value::Placeholder(0)` 哨兵，
//! `assign_indices` 为 AST 中所有占位符分配递增索引，
//! `bind_params` 将占位符替换为 `params[index]`。
//!
//! LIMIT/OFFSET 占位符使用 `u64::MAX` 哨兵值：
//! 解析器遇到 `LIMIT ?` 时设置 `limit = Some(u64::MAX)`，
//! 绑定阶段将其替换为 `params[index]` 的整数值。

/// LIMIT/OFFSET 占位符哨兵值。
const LIMIT_PLACEHOLDER: u64 = u64::MAX;

use super::parser::{OnConflictValue, SetExpr, Stmt, WhereCondition, WhereExpr};
use crate::types::Value;
use crate::Error;

/// 从参数中提取 LIMIT/OFFSET 的非负整数值。
/// 非整数或负数均返回明确错误。
fn resolve_limit_param(param: &Value, label: &str) -> Result<u64, Error> {
    match param {
        Value::Integer(n) => {
            if *n < 0 {
                Err(Error::SqlExec(format!(
                    "{} value must be non-negative, got {}",
                    label, n
                )))
            } else {
                Ok(*n as u64)
            }
        }
        other => Err(Error::SqlExec(format!(
            "{} parameter must be an integer, got {:?}",
            label, other
        ))),
    }
}

/// 为 Stmt 中所有 Placeholder(0) 哨兵分配递增索引，返回占位符总数。
fn assign_indices(stmt: &mut Stmt) -> usize {
    let mut counter = 0usize;
    match stmt {
        Stmt::Insert {
            values,
            on_conflict,
            ..
        } => {
            for row in values.iter_mut() {
                for val in row.iter_mut() {
                    if matches!(val, Value::Placeholder(_)) {
                        *val = Value::Placeholder(counter);
                        counter += 1;
                    }
                }
            }
            if let Some(oc) = on_conflict {
                for (_, ocv) in oc.assignments.iter_mut() {
                    if let OnConflictValue::Literal(val) = ocv {
                        if matches!(val, Value::Placeholder(_)) {
                            *val = Value::Placeholder(counter);
                            counter += 1;
                        }
                    }
                }
            }
        }
        Stmt::Select {
            where_clause,
            limit,
            offset,
            ..
        } => {
            if let Some(wc) = where_clause {
                assign_where_indices(wc, &mut counter);
            }
            if *limit == Some(LIMIT_PLACEHOLDER) {
                counter += 1;
            }
            if *offset == Some(LIMIT_PLACEHOLDER) {
                counter += 1;
            }
        }
        Stmt::Update {
            assignments,
            where_clause,
            limit,
            ..
        } => {
            for (_, expr) in assignments.iter_mut() {
                match expr {
                    SetExpr::Literal(val) => {
                        if matches!(val, Value::Placeholder(_)) {
                            *val = Value::Placeholder(counter);
                            counter += 1;
                        }
                    }
                    SetExpr::ColumnArith(_, _, val) => {
                        if matches!(val, Value::Placeholder(_)) {
                            *val = Value::Placeholder(counter);
                            counter += 1;
                        }
                    }
                    SetExpr::ColumnRef(_, _) => {} // 列引用无占位符
                }
            }
            if let Some(wc) = where_clause {
                assign_where_indices(wc, &mut counter);
            }
            if *limit == Some(LIMIT_PLACEHOLDER) {
                counter += 1;
            }
        }
        Stmt::Delete {
            where_clause: Some(wc),
            ..
        } => {
            assign_where_indices(wc, &mut counter);
        }
        _ => {}
    }
    counter
}

/// 递归为 WhereExpr 中的占位符分配索引。
fn assign_where_indices(expr: &mut WhereExpr, counter: &mut usize) {
    match expr {
        WhereExpr::And(children) | WhereExpr::Or(children) => {
            for child in children.iter_mut() {
                assign_where_indices(child, counter);
            }
        }
        WhereExpr::Leaf(cond) => {
            assign_cond_indices(cond, counter);
        }
    }
}

/// 为单个 WhereCondition 中的占位符分配索引。
fn assign_cond_indices(cond: &mut WhereCondition, counter: &mut usize) {
    if matches!(cond.value, Value::Placeholder(_)) {
        cond.value = Value::Placeholder(*counter);
        *counter += 1;
    }
    for val in cond.in_values.iter_mut() {
        if matches!(val, Value::Placeholder(_)) {
            *val = Value::Placeholder(*counter);
            *counter += 1;
        }
    }
    if let Some(ref mut high) = cond.value_high {
        if matches!(high, Value::Placeholder(_)) {
            *high = Value::Placeholder(*counter);
            *counter += 1;
        }
    }
}

/// 将 Stmt 中所有 Placeholder(index) 替换为 `params[index]`。
/// 参数数量必须与占位符数量完全匹配。
pub(crate) fn bind_params(stmt: &mut Stmt, params: &[Value]) -> Result<(), Error> {
    let expected = assign_indices(stmt);
    if expected != params.len() {
        return Err(Error::SqlExec(format!(
            "parameter count mismatch: SQL has {} placeholders but {} parameters provided",
            expected,
            params.len()
        )));
    }
    if expected == 0 {
        return Ok(());
    }
    match stmt {
        Stmt::Insert {
            values,
            on_conflict,
            ..
        } => {
            for row in values.iter_mut() {
                for val in row.iter_mut() {
                    if let Value::Placeholder(idx) = val {
                        *val = params[*idx].clone();
                    }
                }
            }
            if let Some(oc) = on_conflict {
                for (_, ocv) in oc.assignments.iter_mut() {
                    if let OnConflictValue::Literal(val) = ocv {
                        if let Value::Placeholder(idx) = val {
                            *val = params[*idx].clone();
                        }
                    }
                }
            }
        }
        Stmt::Select {
            where_clause,
            limit,
            offset,
            ..
        } => {
            // 先计数（bind_where 会替换占位符，之后无法计数）
            let where_ph_count = count_where_placeholders(where_clause);
            if let Some(wc) = where_clause {
                bind_where(wc, params);
            }
            // LIMIT/OFFSET 哨兵替换：从 params 中取整数值
            let mut idx = where_ph_count;
            if *limit == Some(LIMIT_PLACEHOLDER) {
                *limit = Some(resolve_limit_param(&params[idx], "LIMIT")?);
                idx += 1;
            }
            if *offset == Some(LIMIT_PLACEHOLDER) {
                *offset = Some(resolve_limit_param(&params[idx], "OFFSET")?);
            }
        }
        Stmt::Update {
            assignments,
            where_clause,
            limit,
            ..
        } => {
            // 先计数 SET + WHERE 占位符（绑定前）
            let mut set_count = 0usize;
            for (_, expr) in assignments.iter() {
                match expr {
                    SetExpr::Literal(val) if matches!(val, Value::Placeholder(_)) => set_count += 1,
                    SetExpr::ColumnArith(_, _, val) if matches!(val, Value::Placeholder(_)) => {
                        set_count += 1
                    }
                    _ => {}
                }
            }
            let where_ph_count = count_where_placeholders(where_clause);
            // 绑定 SET
            for (_, expr) in assignments.iter_mut() {
                match expr {
                    SetExpr::Literal(val) => {
                        if let Value::Placeholder(idx) = val {
                            *val = params[*idx].clone();
                        }
                    }
                    SetExpr::ColumnArith(_, _, val) => {
                        if let Value::Placeholder(idx) = val {
                            *val = params[*idx].clone();
                        }
                    }
                    SetExpr::ColumnRef(_, _) => {}
                }
            }
            if let Some(wc) = where_clause {
                bind_where(wc, params);
            }
            // LIMIT 哨兵替换
            if *limit == Some(LIMIT_PLACEHOLDER) {
                let idx = set_count + where_ph_count;
                *limit = Some(resolve_limit_param(&params[idx], "LIMIT")?);
            }
        }
        Stmt::Delete {
            where_clause: Some(wc),
            ..
        } => {
            bind_where(wc, params);
        }
        _ => {}
    }
    Ok(())
}

/// 计算 WhereExpr 中的占位符数量（不修改 AST）。
fn count_where_placeholders(wc: &Option<WhereExpr>) -> usize {
    match wc {
        Some(expr) => count_where_ph(expr),
        None => 0,
    }
}

fn count_where_ph(expr: &WhereExpr) -> usize {
    match expr {
        WhereExpr::And(children) | WhereExpr::Or(children) => {
            children.iter().map(count_where_ph).sum()
        }
        WhereExpr::Leaf(cond) => {
            let mut n = 0;
            if matches!(cond.value, Value::Placeholder(_)) {
                n += 1;
            }
            n += cond
                .in_values
                .iter()
                .filter(|v| matches!(v, Value::Placeholder(_)))
                .count();
            if matches!(cond.value_high, Some(Value::Placeholder(_))) {
                n += 1;
            }
            n
        }
    }
}

/// 递归替换 WhereExpr 中的占位符。
fn bind_where(expr: &mut WhereExpr, params: &[Value]) {
    match expr {
        WhereExpr::And(children) | WhereExpr::Or(children) => {
            for child in children.iter_mut() {
                bind_where(child, params);
            }
        }
        WhereExpr::Leaf(cond) => {
            bind_cond(cond, params);
        }
    }
}

/// 替换单个 WhereCondition 中的占位符。
fn bind_cond(cond: &mut WhereCondition, params: &[Value]) {
    if let Value::Placeholder(idx) = &cond.value {
        cond.value = params[*idx].clone();
    }
    for val in cond.in_values.iter_mut() {
        if let Value::Placeholder(idx) = val {
            *val = params[*idx].clone();
        }
    }
    if let Some(ref mut high) = cond.value_high {
        if let Value::Placeholder(idx) = high {
            *high = params[*idx].clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse;

    #[test]
    fn bind_insert_values() {
        let mut stmt = parse("INSERT INTO t (id, name) VALUES (?, ?)").unwrap();
        bind_params(&mut stmt, &[Value::Integer(1), Value::Text("Alice".into())]).unwrap();
        if let Stmt::Insert { values, .. } = &stmt {
            assert_eq!(values[0][0], Value::Integer(1));
            assert_eq!(values[0][1], Value::Text("Alice".into()));
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn bind_select_where() {
        let mut stmt = parse("SELECT * FROM t WHERE id = ? AND name = ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(42), Value::Text("Bob".into())]).unwrap();
        if let Stmt::Select { where_clause, .. } = &stmt {
            let wc = where_clause.as_ref().unwrap();
            if let WhereExpr::And(children) = wc {
                if let WhereExpr::Leaf(c) = &children[0] {
                    assert_eq!(c.value, Value::Integer(42));
                }
                if let WhereExpr::Leaf(c) = &children[1] {
                    assert_eq!(c.value, Value::Text("Bob".into()));
                }
            } else {
                panic!("expected And");
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn bind_update_set_and_where() {
        let mut stmt = parse("UPDATE t SET name = ? WHERE id = ?").unwrap();
        bind_params(
            &mut stmt,
            &[Value::Text("Charlie".into()), Value::Integer(1)],
        )
        .unwrap();
        if let Stmt::Update {
            assignments,
            where_clause,
            ..
        } = &stmt
        {
            if let SetExpr::Literal(val) = &assignments[0].1 {
                assert_eq!(*val, Value::Text("Charlie".into()));
            }
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Integer(1));
            }
        } else {
            panic!("expected Update");
        }
    }

    #[test]
    fn bind_delete_where() {
        let mut stmt = parse("DELETE FROM t WHERE id = ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(99)]).unwrap();
        if let Stmt::Delete { where_clause, .. } = &stmt {
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Integer(99));
            }
        } else {
            panic!("expected Delete");
        }
    }

    #[test]
    fn bind_param_count_mismatch_too_few() {
        let mut stmt = parse("SELECT * FROM t WHERE id = ? AND name = ?").unwrap();
        let result = bind_params(&mut stmt, &[Value::Integer(1)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mismatch"));
    }

    #[test]
    fn bind_param_count_mismatch_too_many() {
        let mut stmt = parse("SELECT * FROM t WHERE id = ?").unwrap();
        let result = bind_params(&mut stmt, &[Value::Integer(1), Value::Integer(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn bind_no_placeholders_no_params() {
        let mut stmt = parse("SELECT * FROM t WHERE id = 1").unwrap();
        bind_params(&mut stmt, &[]).unwrap();
    }

    #[test]
    fn bind_in_list_placeholders() {
        let mut stmt = parse("SELECT * FROM t WHERE id IN (?, ?, ?)").unwrap();
        bind_params(
            &mut stmt,
            &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        )
        .unwrap();
        if let Stmt::Select { where_clause, .. } = &stmt {
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.in_values.len(), 3);
                assert_eq!(c.in_values[0], Value::Integer(1));
                assert_eq!(c.in_values[2], Value::Integer(3));
            }
        }
    }

    #[test]
    fn bind_between_placeholders() {
        let mut stmt = parse("SELECT * FROM t WHERE id BETWEEN ? AND ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(10), Value::Integer(20)]).unwrap();
        if let Stmt::Select { where_clause, .. } = &stmt {
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Integer(10));
                assert_eq!(c.value_high, Some(Value::Integer(20)));
            }
        }
    }

    #[test]
    fn bind_multi_row_insert() {
        let mut stmt = parse("INSERT INTO t (id, name) VALUES (?, ?), (?, ?)").unwrap();
        bind_params(
            &mut stmt,
            &[
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(2),
                Value::Text("B".into()),
            ],
        )
        .unwrap();
        if let Stmt::Insert { values, .. } = &stmt {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0][0], Value::Integer(1));
            assert_eq!(values[1][1], Value::Text("B".into()));
        }
    }

    #[test]
    fn bind_select_limit_placeholder() {
        let mut stmt = parse("SELECT * FROM t LIMIT ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(10)]).unwrap();
        if let Stmt::Select { limit, .. } = &stmt {
            assert_eq!(*limit, Some(10));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn bind_select_limit_offset_placeholder() {
        let mut stmt = parse("SELECT * FROM t LIMIT ? OFFSET ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(20), Value::Integer(5)]).unwrap();
        if let Stmt::Select { limit, offset, .. } = &stmt {
            assert_eq!(*limit, Some(20));
            assert_eq!(*offset, Some(5));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn bind_select_where_and_limit() {
        let mut stmt = parse("SELECT * FROM t WHERE id = ? LIMIT ?").unwrap();
        bind_params(&mut stmt, &[Value::Integer(42), Value::Integer(10)]).unwrap();
        if let Stmt::Select {
            where_clause,
            limit,
            ..
        } = &stmt
        {
            assert_eq!(*limit, Some(10));
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Integer(42));
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn bind_select_where_limit_offset() {
        let mut stmt =
            parse("SELECT * FROM t WHERE name = ? ORDER BY id LIMIT ? OFFSET ?").unwrap();
        bind_params(
            &mut stmt,
            &[
                Value::Text("x".into()),
                Value::Integer(50),
                Value::Integer(10),
            ],
        )
        .unwrap();
        if let Stmt::Select {
            where_clause,
            limit,
            offset,
            ..
        } = &stmt
        {
            assert_eq!(*limit, Some(50));
            assert_eq!(*offset, Some(10));
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Text("x".into()));
            }
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn bind_limit_non_integer_errors() {
        let mut stmt = parse("SELECT * FROM t LIMIT ?").unwrap();
        let result = bind_params(&mut stmt, &[Value::Text("10".into())]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be an integer"));
    }

    #[test]
    fn bind_limit_negative_errors() {
        let mut stmt = parse("SELECT * FROM t LIMIT ?").unwrap();
        let result = bind_params(&mut stmt, &[Value::Integer(-1)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-negative"));
    }

    #[test]
    fn bind_offset_non_integer_errors() {
        let mut stmt = parse("SELECT * FROM t LIMIT 10 OFFSET ?").unwrap();
        let result = bind_params(&mut stmt, &[Value::Float(5.0)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be an integer"));
    }

    #[test]
    fn bind_update_where_and_limit_placeholder() {
        let mut stmt = parse("UPDATE t SET name = ? WHERE id = ? LIMIT ?").unwrap();
        bind_params(
            &mut stmt,
            &[
                Value::Text("x".into()),
                Value::Integer(1),
                Value::Integer(5),
            ],
        )
        .unwrap();
        if let Stmt::Update {
            assignments,
            where_clause,
            limit,
            ..
        } = &stmt
        {
            assert_eq!(*limit, Some(5));
            if let SetExpr::Literal(val) = &assignments[0].1 {
                assert_eq!(*val, Value::Text("x".into()));
            }
            if let Some(WhereExpr::Leaf(c)) = where_clause.as_ref() {
                assert_eq!(c.value, Value::Integer(1));
            }
        } else {
            panic!("expected Update");
        }
    }
}
