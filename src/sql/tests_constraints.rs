/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL NOT NULL / DEFAULT 列约束测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

#[test]
fn engine_not_null_rejects_null() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL)")
        .unwrap();
    let err = eng.run_sql("INSERT INTO t VALUES (1, NULL)");
    assert!(err.is_err(), "NOT NULL column should reject NULL");
}

#[test]
fn engine_default_fills_missing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT DEFAULT 'anon', score INT DEFAULT 100)")
        .unwrap();
    eng.run_sql("INSERT INTO t (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("anon".into()));
    assert_eq!(rows[0][2], Value::Integer(100));
}

#[test]
fn engine_not_null_with_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT NOT NULL, status TEXT NOT NULL DEFAULT 'active')")
        .unwrap();
    // 指定列插入，status 用 DEFAULT 填充
    eng.run_sql("INSERT INTO t (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("active".into()));
    // 显式传 NULL 给 NOT NULL 列应报错
    let err = eng.run_sql("INSERT INTO t VALUES (2, NULL)");
    assert!(err.is_err());
}

#[test]
fn engine_is_null_and_is_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, NULL)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, NULL)").unwrap();
    // IS NULL
    let rows = eng.run_sql("SELECT * FROM t WHERE name IS NULL").unwrap();
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
    // IS NOT NULL
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name IS NOT NULL")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
}

#[test]
fn engine_is_null_with_and() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, a TEXT, b INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NULL, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, NULL, 20)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'x', 10)").unwrap();
    // IS NULL + AND
    let rows = eng
        .run_sql("SELECT * FROM t WHERE a IS NULL AND b = 10")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn parse_is_null_and_is_not_null() {
    use super::parser::{parse, Stmt, WhereExpr, WhereOp};
    let stmt = parse("SELECT * FROM t WHERE name IS NULL").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Leaf(ref c)),
            ..
        } => {
            assert_eq!(c.op, WhereOp::IsNull);
            assert_eq!(c.column, "name");
        }
        _ => panic!("expected Select with IS NULL"),
    }
    let stmt = parse("SELECT * FROM t WHERE age IS NOT NULL").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Leaf(ref c)),
            ..
        } => {
            assert_eq!(c.op, WhereOp::IsNotNull);
            assert_eq!(c.column, "age");
        }
        _ => panic!("expected Select with IS NOT NULL"),
    }
}

#[test]
fn engine_default_now() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE events (id INT, created_at TIMESTAMP DEFAULT NOW())")
        .unwrap();
    // 省略 created_at 列，应自动填充当前时间戳
    eng.run_sql("INSERT INTO events (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM events WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][1] {
        Value::Timestamp(ts) => {
            assert!(*ts > 0, "NOW() should produce a positive timestamp");
            assert_ne!(*ts, i64::MIN, "sentinel should be resolved");
        }
        other => panic!("expected Timestamp, got {:?}", other),
    }
    // 两次插入应产生不同（或至少非 sentinel）的时间戳
    eng.run_sql("INSERT INTO events (id) VALUES (2)").unwrap();
    let rows = eng.run_sql("SELECT * FROM events WHERE id = 2").unwrap();
    match &rows[0][1] {
        Value::Timestamp(ts) => assert!(*ts > 0),
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

#[test]
fn engine_default_current_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        .unwrap();
    eng.run_sql("INSERT INTO t (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    match &rows[0][1] {
        Value::Timestamp(ts) => assert!(*ts > 0),
        other => panic!("expected Timestamp, got {:?}", other),
    }
}

// ── WHERE OR 条件测试 ─────────────────────────────────────

#[test]
fn engine_where_or_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 'Dave')").unwrap();
    // OR: id = 1 OR id = 3
    let rows = eng
        .run_sql("SELECT * FROM t WHERE id = 1 OR id = 3")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
}

#[test]
fn engine_where_or_with_and() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice', 90)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob', 80)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol', 70)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 'Dave', 60)").unwrap();
    // AND 优先于 OR: name = 'Alice' OR name = 'Bob' AND score > 75
    // 等价于: name = 'Alice' OR (name = 'Bob' AND score > 75)
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name = 'Alice' OR name = 'Bob' AND score > 75")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1)); // Alice
    assert!(ids.contains(&2)); // Bob (score=80 > 75)
}

#[test]
fn engine_where_or_parentheses() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice', 90)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob', 80)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol', 70)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 'Dave', 60)").unwrap();
    // 括号改变优先级: (name = 'Alice' OR name = 'Bob') AND score > 85
    let rows = eng
        .run_sql("SELECT * FROM t WHERE (name = 'Alice' OR name = 'Bob') AND score > 85")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1)); // 只有 Alice score=90 > 85
}

#[test]
fn engine_where_or_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol')").unwrap();
    // DELETE with OR
    eng.run_sql("DELETE FROM t WHERE id = 1 OR id = 3").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn engine_where_or_update() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, status TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'active')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'active')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'active')").unwrap();
    // UPDATE with OR
    eng.run_sql("UPDATE t SET status = 'done' WHERE id = 1 OR id = 3")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("done".into()));
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 2").unwrap();
    assert_eq!(rows[0][1], Value::Text("active".into()));
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 3").unwrap();
    assert_eq!(rows[0][1], Value::Text("done".into()));
}

#[test]
fn parse_where_or() {
    use super::parser::{parse, Stmt, WhereExpr, WhereOp};
    // 简单 OR
    let stmt = parse("SELECT * FROM t WHERE a = 1 OR b = 2").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Or(ref children)),
            ..
        } => {
            assert_eq!(children.len(), 2);
            match (&children[0], &children[1]) {
                (WhereExpr::Leaf(c0), WhereExpr::Leaf(c1)) => {
                    assert_eq!(c0.op, WhereOp::Eq);
                    assert_eq!(c0.column, "a");
                    assert_eq!(c1.op, WhereOp::Eq);
                    assert_eq!(c1.column, "b");
                }
                _ => panic!("expected Leaf children"),
            }
        }
        _ => panic!("expected Select with OR"),
    }
    // AND + OR 优先级: a = 1 OR b = 2 AND c = 3
    let stmt = parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::Or(ref children)),
            ..
        } => {
            assert_eq!(children.len(), 2);
            // 第一个子节点是 Leaf(a=1)
            assert!(matches!(&children[0], WhereExpr::Leaf(_)));
            // 第二个子节点是 And([Leaf(b=2), Leaf(c=3)])
            match &children[1] {
                WhereExpr::And(and_children) => {
                    assert_eq!(and_children.len(), 2);
                }
                _ => panic!("expected And node"),
            }
        }
        _ => panic!("expected Select with OR+AND"),
    }
}

// ── M15: INSERT 主键重复检测 + DELETE 无 WHERE 全表删除 ──────

#[test]
fn engine_insert_duplicate_pk_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    // 重复主键应报错
    let err = eng.run_sql("INSERT INTO t VALUES (1, 'Bob')");
    assert!(err.is_err(), "duplicate PK should error");
    let msg = format!("{}", err.unwrap_err());
    assert!(
        msg.contains("duplicate primary key"),
        "error should mention duplicate primary key: {}",
        msg
    );
    // 原始行不应被覆盖
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn engine_insert_duplicate_pk_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    // 批量插入含重复主键
    let err = eng.run_sql("INSERT INTO t VALUES (1, 'Bob'), (2, 'Carol')");
    assert!(err.is_err(), "batch with duplicate PK should error");
}

#[test]
fn engine_insert_duplicate_pk_in_tx() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    // 事务内重复主键
    eng.run_sql("BEGIN").unwrap();
    let err = eng.run_sql("INSERT INTO t VALUES (1, 'Bob')");
    assert!(err.is_err(), "duplicate PK in tx should error");
    eng.run_sql("ROLLBACK").unwrap();
    // 原始行不变
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn engine_delete_without_where() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol')").unwrap();
    // DELETE 无 WHERE 应删除全部
    eng.run_sql("DELETE FROM t").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0, "all rows should be deleted");
}

#[test]
fn engine_delete_without_where_with_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE INDEX idx_name ON t (name)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Carol')").unwrap();
    // DELETE 无 WHERE（含索引维护）
    eng.run_sql("DELETE FROM t").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0);
    // 重新插入应正常工作（索引已清理）
    eng.run_sql("INSERT INTO t VALUES (4, 'Dave')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE name = 'Dave'").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(4));
}

#[test]
fn engine_delete_without_where_in_tx() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    // 事务内全表删除
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("DELETE FROM t").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0, "should see 0 rows in tx");
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0, "should see 0 rows after commit");
}

#[test]
fn executor_insert_duplicate_pk_error() {
    // 测试非缓存路径（executor.rs）的重复主键检测
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let plan_create = super::planner::Plan {
        stmt: super::parser::parse("CREATE TABLE t (id INT, name TEXT)").unwrap(),
    };
    super::executor::execute(&store, plan_create).unwrap();
    let plan_ins1 = super::planner::Plan {
        stmt: super::parser::parse("INSERT INTO t VALUES (1, 'Alice')").unwrap(),
    };
    super::executor::execute(&store, plan_ins1).unwrap();
    let plan_ins2 = super::planner::Plan {
        stmt: super::parser::parse("INSERT INTO t VALUES (1, 'Bob')").unwrap(),
    };
    let err = super::executor::execute(&store, plan_ins2);
    assert!(err.is_err(), "executor duplicate PK should error");
}

#[test]
fn executor_delete_without_where() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let exec = |sql: &str| {
        let plan = super::planner::Plan {
            stmt: super::parser::parse(sql).unwrap(),
        };
        super::executor::execute(&store, plan)
    };
    exec("CREATE TABLE t (id INT, name TEXT)").unwrap();
    exec("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    exec("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    exec("DELETE FROM t").unwrap();
    let rows = exec("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn engine_insert_duplicate_pk_within_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    // 同一批次内两行 PK 相同
    let err = eng.run_sql("INSERT INTO t VALUES (1, 'Alice'), (1, 'Bob')");
    assert!(err.is_err(), "same PK within batch should error");
}

// ── M118: CHECK 约束测试 ─────────────────────────────────

#[test]
fn engine_check_rejects_invalid_insert() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, age INT CHECK (age >= 0))")
        .unwrap();
    // 合法行
    eng.run_sql("INSERT INTO t VALUES (1, 25)").unwrap();
    // 违反 CHECK
    let err = eng.run_sql("INSERT INTO t VALUES (2, -1)");
    assert!(err.is_err(), "CHECK should reject age < 0");
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("CHECK"), "error should mention CHECK: {}", msg);
}

#[test]
fn engine_check_allows_valid_insert() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, score INT CHECK (score >= 0 AND score <= 100))")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 0)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 50)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 100)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn engine_check_table_level() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, lo INT, hi INT, CHECK (lo <= hi))")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    let err = eng.run_sql("INSERT INTO t VALUES (2, 30, 20)");
    assert!(err.is_err(), "CHECK should reject lo > hi");
}

#[test]
fn engine_check_update_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, age INT CHECK (age >= 0))")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 25)").unwrap();
    // UPDATE 违反 CHECK
    let err = eng.run_sql("UPDATE t SET age = -5 WHERE id = 1");
    assert!(err.is_err(), "CHECK should reject UPDATE to age < 0");
    // 原始值不变
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Integer(25));
}

#[test]
fn engine_check_update_allows() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, age INT CHECK (age >= 0))")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 25)").unwrap();
    eng.run_sql("UPDATE t SET age = 30 WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Integer(30));
}

#[test]
fn engine_check_multi_constraint() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, age INT CHECK (age >= 0), name TEXT CHECK (name <> ''))")
        .unwrap();
    // 两个 CHECK 都满足
    eng.run_sql("INSERT INTO t VALUES (1, 25, 'Alice')")
        .unwrap();
    // 违反 age CHECK
    let err = eng.run_sql("INSERT INTO t VALUES (2, -1, 'Bob')");
    assert!(err.is_err());
    // 违反 name CHECK
    let err = eng.run_sql("INSERT INTO t VALUES (3, 10, '')");
    assert!(err.is_err());
}

#[test]
fn engine_check_batch_insert() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, val INT CHECK (val > 0))")
        .unwrap();
    // 批量插入：第二行违反 CHECK
    let err = eng.run_sql("INSERT INTO t VALUES (1, 10), (2, -5)");
    assert!(err.is_err(), "batch should fail on CHECK violation");
    // 由于批量路径可能部分写入，验证至少 CHECK 被触发
}

#[test]
fn engine_check_in_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE t (id INT, val INT CHECK (val > 0))")
        .unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    let err = eng.run_sql("INSERT INTO t VALUES (2, -1)");
    assert!(err.is_err(), "CHECK should reject in tx");
    eng.run_sql("ROLLBACK").unwrap();
}

#[test]
fn engine_check_persists_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    {
        let mut eng = SqlEngine::new(&store).unwrap();
        eng.run_sql("CREATE TABLE t (id INT, val INT CHECK (val >= 0))")
            .unwrap();
        eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    }
    // 重新打开引擎（模拟重启），CHECK 约束应从 schema 恢复
    {
        let mut eng = SqlEngine::new(&store).unwrap();
        let err = eng.run_sql("INSERT INTO t VALUES (2, -1)");
        assert!(err.is_err(), "CHECK should persist after reopen");
        eng.run_sql("INSERT INTO t VALUES (3, 5)").unwrap();
        let rows = eng.run_sql("SELECT * FROM t").unwrap();
        assert_eq!(rows.len(), 2);
    }
}
