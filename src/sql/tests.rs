/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 引擎基础单元测试：CRUD、索引、快速路径、parser。

use super::engine::SqlEngine;
use super::parser::{parse, AlterAction, JoinType, Stmt, WhereExpr, WhereOp};
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

#[test]
fn engine_create_insert_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE users (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    eng.run_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM users").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_select_by_pk() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (10, 'ten')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (20, 'twenty')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 10").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("ten".into()));
}

#[test]
fn engine_update_delete() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql("UPDATE t SET v = 'new' WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("new".into()));
    eng.run_sql("DELETE FROM t WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert!(rows.is_empty());
}

#[test]
fn engine_drop_table_if_exists() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("DROP TABLE IF EXISTS ghost").unwrap();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    eng.run_sql("DROP TABLE t").unwrap();
    assert!(eng.run_sql("SELECT * FROM t").is_err());
}

#[test]
fn engine_count_star() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, x TEXT)").unwrap();
    for i in 0..5 {
        eng.run_sql(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i))
            .unwrap();
    }
    let rows = eng.run_sql("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(5)]]);
}

#[test]
fn engine_order_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'c')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id ASC").unwrap();
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[2][0], Value::Integer(3));
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id DESC").unwrap();
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[test]
fn engine_multi_condition_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, age INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 20, 'Alice')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 30, 'Bob')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 20, 'Carol')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE age = 20 AND name = 'Carol'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[test]
fn engine_comparison_operators() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, score INT)").unwrap();
    for i in 1..=5 {
        eng.run_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    let rows = eng.run_sql("SELECT * FROM t WHERE score > 30").unwrap();
    assert_eq!(rows.len(), 2);
    let rows = eng.run_sql("SELECT * FROM t WHERE score <= 20").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_create_index_and_lookup() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, city TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Beijing')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Shanghai')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'Beijing')").unwrap();
    eng.run_sql("CREATE INDEX idx_city ON t (city)").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE city = 'Beijing'")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn engine_multi_row_insert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn engine_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    for i in 0..10 {
        eng.run_sql(&format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
    }
    let rows = eng.run_sql("SELECT * FROM t LIMIT 3").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn engine_trailing_semicolon() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT);").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1);").unwrap();
    let rows = eng.run_sql("SELECT * FROM t;").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn engine_create_table_if_not_exists() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    eng.run_sql("CREATE TABLE IF NOT EXISTS t (id INT)")
        .unwrap();
}

#[test]
fn engine_escaped_quotes() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, s TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'it''s ok')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("it's ok".into()));
}

#[test]
fn fast_path_select_by_pk() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (42, 'fast')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 42").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("fast".into()));
}

#[test]
fn fast_path_insert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (99, 'speed')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE id = 99").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("speed".into()));
}

// ── parser 单元测试 ──────────────────────────────────────

#[test]
fn parse_insert_without_columns() {
    let stmt = parse("INSERT INTO t VALUES (1, 'hello')").unwrap();
    match stmt {
        Stmt::Insert {
            table,
            columns,
            values,
            ..
        } => {
            assert_eq!(table, "t");
            assert!(columns.is_empty());
            assert_eq!(values.len(), 1);
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_multi_row_values() {
    let stmt = parse("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    match stmt {
        Stmt::Insert { values, .. } => assert_eq!(values.len(), 2),
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_drop_table() {
    let stmt = parse("DROP TABLE IF EXISTS t").unwrap();
    match stmt {
        Stmt::DropTable { name, if_exists } => {
            assert_eq!(name, "t");
            assert!(if_exists);
        }
        _ => panic!("expected DropTable"),
    }
}

#[test]
fn parse_where_multi_and() {
    let stmt = parse("SELECT * FROM t WHERE a = 1 AND b > 2").unwrap();
    match stmt {
        Stmt::Select {
            where_clause: Some(WhereExpr::And(ref children)),
            ..
        } => {
            assert_eq!(children.len(), 2);
            match (&children[0], &children[1]) {
                (WhereExpr::Leaf(c0), WhereExpr::Leaf(c1)) => {
                    assert_eq!(c0.op, WhereOp::Eq);
                    assert_eq!(c1.op, WhereOp::Gt);
                }
                _ => panic!("expected Leaf children"),
            }
        }
        _ => panic!("expected Select with WHERE"),
    }
}

#[test]
fn parse_order_by_desc() {
    let stmt = parse("SELECT * FROM t ORDER BY id DESC").unwrap();
    match stmt {
        Stmt::Select {
            order_by: Some(ref ob),
            ..
        } => {
            assert_eq!(ob.len(), 1);
            assert_eq!(ob[0].0, "id");
            assert!(ob[0].1);
        }
        _ => panic!("expected Select with ORDER BY"),
    }
}

#[test]
fn parse_count_star() {
    let stmt = parse("SELECT COUNT(*) FROM t").unwrap();
    match stmt {
        Stmt::Select { columns, .. } => {
            assert!(columns[0].to_uppercase().contains("COUNT("));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn parse_insert_or_replace() {
    let stmt = parse("INSERT OR REPLACE INTO t VALUES (1, 'x')").unwrap();
    match stmt {
        Stmt::Insert {
            table, or_replace, ..
        } => {
            assert_eq!(table, "t");
            assert!(or_replace);
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn parse_varchar_type() {
    let stmt = parse("CREATE TABLE t (id INT, name VARCHAR(255))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, crate::types::ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn engine_show_tables() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    // 空库
    let rows = eng.run_sql("SHOW TABLES").unwrap();
    assert!(rows.is_empty());
    // 建两张表
    eng.run_sql("CREATE TABLE alpha (id INT, x TEXT)").unwrap();
    eng.run_sql("CREATE TABLE beta (id INT, y INT)").unwrap();
    let rows = eng.run_sql("SHOW TABLES").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("alpha".into()));
    assert_eq!(rows[1][0], Value::Text("beta".into()));
    // 删表后减少
    eng.run_sql("DROP TABLE alpha").unwrap();
    let rows = eng.run_sql("SHOW TABLES").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("beta".into()));
}

#[test]
fn engine_describe_table() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE users (id INT NOT NULL, name TEXT, score FLOAT DEFAULT 2.5)")
        .unwrap();
    let rows = eng.run_sql("DESCRIBE users").unwrap();
    assert_eq!(rows.len(), 3);
    // 列名
    assert_eq!(rows[0][0], Value::Text("id".into()));
    assert_eq!(rows[1][0], Value::Text("name".into()));
    assert_eq!(rows[2][0], Value::Text("score".into()));
    // 主键标记
    assert_eq!(rows[0][2], Value::Text("YES".into()));
    assert_eq!(rows[1][2], Value::Text("NO".into()));
    // Nullable
    assert_eq!(rows[0][3], Value::Text("NO".into())); // NOT NULL
    assert_eq!(rows[1][3], Value::Text("YES".into()));
    // Default
    assert_eq!(rows[0][4], Value::Null);
    assert_eq!(rows[2][4], Value::Float(2.5));
    // DESC 别名也能用
    let rows2 = eng.run_sql("DESC users").unwrap();
    assert_eq!(rows2.len(), 3);
    // 不存在的表报错
    assert!(eng.run_sql("DESCRIBE nonexist").is_err());
    // 反引号包裹的表名也能正确 DESCRIBE（回归：快速路径曾遗漏 unquote）
    let rows3 = eng.run_sql("DESCRIBE `users`").unwrap();
    assert_eq!(rows3.len(), 3);
    assert_eq!(rows3[0][0], Value::Text("id".into()));
    let rows4 = eng.run_sql("DESC `users`").unwrap();
    assert_eq!(rows4.len(), 3);
}

#[test]
fn select_column_projection() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE items (id INT, name TEXT, price FLOAT)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, 'apple', 1.5)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (2, 'banana', 2.0)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (3, 'cherry', 3.5)")
        .unwrap();
    // SELECT 指定列
    let rows = eng
        .run_sql("SELECT name, price FROM items ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].len(), 2); // 只有 2 列
    assert_eq!(rows[0][0], Value::Text("apple".into()));
    assert_eq!(rows[0][1], Value::Float(1.5));
    // SELECT 单列
    let rows = eng.run_sql("SELECT name FROM items WHERE id = 2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    assert_eq!(rows[0][0], Value::Text("banana".into()));
    // SELECT * 仍返回全部列
    let rows = eng.run_sql("SELECT * FROM items WHERE id = 1").unwrap();
    assert_eq!(rows[0].len(), 3);
}

#[test]
fn select_aggregates_sum_avg_min_max() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::storage::Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE scores (id INT, name TEXT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO scores VALUES (1, 'a', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO scores VALUES (2, 'b', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO scores VALUES (3, 'c', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO scores VALUES (4, 'd', 40)")
        .unwrap();
    // COUNT(*)
    let rows = eng.run_sql("SELECT COUNT(*) FROM scores").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(4)]]);
    // SUM
    let rows = eng.run_sql("SELECT SUM(score) FROM scores").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(100)]]);
    // AVG
    let rows = eng.run_sql("SELECT AVG(score) FROM scores").unwrap();
    assert_eq!(rows, vec![vec![Value::Float(25.0)]]);
    // MIN
    let rows = eng.run_sql("SELECT MIN(score) FROM scores").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(10)]]);
    // MAX
    let rows = eng.run_sql("SELECT MAX(score) FROM scores").unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(40)]]);
    // 多聚合
    let rows = eng
        .run_sql("SELECT COUNT(*), SUM(score), AVG(score) FROM scores")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(4));
    assert_eq!(rows[0][1], Value::Integer(100));
    assert_eq!(rows[0][2], Value::Float(25.0));
    // 带 WHERE 的聚合
    let rows = eng
        .run_sql("SELECT SUM(score) FROM scores WHERE score > 15")
        .unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(90)]]);
}
#[test]
fn engine_multi_column_order_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, age INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 20, 'Charlie')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 30, 'Alice')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 20, 'Alice')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 30, 'Bob')").unwrap();
    // 多列排序：age ASC, name ASC
    let rows = eng
        .run_sql("SELECT * FROM t ORDER BY age ASC, name ASC")
        .unwrap();
    assert_eq!(rows.len(), 4);
    // age=20 组：Alice(id=3) < Charlie(id=1)
    assert_eq!(rows[0][0], Value::Integer(3));
    assert_eq!(rows[1][0], Value::Integer(1));
    // age=30 组：Alice(id=2) < Bob(id=4)
    assert_eq!(rows[2][0], Value::Integer(2));
    assert_eq!(rows[3][0], Value::Integer(4));
    // 多列排序：age DESC, name DESC
    let rows = eng
        .run_sql("SELECT * FROM t ORDER BY age DESC, name DESC")
        .unwrap();
    // age=30 组先出：Bob(id=4) > Alice(id=2)
    assert_eq!(rows[0][0], Value::Integer(4));
    assert_eq!(rows[1][0], Value::Integer(2));
    // age=20 组：Charlie(id=1) > Alice(id=3)
    assert_eq!(rows[2][0], Value::Integer(1));
    assert_eq!(rows[3][0], Value::Integer(3));
}

#[test]
fn update_set_column_arith_add() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE counters (id INT, count INT, score FLOAT)")
        .unwrap();
    eng.run_sql("INSERT INTO counters VALUES (1, 10, 3.5)")
        .unwrap();
    eng.run_sql("UPDATE counters SET count = count + 5 WHERE id = 1")
        .unwrap();
    let rows = eng
        .run_sql("SELECT count FROM counters WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(15));
}

#[test]
fn update_set_column_arith_sub() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    eng.run_sql("UPDATE t SET val = val - 30 WHERE id = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Integer(70));
}

#[test]
fn update_set_column_arith_mul_float() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, price FLOAT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 9.5)").unwrap();
    eng.run_sql("UPDATE t SET price = price * 2 WHERE id = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT price FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Float(19.0));
}

#[test]
fn update_set_column_arith_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NULL)").unwrap();
    eng.run_sql("UPDATE t SET val = val + 1 WHERE id = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn update_set_column_arith_div_zero() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 42)").unwrap();
    let err = eng
        .run_sql("UPDATE t SET val = val / 0 WHERE id = 1")
        .unwrap_err();
    assert!(err.to_string().contains("除零"), "应报除零错误: {}", err);
}

#[test]
fn update_set_literal_still_works() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql("UPDATE t SET name = 'new' WHERE id = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Text("new".into()));
}

// ── 参数化查询 (run_sql_param) ──

#[test]
fn param_insert_and_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql_param(
        "INSERT INTO t (id, name) VALUES (?, ?)",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let rows = eng
        .run_sql_param("SELECT * FROM t WHERE id = ?", &[Value::Integer(1)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn param_update_and_delete() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql_param(
        "UPDATE t SET name = ? WHERE id = ?",
        &[Value::Text("new".into()), Value::Integer(1)],
    )
    .unwrap();
    let rows = eng.run_sql("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Text("new".into()));

    eng.run_sql_param("DELETE FROM t WHERE id = ?", &[Value::Integer(1)])
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn param_multi_row_insert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql_param(
        "INSERT INTO t (id, name) VALUES (?, ?), (?, ?)",
        &[
            Value::Integer(1),
            Value::Text("A".into()),
            Value::Integer(2),
            Value::Text("B".into()),
        ],
    )
    .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn param_count_mismatch_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    let err = eng
        .run_sql_param("SELECT * FROM t WHERE id = ?", &[])
        .unwrap_err();
    assert!(
        err.to_string().contains("mismatch"),
        "应报参数数量不匹配: {}",
        err
    );
}

#[test]
fn param_no_placeholders_empty_params() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    let rows = eng
        .run_sql_param("SELECT * FROM t WHERE id = 1", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn param_where_in_list() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'A')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'B')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'C')").unwrap();
    let rows = eng
        .run_sql_param(
            "SELECT * FROM t WHERE id IN (?, ?)",
            &[Value::Integer(1), Value::Integer(3)],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn param_where_between() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    for i in 1..=5 {
        eng.run_sql(&format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
    }
    let rows = eng
        .run_sql_param(
            "SELECT * FROM t WHERE id BETWEEN ? AND ?",
            &[Value::Integer(2), Value::Integer(4)],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn param_pg_dollar_placeholders() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql_param(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let rows = eng
        .run_sql_param("SELECT * FROM t WHERE id = $1", &[Value::Integer(1)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn param_pg_dollar_mixed_with_string() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val TEXT)").unwrap();
    eng.run_sql_param(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Integer(1), Value::Text("price is $5".into())],
    )
    .unwrap();
    let rows = eng
        .run_sql_param("SELECT * FROM t WHERE id = $1", &[Value::Integer(1)])
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn insert_returning_star() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    let rows = eng
        .run_sql("INSERT INTO t VALUES (1, 'Alice') RETURNING *")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_returning_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT, score INT)")
        .unwrap();
    let rows = eng
        .run_sql("INSERT INTO t VALUES (1, 'Bob', 95) RETURNING id, score")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Integer(95));
}

#[test]
fn update_returning_star() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    let rows = eng
        .run_sql("UPDATE t SET name = 'Bob' WHERE id = 1 RETURNING *")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Bob".into()));
}

// ── M102: INSERT INTO ... SELECT ──

#[test]
fn insert_into_select_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE src (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE TABLE dst (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    eng.run_sql("INSERT INTO dst SELECT * FROM src").unwrap();
    let rows = eng.run_sql("SELECT * FROM dst ORDER BY id").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
    assert_eq!(rows[2][1], Value::Text("Charlie".into()));
}

#[test]
fn insert_into_select_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE src (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE TABLE dst (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    eng.run_sql("INSERT INTO dst SELECT * FROM src WHERE id > 1")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM dst ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn insert_into_select_with_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE src (id INT, name TEXT, age INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE dst (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        .unwrap();
    eng.run_sql("INSERT INTO dst (id, name) SELECT id, name FROM src")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM dst ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

#[test]
fn insert_into_select_self_copy() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    // 自引用 + OR IGNORE：SELECT 先全量获取再 INSERT，PK 冲突静默跳过
    eng.run_sql("INSERT OR IGNORE INTO t SELECT * FROM t")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2); // 无新增（PK 冲突全部跳过）
}

#[test]
fn insert_or_replace_into_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE src (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE TABLE dst (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO dst VALUES (1, 'Old')").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'New'), (2, 'Bob')")
        .unwrap();
    eng.run_sql("INSERT OR REPLACE INTO dst SELECT * FROM src")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM dst ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("New".into()));
    assert_eq!(rows[1][1], Value::Text("Bob".into()));
}

#[test]
fn insert_or_ignore_into_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE src (id INT, name TEXT)").unwrap();
    eng.run_sql("CREATE TABLE dst (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO dst VALUES (1, 'Keep')").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'Overwrite'), (2, 'New')")
        .unwrap();
    eng.run_sql("INSERT OR IGNORE INTO dst SELECT * FROM src")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM dst ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("Keep".into()));
    assert_eq!(rows[1][1], Value::Text("New".into()));
}

#[test]
fn insert_into_select_parser() {
    let stmt = parse("INSERT INTO dst SELECT * FROM src WHERE id > 1").unwrap();
    match stmt {
        Stmt::Insert {
            table,
            source_select,
            ..
        } => {
            assert_eq!(table, "dst");
            assert!(source_select.is_some());
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn insert_into_select_with_cols_parser() {
    let stmt = parse("INSERT INTO dst (a, b) SELECT x, y FROM src").unwrap();
    match stmt {
        Stmt::Insert {
            table,
            columns,
            source_select,
            ..
        } => {
            assert_eq!(table, "dst");
            assert_eq!(columns, vec!["a", "b"]);
            assert!(source_select.is_some());
        }
        _ => panic!("expected Insert"),
    }
}

// ===== M103: ALTER TABLE RENAME TO =====

#[test]
fn alter_table_rename_to_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE old_t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO old_t VALUES (1, 'alice')")
        .unwrap();
    eng.run_sql("INSERT INTO old_t VALUES (2, 'bob')").unwrap();
    eng.run_sql("ALTER TABLE old_t RENAME TO new_t").unwrap();
    // 新表可查
    let rows = eng.run_sql("SELECT * FROM new_t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("alice".into()));
    assert_eq!(rows[1][1], Value::Text("bob".into()));
    // 旧表不存在
    assert!(eng.run_sql("SELECT * FROM old_t").is_err());
    // SHOW TABLES 只有 new_t
    let tables = eng.run_sql("SHOW TABLES").unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0][0], Value::Text("new_t".into()));
}

#[test]
fn alter_table_rename_to_with_index() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE idx_t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_val ON idx_t(val)").unwrap();
    eng.run_sql("INSERT INTO idx_t VALUES (1, 'x')").unwrap();
    eng.run_sql("INSERT INTO idx_t VALUES (2, 'y')").unwrap();
    eng.run_sql("ALTER TABLE idx_t RENAME TO idx_t2").unwrap();
    // 数据完整
    let rows = eng.run_sql("SELECT * FROM idx_t2 ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    // 索引迁移：SHOW INDEXES 应显示新表名
    let idxs = eng.run_sql("SHOW INDEXES ON idx_t2").unwrap();
    assert_eq!(idxs.len(), 1);
    assert_eq!(idxs[0][1], Value::Text("idx_t2".into()));
}

#[test]
fn alter_table_rename_to_nonexistent() {
    let (_dir, mut eng) = tmp_engine();
    let r = eng.run_sql("ALTER TABLE no_such RENAME TO new_name");
    assert!(r.is_err());
    let msg = format!("{}", r.unwrap_err());
    assert!(msg.contains("不存在"));
}

#[test]
fn alter_table_rename_to_existing_target() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    let r = eng.run_sql("ALTER TABLE t1 RENAME TO t2");
    assert!(r.is_err());
    let msg = format!("{}", r.unwrap_err());
    assert!(msg.contains("已存在"));
}

#[test]
fn alter_table_rename_to_empty_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE empty_t (id INTEGER, v TEXT)")
        .unwrap();
    eng.run_sql("ALTER TABLE empty_t RENAME TO empty_t2")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM empty_t2").unwrap();
    assert_eq!(rows.len(), 0);
    // 可以正常插入
    eng.run_sql("INSERT INTO empty_t2 VALUES (1, 'ok')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM empty_t2").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn alter_table_rename_to_parser() {
    let stmt = parse("ALTER TABLE foo RENAME TO bar").unwrap();
    match stmt {
        Stmt::AlterTable { table, action } => {
            assert_eq!(table, "foo");
            match action {
                AlterAction::RenameTo { new_name } => assert_eq!(new_name, "bar"),
                _ => panic!("expected RenameTo"),
            }
        }
        _ => panic!("expected AlterTable"),
    }
}

// ===== M104: AUTOINCREMENT =====

#[test]
fn autoincrement_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'bob')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
}

#[test]
fn autoincrement_explicit_id() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (10, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'b')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(10));
    // 自增应从 10 之后继续
    assert_eq!(rows[1][0], Value::Integer(11));
}

#[test]
fn autoincrement_multi_row() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'a'), (NULL, 'b'), (NULL, 'c')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[2][0], Value::Integer(3));
}

#[test]
fn autoincrement_no_reuse_after_delete() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'a')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'b')").unwrap();
    eng.run_sql("DELETE FROM t WHERE id = 2").unwrap();
    eng.run_sql("INSERT INTO t VALUES (NULL, 'c')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    // ID 2 不回收，新行应为 3
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn autoincrement_with_column_list() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t (name, age) VALUES ('alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t (name, age) VALUES ('bob', 25)")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
}

#[test]
fn autoincrement_parser() {
    let stmt = parse("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert!(columns[0].auto_increment);
            assert!(!columns[1].auto_increment);
        }
        _ => panic!("expected CreateTable"),
    }
}

// ===== M105: DELETE ... RETURNING =====

#[test]
fn delete_returning_star() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr1 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO dr1 VALUES (1, 'alice')").unwrap();
    eng.run_sql("INSERT INTO dr1 VALUES (2, 'bob')").unwrap();
    let rows = eng
        .run_sql("DELETE FROM dr1 WHERE id = 1 RETURNING *")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Text("alice".into()));
    // 确认行已删除
    let remain = eng.run_sql("SELECT * FROM dr1").unwrap();
    assert_eq!(remain.len(), 1);
}

#[test]
fn delete_returning_specific_cols() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr2 (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO dr2 VALUES (1, 'alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO dr2 VALUES (2, 'bob', 25)")
        .unwrap();
    let rows = eng
        .run_sql("DELETE FROM dr2 WHERE id = 2 RETURNING name, age")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("bob".into()));
    assert_eq!(rows[0][1], Value::Integer(25));
}

#[test]
fn delete_returning_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr3 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO dr3 VALUES (1, 'alice')").unwrap();
    let rows = eng
        .run_sql("DELETE FROM dr3 WHERE id = 999 RETURNING *")
        .unwrap();
    assert_eq!(rows.len(), 0);
    let remain = eng.run_sql("SELECT * FROM dr3").unwrap();
    assert_eq!(remain.len(), 1);
}

#[test]
fn delete_returning_all_rows() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr4 (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO dr4 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO dr4 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO dr4 VALUES (3, 'c')").unwrap();
    let rows = eng.run_sql("DELETE FROM dr4 RETURNING *").unwrap();
    assert_eq!(rows.len(), 3);
    let remain = eng.run_sql("SELECT * FROM dr4").unwrap();
    assert_eq!(remain.len(), 0);
}

#[test]
fn delete_returning_with_where_multi() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr5 (id INTEGER, score INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO dr5 VALUES (1, 80)").unwrap();
    eng.run_sql("INSERT INTO dr5 VALUES (2, 60)").unwrap();
    eng.run_sql("INSERT INTO dr5 VALUES (3, 90)").unwrap();
    let rows = eng
        .run_sql("DELETE FROM dr5 WHERE score < 85 RETURNING id")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let remain = eng.run_sql("SELECT * FROM dr5").unwrap();
    assert_eq!(remain.len(), 1);
    assert_eq!(remain[0][1], Value::Integer(90));
}

#[test]
fn delete_without_returning_unchanged() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dr6 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO dr6 VALUES (1, 'alice')").unwrap();
    let rows = eng.run_sql("DELETE FROM dr6 WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn delete_returning_parser() {
    let stmt = parse("DELETE FROM users WHERE id = 1 RETURNING name, email").unwrap();
    match stmt {
        Stmt::Delete {
            table,
            where_clause,
            returning,
            ..
        } => {
            assert_eq!(table, "users");
            assert!(where_clause.is_some());
            let ret = returning.unwrap();
            assert_eq!(ret, vec!["name", "email"]);
        }
        _ => panic!("expected Delete"),
    }
    let stmt2 = parse("DELETE FROM users WHERE id = 1").unwrap();
    match stmt2 {
        Stmt::Delete { returning, .. } => assert!(returning.is_none()),
        _ => panic!("expected Delete"),
    }
}

// ===== M106: CROSS JOIN =====

#[test]
fn cross_join_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE colors (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE sizes (id INTEGER, label TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO colors VALUES (1, 'red')").unwrap();
    eng.run_sql("INSERT INTO colors VALUES (2, 'blue')")
        .unwrap();
    eng.run_sql("INSERT INTO sizes VALUES (10, 'S')").unwrap();
    eng.run_sql("INSERT INTO sizes VALUES (20, 'M')").unwrap();
    eng.run_sql("INSERT INTO sizes VALUES (30, 'L')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM colors CROSS JOIN sizes")
        .unwrap();
    // 2 × 3 = 6 行
    assert_eq!(rows.len(), 6);
    // 每行 4 列（colors.id, colors.name, sizes.id, sizes.label）
    assert_eq!(rows[0].len(), 4);
}

#[test]
fn cross_join_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE cja (id INTEGER, x TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE cjb (bid INTEGER, y TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO cja VALUES (1, 'a1')").unwrap();
    eng.run_sql("INSERT INTO cja VALUES (2, 'a2')").unwrap();
    eng.run_sql("INSERT INTO cjb VALUES (10, 'b1')").unwrap();
    eng.run_sql("INSERT INTO cjb VALUES (20, 'b2')").unwrap();
    let rows = eng
        .run_sql("SELECT x, y FROM cja CROSS JOIN cjb WHERE id = 1")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("a1".into()));
}

#[test]
fn cross_join_with_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (10)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (20)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (30)").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t1 CROSS JOIN t2 LIMIT 3")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn cross_join_empty_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    // t2 为空 → 笛卡尔积为空
    let rows = eng.run_sql("SELECT * FROM t1 CROSS JOIN t2").unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn cross_join_parser() {
    let stmt = parse("SELECT * FROM a CROSS JOIN b").unwrap();
    match stmt {
        Stmt::Select { table, join, .. } => {
            assert_eq!(table, "a");
            let jc = join.unwrap();
            assert_eq!(jc.join_type, JoinType::Cross);
            assert_eq!(jc.table, "b");
            assert!(jc.left_col.is_empty());
            assert!(jc.right_col.is_empty());
        }
        _ => panic!("expected Select"),
    }
}

// ── M107: NATURAL JOIN ──────────────────────────────────────────

#[test]
fn natural_join_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dept (dept_id INTEGER, dname TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE emp (id INTEGER, dname TEXT, salary INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO dept VALUES (1, 'eng')").unwrap();
    eng.run_sql("INSERT INTO dept VALUES (2, 'sales')").unwrap();
    eng.run_sql("INSERT INTO emp VALUES (10, 'eng', 100)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (20, 'sales', 200)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (30, 'eng', 150)")
        .unwrap();
    // NATURAL JOIN 匹配同名列 dname
    let rows = eng.run_sql("SELECT * FROM dept NATURAL JOIN emp").unwrap();
    assert_eq!(rows.len(), 3); // eng×2 + sales×1
}

#[test]
fn natural_join_no_common_cols() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (a INTEGER, b TEXT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (c INTEGER, d TEXT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'x')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 'y')").unwrap();
    // 无同名列 → 退化为 CROSS JOIN
    let rows = eng.run_sql("SELECT * FROM t1 NATURAL JOIN t2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4); // a, b, c, d
}

#[test]
fn natural_join_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE colors (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE items (item_id INTEGER, name TEXT, price INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO colors VALUES (1, 'red')").unwrap();
    eng.run_sql("INSERT INTO colors VALUES (2, 'blue')")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (10, 'red', 50)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (20, 'blue', 80)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (30, 'red', 30)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM colors NATURAL JOIN items WHERE price > 40")
        .unwrap();
    assert_eq!(rows.len(), 2); // red(50) + blue(80)
}

#[test]
fn natural_join_multiple_common_cols() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (a INTEGER, b TEXT, c INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (x INTEGER, a INTEGER, b TEXT, d INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'y', 20)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (100, 1, 'x', 100)")
        .unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (200, 1, 'z', 200)")
        .unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (300, 2, 'y', 300)")
        .unwrap();
    // 同名列 a 和 b 都必须匹配
    let rows = eng.run_sql("SELECT * FROM t1 NATURAL JOIN t2").unwrap();
    assert_eq!(rows.len(), 2); // (1,'x')→100 + (2,'y')→300
}

#[test]
fn natural_join_parser() {
    let stmt = parse("SELECT * FROM a NATURAL JOIN b").unwrap();
    match stmt {
        Stmt::Select { table, join, .. } => {
            assert_eq!(table, "a");
            let jc = join.unwrap();
            assert_eq!(jc.join_type, JoinType::Natural);
            assert_eq!(jc.table, "b");
            assert!(jc.left_col.is_empty());
            assert!(jc.right_col.is_empty());
        }
        _ => panic!("expected Select"),
    }
}

// ── M108: INTERSECT / EXCEPT ────────────────────────────────────

#[test]
fn intersect_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (3, 'c')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (3, 'c')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (4, 'd')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t1 INTERSECT SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 2); // (2,'b') + (3,'c')
}

#[test]
fn intersect_all() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (pk INTEGER, v TEXT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (pk INTEGER, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (3, 'b')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (10, 'a')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (20, 'b')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (30, 'b')").unwrap();
    // SELECT v: t1 有 a,a,b; t2 有 a,b,b
    // INTERSECT ALL on v: a(min(2,1)=1) + b(min(1,2)=1) = 2 行
    let rows = eng
        .run_sql("SELECT v FROM t1 INTERSECT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn except_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (3, 'c')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (4, 'd')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t1 EXCEPT SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 2); // (1,'a') + (3,'c')
}

#[test]
fn except_all() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (pk INTEGER, v TEXT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (pk INTEGER, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (3, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (4, 'b')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (10, 'a')").unwrap();
    // SELECT v: t1 有 a,a,a,b; t2 有 a
    // EXCEPT ALL on v: a 消耗 1 个，剩 a,a,b = 3 行
    let rows = eng
        .run_sql("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn intersect_empty_result() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2)").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t1 INTERSECT SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn except_empty_right() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2)").unwrap();
    // t2 为空 → EXCEPT 返回全部左表
    let rows = eng
        .run_sql("SELECT * FROM t1 EXCEPT SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn union_still_works() {
    // 确保 UNION 没被破坏
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (3)").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t1 UNION SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows.len(), 3); // 1,2,3 去重
    let rows_all = eng
        .run_sql("SELECT * FROM t1 UNION ALL SELECT * FROM t2")
        .unwrap();
    assert_eq!(rows_all.len(), 4); // 1,2,2,3
}

// ── M109: GLOB ──────────────────────────────────────────────────

#[test]
fn glob_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE files (id INTEGER, path TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO files VALUES (1, 'src/main.rs')")
        .unwrap();
    eng.run_sql("INSERT INTO files VALUES (2, 'src/lib.rs')")
        .unwrap();
    eng.run_sql("INSERT INTO files VALUES (3, 'tests/test.rs')")
        .unwrap();
    eng.run_sql("INSERT INTO files VALUES (4, 'README.md')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM files WHERE path GLOB 'src/*'")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn glob_question_mark() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'cat')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'cut')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'cart')").unwrap();
    // ? 匹配单字符
    let rows = eng.run_sql("SELECT * FROM t WHERE v GLOB 'c?t'").unwrap();
    assert_eq!(rows.len(), 2); // cat, cut
}

#[test]
fn glob_case_sensitive() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'Hello')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'hello')").unwrap();
    // GLOB 大小写敏感
    let rows = eng.run_sql("SELECT * FROM t WHERE name GLOB 'H*'").unwrap();
    assert_eq!(rows.len(), 1);
    let rows2 = eng.run_sql("SELECT * FROM t WHERE name GLOB 'h*'").unwrap();
    assert_eq!(rows2.len(), 1);
}

#[test]
fn glob_not() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, ext TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, '.rs')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, '.py')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, '.rs')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE ext NOT GLOB '*.py'")
        .unwrap();
    assert_eq!(rows.len(), 2); // .rs, .rs
}

#[test]
fn glob_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, v TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'abc')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t WHERE v GLOB 'xyz*'").unwrap();
    assert_eq!(rows.len(), 0);
}

// ── M110: SAVEPOINT / RELEASE / ROLLBACK TO ──

#[test]
fn savepoint_basic_rollback_to() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, v TEXT)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("SAVEPOINT sp1").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    // 回滚到 sp1，应丢弃 id=2
    eng.run_sql("ROLLBACK TO sp1").unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn savepoint_release() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, v TEXT)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a')").unwrap();
    eng.run_sql("SAVEPOINT sp1").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'b')").unwrap();
    // RELEASE 保留当前写入
    eng.run_sql("RELEASE sp1").unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn savepoint_nested() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    eng.run_sql("SAVEPOINT sp1").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2)").unwrap();
    eng.run_sql("SAVEPOINT sp2").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3)").unwrap();
    // 回滚到 sp1，丢弃 id=2 和 id=3，sp2 也被移除
    eng.run_sql("ROLLBACK TO sp1").unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn savepoint_not_in_tx() {
    let (_dir, mut eng) = tmp_engine();
    assert!(eng.run_sql("SAVEPOINT sp1").is_err());
    assert!(eng.run_sql("RELEASE sp1").is_err());
    assert!(eng.run_sql("ROLLBACK TO sp1").is_err());
}

#[test]
fn savepoint_release_savepoint_syntax() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    eng.run_sql("SAVEPOINT sp1").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2)").unwrap();
    // RELEASE SAVEPOINT 语法（带 SAVEPOINT 关键字）
    eng.run_sql("RELEASE SAVEPOINT sp1").unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn savepoint_rollback_to_savepoint_syntax() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER)").unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1)").unwrap();
    eng.run_sql("SAVEPOINT sp1").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2)").unwrap();
    // ROLLBACK TO SAVEPOINT 语法
    eng.run_sql("ROLLBACK TO SAVEPOINT sp1").unwrap();
    eng.run_sql("COMMIT").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

// ── M111: CREATE UNIQUE INDEX ─────────────────────────────

#[test]
fn unique_index_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'bob', 25)").unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    // 正常插入不同值
    eng.run_sql("INSERT INTO t VALUES (3, 'charlie', 28)")
        .unwrap();
    // 插入重复值应失败
    let err = eng.run_sql("INSERT INTO t VALUES (4, 'alice', 22)");
    assert!(err.is_err());
    assert!(
        format!("{:?}", err.unwrap_err()).contains("UNIQUE constraint failed"),
        "应包含 UNIQUE constraint failed 错误信息"
    );
    // 确认只有 3 行
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn unique_index_insert_or_ignore() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, email TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'a@b.com')").unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_email ON t(email)")
        .unwrap();
    // INSERT OR IGNORE 应静默跳过
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (2, 'a@b.com')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn unique_index_insert_or_replace() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, code TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'X')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'Y')").unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_code ON t(code)")
        .unwrap();
    // INSERT OR REPLACE 同 PK 应替换（不触发唯一冲突）
    eng.run_sql("INSERT OR REPLACE INTO t VALUES (1, 'X')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn unique_index_null_allowed() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, tag TEXT)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_tag ON t(tag)")
        .unwrap();
    // NULL 值不参与唯一约束（SQL 标准）
    eng.run_sql("INSERT INTO t VALUES (1, NULL)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, NULL)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn unique_index_update_violation() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'alice')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'bob')").unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    // UPDATE 导致唯一冲突
    let err = eng.run_sql("UPDATE t SET name = 'alice' WHERE id = 2");
    assert!(err.is_err());
    assert!(format!("{:?}", err.unwrap_err()).contains("UNIQUE constraint failed"));
    // 确认数据未变
    let rows = eng.run_sql("SELECT name FROM t WHERE id = 2").unwrap();
    assert_eq!(rows[0][0], Value::Text("bob".into()));
}

#[test]
fn unique_index_update_same_value_ok() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'alice', 30)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    // UPDATE 不改唯一列，应成功
    eng.run_sql("UPDATE t SET age = 31 WHERE id = 1").unwrap();
    let rows = eng.run_sql("SELECT age FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Integer(31));
}

#[test]
fn unique_index_backfill_rejects_duplicates() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'dup')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'dup')").unwrap();
    // 已有重复数据时创建唯一索引应失败
    let err = eng.run_sql("CREATE UNIQUE INDEX idx_val ON t(val)");
    assert!(err.is_err());
    assert!(format!("{:?}", err.unwrap_err()).contains("UNIQUE 约束冲突"));
}

#[test]
fn unique_index_in_transaction() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, code TEXT)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_code ON t(code)")
        .unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'A')").unwrap();
    // 事务内插入重复值应失败
    let err = eng.run_sql("INSERT INTO t VALUES (2, 'A')");
    assert!(err.is_err());
    eng.run_sql("ROLLBACK").unwrap();
}

#[test]
fn unique_index_show_indexes_marker() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    let rows = eng.run_sql("SHOW INDEXES ON t").unwrap();
    assert_eq!(rows.len(), 1);
    // SHOW INDEXES 应显示 [UNIQUE] 标记
    let idx_name = format!("{:?}", rows[0][0]);
    assert!(
        idx_name.contains("UNIQUE"),
        "SHOW INDEXES 应显示 UNIQUE 标记: {}",
        idx_name
    );
}

#[test]
fn unique_index_drop_and_reinsert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'alice')").unwrap();
    // 删除唯一索引后应允许重复
    eng.run_sql("DROP INDEX idx_name").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'alice')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

// ── M112：复合索引测试 ──────────────────────────────────────

#[test]
fn composite_index_create_and_query() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER, c TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_ab ON t(a, b)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10, 'foo')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'x', 20, 'bar')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'y', 10, 'baz')")
        .unwrap();
    // SHOW INDEXES 应显示复合列
    let rows = eng.run_sql("SHOW INDEXES ON t").unwrap();
    assert_eq!(rows.len(), 1);
    let col_display = format!("{:?}", rows[0][2]);
    assert!(col_display.contains("a,b"), "应显示复合列: {}", col_display);
    // 查询应正常工作
    let rows = eng.run_sql("SELECT * FROM t WHERE a = 'x'").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn composite_unique_index_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'x', 20)").unwrap(); // 不同组合，OK
    eng.run_sql("INSERT INTO t VALUES (3, 'y', 10)").unwrap(); // 不同组合，OK
                                                               // 重复组合应报错
    let err = eng.run_sql("INSERT INTO t VALUES (4, 'x', 10)");
    assert!(err.is_err(), "复合唯一索引应拒绝重复组合");
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("UNIQUE"), "错误信息应包含 UNIQUE: {}", msg);
}

#[test]
fn composite_unique_index_null_bypass() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NULL, 10)").unwrap();
    // NULL 不参与唯一约束，应允许重复
    eng.run_sql("INSERT INTO t VALUES (2, NULL, 10)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn composite_index_update_maintains_uniqueness() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
    // UPDATE 到已存在的组合应报错
    let err = eng.run_sql("UPDATE t SET a = 'x', b = 10 WHERE id = 2");
    assert!(err.is_err(), "UPDATE 应检查复合唯一约束");
    // UPDATE 到不同组合应成功
    eng.run_sql("UPDATE t SET a = 'x', b = 20 WHERE id = 2")
        .unwrap();
}

#[test]
fn composite_index_drop_column_cascade() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER, c TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_ab ON t(a, b)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10, 'foo')")
        .unwrap();
    // DROP COLUMN a 应级联删除包含 a 的复合索引
    eng.run_sql("ALTER TABLE t DROP COLUMN a").unwrap();
    let rows = eng.run_sql("SHOW INDEXES ON t").unwrap();
    assert_eq!(rows.len(), 0, "DROP COLUMN 应级联删除复合索引");
}

#[test]
fn composite_index_rename_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_ab ON t(a, b)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    // RENAME COLUMN a → aa 应更新复合索引元数据
    eng.run_sql("ALTER TABLE t RENAME COLUMN a TO aa").unwrap();
    let rows = eng.run_sql("SHOW INDEXES ON t").unwrap();
    assert_eq!(rows.len(), 1);
    let col_display = format!("{:?}", rows[0][2]);
    assert!(
        col_display.contains("aa,b"),
        "RENAME 后应显示新列名: {}",
        col_display
    );
    // 插入后索引仍应正常工作
    eng.run_sql("INSERT INTO t VALUES (2, 'y', 20)").unwrap();
}

#[test]
fn composite_index_three_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER, c TEXT)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_abc ON t(a, b, c)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10, 'p')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'x', 10, 'q')")
        .unwrap(); // c 不同，OK
    let err = eng.run_sql("INSERT INTO t VALUES (3, 'x', 10, 'p')");
    assert!(err.is_err(), "三列复合唯一索引应拒绝完全重复");
}

#[test]
fn composite_index_tx_unique_check() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("BEGIN").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    // 事务内重复组合应报错
    let err = eng.run_sql("INSERT INTO t VALUES (2, 'x', 10)");
    assert!(err.is_err(), "事务内应检查复合唯一约束");
    eng.run_sql("ROLLBACK").unwrap();
}

#[test]
fn composite_index_or_ignore() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    // OR IGNORE 应静默跳过
    eng.run_sql("INSERT OR IGNORE INTO t VALUES (2, 'x', 10)")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn composite_index_backfill_unique_violation() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'x', 10)").unwrap();
    // 回填时发现重复值应报错
    let err = eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)");
    assert!(err.is_err(), "回填时应检测重复值");
}

#[test]
fn composite_index_max_columns() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT, g TEXT, h TEXT, i TEXT)")
        .unwrap();
    // 8 列应成功
    eng.run_sql("CREATE INDEX idx8 ON t(a, b, c, d, e, f, g, h)")
        .unwrap();
    // 9 列应报错
    let err = eng.run_sql("CREATE INDEX idx9 ON t(a, b, c, d, e, f, g, h, i)");
    assert!(err.is_err(), "超过 8 列应报错");
}

#[test]
fn composite_index_drop_index() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, a TEXT, b INTEGER)")
        .unwrap();
    eng.run_sql("CREATE UNIQUE INDEX idx_ab ON t(a, b)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'x', 10)").unwrap();
    // DROP INDEX 后应允许重复
    eng.run_sql("DROP INDEX idx_ab").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'x', 10)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 2);
}

// ==================== M113: CTE (WITH 子句) ====================

#[test]
fn cte_basic_single() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    eng.run_sql("INSERT INTO users VALUES (3, 'Carol', 35)")
        .unwrap();
    let rows = eng
        .run_sql("WITH seniors AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM seniors")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn cte_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INTEGER, name TEXT, price INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, 'A', 100)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (2, 'B', 200)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (3, 'C', 300)")
        .unwrap();
    let rows = eng
        .run_sql(
            "WITH expensive AS (SELECT * FROM items WHERE price > 150) \
             SELECT * FROM expensive WHERE price < 250",
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("B".into()));
}

#[test]
fn cte_multiple() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 20)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 30)").unwrap();
    let rows = eng
        .run_sql(
            "WITH low AS (SELECT * FROM t WHERE val <= 15), \
             high AS (SELECT * FROM t WHERE val >= 25) \
             SELECT * FROM high",
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(30));
}

#[test]
fn cte_chained_reference() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 20)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 30)").unwrap();
    // 第二个 CTE 引用第一个 CTE
    let rows = eng
        .run_sql(
            "WITH base AS (SELECT * FROM t WHERE val >= 20), \
             filtered AS (SELECT * FROM base WHERE val <= 25) \
             SELECT * FROM filtered",
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(20));
}

#[test]
fn cte_empty_result() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'A')").unwrap();
    let rows = eng
        .run_sql("WITH empty AS (SELECT * FROM t WHERE id > 999) SELECT * FROM empty")
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn cte_parse_error_missing_as() {
    let (_dir, mut eng) = tmp_engine();
    let err = eng.run_sql("WITH foo (SELECT * FROM t) SELECT * FROM foo");
    assert!(err.is_err());
}

#[test]
fn cte_parse_error_no_select() {
    let (_dir, mut eng) = tmp_engine();
    let err = eng.run_sql("WITH foo AS (SELECT * FROM t) INSERT INTO t VALUES (1)");
    assert!(err.is_err());
}

#[test]
fn cte_no_shadow_real_table() {
    // CTE 名称与真实表同名时，CTE 优先且不破坏真实表
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 200)").unwrap();
    // CTE 名称也叫 t，但只选 val > 150 的行
    let rows = eng
        .run_sql("WITH t AS (SELECT * FROM t WHERE val > 150) SELECT * FROM t")
        .unwrap();
    // CTE 结果应只有 1 行
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(200));
    // 真实表不受影响
    let rows2 = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows2.len(), 2);
}

// ── M114: REGEXP / NOT REGEXP ──

#[test]
fn regexp_basic_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'world')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'hello world')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name REGEXP '^hello'")
        .unwrap();
    assert_eq!(rows.len(), 2); // 'hello' and 'hello world'
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn regexp_not_regexp() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'world')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'hello world')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name NOT REGEXP '^hello'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn regexp_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name REGEXP '^xyz'")
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn regexp_digit_pattern() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, code TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'abc123')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'abcdef')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, '999')").unwrap();
    let rows = eng
        .run_sql(r"SELECT * FROM t WHERE code REGEXP '\d+'")
        .unwrap();
    assert_eq!(rows.len(), 2); // 'abc123' and '999'
}

#[test]
fn regexp_null_returns_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, NULL)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'hello')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name REGEXP 'hello'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn regexp_invalid_pattern_returns_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    // 无效正则（未闭合括号）— 返回空结果而非报错
    let rows = eng
        .run_sql("SELECT * FROM t WHERE name REGEXP '(unclosed'")
        .unwrap();
    assert_eq!(rows.len(), 0);
}

// ── M115: LIKE ... ESCAPE ──

#[test]
fn like_escape_percent() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, '100% done')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, '100 done')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, '100xyz done')")
        .unwrap();
    // 用 \ 转义 %，匹配字面 '%'
    let rows = eng
        .run_sql(r"SELECT * FROM t WHERE val LIKE '100\% done' ESCAPE '\'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn like_escape_underscore() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'file_name')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'filename')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'fileXname')")
        .unwrap();
    // 用 ! 转义 _
    let rows = eng
        .run_sql("SELECT * FROM t WHERE val LIKE 'file!_name' ESCAPE '!'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

#[test]
fn like_escape_with_wildcards() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'abc%def')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'abc123def')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'xyzabc%defgh')")
        .unwrap();
    // %转义%% — 匹配包含字面 '%' 的行
    let rows = eng
        .run_sql(r"SELECT * FROM t WHERE val LIKE '%\%%' ESCAPE '\'")
        .unwrap();
    assert_eq!(rows.len(), 2); // 'abc%def' and 'xyzabc%defgh'
}

#[test]
fn not_like_escape() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, '100% done')")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, '100 done')").unwrap();
    let rows = eng
        .run_sql(r"SELECT * FROM t WHERE val NOT LIKE '100\% done' ESCAPE '\'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn like_no_escape_unchanged() {
    // 无 ESCAPE 子句时行为不变
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'world')").unwrap();
    let rows = eng
        .run_sql("SELECT * FROM t WHERE val LIKE 'hel%'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
}

// ============================================================
// M116: UPDATE ... FROM 跨表更新
// ============================================================

#[test]
fn update_from_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, score INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, new_score INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 20)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 200)").unwrap();
    let res = eng
        .run_sql("UPDATE t1 SET score = t2.new_score FROM t2 WHERE t1.id = t2.id")
        .unwrap();
    assert_eq!(res[0][0], Value::Integer(2)); // 2 rows updated
    let rows = eng.run_sql("SELECT * FROM t1 ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Integer(100));
    assert_eq!(rows[1][1], Value::Integer(200));
}

#[test]
fn update_from_multi_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE dst (id INTEGER, name TEXT, val INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE src (id INTEGER, name TEXT, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO dst VALUES (1, 'old', 0)").unwrap();
    eng.run_sql("INSERT INTO src VALUES (1, 'new', 99)")
        .unwrap();
    eng.run_sql("UPDATE dst SET name = src.name, val = src.val FROM src WHERE dst.id = src.id")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM dst").unwrap();
    assert_eq!(rows[0][1], Value::Text("new".into()));
    assert_eq!(rows[0][2], Value::Integer(99));
}

#[test]
fn update_from_source_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, val INTEGER, active INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 0)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 0)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (1, 10, 1)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 20, 0)").unwrap();
    // 只更新 active=1 的源行
    eng.run_sql("UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id AND t2.active = 1")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t1 ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Integer(10)); // id=1 updated
    assert_eq!(rows[1][1], Value::Integer(0)); // id=2 not updated
}

#[test]
fn update_from_no_join_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, val INTEGER)")
        .unwrap();
    let res = eng.run_sql("UPDATE t1 SET val = t2.val FROM t2");
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("连接条件"));
}

#[test]
fn update_from_same_table_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, val INTEGER)")
        .unwrap();
    let res = eng.run_sql("UPDATE t1 SET val = t1.val FROM t1");
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("同名"));
}

#[test]
fn update_from_partial_match() {
    // 源表只有部分行匹配，未匹配行不变
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (2, 'b')").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (3, 'c')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (2, 'B')").unwrap();
    let res = eng
        .run_sql("UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id")
        .unwrap();
    assert_eq!(res[0][0], Value::Integer(1)); // only 1 row updated
    let rows = eng.run_sql("SELECT * FROM t1 ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Text("a".into())); // unchanged
    assert_eq!(rows[1][1], Value::Text("B".into())); // updated
    assert_eq!(rows[2][1], Value::Text("c".into())); // unchanged
}

// ============================================================
// M117: UPDATE ... ORDER BY ... LIMIT
// ============================================================

#[test]
fn update_order_by_limit_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, score INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 30)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 20)").unwrap();
    // 按 score ASC 更新前 2 行
    let res = eng
        .run_sql("UPDATE t SET score = 0 ORDER BY score LIMIT 2")
        .unwrap();
    assert_eq!(res[0][0], Value::Integer(2));
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Integer(30)); // id=1, score=30 未更新（最大）
    assert_eq!(rows[1][1], Value::Integer(0)); // id=2, score=10 → 0
    assert_eq!(rows[2][1], Value::Integer(0)); // id=3, score=20 → 0
}

#[test]
fn update_order_by_desc_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 200)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 300)").unwrap();
    // 按 val DESC 更新前 1 行（最大的）
    let res = eng
        .run_sql("UPDATE t SET val = 0 ORDER BY val DESC LIMIT 1")
        .unwrap();
    assert_eq!(res[0][0], Value::Integer(1));
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Integer(100));
    assert_eq!(rows[1][1], Value::Integer(200));
    assert_eq!(rows[2][1], Value::Integer(0)); // id=3 was 300, now 0
}

#[test]
fn update_where_order_by_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, status TEXT, ts INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'active', 100)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'active', 50)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 'done', 200)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (4, 'active', 75)")
        .unwrap();
    // 只更新 active 中最旧的 2 条（ts ASC: 50, 75, 100 → 前2条是 id=2 和 id=4）
    let res = eng
        .run_sql("UPDATE t SET status = 'expired' WHERE status = 'active' ORDER BY ts LIMIT 2")
        .unwrap();
    assert_eq!(res[0][0], Value::Integer(2));
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows[0][1], Value::Text("active".into())); // id=1, ts=100 → still active
    assert_eq!(rows[1][1], Value::Text("expired".into())); // id=2, ts=50 → expired
    assert_eq!(rows[2][1], Value::Text("done".into())); // id=3 unchanged
    assert_eq!(rows[3][1], Value::Text("expired".into())); // id=4, ts=75 → expired
}

#[test]
fn update_limit_only() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 1)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 2)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 3)").unwrap();
    // LIMIT without ORDER BY — 更新任意 2 行
    let res = eng.run_sql("UPDATE t SET val = 0 LIMIT 2").unwrap();
    assert_eq!(res[0][0], Value::Integer(2));
    let rows = eng.run_sql("SELECT * FROM t ORDER BY id").unwrap();
    let zeros = rows.iter().filter(|r| r[1] == Value::Integer(0)).count();
    assert_eq!(zeros, 2);
}

#[test]
fn update_from_with_order_by_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INTEGER, val INTEGER)")
        .unwrap();
    eng.run_sql("CREATE TABLE t2 (id INTEGER, val INTEGER)")
        .unwrap();
    let res = eng
        .run_sql("UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id ORDER BY t1.id LIMIT 1");
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("ORDER BY"));
}

#[test]
fn order_by_nulls_first_asc() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_nf (id INT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_nf VALUES (1, 30)").unwrap();
    eng.run_sql("INSERT INTO t_nf VALUES (2, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_nf VALUES (3, 10)").unwrap();
    eng.run_sql("INSERT INTO t_nf VALUES (4, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_nf VALUES (5, 20)").unwrap();
    // ASC NULLS FIRST: NULLs should come first
    let rows = eng
        .run_sql("SELECT id, score FROM t_nf ORDER BY score ASC NULLS FIRST")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // First two rows should have NULL scores (ids 2 and 4)
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[1][1], Value::Null);
    // Then 10, 20, 30
    assert_eq!(rows[2][1], Value::Integer(10));
    assert_eq!(rows[3][1], Value::Integer(20));
    assert_eq!(rows[4][1], Value::Integer(30));
}

#[test]
fn order_by_nulls_last_desc() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_nl (id INT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_nl VALUES (1, 30)").unwrap();
    eng.run_sql("INSERT INTO t_nl VALUES (2, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_nl VALUES (3, 10)").unwrap();
    eng.run_sql("INSERT INTO t_nl VALUES (4, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_nl VALUES (5, 20)").unwrap();
    // DESC NULLS LAST: NULLs should come last
    let rows = eng
        .run_sql("SELECT id, score FROM t_nl ORDER BY score DESC NULLS LAST")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // 30, 20, 10 first
    assert_eq!(rows[0][1], Value::Integer(30));
    assert_eq!(rows[1][1], Value::Integer(20));
    assert_eq!(rows[2][1], Value::Integer(10));
    // Last two rows should have NULL scores
    assert_eq!(rows[3][1], Value::Null);
    assert_eq!(rows[4][1], Value::Null);
}

#[test]
fn order_by_nulls_default_behavior() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_nd (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t_nd VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t_nd VALUES (2, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_nd VALUES (3, 20)").unwrap();
    // ASC default: NULLS LAST (PostgreSQL compatible)
    let rows_asc = eng
        .run_sql("SELECT id, val FROM t_nd ORDER BY val ASC")
        .unwrap();
    assert_eq!(rows_asc[0][1], Value::Integer(10));
    assert_eq!(rows_asc[1][1], Value::Integer(20));
    assert_eq!(rows_asc[2][1], Value::Null);
    // DESC default: NULLS FIRST (PostgreSQL compatible)
    let rows_desc = eng
        .run_sql("SELECT id, val FROM t_nd ORDER BY val DESC")
        .unwrap();
    assert_eq!(rows_desc[0][1], Value::Null);
    assert_eq!(rows_desc[1][1], Value::Integer(20));
    assert_eq!(rows_desc[2][1], Value::Integer(10));
}

#[test]
fn order_by_nulls_multi_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_nm (id INT, cat TEXT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_nm VALUES (1, 'A', 10)").unwrap();
    eng.run_sql("INSERT INTO t_nm VALUES (2, 'A', NULL)")
        .unwrap();
    eng.run_sql("INSERT INTO t_nm VALUES (3, 'B', 20)").unwrap();
    eng.run_sql("INSERT INTO t_nm VALUES (4, 'B', NULL)")
        .unwrap();
    // Multi-column: cat ASC, score ASC NULLS FIRST
    let rows = eng
        .run_sql("SELECT id FROM t_nm ORDER BY cat ASC, score ASC NULLS FIRST")
        .unwrap();
    // A group: NULL first (id=2), then 10 (id=1)
    // B group: NULL first (id=4), then 20 (id=3)
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(1));
    assert_eq!(rows[2][0], Value::Integer(4));
    assert_eq!(rows[3][0], Value::Integer(3));
}

#[test]
fn fetch_first_rows_only() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_fetch (id INT, name TEXT)")
        .unwrap();
    for i in 1..=10 {
        eng.run_sql(&format!("INSERT INTO t_fetch VALUES ({}, 'n{}')", i, i))
            .unwrap();
    }
    // FETCH FIRST 3 ROWS ONLY = LIMIT 3
    let rows = eng
        .run_sql("SELECT id FROM t_fetch ORDER BY id ASC FETCH FIRST 3 ROWS ONLY")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[2][0], Value::Integer(3));
}

#[test]
fn fetch_next_rows_only() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_fn (id INT)").unwrap();
    for i in 1..=5 {
        eng.run_sql(&format!("INSERT INTO t_fn VALUES ({})", i))
            .unwrap();
    }
    // FETCH NEXT = FETCH FIRST 的别名
    let rows = eng
        .run_sql("SELECT id FROM t_fn ORDER BY id ASC FETCH NEXT 2 ROWS ONLY")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
}

#[test]
fn fetch_first_with_offset() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_fo (id INT)").unwrap();
    for i in 1..=10 {
        eng.run_sql(&format!("INSERT INTO t_fo VALUES ({})", i))
            .unwrap();
    }
    // OFFSET 3 FETCH FIRST 2 ROWS ONLY
    let rows = eng
        .run_sql("SELECT id FROM t_fo ORDER BY id ASC OFFSET 3 FETCH FIRST 2 ROWS ONLY")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(4));
    assert_eq!(rows[1][0], Value::Integer(5));
}

#[test]
fn fetch_first_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_fw (id INT, val INT)").unwrap();
    for i in 1..=10 {
        eng.run_sql(&format!("INSERT INTO t_fw VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    // WHERE + FETCH FIRST
    let rows = eng
        .run_sql("SELECT id FROM t_fw WHERE val > 50 ORDER BY id ASC FETCH FIRST 3 ROWS ONLY")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(6));
    assert_eq!(rows[1][0], Value::Integer(7));
    assert_eq!(rows[2][0], Value::Integer(8));
}

#[test]
fn array_agg_group_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_aa (id INT, cat TEXT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aa VALUES (1, 'a', 'x')")
        .unwrap();
    eng.run_sql("INSERT INTO t_aa VALUES (2, 'a', 'y')")
        .unwrap();
    eng.run_sql("INSERT INTO t_aa VALUES (3, 'b', 'z')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT cat, ARRAY_AGG(name) FROM t_aa GROUP BY cat ORDER BY cat")
        .unwrap();
    assert_eq!(rows.len(), 2);
    // cat='a' → ["x","y"]
    if let Value::Text(ref s) = rows[0][1] {
        let arr: Vec<String> = serde_json::from_str(s).unwrap();
        assert_eq!(arr, vec!["x", "y"]);
    } else {
        panic!("expected Text");
    }
    // cat='b' → ["z"]
    if let Value::Text(ref s) = rows[1][1] {
        let arr: Vec<String> = serde_json::from_str(s).unwrap();
        assert_eq!(arr, vec!["z"]);
    } else {
        panic!("expected Text");
    }
}

#[test]
fn array_agg_skips_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_aan (id INT, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aan VALUES (1, 'hello')")
        .unwrap();
    eng.run_sql("INSERT INTO t_aan VALUES (2, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_aan VALUES (3, 'world')")
        .unwrap();
    let rows = eng.run_sql("SELECT ARRAY_AGG(val) FROM t_aan").unwrap();
    assert_eq!(rows.len(), 1);
    // NULL 被跳过，只有 ["hello","world"]
    if let Value::Text(ref s) = rows[0][0] {
        let arr: Vec<String> = serde_json::from_str(s).unwrap();
        assert_eq!(arr, vec!["hello", "world"]);
    } else {
        panic!("expected Text");
    }
}

#[test]
fn array_agg_all_null_returns_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_aan2 (id INT, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aan2 VALUES (1, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_aan2 VALUES (2, NULL)").unwrap();
    let rows = eng.run_sql("SELECT ARRAY_AGG(val) FROM t_aan2").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn array_agg_integers() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_aai (id INT, grp TEXT, num INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aai VALUES (1, 'x', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aai VALUES (2, 'x', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO t_aai VALUES (3, 'x', 30)")
        .unwrap();
    let rows = eng.run_sql("SELECT ARRAY_AGG(num) FROM t_aai").unwrap();
    assert_eq!(rows.len(), 1);
    if let Value::Text(ref s) = rows[0][0] {
        let arr: Vec<i64> = serde_json::from_str(s).unwrap();
        assert_eq!(arr, vec![10, 20, 30]);
    } else {
        panic!("expected Text");
    }
}

// ── M160: DISTINCT ON ──

#[test]
fn distinct_on_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_don (id INT, cat TEXT, val INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don VALUES (1, 'a', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don VALUES (2, 'a', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don VALUES (3, 'b', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don VALUES (4, 'b', 40)")
        .unwrap();
    // DISTINCT ON (cat) 按 cat 去重，保留每组第一行（按 id ASC 排序）
    let rows = eng
        .run_sql("SELECT DISTINCT ON (cat) id, cat, val FROM t_don ORDER BY cat, id")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("a".into()));
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][1], Value::Text("b".into()));
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn distinct_on_multi_col() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_don2 (id INT, cat TEXT, sub TEXT, val INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don2 VALUES (1, 'a', 'x', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don2 VALUES (2, 'a', 'x', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don2 VALUES (3, 'a', 'y', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don2 VALUES (4, 'b', 'x', 40)")
        .unwrap();
    // DISTINCT ON (cat, sub) — 多列去重
    let rows = eng
        .run_sql("SELECT DISTINCT ON (cat, sub) id, cat, sub FROM t_don2 ORDER BY cat, sub, id")
        .unwrap();
    assert_eq!(rows.len(), 3); // (a,x), (a,y), (b,x)
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(3));
    assert_eq!(rows[2][0], Value::Integer(4));
}

#[test]
fn distinct_on_with_limit() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_don3 (id INT, cat TEXT, val INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don3 VALUES (1, 'a', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don3 VALUES (2, 'b', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don3 VALUES (3, 'c', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don3 VALUES (4, 'a', 40)")
        .unwrap();
    let rows = eng
        .run_sql("SELECT DISTINCT ON (cat) id, cat FROM t_don3 ORDER BY cat, id LIMIT 2")
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::Text("a".into()));
    assert_eq!(rows[1][1], Value::Text("b".into()));
}

#[test]
fn distinct_on_no_order_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_don4 (id INT, cat TEXT, val INT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don4 VALUES (1, 'a', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don4 VALUES (2, 'a', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO t_don4 VALUES (3, 'b', 30)")
        .unwrap();
    // 无 ORDER BY — 仍然去重，但行顺序不确定
    let rows = eng
        .run_sql("SELECT DISTINCT ON (cat) cat, val FROM t_don4")
        .unwrap();
    assert_eq!(rows.len(), 2);
    let cats: Vec<&str> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.as_str(),
            _ => "",
        })
        .collect();
    assert!(cats.contains(&"a"));
    assert!(cats.contains(&"b"));
}

// ========== M161: PERCENTILE_CONT / PERCENTILE_DISC ==========

#[test]
fn percentile_cont_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_pct1 (id INT, score REAL)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct1 VALUES (1, 10.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct1 VALUES (2, 20.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct1 VALUES (3, 30.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct1 VALUES (4, 40.0)").unwrap();

    // 中位数 (0.5): 线性插值 → 25.0
    let rows = eng
        .run_sql("SELECT PERCENTILE_CONT(0.5, score) FROM t_pct1")
        .unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Float(f) => assert!((f - 25.0).abs() < 1e-9, "expected 25.0, got {}", f),
        other => panic!("expected Float, got {:?}", other),
    }

    // 0 百分位 → 最小值 10.0
    let rows = eng
        .run_sql("SELECT PERCENTILE_CONT(0.0, score) FROM t_pct1")
        .unwrap();
    match &rows[0][0] {
        Value::Float(f) => assert!((f - 10.0).abs() < 1e-9),
        other => panic!("expected Float, got {:?}", other),
    }

    // 1.0 百分位 → 最大值 40.0
    let rows = eng
        .run_sql("SELECT PERCENTILE_CONT(1.0, score) FROM t_pct1")
        .unwrap();
    match &rows[0][0] {
        Value::Float(f) => assert!((f - 40.0).abs() < 1e-9),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn percentile_disc_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_pct2 (id INT, score REAL)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct2 VALUES (1, 10.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct2 VALUES (2, 20.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct2 VALUES (3, 30.0)").unwrap();
    eng.run_sql("INSERT INTO t_pct2 VALUES (4, 40.0)").unwrap();

    // 离散百分位 0.5 → ceil(0.5*4)=2 → index 1 → 20.0
    let rows = eng
        .run_sql("SELECT PERCENTILE_DISC(0.5, score) FROM t_pct2")
        .unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Float(f) => assert!((f - 20.0).abs() < 1e-9, "expected 20.0, got {}", f),
        other => panic!("expected Float, got {:?}", other),
    }

    // 离散百分位 0.0 → ceil(0)=0 → clamp(1,4)-1=0 → 10.0
    let rows = eng
        .run_sql("SELECT PERCENTILE_DISC(0.0, score) FROM t_pct2")
        .unwrap();
    match &rows[0][0] {
        Value::Float(f) => assert!((f - 10.0).abs() < 1e-9),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn percentile_with_group_by() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_pct3 (id INT, cat TEXT, val REAL)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct3 VALUES (1, 'a', 10.0)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct3 VALUES (2, 'a', 20.0)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct3 VALUES (3, 'a', 30.0)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct3 VALUES (4, 'b', 100.0)")
        .unwrap();
    eng.run_sql("INSERT INTO t_pct3 VALUES (5, 'b', 200.0)")
        .unwrap();

    let rows = eng
        .run_sql("SELECT cat, PERCENTILE_CONT(0.5, val) FROM t_pct3 GROUP BY cat ORDER BY cat")
        .unwrap();
    assert_eq!(rows.len(), 2);
    // a: [10,20,30] → median = 20.0
    match &rows[0][1] {
        Value::Float(f) => assert!(
            (f - 20.0).abs() < 1e-9,
            "a median: expected 20.0, got {}",
            f
        ),
        other => panic!("expected Float, got {:?}", other),
    }
    // b: [100,200] → median = 150.0
    match &rows[1][1] {
        Value::Float(f) => assert!(
            (f - 150.0).abs() < 1e-9,
            "b median: expected 150.0, got {}",
            f
        ),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn percentile_empty_and_null() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_pct4 (id INT, val REAL)")
        .unwrap();

    // 空表 → NULL
    let rows = eng
        .run_sql("SELECT PERCENTILE_CONT(0.5, val) FROM t_pct4")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Null));

    // 全 NULL → NULL
    eng.run_sql("INSERT INTO t_pct4 VALUES (1, NULL)").unwrap();
    eng.run_sql("INSERT INTO t_pct4 VALUES (2, NULL)").unwrap();
    let rows = eng
        .run_sql("SELECT PERCENTILE_CONT(0.5, val) FROM t_pct4")
        .unwrap();
    assert!(matches!(rows[0][0], Value::Null));
}

// ── M163: DELETE ... USING ──

#[test]
fn delete_using_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE targets (id INT, name TEXT, src_id INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE sources (id INT, status TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO targets VALUES (1, 'a', 10)")
        .unwrap();
    eng.run_sql("INSERT INTO targets VALUES (2, 'b', 20)")
        .unwrap();
    eng.run_sql("INSERT INTO targets VALUES (3, 'c', 30)")
        .unwrap();
    eng.run_sql("INSERT INTO sources VALUES (10, 'done')")
        .unwrap();
    eng.run_sql("INSERT INTO sources VALUES (20, 'done')")
        .unwrap();
    // 删除 targets 中 src_id 匹配 sources.id 的行
    let result = eng
        .run_sql("DELETE FROM targets USING sources WHERE targets.src_id = sources.id")
        .unwrap();
    assert_eq!(result[0][0], Value::Integer(2)); // 删除 2 行
    let remaining = eng.run_sql("SELECT * FROM targets").unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0][1], Value::Text("c".into())); // id=3 保留
}

#[test]
fn delete_using_with_source_filter() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, category INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE cats (id INT, active INT)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO items VALUES (2, 200)").unwrap();
    eng.run_sql("INSERT INTO items VALUES (3, 100)").unwrap();
    eng.run_sql("INSERT INTO cats VALUES (100, 0)").unwrap(); // inactive
    eng.run_sql("INSERT INTO cats VALUES (200, 1)").unwrap(); // active
                                                              // 只删除 inactive category 的 items
    let result = eng
        .run_sql("DELETE FROM items USING cats WHERE items.category = cats.id AND cats.active = 0")
        .unwrap();
    assert_eq!(result[0][0], Value::Integer(2)); // id=1,3
    let remaining = eng.run_sql("SELECT * FROM items").unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0][0], Value::Integer(2));
}

#[test]
fn delete_using_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_del (id INT, ref_id INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE t_src (id INT, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t_del VALUES (1, 999)").unwrap();
    eng.run_sql("INSERT INTO t_src VALUES (1, 'x')").unwrap();
    // 无匹配：t_del.ref_id=999 不在 t_src.id 中
    let result = eng
        .run_sql("DELETE FROM t_del USING t_src WHERE t_del.ref_id = t_src.id")
        .unwrap();
    assert_eq!(result[0][0], Value::Integer(0));
    let remaining = eng.run_sql("SELECT * FROM t_del").unwrap();
    assert_eq!(remaining.len(), 1);
}

#[test]
fn delete_using_same_table_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t_self (id INT, val TEXT)")
        .unwrap();
    let err = eng
        .run_sql("DELETE FROM t_self USING t_self WHERE t_self.id = t_self.id")
        .unwrap_err();
    assert!(err.to_string().contains("源表不能与目标表同名"));
}

#[test]
fn delete_using_parser() {
    let stmt = parse("DELETE FROM t1 USING t2 WHERE t1.id = t2.id").unwrap();
    match stmt {
        Stmt::Delete {
            table,
            using_table,
            where_clause,
            ..
        } => {
            assert_eq!(table, "t1");
            assert_eq!(using_table, Some("t2".to_string()));
            assert!(where_clause.is_some());
        }
        _ => panic!("expected Delete"),
    }
}

// ===== M164: COMMENT ON TABLE/COLUMN =====

#[test]
fn comment_on_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("COMMENT ON TABLE items IS '商品表'").unwrap();
    let desc = eng.run_sql("DESCRIBE items").unwrap();
    // DESCRIBE 返回 7 列：name, type, pk, nullable, default, fk, comment
    assert_eq!(desc[0].len(), 7);
}

#[test]
fn comment_on_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("COMMENT ON COLUMN items.name IS '商品名称'")
        .unwrap();
    let desc = eng.run_sql("DESCRIBE items").unwrap();
    // name 列（第 2 行）的 comment 列（第 7 列）
    assert_eq!(desc[1][6], Value::Text("商品名称".to_string()));
    // id 列无注释
    assert_eq!(desc[0][6], Value::Text(String::new()));
}

#[test]
fn comment_on_nonexistent_table() {
    let (_dir, mut eng) = tmp_engine();
    let err = eng.run_sql("COMMENT ON TABLE ghost IS 'nope'").unwrap_err();
    assert!(err.to_string().contains("表不存在"));
}

#[test]
fn comment_on_nonexistent_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, name TEXT)")
        .unwrap();
    let err = eng
        .run_sql("COMMENT ON COLUMN items.foo IS 'nope'")
        .unwrap_err();
    assert!(err.to_string().contains("列不存在"));
}

#[test]
fn comment_on_parser() {
    let stmt = parse("COMMENT ON TABLE users IS 'user table'").unwrap();
    match stmt {
        Stmt::Comment {
            table,
            column,
            text,
        } => {
            assert_eq!(table, "users");
            assert!(column.is_none());
            assert_eq!(text, "user table");
        }
        _ => panic!("expected Comment"),
    }
    let stmt2 = parse("COMMENT ON COLUMN users.name IS 'display name'").unwrap();
    match stmt2 {
        Stmt::Comment {
            table,
            column,
            text,
        } => {
            assert_eq!(table, "users");
            assert_eq!(column, Some("name".to_string()));
            assert_eq!(text, "display name");
        }
        _ => panic!("expected Comment"),
    }
}

// ===== M165: ALTER TABLE SET DEFAULT / DROP DEFAULT =====

#[test]
fn alter_set_default() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t165 (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("ALTER TABLE t165 ALTER COLUMN name SET DEFAULT 'unknown'")
        .unwrap();
    // INSERT 不指定 name 列，应使用新默认值
    eng.run_sql("INSERT INTO t165 (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t165 WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Text("unknown".to_string()));
}

#[test]
fn alter_drop_default() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t165b (id INT, score INT DEFAULT 100)")
        .unwrap();
    eng.run_sql("ALTER TABLE t165b ALTER COLUMN score DROP DEFAULT")
        .unwrap();
    // INSERT 不指定 score 列，应为 NULL（默认值已删除）
    eng.run_sql("INSERT INTO t165b (id) VALUES (1)").unwrap();
    let rows = eng.run_sql("SELECT * FROM t165b WHERE id = 1").unwrap();
    assert_eq!(rows[0][1], Value::Null);
}

#[test]
fn alter_set_default_nonexistent_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t165c (id INT, name TEXT)")
        .unwrap();
    let err = eng
        .run_sql("ALTER TABLE t165c ALTER COLUMN ghost SET DEFAULT 'x'")
        .unwrap_err();
    assert!(err.to_string().contains("列不存在"));
}

#[test]
fn alter_set_default_parser() {
    let stmt = parse("ALTER TABLE users ALTER COLUMN name SET DEFAULT 'anon'").unwrap();
    match stmt {
        Stmt::AlterTable {
            table,
            action: AlterAction::SetDefault { column, value },
        } => {
            assert_eq!(table, "users");
            assert_eq!(column, "name");
            assert_eq!(value, Value::Text("anon".to_string()));
        }
        _ => panic!("expected AlterTable SetDefault"),
    }
    let stmt2 = parse("ALTER TABLE users ALTER COLUMN score DROP DEFAULT").unwrap();
    match stmt2 {
        Stmt::AlterTable {
            table,
            action: AlterAction::DropDefault { column },
        } => {
            assert_eq!(table, "users");
            assert_eq!(column, "score");
        }
        _ => panic!("expected AlterTable DropDefault"),
    }
}

// ===== M166: ALTER TABLE ADD CONSTRAINT UNIQUE =====

#[test]
fn alter_add_constraint_unique() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t166 (id INT, email TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t166 VALUES (1, 'a@b.com')")
        .unwrap();
    eng.run_sql("ALTER TABLE t166 ADD CONSTRAINT uq_email UNIQUE(email)")
        .unwrap();
    // 重复 email 应报错
    let err = eng
        .run_sql("INSERT INTO t166 VALUES (2, 'a@b.com')")
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE") || err.to_string().contains("唯一"));
    // 不同 email 应成功
    eng.run_sql("INSERT INTO t166 VALUES (3, 'c@d.com')")
        .unwrap();
}

#[test]
fn alter_add_constraint_unique_composite() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t166b (id INT, a TEXT, b TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t166b VALUES (1, 'x', 'y')")
        .unwrap();
    eng.run_sql("ALTER TABLE t166b ADD CONSTRAINT uq_ab UNIQUE(a, b)")
        .unwrap();
    // 相同 (a,b) 组合应报错
    let err = eng
        .run_sql("INSERT INTO t166b VALUES (2, 'x', 'y')")
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE") || err.to_string().contains("唯一"));
    // 不同组合应成功
    eng.run_sql("INSERT INTO t166b VALUES (3, 'x', 'z')")
        .unwrap();
}

#[test]
fn alter_add_constraint_parser() {
    let stmt = parse("ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE(email)").unwrap();
    match stmt {
        Stmt::CreateIndex {
            index_name,
            table,
            columns,
            unique,
        } => {
            assert_eq!(index_name, "uq_email");
            assert_eq!(table, "users");
            assert_eq!(columns, vec!["email".to_string()]);
            assert!(unique);
        }
        _ => panic!("expected CreateIndex"),
    }
}

// ── M167: MySQL 风格多表 UPDATE (UPDATE t1 JOIN t2 ON ... SET ...) ──

#[test]
fn update_join_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE updates (id INT, new_price INT)")
        .unwrap();
    eng.run_sql("INSERT INTO products VALUES (1, 'A', 100), (2, 'B', 200), (3, 'C', 300)")
        .unwrap();
    eng.run_sql("INSERT INTO updates VALUES (1, 150), (3, 350)")
        .unwrap();
    eng.run_sql(
        "UPDATE products JOIN updates ON products.id = updates.id SET products.price = updates.new_price",
    )
    .unwrap();
    let rows = eng
        .run_sql("SELECT id, price FROM products ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Integer(150)); // 1: 100→150
    assert_eq!(rows[1][1], Value::Integer(200)); // 2: 不变
    assert_eq!(rows[2][1], Value::Integer(350)); // 3: 300→350
}

#[test]
fn update_join_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE emp (id INT, name TEXT, dept_id INT, salary INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE dept (id INT, bonus INT)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (1, 'Alice', 10, 1000), (2, 'Bob', 20, 2000), (3, 'Carol', 10, 1500)")
        .unwrap();
    eng.run_sql("INSERT INTO dept VALUES (10, 500), (20, 300)")
        .unwrap();
    // 只更新 dept_id=10 的员工
    eng.run_sql(
        "UPDATE emp JOIN dept ON emp.dept_id = dept.id SET emp.salary = dept.bonus WHERE emp.dept_id = 10",
    )
    .unwrap();
    let rows = eng
        .run_sql("SELECT id, salary FROM emp ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Integer(500)); // Alice: dept 10 → bonus 500
    assert_eq!(rows[1][1], Value::Integer(2000)); // Bob: dept 20, 不匹配 WHERE
    assert_eq!(rows[2][1], Value::Integer(500)); // Carol: dept 10 → bonus 500
}

#[test]
fn update_inner_join_syntax() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INT, val TEXT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INT, val TEXT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 'old')").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (1, 'new')").unwrap();
    // INNER JOIN 语法
    eng.run_sql("UPDATE t1 INNER JOIN t2 ON t1.id = t2.id SET t1.val = t2.val")
        .unwrap();
    let rows = eng.run_sql("SELECT val FROM t1").unwrap();
    assert_eq!(rows[0][0], Value::Text("new".into()));
}

#[test]
fn update_join_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INT, val INT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (99, 999)").unwrap();
    // 无匹配行，更新 0 行
    let rows = eng
        .run_sql("UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.val = t2.val")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(0));
    // 原值不变
    let data = eng.run_sql("SELECT val FROM t1").unwrap();
    assert_eq!(data[0][0], Value::Integer(100));
}

#[test]
fn update_join_same_table_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INT, val INT)").unwrap();
    let err = eng
        .run_sql("UPDATE t1 JOIN t1 ON t1.id = t1.id SET t1.val = 1")
        .unwrap_err();
    assert!(err.to_string().contains("同名"));
}

#[test]
fn update_join_parser() {
    let stmt =
        parse("UPDATE t1 JOIN t2 ON t1.id = t2.id SET t1.name = t2.name WHERE t2.active = 1")
            .unwrap();
    match stmt {
        Stmt::Update {
            table,
            from_table,
            assignments,
            where_clause,
            ..
        } => {
            assert_eq!(table, "t1");
            assert_eq!(from_table, Some("t2".to_string()));
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, "name");
            assert!(where_clause.is_some());
        }
        _ => panic!("expected Update"),
    }
}

// ── M168: MySQL 风格多表 DELETE (DELETE t1 FROM t1 JOIN t2 ON ...) ──

#[test]
fn delete_join_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE orders (id INT, customer_id INT, amount INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE blacklist (id INT, customer_id INT)")
        .unwrap();
    eng.run_sql("INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)")
        .unwrap();
    eng.run_sql("INSERT INTO blacklist VALUES (1, 10)").unwrap();
    eng.run_sql(
        "DELETE orders FROM orders JOIN blacklist ON orders.customer_id = blacklist.customer_id",
    )
    .unwrap();
    let rows = eng.run_sql("SELECT * FROM orders ORDER BY id").unwrap();
    assert_eq!(rows.len(), 1); // 只剩 customer_id=20 的订单
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn delete_join_with_where() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE items (id INT, category INT, val INT)")
        .unwrap();
    eng.run_sql("CREATE TABLE expired (id INT, category INT)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
        .unwrap();
    eng.run_sql("INSERT INTO expired VALUES (1, 1)").unwrap();
    // 只删除 val < 15 的匹配行（WHERE 条件不带表前缀，归目标表过滤）
    eng.run_sql(
        "DELETE items FROM items JOIN expired ON items.category = expired.category WHERE val < 15",
    )
    .unwrap();
    let rows = eng.run_sql("SELECT * FROM items ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2); // id=1 被删，id=2 和 id=3 保留
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn delete_join_no_match() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INT, val INT)").unwrap();
    eng.run_sql("CREATE TABLE t2 (id INT, ref_id INT)").unwrap();
    eng.run_sql("INSERT INTO t1 VALUES (1, 100)").unwrap();
    eng.run_sql("INSERT INTO t2 VALUES (1, 999)").unwrap();
    let rows = eng
        .run_sql("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.ref_id")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(0)); // 0 行删除
    let data = eng.run_sql("SELECT * FROM t1").unwrap();
    assert_eq!(data.len(), 1);
}

#[test]
fn delete_join_same_table_error() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t1 (id INT)").unwrap();
    let err = eng
        .run_sql("DELETE t1 FROM t1 JOIN t1 ON t1.id = t1.id")
        .unwrap_err();
    assert!(err.to_string().contains("同名"));
}

#[test]
fn delete_join_parser() {
    let stmt = parse("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id WHERE t2.active = 0").unwrap();
    match stmt {
        Stmt::Delete {
            table,
            using_table,
            where_clause,
            ..
        } => {
            assert_eq!(table, "t1");
            assert_eq!(using_table, Some("t2".to_string()));
            assert!(where_clause.is_some());
        }
        _ => panic!("expected Delete"),
    }
}

// ========== M169: ALTER TABLE ALTER COLUMN TYPE / MODIFY ==========

#[test]
fn alter_type_pg_syntax() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169 (id INT, score TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t169 VALUES (1, '42')").unwrap();
    eng.run_sql("INSERT INTO t169 VALUES (2, '99')").unwrap();
    eng.run_sql("ALTER TABLE t169 ALTER COLUMN score TYPE INTEGER")
        .unwrap();
    let rows = eng
        .run_sql("SELECT id, score FROM t169 ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Integer(42));
    assert_eq!(rows[1][1], Value::Integer(99));
}

#[test]
fn alter_type_mysql_modify() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169b (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t169b VALUES (1, 100)").unwrap();
    eng.run_sql("ALTER TABLE t169b MODIFY val TEXT").unwrap();
    let rows = eng
        .run_sql("SELECT id, val FROM t169b ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Text("100".to_string()));
}

#[test]
fn alter_type_mysql_modify_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169c (id INT, val FLOAT)")
        .unwrap();
    eng.run_sql("INSERT INTO t169c VALUES (1, 3.14)").unwrap();
    eng.run_sql("ALTER TABLE t169c MODIFY COLUMN val INTEGER")
        .unwrap();
    let rows = eng
        .run_sql("SELECT id, val FROM t169c ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Integer(3));
}

#[test]
fn alter_type_pk_forbidden() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169d (id INT, name TEXT)")
        .unwrap();
    let err = eng
        .run_sql("ALTER TABLE t169d ALTER COLUMN id TYPE TEXT")
        .unwrap_err();
    assert!(err.to_string().contains("主键"));
}

#[test]
fn alter_type_convert_fail() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169e (id INT, name TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t169e VALUES (1, 'hello')")
        .unwrap();
    let err = eng
        .run_sql("ALTER TABLE t169e ALTER COLUMN name TYPE INTEGER")
        .unwrap_err();
    assert!(err.to_string().contains("转换"));
}

#[test]
fn alter_type_null_preserved() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169f (id INT, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t169f VALUES (1, NULL)").unwrap();
    eng.run_sql("INSERT INTO t169f VALUES (2, '77')").unwrap();
    eng.run_sql("ALTER TABLE t169f ALTER COLUMN val TYPE INTEGER")
        .unwrap();
    let rows = eng
        .run_sql("SELECT id, val FROM t169f ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[1][1], Value::Integer(77));
}

#[test]
fn alter_type_same_type_noop() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t169g (id INT, val TEXT)")
        .unwrap();
    eng.run_sql("INSERT INTO t169g VALUES (1, 'ok')").unwrap();
    eng.run_sql("ALTER TABLE t169g ALTER COLUMN val TYPE TEXT")
        .unwrap();
    let rows = eng
        .run_sql("SELECT id, val FROM t169g ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][1], Value::Text("ok".to_string()));
}

#[test]
fn insert_21_columns_parameterized() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE causal_events (
        id TEXT NOT NULL, kind TEXT NOT NULL, signalKey TEXT NOT NULL, signals TEXT,
        errorSignature TEXT, strategyId TEXT, strategyCategory TEXT, mutationId TEXT,
        mutationCategory TEXT, mutationRiskLevel TEXT, mutationTriggerSignals TEXT,
        mutationExpectedEffect TEXT, personalityKey TEXT, personalityState TEXT,
        outcomeStatus TEXT, outcomeScore FLOAT, outcomeNote TEXT,
        actionId TEXT, hypothesisId TEXT, metadata TEXT, createdAt TEXT NOT NULL
    )").unwrap();

    let result = eng.run_sql_param(
        "INSERT INTO causal_events (id, kind, signalKey, signals, errorSignature, strategyId, strategyCategory, mutationId, mutationCategory, mutationRiskLevel, mutationTriggerSignals, mutationExpectedEffect, personalityKey, personalityState, outcomeStatus, outcomeScore, outcomeNote, actionId, hypothesisId, metadata, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        &[
            Value::Text("evt-1".into()), Value::Text("outcome".into()), Value::Text("sig-1".into()), Value::Text("[]".into()),
            Value::Null, Value::Null, Value::Null, Value::Null,
            Value::Null, Value::Null, Value::Null, Value::Null,
            Value::Null, Value::Null, Value::Text("success".into()),
            Value::Float(1.0), Value::Null,
            Value::Null, Value::Null,
            Value::Null, Value::Text("2026-01-01 00:00:00".into()),
        ],
    );
    assert!(result.is_ok(), "21-col INSERT failed: {:?}", result.err());

    let rows = eng.run_sql("SELECT id, kind, outcomeScore FROM causal_events").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("evt-1".into()));
    assert_eq!(rows[0][2], Value::Float(1.0));
}

// ── Date / Time 类型集成测试 ────────────────────────────────

#[test]
fn engine_date_time_create_insert_select() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE events (id INT, event_date DATE, event_time TIME)")
        .unwrap();
    eng.run_sql("INSERT INTO events (id, event_date, event_time) VALUES (1, DATE '2024-03-01', TIME '12:30:45')")
        .unwrap();
    eng.run_sql("INSERT INTO events (id, event_date, event_time) VALUES (2, DATE '2024-06-15', TIME '09:00:00')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM events").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(matches!(rows[0][1], Value::Date(_)));
    assert!(matches!(rows[0][2], Value::Time(_)));
    assert!(matches!(rows[1][1], Value::Date(_)));
    assert!(matches!(rows[1][2], Value::Time(_)));
}

#[test]
fn engine_date_time_cast() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE, t TIME)")
        .unwrap();
    eng.run_sql("INSERT INTO t (id, d, t) VALUES (1, DATE '2024-03-01', TIME '12:30:45')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT CAST(d AS TEXT), CAST(t AS TEXT) FROM t")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("2024-03-01".into()));
    assert_eq!(rows[0][1], Value::Text("12:30:45".into()));
}

#[test]
fn engine_date_time_null_handling() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, d DATE, t TIME)")
        .unwrap();
    eng.run_sql("INSERT INTO t (id, d, t) VALUES (1, NULL, NULL)")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[0][2], Value::Null);
}
