/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M92: JOIN 单元测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn setup() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let engine = SqlEngine::new(&store).unwrap();
    (dir, engine)
}

fn text(s: &str) -> Value {
    Value::Text(s.to_string())
}
#[allow(dead_code)]
fn int(n: i64) -> Value {
    Value::Integer(n)
}

fn setup_two_tables(e: &mut SqlEngine) {
    e.run_sql("CREATE TABLE users (id INT, name TEXT, dept_id INT)")
        .unwrap();
    e.run_sql("CREATE TABLE depts (id INT, dept_name TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO users (id, name, dept_id) VALUES (1, 'Alice', 10)")
        .unwrap();
    e.run_sql("INSERT INTO users (id, name, dept_id) VALUES (2, 'Bob', 20)")
        .unwrap();
    e.run_sql("INSERT INTO users (id, name, dept_id) VALUES (3, 'Charlie', 10)")
        .unwrap();
    e.run_sql("INSERT INTO users (id, name, dept_id) VALUES (4, 'Diana', 30)")
        .unwrap();
    e.run_sql("INSERT INTO depts (id, dept_name) VALUES (10, 'Engineering')")
        .unwrap();
    e.run_sql("INSERT INTO depts (id, dept_name) VALUES (20, 'Marketing')")
        .unwrap();
    // dept 30 intentionally missing for LEFT JOIN test
}

// ── INNER JOIN ──

#[test]
fn inner_join_pk() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT * FROM users JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    // Alice(10), Bob(20), Charlie(10) match; Diana(30) no match
    assert_eq!(rows.len(), 3);
}

#[test]
fn inner_join_explicit() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT * FROM users INNER JOIN depts ON dept_id = id")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn inner_join_select_columns() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT name, dept_name FROM users JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    assert_eq!(rows.len(), 3);
    // Each row should have 2 columns
    for row in &rows {
        assert_eq!(row.len(), 2);
    }
    // Check Alice -> Engineering
    let alice_row = rows.iter().find(|r| r[0] == text("Alice")).unwrap();
    assert_eq!(alice_row[1], text("Engineering"));
}

#[test]
fn inner_join_with_where() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql(
            "SELECT name, dept_name FROM users JOIN depts ON users.dept_id = depts.id WHERE dept_name = 'Engineering'",
        )
        .unwrap();
    assert_eq!(rows.len(), 2); // Alice and Charlie
}

#[test]
fn inner_join_with_limit() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT * FROM users JOIN depts ON users.dept_id = depts.id LIMIT 2")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

// ── LEFT JOIN ──

#[test]
fn left_join_with_nulls() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT * FROM users LEFT JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    // All 4 users should appear; Diana has NULL dept columns
    assert_eq!(rows.len(), 4);
    let diana_row = rows.iter().find(|r| r[1] == text("Diana")).unwrap();
    // Right table columns (id, dept_name) should be NULL for Diana
    let right_start = 3; // users has 3 cols
    assert_eq!(diana_row[right_start], Value::Null);
    assert_eq!(diana_row[right_start + 1], Value::Null);
}

#[test]
fn left_join_select_columns() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT name, dept_name FROM users LEFT JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    assert_eq!(rows.len(), 4);
    let diana = rows.iter().find(|r| r[0] == text("Diana")).unwrap();
    assert_eq!(diana[1], Value::Null);
}

// ── JOIN with ORDER BY ──

#[test]
fn join_order_by() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql(
            "SELECT name, dept_name FROM users JOIN depts ON users.dept_id = depts.id ORDER BY name",
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], text("Alice"));
    assert_eq!(rows[1][0], text("Bob"));
    assert_eq!(rows[2][0], text("Charlie"));
}

// ── JOIN with index on right table ──

#[test]
fn join_with_index() {
    let (_dir, mut e) = setup();
    e.run_sql("CREATE TABLE orders (id INT, user_id INT, amount INT)")
        .unwrap();
    e.run_sql("CREATE TABLE customers (id INT, cname TEXT)")
        .unwrap();
    e.run_sql("CREATE INDEX idx_orders_uid ON orders(user_id)")
        .unwrap();
    e.run_sql("INSERT INTO customers (id, cname) VALUES (1, 'Alice')")
        .unwrap();
    e.run_sql("INSERT INTO customers (id, cname) VALUES (2, 'Bob')")
        .unwrap();
    e.run_sql("INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50)")
        .unwrap();
    e.run_sql("INSERT INTO orders (id, user_id, amount) VALUES (101, 1, 30)")
        .unwrap();
    e.run_sql("INSERT INTO orders (id, user_id, amount) VALUES (102, 2, 80)")
        .unwrap();

    let rows = e
        .run_sql("SELECT cname, amount FROM customers JOIN orders ON customers.id = orders.user_id")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

// ── Parser tests ──

#[test]
fn parse_inner_join() {
    use super::parser::{parse, JoinType, Stmt};
    let stmt = parse("SELECT * FROM a JOIN b ON a.x = b.y").unwrap();
    match stmt {
        Stmt::Select { join: Some(j), .. } => {
            assert_eq!(j.join_type, JoinType::Inner);
            assert_eq!(j.table, "b");
            assert_eq!(j.left_col, "x");
            assert_eq!(j.right_col, "y");
        }
        _ => panic!("expected Select with JOIN"),
    }
}

#[test]
fn parse_left_join() {
    use super::parser::{parse, JoinType, Stmt};
    let stmt = parse("SELECT * FROM a LEFT JOIN b ON a.x = b.y WHERE z = 1").unwrap();
    match stmt {
        Stmt::Select {
            join: Some(j),
            where_clause: Some(_),
            ..
        } => {
            assert_eq!(j.join_type, JoinType::Left);
            assert_eq!(j.table, "b");
        }
        _ => panic!("expected Select with LEFT JOIN + WHERE"),
    }
}

#[test]
fn parse_no_join() {
    use super::parser::{parse, Stmt};
    let stmt = parse("SELECT * FROM t WHERE id = 1").unwrap();
    match stmt {
        Stmt::Select { join: None, .. } => {}
        _ => panic!("expected Select without JOIN"),
    }
}

#[test]
fn join_with_table_alias_as() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT name, dept_name FROM users AS u JOIN depts AS d ON u.dept_id = d.id")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn join_with_implicit_alias() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    let rows = e
        .run_sql("SELECT name, dept_name FROM users u JOIN depts d ON u.dept_id = d.id")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn right_join_basic() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    // RIGHT JOIN: 所有 depts 都出现，即使没有匹配的 users
    let rows = e
        .run_sql("SELECT name, dept_name FROM users RIGHT JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    // dept 10 匹配 Alice+Charlie, dept 20 匹配 Bob, dept 30 无匹配但 RIGHT JOIN 保留
    assert!(
        rows.len() >= 3,
        "RIGHT JOIN should include unmatched right rows, got {}",
        rows.len()
    );
}

#[test]
fn multi_table_join_three_tables() {
    let (_dir, mut e) = setup();
    e.run_sql("CREATE TABLE orders (id INT, user_id INT, product_id INT)")
        .unwrap();
    e.run_sql("CREATE TABLE users (id INT, name TEXT)").unwrap();
    e.run_sql("CREATE TABLE products (id INT, pname TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO orders VALUES (1, 10, 100)").unwrap();
    e.run_sql("INSERT INTO orders VALUES (2, 20, 100)").unwrap();
    e.run_sql("INSERT INTO orders VALUES (3, 10, 200)").unwrap();
    e.run_sql("INSERT INTO users VALUES (10, 'Alice')").unwrap();
    e.run_sql("INSERT INTO users VALUES (20, 'Bob')").unwrap();
    e.run_sql("INSERT INTO products VALUES (100, 'Widget')")
        .unwrap();
    e.run_sql("INSERT INTO products VALUES (200, 'Gadget')")
        .unwrap();

    // 三表 JOIN: orders JOIN users ON user_id = id JOIN products ON product_id = id
    let rows = e
        .run_sql(
            "SELECT * FROM orders \
             JOIN users ON orders.user_id = users.id \
             JOIN products ON orders.product_id = products.id",
        )
        .unwrap();
    // 3 orders × 匹配 users × 匹配 products = 3 行
    assert_eq!(rows.len(), 3, "三表 JOIN 应返回 3 行, got {}", rows.len());
    // 第一行: order 1 → Alice → Widget
    // 列顺序: orders(id, user_id, product_id) + users(id, name) + products(id, pname)
    // = [1, 10, 100, 10, 'Alice', 100, 'Widget']
}

// ── M162: FULL OUTER JOIN ──

#[test]
fn full_join_basic() {
    let (_dir, mut e) = setup();
    setup_two_tables(&mut e);
    // users: dept_id 10,20,10,30; depts: id 10,20 (no 30)
    let rows = e
        .run_sql("SELECT * FROM users FULL OUTER JOIN depts ON users.dept_id = depts.id")
        .unwrap();
    // LEFT part: 4 users (Diana dept_id=30 → right NULL)
    // RIGHT unmatched: none (depts 10,20 both matched)
    // Total: Alice(10)+Bob(20)+Charlie(10)+Diana(30,NULL) = 4 rows
    assert_eq!(
        rows.len(),
        4,
        "FULL JOIN should return 4 rows, got {}",
        rows.len()
    );
}

#[test]
fn full_join_with_unmatched_right() {
    let (_dir, mut e) = setup();
    e.run_sql("CREATE TABLE t_left (id INT, val TEXT)").unwrap();
    e.run_sql("CREATE TABLE t_right (id INT, info TEXT)")
        .unwrap();
    e.run_sql("INSERT INTO t_left VALUES (1, 'a')").unwrap();
    e.run_sql("INSERT INTO t_left VALUES (2, 'b')").unwrap();
    e.run_sql("INSERT INTO t_right VALUES (2, 'x')").unwrap();
    e.run_sql("INSERT INTO t_right VALUES (3, 'y')").unwrap();

    let rows = e
        .run_sql("SELECT * FROM t_left FULL JOIN t_right ON t_left.id = t_right.id")
        .unwrap();
    // Left 1 → no right match → [1, 'a', NULL, NULL]
    // Left 2 → right 2 match → [2, 'b', 2, 'x']
    // Right 3 → no left match → [NULL, NULL, 3, 'y']
    assert_eq!(
        rows.len(),
        3,
        "FULL JOIN should return 3 rows, got {}",
        rows.len()
    );

    // 验证有一行左表全 NULL（右表 id=3 未匹配）
    let null_left_rows: Vec<_> = rows
        .iter()
        .filter(|r| matches!(r[0], Value::Null))
        .collect();
    assert_eq!(null_left_rows.len(), 1, "should have 1 unmatched right row");
    // 该行右表 id=3
    assert_eq!(null_left_rows[0][2], Value::Integer(3));
    assert_eq!(null_left_rows[0][3], text("y"));

    // 验证有一行右表全 NULL（左表 id=1 未匹配）
    let null_right_rows: Vec<_> = rows
        .iter()
        .filter(|r| matches!(r[2], Value::Null))
        .collect();
    assert_eq!(null_right_rows.len(), 1, "should have 1 unmatched left row");
    assert_eq!(null_right_rows[0][0], Value::Integer(1));
    assert_eq!(null_right_rows[0][1], text("a"));
}

#[test]
fn full_join_both_empty_sides() {
    let (_dir, mut e) = setup();
    e.run_sql("CREATE TABLE fl (id INT, x TEXT)").unwrap();
    e.run_sql("CREATE TABLE fr (id INT, y TEXT)").unwrap();
    // 两表都空
    let rows = e
        .run_sql("SELECT * FROM fl FULL JOIN fr ON fl.id = fr.id")
        .unwrap();
    assert_eq!(rows.len(), 0);

    // 只有右表有数据
    e.run_sql("INSERT INTO fr VALUES (1, 'hello')").unwrap();
    let rows = e
        .run_sql("SELECT * FROM fl FULL JOIN fr ON fl.id = fr.id")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Null)); // 左表 NULL
    assert_eq!(rows[0][2], Value::Integer(1));
}

#[test]
fn full_join_parser() {
    use super::parser::{parse, JoinType, Stmt};
    let stmt = parse("SELECT * FROM a FULL OUTER JOIN b ON a.x = b.y").unwrap();
    match stmt {
        Stmt::Select { join: Some(j), .. } => {
            assert_eq!(j.join_type, JoinType::Full);
            assert_eq!(j.table, "b");
        }
        _ => panic!("expected SELECT with FULL JOIN"),
    }
    // FULL JOIN (without OUTER)
    let stmt2 = parse("SELECT * FROM a FULL JOIN b ON a.x = b.y").unwrap();
    match stmt2 {
        Stmt::Select { join: Some(j), .. } => assert_eq!(j.join_type, JoinType::Full),
        _ => panic!("expected SELECT with FULL JOIN"),
    }
}
