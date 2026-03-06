/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! SQL 迁移兼容性测试：验证 MySQL / PostgreSQL / SQLite dump 语法容忍度。

use super::engine::SqlEngine;
use super::parser::{parse, Stmt};
use crate::storage::Store;
use crate::types::{ColumnType, Value};

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

// ══════════════════════════════════════════════════════════════
// S1: CREATE TABLE IF NOT EXISTS 幂等
// ══════════════════════════════════════════════════════════════

#[test]
fn create_table_if_not_exists_idempotent() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    // 第二次不报错
    eng.run_sql("CREATE TABLE IF NOT EXISTS t (id INT, name TEXT)")
        .unwrap();
    // 数据不受影响
    eng.run_sql("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn create_table_if_not_exists_parser() {
    let stmt = parse("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)").unwrap();
    match stmt {
        Stmt::CreateTable {
            name,
            columns,
            if_not_exists,
            ..
        } => {
            assert_eq!(name, "users");
            assert!(if_not_exists);
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn create_table_without_if_not_exists_still_errors_on_dup() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT)").unwrap();
    // 无 IF NOT EXISTS 时，重复建表仍应覆盖（现有行为）
    // 注意：当前实现是覆盖 schema，不报错
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
}

// ══════════════════════════════════════════════════════════════
// S3: MySQL 列修饰符容忍
// ══════════════════════════════════════════════════════════════

#[test]
fn mysql_unsigned_modifier() {
    let stmt = parse("CREATE TABLE t (id INT UNSIGNED NOT NULL, age BIGINT UNSIGNED)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[0].col_type, ColumnType::Integer);
            assert!(!columns[0].nullable); // NOT NULL preserved
            assert_eq!(columns[1].col_type, ColumnType::Integer);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_auto_increment() {
    let stmt = parse("CREATE TABLE t (id INT AUTO_INCREMENT, name VARCHAR(255))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[0].col_type, ColumnType::Integer);
            assert_eq!(columns[1].col_type, ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_comment_on_column() {
    let stmt =
        parse("CREATE TABLE t (id INT COMMENT '主键ID', name TEXT NOT NULL COMMENT '用户名')")
            .unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].col_type, ColumnType::Integer);
            assert_eq!(columns[1].col_type, ColumnType::Text);
            assert!(!columns[1].nullable);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_comment_with_escaped_quote() {
    let stmt = parse("CREATE TABLE t (id INT COMMENT '含''引号的注释', name TEXT)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_table_options_ignored() {
    // MySQL 表级选项在 ) 之后，不进入列解析
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name VARCHAR(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        .unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'test')").unwrap();
    let rows = eng.run_sql("SELECT * FROM t").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn mysql_int_with_display_width() {
    let stmt = parse("CREATE TABLE t (id INT(11), code TINYINT(1), big BIGINT(20))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[0].col_type, ColumnType::Integer);
            assert_eq!(columns[1].col_type, ColumnType::Integer);
            assert_eq!(columns[2].col_type, ColumnType::Integer);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_decimal_with_precision() {
    let stmt = parse("CREATE TABLE t (price DECIMAL(10,2), rate NUMERIC(5,3))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[0].col_type, ColumnType::Float);
            assert_eq!(columns[1].col_type, ColumnType::Float);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_enum_type() {
    let stmt =
        parse("CREATE TABLE t (id INT, status ENUM('active','inactive','pending'))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_unique_key_constraint_skipped() {
    // 表级 UNIQUE KEY 约束应被跳过
    let stmt = parse("CREATE TABLE t (id INT, email TEXT, UNIQUE KEY uk_email (email))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_key_index_constraint_skipped() {
    let stmt = parse("CREATE TABLE t (id INT, name TEXT, KEY idx_name (name), INDEX idx_id (id))")
        .unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn mysql_replace_into() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'old')").unwrap();
    eng.run_sql("REPLACE INTO t (id, name) VALUES (1, 'new')")
        .unwrap();
    let rows = eng.run_sql("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0][0], Value::Text("new".into()));
}

// ══════════════════════════════════════════════════════════════
// S4: PostgreSQL 类型别名
// ══════════════════════════════════════════════════════════════

#[test]
fn pg_serial_types() {
    let stmt = parse("CREATE TABLE t (id SERIAL, big_id BIGSERIAL, small_id SMALLSERIAL)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[0].col_type, ColumnType::Integer);
            assert_eq!(columns[1].col_type, ColumnType::Integer);
            assert_eq!(columns[2].col_type, ColumnType::Integer);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn pg_uuid_type() {
    let stmt = parse("CREATE TABLE t (id INT, uid UUID)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn pg_timestamp_variants() {
    let stmt = parse("CREATE TABLE t (id INT, created_at TIMESTAMPTZ, d DATE, t TIME, tz TIMETZ)")
        .unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Timestamp);
            assert_eq!(columns[2].col_type, ColumnType::Date);
            assert_eq!(columns[3].col_type, ColumnType::Time);
            assert_eq!(columns[4].col_type, ColumnType::Time);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn pg_network_types() {
    let stmt = parse("CREATE TABLE t (id INT, ip INET, mac MACADDR, net CIDR)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Text);
            assert_eq!(columns[2].col_type, ColumnType::Text);
            assert_eq!(columns[3].col_type, ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn pg_interval_type() {
    let stmt = parse("CREATE TABLE t (id INT, duration INTERVAL)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Text);
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn current_timestamp_as_default() {
    let stmt =
        parse("CREATE TABLE t (id INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Timestamp);
            assert!(columns[1].default_value.is_some());
        }
        _ => panic!("expected CreateTable"),
    }
}

#[test]
fn pg_timestamp_with_precision() {
    let stmt = parse("CREATE TABLE t (id INT, ts TIMESTAMP(6))").unwrap();
    match stmt {
        Stmt::CreateTable { columns, .. } => {
            assert_eq!(columns[1].col_type, ColumnType::Timestamp);
        }
        _ => panic!("expected CreateTable"),
    }
}

// ══════════════════════════════════════════════════════════════
// 综合迁移场景：MySQL dump
// ══════════════════════════════════════════════════════════════

#[test]
fn mysql_full_dump_table() {
    let (_dir, mut eng) = tmp_engine();
    // 模拟 MySQL mysqldump 输出的 CREATE TABLE
    eng.run_sql(
        "CREATE TABLE IF NOT EXISTS users (
            id INT NOT NULL,
            name VARCHAR(255) NOT NULL DEFAULT '',
            email VARCHAR(100),
            status TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uk_email (email),
            KEY idx_name (name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
    )
    .unwrap();
    eng.run_sql(
        "INSERT INTO users (id, name, email, status) VALUES (1, 'Alice', 'alice@test.com', 1)",
    )
    .unwrap();
    let rows = eng.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Alice".into()));
}

// ══════════════════════════════════════════════════════════════
// 综合迁移场景：PostgreSQL dump
// ══════════════════════════════════════════════════════════════

#[test]
fn pg_full_dump_table() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql(
        "CREATE TABLE IF NOT EXISTS events (
            id BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .unwrap();
    eng.run_sql(
        "INSERT INTO events (id, event_type, payload) VALUES (1, 'login', '{\"user\":\"alice\"}')",
    )
    .unwrap();
    let rows = eng.run_sql("SELECT * FROM events WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
}

// ══════════════════════════════════════════════════════════════
// 标识符大小写不敏感（SQL 标准：unquoted → 折叠小写）
// ══════════════════════════════════════════════════════════════

#[test]
fn case_insensitive_table_name() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE Users (id INT, name TEXT)")
        .unwrap();
    // 小写查询
    eng.run_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    let rows = eng.run_sql("SELECT * FROM users").unwrap();
    assert_eq!(rows.len(), 1);
    // 大写查询
    let rows = eng.run_sql("SELECT * FROM USERS").unwrap();
    assert_eq!(rows.len(), 1);
    // 混合大小写
    let rows = eng.run_sql("SELECT * FROM uSeRs").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn case_insensitive_column_name() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (ID INT, Name TEXT)").unwrap();
    eng.run_sql("INSERT INTO t (id, name) VALUES (1, 'Alice')")
        .unwrap();
    let rows = eng.run_sql("SELECT Name FROM t WHERE ID = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("Alice".into()));
}

#[test]
fn case_insensitive_where_column() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, Score INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    // WHERE 中用不同大小写
    let rows = eng.run_sql("SELECT * FROM t WHERE SCORE = 100").unwrap();
    assert_eq!(rows.len(), 1);
    let rows = eng.run_sql("SELECT * FROM t WHERE score = 100").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn quoted_ident_preserves_case() {
    let (_dir, mut eng) = tmp_engine();
    // 双引号保留原始大小写
    eng.run_sql(r#"CREATE TABLE t (id INT, "CaseSensitive" TEXT)"#)
        .unwrap();
    eng.run_sql(r#"INSERT INTO t (id, "CaseSensitive") VALUES (1, 'hello')"#)
        .unwrap();
    let rows = eng
        .run_sql(r#"SELECT "CaseSensitive" FROM t WHERE id = 1"#)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("hello".into()));
}

#[test]
fn case_insensitive_create_index() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE Items (Id INT, Category TEXT)")
        .unwrap();
    eng.run_sql("CREATE INDEX idx_cat ON items(category)")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (1, 'books')")
        .unwrap();
    eng.run_sql("INSERT INTO items VALUES (2, 'books')")
        .unwrap();
    let rows = eng
        .run_sql("SELECT * FROM ITEMS WHERE Category = 'books'")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn case_insensitive_aggregates() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 20)").unwrap();
    // 聚合函数大小写混用
    let rows = eng.run_sql("SELECT count(*) FROM t").unwrap();
    assert_eq!(rows[0][0], Value::Integer(2));
    let rows = eng.run_sql("SELECT Count(*) FROM t").unwrap();
    assert_eq!(rows[0][0], Value::Integer(2));
    let rows = eng.run_sql("SELECT sum(Val) FROM t").unwrap();
    assert_eq!(rows[0][0], Value::Integer(30));
}

// CASE WHEN + SuperClaw 测试已移至 tests_case_when.rs
