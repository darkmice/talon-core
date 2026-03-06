/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! CASE WHEN 表达式 + SuperClaw 迁移场景测试。

use super::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn tmp_engine() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let eng = SqlEngine::new(&store).unwrap();
    (dir, eng)
}

// ══════════════════════════════════════════════════════════════
// CASE WHEN 表达式
// ══════════════════════════════════════════════════════════════

#[test]
fn case_when_basic() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE students (id INT, score INT)")
        .unwrap();
    eng.run_sql("INSERT INTO students VALUES (1, 95)").unwrap();
    eng.run_sql("INSERT INTO students VALUES (2, 72)").unwrap();
    eng.run_sql("INSERT INTO students VALUES (3, 45)").unwrap();
    // ORDER BY id 保证行顺序
    let rows = eng.run_sql(
        "SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END FROM students ORDER BY id",
    ).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("A".into()));
    assert_eq!(rows[1][1], Value::Text("B".into()));
    assert_eq!(rows[2][1], Value::Text("C".into()));
}

#[test]
fn case_when_no_else() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 20)").unwrap();
    let rows = eng
        .run_sql("SELECT CASE WHEN val = 10 THEN 'ten' END FROM t ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("ten".into()));
    assert_eq!(rows[1][0], Value::Null);
}

#[test]
fn case_when_with_comparison_ops() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, x INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 5)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 10)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (3, 15)").unwrap();
    let rows = eng.run_sql(
        "SELECT CASE WHEN x > 12 THEN 'high' WHEN x < 8 THEN 'low' ELSE 'mid' END FROM t ORDER BY id",
    ).unwrap();
    assert_eq!(rows[0][0], Value::Text("low".into()));
    assert_eq!(rows[1][0], Value::Text("mid".into()));
    assert_eq!(rows[2][0], Value::Text("high".into()));
}

#[test]
fn case_when_integer_result() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, status TEXT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 'active')").unwrap();
    eng.run_sql("INSERT INTO t VALUES (2, 'inactive')").unwrap();
    let rows = eng
        .run_sql("SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM t ORDER BY id")
        .unwrap();
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(0));
}

#[test]
fn case_when_with_as_alias() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE t (id INT, val INT)").unwrap();
    eng.run_sql("INSERT INTO t VALUES (1, 100)").unwrap();
    let rows = eng
        .run_sql("SELECT CASE WHEN val >= 100 THEN 'pass' ELSE 'fail' END AS result FROM t")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("pass".into()));
}

// ══════════════════════════════════════════════════════════════
// SuperClaw 迁移场景（真实用户 SQL）
// ══════════════════════════════════════════════════════════════

#[test]
fn superclaw_session_entries_upsert() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql(
        "CREATE TABLE IF NOT EXISTS session_entries (
            id INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            type TEXT NOT NULL,
            ts INTEGER NOT NULL,
            data TEXT,
            dedup_key TEXT,
            UNIQUE(session_id, dedup_key)
        )",
    )
    .unwrap();
    // 首次插入
    eng.run_sql_param(
        "INSERT INTO session_entries (id, session_id, type, ts, data, dedup_key) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(session_id, dedup_key) DO UPDATE SET data = excluded.data, ts = excluded.ts",
        &[
            Value::Integer(1),
            Value::Text("s1".into()),
            Value::Text("message".into()),
            Value::Integer(1000),
            Value::Text("hello".into()),
            Value::Text("dk1".into()),
        ],
    ).unwrap();
    let rows = eng
        .run_sql("SELECT data, ts FROM session_entries WHERE id = 1")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("hello".into()));
    assert_eq!(rows[0][1], Value::Integer(1000));
    // UPSERT：相同 (session_id, dedup_key) → 更新 data 和 ts
    eng.run_sql_param(
        "INSERT INTO session_entries (id, session_id, type, ts, data, dedup_key) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(session_id, dedup_key) DO UPDATE SET data = excluded.data, ts = excluded.ts",
        &[
            Value::Integer(2),
            Value::Text("s1".into()),
            Value::Text("message".into()),
            Value::Integer(2000),
            Value::Text("updated".into()),
            Value::Text("dk1".into()),
        ],
    ).unwrap();
    let rows = eng
        .run_sql("SELECT data, ts FROM session_entries WHERE session_id = 's1'")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("updated".into()));
    assert_eq!(rows[0][1], Value::Integer(2000));
    // 不同 dedup_key → 新增行
    eng.run_sql_param(
        "INSERT INTO session_entries (id, session_id, type, ts, data, dedup_key) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(session_id, dedup_key) DO UPDATE SET data = excluded.data, ts = excluded.ts",
        &[
            Value::Integer(3),
            Value::Text("s1".into()),
            Value::Text("message".into()),
            Value::Integer(3000),
            Value::Text("new msg".into()),
            Value::Text("dk2".into()),
        ],
    ).unwrap();
    let rows = eng
        .run_sql("SELECT * FROM session_entries WHERE session_id = 's1' ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn superclaw_ddl_init_schema() {
    let (_dir, mut eng) = tmp_engine();
    eng.run_sql("CREATE TABLE IF NOT EXISTS sessions (id TEXT NOT NULL, user_id TEXT, title TEXT, status TEXT, created_at TEXT, updated_at TEXT)").unwrap();
    eng.run_sql("CREATE TABLE IF NOT EXISTS llm_providers (id TEXT NOT NULL, name TEXT NOT NULL, displayName TEXT, type TEXT NOT NULL, enabled INTEGER, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL)").unwrap();
    eng.run_sql("CREATE TABLE IF NOT EXISTS sessions (id TEXT NOT NULL, user_id TEXT, title TEXT, status TEXT, created_at TEXT, updated_at TEXT)").unwrap();
    eng.run_sql("ALTER TABLE sessions ADD COLUMN user_id TEXT DEFAULT 'desktop'")
        .unwrap_or_default();
    eng.run_sql("ALTER TABLE sessions ADD COLUMN status TEXT DEFAULT 'active'")
        .unwrap_or_default();
    eng.run_sql_param(
        "INSERT INTO sessions (id, user_id, title, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        &[Value::Text("s1".into()), Value::Text("desktop".into()), Value::Text("Chat 1".into()), Value::Text("active".into()), Value::Text("2026-02-25".into()), Value::Text("2026-02-25".into())],
    ).unwrap();
    let rows = eng.run_sql("SELECT id, title, created_at, updated_at FROM sessions ORDER BY updated_at DESC LIMIT 10").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("Chat 1".into()));
    eng.run_sql_param(
        "UPDATE sessions SET title = ?, updated_at = ? WHERE id = ?",
        &[
            Value::Text("Renamed".into()),
            Value::Text("2026-02-26".into()),
            Value::Text("s1".into()),
        ],
    )
    .unwrap();
    let rows = eng
        .run_sql("SELECT title FROM sessions WHERE id = 's1'")
        .unwrap();
    assert_eq!(rows[0][0], Value::Text("Renamed".into()));
    eng.run_sql("DELETE FROM sessions WHERE id = 's1'").unwrap();
    let rows = eng.run_sql("SELECT * FROM sessions").unwrap();
    assert_eq!(rows.len(), 0);
}
