/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M39 测试：INSERT ... ON CONFLICT (UPSERT) + ALTER TABLE RENAME COLUMN。

#[cfg(test)]
mod tests {
    use crate::sql::SqlEngine;
    use crate::storage::Store;
    use crate::types::Value;

    fn setup() -> (tempfile::TempDir, Store, SqlEngine) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let mut e = SqlEngine::new(&store).unwrap();
        e.run_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
            .unwrap();
        e.run_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        e.run_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        (dir, store, e)
    }

    // ── ON CONFLICT 基本 UPSERT ──────────────────────────

    #[test]
    fn upsert_basic_conflict_update() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql(
            "INSERT INTO users VALUES (1, 'Alice2', 35) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age",
        )
        .unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("Alice2".into()));
        assert_eq!(rows[0][2], Value::Integer(35));
    }

    #[test]
    fn upsert_no_conflict_inserts() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql(
            "INSERT INTO users VALUES (3, 'Charlie', 28) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
        )
        .unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 3").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("Charlie".into()));
    }

    #[test]
    fn upsert_with_literal_value() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql(
            "INSERT INTO users VALUES (1, 'Ignored', 99) \
             ON CONFLICT (id) DO UPDATE SET name = 'Updated', age = 42",
        )
        .unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("Updated".into()));
        assert_eq!(rows[0][2], Value::Integer(42));
    }

    #[test]
    fn upsert_partial_update() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql(
            "INSERT INTO users VALUES (2, 'Ignored', 50) \
             ON CONFLICT (id) DO UPDATE SET age = EXCLUDED.age",
        )
        .unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 2").unwrap();
        assert_eq!(rows[0][1], Value::Text("Bob".into()));
        assert_eq!(rows[0][2], Value::Integer(50));
    }

    #[test]
    fn upsert_with_secondary_index() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("CREATE INDEX idx_name ON users(name)").unwrap();
        e.run_sql(
            "INSERT INTO users VALUES (1, 'Alice_new', 30) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
        )
        .unwrap();
        let rows = e
            .run_sql("SELECT * FROM users WHERE name = 'Alice_new'")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(1));
        let old = e
            .run_sql("SELECT * FROM users WHERE name = 'Alice'")
            .unwrap();
        assert_eq!(old.len(), 0);
    }

    #[test]
    fn upsert_in_transaction() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("BEGIN").unwrap();
        e.run_sql(
            "INSERT INTO users VALUES (1, 'TxAlice', 40) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age",
        )
        .unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("TxAlice".into()));
        e.run_sql("COMMIT").unwrap();
        let rows = e.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("TxAlice".into()));
        assert_eq!(rows[0][2], Value::Integer(40));
    }

    #[test]
    fn upsert_batch_mixed() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql(
            "INSERT INTO users VALUES (1, 'A_up', 31), (5, 'Eve', 22) \
             ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age",
        )
        .unwrap();
        let r1 = e.run_sql("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(r1[0][1], Value::Text("A_up".into()));
        let r5 = e.run_sql("SELECT * FROM users WHERE id = 5").unwrap();
        assert_eq!(r5.len(), 1);
        assert_eq!(r5[0][1], Value::Text("Eve".into()));
    }

    #[test]
    fn upsert_error_update_pk() {
        let (_d, _s, ref mut e) = setup();
        let res = e.run_sql(
            "INSERT INTO users VALUES (1, 'X', 1) \
             ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id",
        );
        assert!(res.is_err());
    }

    #[test]
    fn upsert_error_nonexistent_column() {
        let (_d, _s, ref mut e) = setup();
        let res = e.run_sql(
            "INSERT INTO users VALUES (1, 'X', 1) \
             ON CONFLICT (id) DO UPDATE SET ghost = EXCLUDED.ghost",
        );
        assert!(res.is_err());
    }

    // ── ON CONFLICT 非缓存路径（executor） ───────────────

    #[test]
    fn upsert_executor_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        crate::sql::run(&store, "CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        crate::sql::run(&store, "INSERT INTO t VALUES (1, 'old')").unwrap();
        crate::sql::run(
            &store,
            "INSERT INTO t VALUES (1, 'new') \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
        )
        .unwrap();
        let rows = crate::sql::run(&store, "SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("new".into()));
    }

    #[test]
    fn upsert_executor_no_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        crate::sql::run(&store, "CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        crate::sql::run(
            &store,
            "INSERT INTO t VALUES (1, 'hello') \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
        )
        .unwrap();
        let rows = crate::sql::run(&store, "SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(rows[0][1], Value::Text("hello".into()));
    }

    // ── RENAME COLUMN ────────────────────────────────────

    #[test]
    fn rename_column_basic() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("ALTER TABLE users RENAME COLUMN name TO username")
            .unwrap();
        let rows = e
            .run_sql("SELECT username FROM users WHERE id = 1")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("Alice".into()));
    }

    #[test]
    fn rename_column_insert_select() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("ALTER TABLE users RENAME COLUMN name TO username")
            .unwrap();
        e.run_sql("INSERT INTO users (id, username, age) VALUES (3, 'Charlie', 28)")
            .unwrap();
        let rows = e
            .run_sql("SELECT username FROM users WHERE id = 3")
            .unwrap();
        assert_eq!(rows[0][0], Value::Text("Charlie".into()));
    }

    #[test]
    fn rename_column_where_clause() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("ALTER TABLE users RENAME COLUMN name TO username")
            .unwrap();
        let rows = e
            .run_sql("SELECT * FROM users WHERE username = 'Bob'")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(2));
    }

    #[test]
    fn rename_column_with_secondary_index() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("CREATE INDEX idx_name ON users(name)").unwrap();
        e.run_sql("ALTER TABLE users RENAME COLUMN name TO username")
            .unwrap();
        let rows = e
            .run_sql("SELECT * FROM users WHERE username = 'Alice'")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(1));
    }

    #[test]
    fn rename_column_error_dropped() {
        let (_d, _s, ref mut e) = setup();
        e.run_sql("ALTER TABLE users DROP COLUMN age").unwrap();
        let res = e.run_sql("ALTER TABLE users RENAME COLUMN age TO new_age");
        assert!(res.is_err());
    }

    #[test]
    fn rename_column_error_to_existing() {
        let (_d, _s, ref mut e) = setup();
        let res = e.run_sql("ALTER TABLE users RENAME COLUMN name TO age");
        assert!(res.is_err());
    }

    #[test]
    fn rename_column_error_nonexistent() {
        let (_d, _s, ref mut e) = setup();
        let res = e.run_sql("ALTER TABLE users RENAME COLUMN ghost TO new_ghost");
        assert!(res.is_err());
    }

    // ── RENAME COLUMN 非缓存路径 ─────────────────────────

    #[test]
    fn rename_column_executor() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        crate::sql::run(
            &store,
            "CREATE TABLE t (id INTEGER, val TEXT, score INTEGER)",
        )
        .unwrap();
        crate::sql::run(&store, "INSERT INTO t VALUES (1, 'hello', 10)").unwrap();
        crate::sql::run(&store, "ALTER TABLE t RENAME COLUMN val TO label").unwrap();
        let rows = crate::sql::run(&store, "SELECT label FROM t WHERE id = 1").unwrap();
        assert_eq!(rows[0][0], Value::Text("hello".into()));
    }

    // ── ON CONFLICT 解析错误 ─────────────────────────────

    #[test]
    fn parse_on_conflict_missing_paren() {
        let (_d, _s, ref mut e) = setup();
        let res = e.run_sql(
            "INSERT INTO users VALUES (1, 'X', 1) ON CONFLICT id DO UPDATE SET name = 'Y'",
        );
        assert!(res.is_err());
    }

    #[test]
    fn parse_on_conflict_missing_set() {
        let (_d, _s, ref mut e) = setup();
        let res =
            e.run_sql("INSERT INTO users VALUES (1, 'X', 1) ON CONFLICT (id) DO UPDATE name = 'Y'");
        assert!(res.is_err());
    }
}
