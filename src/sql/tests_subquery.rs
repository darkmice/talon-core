/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M152：子查询测试（WHERE col IN/NOT IN (SELECT ...)）。
//! M153：EXISTS / NOT EXISTS 子查询测试。

#[cfg(test)]
mod tests {
    use crate::Talon;

    fn setup() -> (tempfile::TempDir, Talon) {
        let dir = tempfile::tempdir().unwrap();
        let db = Talon::open(dir.path()).unwrap();
        // 创建测试表
        db.run_sql("CREATE TABLE users (id INT, name TEXT, dept_id INT)")
            .unwrap();
        db.run_sql("CREATE TABLE depts (id INT, dname TEXT, active INT)")
            .unwrap();
        db.run_sql("INSERT INTO users VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10), (4, 'Dave', 30)")
            .unwrap();
        db.run_sql(
            "INSERT INTO depts VALUES (10, 'Engineering', 1), (20, 'Sales', 1), (30, 'HR', 0)",
        )
        .unwrap();
        (dir, db)
    }

    #[test]
    fn subquery_in_basic() {
        let (_dir, db) = setup();
        // 查找活跃部门的用户
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id IN (SELECT id FROM depts WHERE active = 1) ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 3); // Alice(10), Bob(20), Charlie(10)
        assert_eq!(rows[0][0], crate::types::Value::Text("Alice".into()));
        assert_eq!(rows[1][0], crate::types::Value::Text("Bob".into()));
        assert_eq!(rows[2][0], crate::types::Value::Text("Charlie".into()));
    }

    #[test]
    fn subquery_not_in_basic() {
        let (_dir, db) = setup();
        // 查找非活跃部门的用户
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id NOT IN (SELECT id FROM depts WHERE active = 1) ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 1); // Dave(30)
        assert_eq!(rows[0][0], crate::types::Value::Text("Dave".into()));
    }

    #[test]
    fn subquery_in_empty_result() {
        let (_dir, db) = setup();
        // 子查询返回空集 → 外层查询无结果
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id IN (SELECT id FROM depts WHERE dname = 'NonExistent')")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn subquery_in_delete() {
        let (_dir, db) = setup();
        // 删除非活跃部门的用户
        db.run_sql("DELETE FROM users WHERE dept_id IN (SELECT id FROM depts WHERE active = 0)")
            .unwrap();
        let rows = db.run_sql("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(rows.len(), 3); // Dave 被删除
    }

    #[test]
    fn subquery_in_update() {
        let (_dir, db) = setup();
        // 更新活跃部门用户的名字前缀
        db.run_sql("UPDATE users SET name = 'VIP' WHERE dept_id IN (SELECT id FROM depts WHERE active = 1)")
            .unwrap();
        let rows = db
            .run_sql("SELECT name FROM users WHERE name = 'VIP'")
            .unwrap();
        assert_eq!(rows.len(), 3); // Alice, Bob, Charlie
    }

    #[test]
    fn subquery_multi_column_error() {
        let (_dir, db) = setup();
        // 子查询返回多列 → 报错
        let result =
            db.run_sql("SELECT name FROM users WHERE dept_id IN (SELECT id, dname FROM depts)");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("一列"), "error: {}", err);
    }

    #[test]
    fn subquery_not_in_with_where() {
        let (_dir, db) = setup();
        // NOT IN 子查询带 WHERE
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id NOT IN (SELECT id FROM depts WHERE active = 0) ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 3); // Alice, Bob, Charlie（排除 HR dept_id=30）
    }

    // ── M153: EXISTS / NOT EXISTS ──

    #[test]
    fn exists_basic() {
        let (_dir, db) = setup();
        // 查找有活跃部门的用户（非关联 EXISTS，子查询有结果 → 返回所有行）
        let rows = db
            .run_sql("SELECT name FROM users WHERE EXISTS (SELECT id FROM depts WHERE active = 1) ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 4); // 子查询有结果，所有用户都返回
    }

    #[test]
    fn exists_no_match() {
        let (_dir, db) = setup();
        // 子查询无结果 → EXISTS 为 false → 外层无结果
        let rows = db
            .run_sql("SELECT name FROM users WHERE EXISTS (SELECT id FROM depts WHERE dname = 'NonExistent')")
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn not_exists_basic() {
        let (_dir, db) = setup();
        // NOT EXISTS：子查询有结果 → false → 外层无结果
        let rows = db
            .run_sql(
                "SELECT name FROM users WHERE NOT EXISTS (SELECT id FROM depts WHERE active = 1)",
            )
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn not_exists_no_match() {
        let (_dir, db) = setup();
        // NOT EXISTS：子查询无结果 → true → 返回所有行
        let rows = db
            .run_sql("SELECT name FROM users WHERE NOT EXISTS (SELECT id FROM depts WHERE dname = 'NonExistent') ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn exists_with_and() {
        let (_dir, db) = setup();
        // EXISTS 与其他条件组合
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id = 10 AND EXISTS (SELECT id FROM depts WHERE active = 1) ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 2); // Alice, Charlie
        assert_eq!(rows[0][0], crate::types::Value::Text("Alice".into()));
        assert_eq!(rows[1][0], crate::types::Value::Text("Charlie".into()));
    }

    #[test]
    fn exists_in_delete() {
        let (_dir, db) = setup();
        // EXISTS 用于 DELETE：删除所有用户（因为存在非活跃部门）
        db.run_sql("DELETE FROM users WHERE EXISTS (SELECT id FROM depts WHERE active = 0)")
            .unwrap();
        let rows = db.run_sql("SELECT * FROM users").unwrap();
        assert_eq!(rows.len(), 0); // 全部删除
    }

    #[test]
    fn not_exists_in_update() {
        let (_dir, db) = setup();
        // NOT EXISTS 用于 UPDATE：子查询有结果 → NOT EXISTS 为 false → 不更新
        db.run_sql("UPDATE users SET name = 'GONE' WHERE NOT EXISTS (SELECT id FROM depts WHERE active = 1)")
            .unwrap();
        let rows = db
            .run_sql("SELECT name FROM users WHERE name = 'GONE'")
            .unwrap();
        assert_eq!(rows.len(), 0); // 没有更新
    }

    #[test]
    fn exists_or_condition() {
        let (_dir, db) = setup();
        // EXISTS 与 OR 组合
        let rows = db
            .run_sql("SELECT name FROM users WHERE dept_id = 30 OR EXISTS (SELECT id FROM depts WHERE dname = 'NonExistent') ORDER BY name")
            .unwrap();
        assert_eq!(rows.len(), 1); // 只有 Dave(dept_id=30)
        assert_eq!(rows[0][0], crate::types::Value::Text("Dave".into()));
    }
}
