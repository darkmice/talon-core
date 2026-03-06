/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M127：FOREIGN KEY 外键约束测试。

use crate::sql::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn setup() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let engine = SqlEngine::new(&store).unwrap();
    (dir, engine)
}

#[test]
fn fk_insert_valid() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1, 'a')").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    let rows = e.run_sql("SELECT * FROM child").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn fk_insert_invalid() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    let err = e.run_sql("INSERT INTO child VALUES (10, 999)").unwrap_err();
    assert!(err.to_string().contains("外键约束失败"), "got: {}", err);
}

#[test]
fn fk_insert_null_bypass() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    // NULL 值跳过 FK 检查（SQL 标准）
    e.run_sql("INSERT INTO child VALUES (1, NULL)").unwrap();
    let rows = e.run_sql("SELECT * FROM child").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn fk_delete_parent_blocked() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1, 'a')").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    let err = e.run_sql("DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.to_string().contains("外键约束阻止删除"), "got: {}", err);
}

#[test]
fn fk_delete_parent_ok_when_no_child_ref() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1)").unwrap();
    e.run_sql("INSERT INTO parent VALUES (2)").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    // 删除 parent id=2 应该成功（没有子行引用）
    e.run_sql("DELETE FROM parent WHERE id = 2").unwrap();
    let rows = e.run_sql("SELECT * FROM parent").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn fk_drop_parent_blocked() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    let err = e.run_sql("DROP TABLE parent").unwrap_err();
    assert!(err.to_string().contains("无法删除表"), "got: {}", err);
}

#[test]
fn fk_drop_parent_ok_after_child_dropped() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("DROP TABLE child").unwrap();
    e.run_sql("DROP TABLE parent").unwrap();
}

#[test]
fn fk_update_child_fk_column_valid() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1)").unwrap();
    e.run_sql("INSERT INTO parent VALUES (2)").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    // 更新 FK 列为另一个有效值
    e.run_sql("UPDATE child SET pid = 2 WHERE id = 10").unwrap();
    let rows = e.run_sql("SELECT pid FROM child WHERE id = 10").unwrap();
    assert_eq!(rows[0][0], Value::Integer(2));
}

#[test]
fn fk_update_child_fk_column_invalid() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1)").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    let err = e
        .run_sql("UPDATE child SET pid = 999 WHERE id = 10")
        .unwrap_err();
    assert!(err.to_string().contains("外键约束失败"), "got: {}", err);
}

#[test]
fn fk_table_level_syntax() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent(id))").unwrap();
    e.run_sql("INSERT INTO parent VALUES (1, 'a')").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    let err = e.run_sql("INSERT INTO child VALUES (20, 999)").unwrap_err();
    assert!(err.to_string().contains("外键约束失败"), "got: {}", err);
}

#[test]
fn fk_describe_shows_fk_info() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    let rows = e.run_sql("DESCRIBE child").unwrap();
    // pid 行应该有 FK 信息（第 6 列）
    let pid_row = &rows[1]; // 第二行是 pid
    let fk_col = &pid_row[5]; // 第 6 列
    if let Value::Text(s) = fk_col {
        assert!(s.contains("REFERENCES parent(id)"), "got: {}", s);
    } else {
        panic!("FK 列应为 Text 类型");
    }
}

#[test]
fn fk_create_table_invalid_parent() {
    let (_d, mut e) = setup();
    // 引用不存在的父表
    let err = e
        .run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES nonexist(id))")
        .unwrap_err();
    assert!(err.to_string().contains("父表不存在"), "got: {}", err);
}

#[test]
fn fk_create_table_invalid_parent_column() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    // 引用不存在的父表列
    let err = e
        .run_sql(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(nonexist))",
        )
        .unwrap_err();
    assert!(err.to_string().contains("列不存在"), "got: {}", err);
}

#[test]
fn fk_batch_insert_valid_and_invalid() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1)").unwrap();
    e.run_sql("INSERT INTO parent VALUES (2)").unwrap();
    // 批量插入：全部有效
    e.run_sql("INSERT INTO child VALUES (10, 1), (20, 2)")
        .unwrap();
    // 批量插入：包含无效值
    let err = e
        .run_sql("INSERT INTO child VALUES (30, 1), (40, 999)")
        .unwrap_err();
    assert!(err.to_string().contains("外键约束失败"), "got: {}", err);
}

#[test]
fn fk_delete_all_parent_blocked() {
    let (_d, mut e) = setup();
    e.run_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    e.run_sql("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
        .unwrap();
    e.run_sql("INSERT INTO parent VALUES (1)").unwrap();
    e.run_sql("INSERT INTO child VALUES (10, 1)").unwrap();
    // DELETE FROM parent（无 WHERE）应被阻止
    let err = e.run_sql("DELETE FROM parent").unwrap_err();
    assert!(err.to_string().contains("外键约束阻止删除"), "got: {}", err);
}
