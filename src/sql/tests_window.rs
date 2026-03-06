/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! M177：窗口函数端到端集成测试。

use crate::sql::engine::SqlEngine;
use crate::storage::Store;
use crate::types::Value;

fn setup() -> (tempfile::TempDir, SqlEngine) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut eng = SqlEngine::new(&store).unwrap();
    eng.run_sql("CREATE TABLE emp (id INTEGER, name TEXT, dept TEXT, salary INTEGER)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (1, 'Alice', 'eng', 100)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (2, 'Bob', 'eng', 120)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (3, 'Carol', 'sales', 90)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (4, 'Dave', 'sales', 110)")
        .unwrap();
    eng.run_sql("INSERT INTO emp VALUES (5, 'Eve', 'eng', 100)")
        .unwrap();
    (dir, eng)
}

#[test]
fn window_row_number() {
    let (_dir, mut eng) = setup();
    // 加 ORDER BY rn 使输出按 row_number 排序
    let rows = eng
        .run_sql("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // 验证所有 rn 值在 1..=5
    let rns: Vec<i64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(v) => *v,
            _ => panic!("expected Integer for rn"),
        })
        .collect();
    for i in 1..=5i64 {
        assert!(rns.contains(&i), "missing rn={}", i);
    }
}

#[test]
fn window_rank_with_partition() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM emp")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // 每个分区内 rank 从 1 开始
    for row in &rows {
        let rnk = match &row[2] {
            Value::Integer(v) => *v,
            _ => panic!("expected Integer for rank"),
        };
        assert!(rnk >= 1);
    }
}

#[test]
fn window_dense_rank() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr FROM emp")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // Bob=120 → 1, Dave=110 → 2, Alice/Eve=100 → 3, Carol=90 → 4
    // 找到所有 dense_rank 值
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(v) => *v,
            _ => panic!("expected Integer"),
        })
        .collect();
    assert!(ranks.contains(&1));
    assert!(ranks.contains(&2));
    assert!(ranks.contains(&3));
    assert!(ranks.contains(&4));
    // 没有 5（因为 dense_rank 无间隔）
    assert!(!ranks.contains(&5));
}

#[test]
fn window_lag_lead() {
    let (_dir, mut eng) = setup();
    // 使用 SELECT * 加 ORDER BY id 确保输出行顺序确定
    let rows = eng
        .run_sql("SELECT *, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev_sal FROM emp ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // row[0] = Alice(id=1): prev_sal = 默认值 0
    // 第 5 列 (id,name,dept,salary,prev_sal) 索引 4 是 prev_sal
    assert_eq!(rows[0][4], Value::Integer(0));
    // row[1] = Bob(id=2): prev_sal = Alice's salary = 100
    assert_eq!(rows[1][4], Value::Integer(100));

    let rows2 = eng
        .run_sql("SELECT *, LEAD(salary, 1) OVER (ORDER BY id) AS next_sal FROM emp ORDER BY id")
        .unwrap();
    assert_eq!(rows2.len(), 5);
    // 最后一行 Eve(id=5): next_sal = NULL
    assert_eq!(rows2[4][4], Value::Null);
    // 第一行 Alice(id=1): next_sal = Bob's salary = 120
    assert_eq!(rows2[0][4], Value::Integer(120));
}

#[test]
fn window_ntile() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT name, NTILE(2) OVER (ORDER BY id) AS bucket FROM emp")
        .unwrap();
    assert_eq!(rows.len(), 5);
    let buckets: Vec<i64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Integer(v) => *v,
            _ => panic!("expected Integer"),
        })
        .collect();
    // 5 行分 2 桶：3+2 或 2+3
    assert!(buckets.iter().all(|&b| b == 1 || b == 2));
}

#[test]
fn window_aggregate_sum_count() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT name, dept, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM emp")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // eng 部门总薪资 = 100 + 120 + 100 = 320
    // sales 部门总薪资 = 90 + 110 = 200
    for row in &rows {
        let dept = match &row[1] {
            Value::Text(s) => s.as_str(),
            _ => panic!("expected Text"),
        };
        let total = match &row[2] {
            Value::Integer(v) => *v,
            _ => panic!("expected Integer"),
        };
        match dept {
            "eng" => assert_eq!(total, 320),
            "sales" => assert_eq!(total, 200),
            _ => panic!("unexpected dept"),
        }
    }
}

#[test]
fn window_with_limit() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM emp LIMIT 3")
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn window_select_star() {
    let (_dir, mut eng) = setup();
    let rows = eng
        .run_sql("SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM emp")
        .unwrap();
    assert_eq!(rows.len(), 5);
    // 每行应有 5 列（id, name, dept, salary, rn）
    assert_eq!(rows[0].len(), 5);
}
