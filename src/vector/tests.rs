/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 向量引擎测试。

use super::quantization::{compute_quantization_params, dequantize_vec, quantize_vec};
use super::*;
use crate::storage::Store;

#[test]
fn vector_insert_search_cosine() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "v").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.9, 0.1, 0.0]).unwrap();
    ve.insert(3, &[0.0, 1.0, 0.0]).unwrap();
    let out = ve.search(&[1.0, 0.0, 0.0], 2, "cosine").unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, 1);
    assert!(out[0].1 < 0.01);
    assert_eq!(out[1].0, 2);
}

#[test]
fn vector_batch_insert_and_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "batch").unwrap();
    ve.insert_batch(&[(1, &[1.0, 0.0]), (2, &[0.0, 1.0]), (3, &[0.7, 0.7])])
        .unwrap();
    assert_eq!(ve.count().unwrap(), 3);

    let results = ve
        .batch_search(&[&[1.0, 0.0], &[0.0, 1.0]], 1, "cosine")
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0][0].0, 1);
    assert_eq!(results[1][0].0, 2);
}

#[test]
fn vector_delete_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "del").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();
    assert_eq!(ve.count().unwrap(), 2);
    ve.delete(1).unwrap();
    assert_eq!(ve.count().unwrap(), 1);
    let out = ve.search(&[1.0, 0.0], 10, "cosine").unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, 2);
}

#[test]
fn vector_l2_and_dot_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "metrics").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();
    ve.insert(3, &[0.5, 0.5]).unwrap();

    let l2 = ve.search(&[1.0, 0.0], 3, "l2").unwrap();
    assert_eq!(l2[0].0, 1);

    let dot = ve.search(&[1.0, 0.0], 3, "dot").unwrap();
    assert_eq!(dot[0].0, 1);
}

#[test]
fn vector_empty_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "empty").unwrap();
    let out = ve.search(&[1.0, 0.0], 5, "cosine").unwrap();
    assert!(out.is_empty());
}

#[test]
fn vector_rebuild_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "rebuild").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(3, &[0.0, 0.0, 1.0]).unwrap();

    let rebuilt = ve.rebuild_index().unwrap();
    assert_eq!(rebuilt, 3);
    assert_eq!(ve.count().unwrap(), 3);

    let out = ve.search(&[1.0, 0.0, 0.0], 1, "cosine").unwrap();
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_set_ef_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "ef").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();
    ve.set_ef_search(100).unwrap();
    let meta = ve.load_or_init_meta().unwrap();
    assert_eq!(meta.ef_search, 100);
    let out = ve.search(&[1.0, 0.0], 1, "cosine").unwrap();
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_many_points_hnsw() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "many").unwrap();

    for i in 0..100u64 {
        let angle = (i as f32) * std::f32::consts::PI * 2.0 / 100.0;
        ve.insert(i, &[angle.cos(), angle.sin()]).unwrap();
    }
    assert_eq!(ve.count().unwrap(), 100);

    let out = ve.search(&[1.0, 0.0], 5, "cosine").unwrap();
    assert_eq!(out.len(), 5);
    assert_eq!(out[0].0, 0);
}

#[test]
fn vector_quantization_roundtrip() {
    let vecs: Vec<Vec<f32>> = vec![
        vec![0.1, 0.5, 0.9],
        vec![0.0, 1.0, 0.3],
        vec![0.8, 0.2, 0.6],
    ];
    let params = compute_quantization_params(&vecs).unwrap();
    for v in &vecs {
        let q = quantize_vec(v, &params);
        let decoded = dequantize_vec(&q, &params);
        for (i, (&orig, &dec)) in v.iter().zip(decoded.iter()).enumerate() {
            let err = (orig - dec).abs();
            let rel = if orig.abs() > f32::EPSILON {
                err / orig.abs()
            } else {
                err
            };
            assert!(
                rel < 0.02 || err < 0.01,
                "维度 {} 精度损失过大: orig={}, decoded={}, rel_err={}",
                i,
                orig,
                dec,
                rel
            );
        }
    }
}

#[test]
fn vector_quantization_enable_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "qsearch").unwrap();

    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.9, 0.1, 0.0]).unwrap();
    ve.insert(3, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(4, &[0.0, 0.0, 1.0]).unwrap();

    let raw_out = ve.search(&[1.0, 0.0, 0.0], 2, "cosine").unwrap();
    assert_eq!(raw_out[0].0, 1);

    ve.enable_quantization().unwrap();
    assert!(ve.is_quantized().unwrap());

    let q_out = ve.search(&[1.0, 0.0, 0.0], 2, "cosine").unwrap();
    assert_eq!(q_out.len(), 2);
    assert_eq!(q_out[0].0, 1);
}

#[test]
fn vector_quantization_disable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "qdis").unwrap();

    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();

    ve.enable_quantization().unwrap();
    assert!(ve.is_quantized().unwrap());

    ve.disable_quantization().unwrap();
    assert!(!ve.is_quantized().unwrap());

    let out = ve.search(&[1.0, 0.0], 1, "cosine").unwrap();
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_quantization_insert_after_enable() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "qauto").unwrap();

    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();

    ve.enable_quantization().unwrap();

    ve.insert(3, &[0.95, 0.05, 0.0]).unwrap();

    let out = ve.search(&[1.0, 0.0, 0.0], 3, "cosine").unwrap();
    assert_eq!(out.len(), 3);
    assert!(out[0].0 == 1 || out[0].0 == 3);
}

#[test]
fn vector_quantization_delete_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "qdel").unwrap();

    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();

    ve.enable_quantization().unwrap();
    ve.delete(1).unwrap();

    assert_eq!(ve.count().unwrap(), 1);
    let out = ve.search(&[1.0, 0.0], 10, "cosine").unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, 2);
}

#[test]
fn vector_quantization_rebuild_preserves() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "qrebuild").unwrap();

    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(3, &[0.0, 0.0, 1.0]).unwrap();

    ve.enable_quantization().unwrap();
    assert!(ve.is_quantized().unwrap());

    let rebuilt = ve.rebuild_index().unwrap();
    assert_eq!(rebuilt, 3);
    assert_eq!(ve.count().unwrap(), 3);
    assert!(ve.is_quantized().unwrap());

    let out = ve.search(&[1.0, 0.0, 0.0], 1, "cosine").unwrap();
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_quantization_l2_metric() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "ql2").unwrap();

    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();
    ve.insert(3, &[0.5, 0.5]).unwrap();

    ve.enable_quantization().unwrap();

    let out = ve.search(&[1.0, 0.0], 3, "l2").unwrap();
    assert_eq!(out[0].0, 1);
}

// ── M76: Metadata Pre-filter 测试 ──

use super::metadata::{MetaFilter, MetaFilterOp, MetaValue};
use std::collections::HashMap;

fn make_meta(pairs: &[(&str, MetaValue)]) -> HashMap<String, MetaValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn vector_metadata_set_get() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "meta").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();

    let meta = make_meta(&[
        ("user_id", MetaValue::String("u1".into())),
        ("score", MetaValue::Float(0.95)),
    ]);
    ve.set_metadata(1, &meta).unwrap();

    let got = ve.get_metadata(1).unwrap().unwrap();
    assert_eq!(got.get("user_id"), Some(&MetaValue::String("u1".into())));
    assert_eq!(got.get("score"), Some(&MetaValue::Float(0.95)));

    // 不存在的 id
    assert!(ve.get_metadata(999).unwrap().is_none());
}

#[test]
fn vector_metadata_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "mdel").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.set_metadata(1, &make_meta(&[("k", MetaValue::String("v".into()))]))
        .unwrap();
    assert!(ve.get_metadata(1).unwrap().is_some());
    ve.delete_metadata(1).unwrap();
    assert!(ve.get_metadata(1).unwrap().is_none());
}

#[test]
fn vector_insert_with_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "iwm").unwrap();
    let meta = make_meta(&[("ns", MetaValue::String("docs".into()))]);
    ve.insert_with_metadata(1, &[1.0, 0.0], &meta).unwrap();
    assert_eq!(ve.count().unwrap(), 1);
    let got = ve.get_metadata(1).unwrap().unwrap();
    assert_eq!(got.get("ns"), Some(&MetaValue::String("docs".into())));
}

#[test]
fn vector_search_with_filter_eq() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "feq").unwrap();

    // 插入 3 个向量，不同 user_id
    ve.insert_with_metadata(
        1,
        &[1.0, 0.0],
        &make_meta(&[("user_id", MetaValue::String("alice".into()))]),
    )
    .unwrap();
    ve.insert_with_metadata(
        2,
        &[0.9, 0.1],
        &make_meta(&[("user_id", MetaValue::String("bob".into()))]),
    )
    .unwrap();
    ve.insert_with_metadata(
        3,
        &[0.8, 0.2],
        &make_meta(&[("user_id", MetaValue::String("alice".into()))]),
    )
    .unwrap();

    // 搜索只返回 alice 的向量
    let filters = vec![MetaFilter {
        field: "user_id".into(),
        op: MetaFilterOp::Eq(MetaValue::String("alice".into())),
    }];
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &filters)
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(id, _)| *id == 1 || *id == 3));
}

#[test]
fn vector_search_with_filter_numeric() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "fnum").unwrap();

    for i in 1..=5u64 {
        let angle = (i as f32 - 1.0) * 0.1;
        ve.insert_with_metadata(
            i,
            &[1.0 - angle, angle],
            &make_meta(&[("priority", MetaValue::Int(i as i64))]),
        )
        .unwrap();
    }

    // Gte: priority >= 3 → ids 3,4,5
    let filters = vec![MetaFilter {
        field: "priority".into(),
        op: MetaFilterOp::Gte(MetaValue::Int(3)),
    }];
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &filters)
        .unwrap();
    assert_eq!(results.len(), 3);
    for (id, _) in &results {
        assert!(*id >= 3);
    }
}

#[test]
fn vector_search_with_filter_in() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "fin").unwrap();

    for i in 1..=4u64 {
        let ns = if i % 2 == 0 { "docs" } else { "code" };
        ve.insert_with_metadata(
            i,
            &[1.0 / i as f32, 0.0],
            &make_meta(&[("ns", MetaValue::String(ns.into()))]),
        )
        .unwrap();
    }

    // In: ns in ["docs"] → ids 2,4
    let filters = vec![MetaFilter {
        field: "ns".into(),
        op: MetaFilterOp::In(vec![MetaValue::String("docs".into())]),
    }];
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &filters)
        .unwrap();
    assert_eq!(results.len(), 2);
    for (id, _) in &results {
        assert!(*id == 2 || *id == 4);
    }
}

#[test]
fn vector_search_with_filter_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "fnm").unwrap();

    ve.insert_with_metadata(
        1,
        &[1.0, 0.0],
        &make_meta(&[("user", MetaValue::String("alice".into()))]),
    )
    .unwrap();

    let filters = vec![MetaFilter {
        field: "user".into(),
        op: MetaFilterOp::Eq(MetaValue::String("nobody".into())),
    }];
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &filters)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn vector_search_with_filter_empty_filters() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "fef").unwrap();

    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();

    // 空过滤条件 → 等同普通搜索
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &[])
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn vector_search_with_filter_ne() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "fne").unwrap();

    ve.insert_with_metadata(
        1,
        &[1.0, 0.0],
        &make_meta(&[("status", MetaValue::String("active".into()))]),
    )
    .unwrap();
    ve.insert_with_metadata(
        2,
        &[0.9, 0.1],
        &make_meta(&[("status", MetaValue::String("deleted".into()))]),
    )
    .unwrap();

    let filters = vec![MetaFilter {
        field: "status".into(),
        op: MetaFilterOp::Ne(MetaValue::String("deleted".into())),
    }];
    let results = ve
        .search_with_filter(&[1.0, 0.0], 10, "cosine", &filters)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
}

#[test]
fn vector_recommend_positive_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "rec").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.9, 0.1, 0.0]).unwrap();
    ve.insert(3, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(4, &[0.0, 0.0, 1.0]).unwrap();
    // 正例 [1,0,0] → 最近的应该是 id=1, id=2
    let out = ve.recommend(&[&[1.0, 0.0, 0.0]], &[], 2, "cosine").unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, 1);
    assert_eq!(out[1].0, 2);
}

#[test]
fn vector_recommend_with_negative() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "rec2").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(3, &[0.7, 0.7, 0.0]).unwrap();
    // 正例 [1,0,0]，负例 [0,1,0] → query 偏向 x 轴远离 y 轴
    let out = ve
        .recommend(&[&[1.0, 0.0, 0.0]], &[&[0.0, 1.0, 0.0]], 3, "cosine")
        .unwrap();
    // id=1 应排第一（纯 x 轴），id=3 次之（混合），id=2 最远（纯 y 轴）
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_recommend_empty_positive_err() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "rec3").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    let err = ve.recommend(&[], &[], 2, "cosine");
    assert!(err.is_err());
}

#[test]
fn vector_discover_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "disc").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.9, 0.1, 0.0]).unwrap();
    ve.insert(3, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(4, &[0.0, 0.0, 1.0]).unwrap();
    // target 接近 id=1, context: 偏好 x 轴(pos)远离 y 轴(neg)
    let pos: &[f32] = &[1.0, 0.0, 0.0];
    let neg: &[f32] = &[0.0, 1.0, 0.0];
    let out = ve
        .discover(&[1.0, 0.0, 0.0], &[(pos, neg)], 2, "cosine")
        .unwrap();
    assert_eq!(out.len(), 2);
    // id=1 和 id=2 应排在前面（接近 x 轴且远离 y 轴）
    assert!(out[0].0 == 1 || out[0].0 == 2);
}

#[test]
fn vector_discover_empty_context() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "disc2").unwrap();
    ve.insert(1, &[1.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0]).unwrap();
    // 空 context → 退化为普通搜索
    let out = ve.discover(&[1.0, 0.0], &[], 2, "cosine").unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, 1);
}

#[test]
fn vector_discover_multi_context() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let ve = VectorEngine::open(&store, "disc3").unwrap();
    ve.insert(1, &[1.0, 0.0, 0.0]).unwrap();
    ve.insert(2, &[0.0, 1.0, 0.0]).unwrap();
    ve.insert(3, &[0.0, 0.0, 1.0]).unwrap();
    ve.insert(4, &[0.7, 0.7, 0.0]).unwrap();
    // 多组 context：偏好 x 轴远离 y 轴 + 偏好 x 轴远离 z 轴
    let ctx: Vec<(&[f32], &[f32])> = vec![
        (&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]),
        (&[1.0, 0.0, 0.0], &[0.0, 0.0, 1.0]),
    ];
    let out = ve.discover(&[0.8, 0.1, 0.1], &ctx, 4, "cosine").unwrap();
    assert!(!out.is_empty());
    // id=1 应该排名靠前（纯 x 轴，两组 context 都偏好它）
    assert_eq!(out[0].0, 1);
}
