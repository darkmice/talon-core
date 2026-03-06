/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 引擎集成测试。

use super::*;
use crate::storage::Store;

fn make_doc(id: &str, fields: &[(&str, &str)]) -> FtsDoc {
    FtsDoc {
        doc_id: id.to_string(),
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

#[test]
fn fts_index_and_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "docs",
        &make_doc(
            "1",
            &[
                ("title", "Rust Programming"),
                ("body", "Rust is a systems programming language"),
            ],
        ),
    )
    .unwrap();
    fts.index_doc(
        "docs",
        &make_doc(
            "2",
            &[
                ("title", "Python Guide"),
                ("body", "Python is great for AI and machine learning"),
            ],
        ),
    )
    .unwrap();
    fts.index_doc(
        "docs",
        &make_doc(
            "3",
            &[
                ("title", "Go Tutorial"),
                ("body", "Go is fast and concurrent"),
            ],
        ),
    )
    .unwrap();
    let hits = fts.search("docs", "rust programming", 10).unwrap();
    assert!(!hits.is_empty(), "should find results");
    assert_eq!(hits[0].doc_id, "1", "Rust doc should rank first");
    assert!(hits[0].score > 0.0);
}

#[test]
fn fts_search_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("empty", &FtsConfig::default()).unwrap();
    fts.index_doc("empty", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    let hits = fts.search("empty", "nonexistent", 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_delete_doc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("del", &FtsConfig::default()).unwrap();
    fts.index_doc("del", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    assert!(fts.get_doc("del", "1").unwrap().is_some());
    assert!(fts.delete_doc("del", "1").unwrap());
    assert!(fts.get_doc("del", "1").unwrap().is_none());
    let hits = fts.search("del", "hello", 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_update_doc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("upd", &FtsConfig::default()).unwrap();
    fts.index_doc("upd", &make_doc("1", &[("text", "old content")]))
        .unwrap();
    fts.index_doc("upd", &make_doc("1", &[("text", "new content")]))
        .unwrap();
    let doc = fts.get_doc("upd", "1").unwrap().unwrap();
    assert_eq!(doc.get("text").unwrap(), "new content");
}

#[test]
fn fts_chinese_search() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("cn", &FtsConfig::default()).unwrap();
    fts.index_doc("cn", &make_doc("1", &[("text", "Python异步编程指南")]))
        .unwrap();
    fts.index_doc("cn", &make_doc("2", &[("text", "Rust系统编程入门")]))
        .unwrap();
    let hits = fts.search("cn", "编程", 10).unwrap();
    assert_eq!(hits.len(), 2, "both docs contain 编");
}

#[test]
fn fts_bm25_ranking() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rank", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "rank",
        &make_doc("1", &[("text", "rust rust rust is great")]),
    )
    .unwrap();
    fts.index_doc("rank", &make_doc("2", &[("text", "rust is a language")]))
        .unwrap();
    let hits = fts.search("rank", "rust", 10).unwrap();
    assert!(hits.len() >= 2);
    assert_eq!(hits[0].doc_id, "1");
    assert!(hits[0].score > hits[1].score);
}

#[test]
fn fts_batch_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("batch", &FtsConfig::default()).unwrap();
    let docs = vec![
        make_doc("1", &[("text", "hello")]),
        make_doc("2", &[("text", "world")]),
        make_doc("3", &[("text", "hello world")]),
    ];
    fts.index_doc_batch("batch", &docs).unwrap();
    let hits = fts.search("batch", "hello", 10).unwrap();
    assert!(hits.len() >= 2);
}

#[test]
fn fts_fuzzy_search_typo() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("fz", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "fz",
        &make_doc("1", &[("text", "machine learning is great")]),
    )
    .unwrap();
    fts.index_doc("fz", &make_doc("2", &[("text", "deep learning tutorial")]))
        .unwrap();
    // 精确搜索 "machne" 应该找不到
    let exact = fts.search("fz", "machne", 10).unwrap();
    assert!(exact.is_empty(), "exact search should miss typo");
    // fuzzy 搜索 "machne" (编辑距离 1) 应该匹配 "machine"
    let fuzzy = fts.search_fuzzy("fz", "machne", 1, 10).unwrap();
    assert!(!fuzzy.is_empty(), "fuzzy search should find machine");
    assert_eq!(fuzzy[0].doc_id, "1");
}

#[test]
fn fts_fuzzy_search_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("fz2", &FtsConfig::default()).unwrap();
    fts.index_doc("fz2", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    // 编辑距离 1 搜 "zzzzz" 不应匹配任何
    let hits = fts.search_fuzzy("fz2", "zzzzz", 1, 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_bench_10k_docs() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bench", &FtsConfig::default()).unwrap();
    // 索引 10000 个文档
    let t0 = Instant::now();
    let count = 10_000usize;
    for i in 0..count {
        let doc = make_doc(
            &format!("doc_{}", i),
            &[(
                "text",
                &format!(
                    "document number {} about rust programming and machine learning topic {}",
                    i,
                    i % 100
                ),
            )],
        );
        fts.index_doc("bench", &doc).unwrap();
    }
    let index_ms = t0.elapsed().as_millis();
    let index_ops = count as f64 / (index_ms as f64 / 1000.0);
    eprintln!(
        "[FTS bench] indexed {} docs in {}ms ({:.0} docs/s)",
        count, index_ms, index_ops
    );
    // 搜索性能：100 次搜索
    let queries = [
        "rust",
        "machine learning",
        "programming",
        "document",
        "topic",
    ];
    let t1 = Instant::now();
    let search_count = 100;
    for i in 0..search_count {
        let q = queries[i % queries.len()];
        let hits = fts.search("bench", q, 10).unwrap();
        assert!(!hits.is_empty());
    }
    let search_ms = t1.elapsed().as_millis();
    let search_ops = search_count as f64 / (search_ms as f64 / 1000.0);
    eprintln!(
        "[FTS bench] {} searches in {}ms ({:.0} qps)",
        search_count, search_ms, search_ops
    );
    assert!(index_ops > 10.0, "index too slow: {:.0} docs/s", index_ops);
    assert!(search_ops > 1.0, "search too slow: {:.0} qps", search_ops);
}

#[test]
fn fts_search_highlights() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("hl", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "hl",
        &make_doc(
            "1",
            &[("title", "Rust Programming"), ("body", "Rust is fast")],
        ),
    )
    .unwrap();
    let hits = fts.search("hl", "rust", 10).unwrap();
    assert!(!hits.is_empty());
    // highlights 应包含 <em>Rust</em>
    let hl = &hits[0].highlights;
    assert!(
        hl.values().any(|v| v.contains("<em>")),
        "highlights should contain <em> tags, got: {:?}",
        hl
    );
    let title_hl = hl.get("title").unwrap();
    assert!(
        title_hl.contains("<em>Rust</em>"),
        "title highlight should contain <em>Rust</em>, got: {}",
        title_hl
    );
}

// ===== M77: 布尔查询测试 =====

use super::bool_query::BoolQuery;

#[test]
fn fts_bool_must_and() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "bq",
        &make_doc("1", &[("text", "rust programming language")]),
    )
    .unwrap();
    fts.index_doc("bq", &make_doc("2", &[("text", "rust is fast")]))
        .unwrap();
    fts.index_doc("bq", &make_doc("3", &[("text", "python programming")]))
        .unwrap();
    // must: rust AND programming → 只有 doc1 同时包含两者
    let q = BoolQuery {
        must: vec!["rust".into(), "programming".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
}

#[test]
fn fts_bool_should_or() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq2", &FtsConfig::default()).unwrap();
    fts.index_doc("bq2", &make_doc("1", &[("text", "rust language")]))
        .unwrap();
    fts.index_doc("bq2", &make_doc("2", &[("text", "python language")]))
        .unwrap();
    fts.index_doc("bq2", &make_doc("3", &[("text", "go concurrency")]))
        .unwrap();
    // should: rust OR python → doc1 和 doc2
    let q = BoolQuery {
        should: vec!["rust".into(), "python".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq2", &q, 10).unwrap();
    assert_eq!(hits.len(), 2);
    let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert!(ids.contains(&"1"));
    assert!(ids.contains(&"2"));
}

#[test]
fn fts_bool_must_not() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq3", &FtsConfig::default()).unwrap();
    fts.index_doc("bq3", &make_doc("1", &[("text", "rust programming")]))
        .unwrap();
    fts.index_doc("bq3", &make_doc("2", &[("text", "rust systems")]))
        .unwrap();
    fts.index_doc("bq3", &make_doc("3", &[("text", "python programming")]))
        .unwrap();
    // should: programming, must_not: python → doc1 (排除 doc3)
    let q = BoolQuery {
        should: vec!["programming".into()],
        must_not: vec!["python".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq3", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
}

#[test]
fn fts_bool_combined() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq4", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "bq4",
        &make_doc("1", &[("text", "rust fast systems programming")]),
    )
    .unwrap();
    fts.index_doc("bq4", &make_doc("2", &[("text", "rust slow legacy code")]))
        .unwrap();
    fts.index_doc("bq4", &make_doc("3", &[("text", "python fast scripting")]))
        .unwrap();
    // must: rust, should: fast, must_not: slow
    // → doc1 (rust+fast, 排除 doc2 因 slow), doc3 被排除因无 rust
    let q = BoolQuery {
        must: vec!["rust".into()],
        should: vec!["fast".into()],
        must_not: vec!["slow".into()],
    };
    let hits = fts.search_bool("bq4", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
    assert!(hits[0].score > 0.0);
}

#[test]
fn fts_bool_empty_query() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq5", &FtsConfig::default()).unwrap();
    fts.index_doc("bq5", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    // must 和 should 都为空 → 返回空
    let q = BoolQuery::default();
    let hits = fts.search_bool("bq5", &q, 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_bool_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq6", &FtsConfig::default()).unwrap();
    fts.index_doc("bq6", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    // must: nonexistent → 无匹配
    let q = BoolQuery {
        must: vec!["nonexistent".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq6", &q, 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_bool_highlights() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq7", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "bq7",
        &make_doc("1", &[("title", "Rust Guide"), ("body", "Rust is fast")]),
    )
    .unwrap();
    let q = BoolQuery {
        must: vec!["rust".into()],
        should: vec!["fast".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq7", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    let hl = &hits[0].highlights;
    assert!(
        hl.values().any(|v| v.contains("<em>")),
        "should have highlights"
    );
}

#[test]
fn fts_bool_only_must_not_returns_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("bq8", &FtsConfig::default()).unwrap();
    fts.index_doc("bq8", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    // 只有 must_not，must/should 为空 → 应返回空（与 ES 行为一致）
    let q = BoolQuery {
        must_not: vec!["hello".into()],
        ..Default::default()
    };
    let hits = fts.search_bool("bq8", &q, 10).unwrap();
    assert!(
        hits.is_empty(),
        "only must_not without must/should should return empty"
    );
}

// ===== M78: 短语搜索测试 =====

#[test]
fn fts_phrase_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "ph",
        &make_doc("1", &[("text", "machine learning is great")]),
    )
    .unwrap();
    fts.index_doc(
        "ph",
        &make_doc("2", &[("text", "learning machine operations")]),
    )
    .unwrap();
    fts.index_doc(
        "ph",
        &make_doc("3", &[("text", "deep machine learning model")]),
    )
    .unwrap();
    // "machine learning" 短语：doc1 和 doc3 包含连续的 machine learning
    let hits = fts.search_phrase("ph", "machine learning", 10).unwrap();
    let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert!(ids.contains(&"1"), "doc1 should match: {:?}", ids);
    assert!(ids.contains(&"3"), "doc3 should match: {:?}", ids);
    // doc2 中 learning 在 machine 前面，不应匹配
    assert!(
        !ids.contains(&"2"),
        "doc2 should NOT match (reversed order): {:?}",
        ids
    );
}

#[test]
fn fts_phrase_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph2", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "ph2",
        &make_doc("1", &[("text", "rust programming fast language safe code")]),
    )
    .unwrap();
    // "fast code" 不连续（中间有 "language" 和 "safe"）
    let hits = fts.search_phrase("ph2", "fast code", 10).unwrap();
    assert!(hits.is_empty(), "should not match non-adjacent words");
}

#[test]
fn fts_phrase_single_token() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph3", &FtsConfig::default()).unwrap();
    fts.index_doc("ph3", &make_doc("1", &[("text", "hello world")]))
        .unwrap();
    // 单 token 退化为普通搜索
    let hits = fts.search_phrase("ph3", "hello", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
}

#[test]
fn fts_phrase_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph4", &FtsConfig::default()).unwrap();
    fts.index_doc("ph4", &make_doc("1", &[("text", "hello")]))
        .unwrap();
    let hits = fts.search_phrase("ph4", "", 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_phrase_highlights() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph5", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "ph5",
        &make_doc(
            "1",
            &[
                ("title", "Deep Learning Guide"),
                ("body", "deep learning is powerful"),
            ],
        ),
    )
    .unwrap();
    let hits = fts.search_phrase("ph5", "deep learning", 10).unwrap();
    assert_eq!(hits.len(), 1);
    let hl = &hits[0].highlights;
    assert!(
        hl.values().any(|v| v.contains("<em>")),
        "should have highlights"
    );
}

#[test]
fn fts_phrase_three_words() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("ph6", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "ph6",
        &make_doc("1", &[("text", "natural language processing is important")]),
    )
    .unwrap();
    fts.index_doc(
        "ph6",
        &make_doc("2", &[("text", "natural processing language")]),
    )
    .unwrap();
    // 三词短语 "natural language processing"
    let hits = fts
        .search_phrase("ph6", "natural language processing", 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
}

// ── M101: 多字段搜索测试 ──

#[test]
fn fts_multi_field_basic_weights() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf1", &FtsConfig::default()).unwrap();
    // doc1: title 包含 "rust"，body 不包含
    fts.index_doc(
        "mf1",
        &make_doc(
            "1",
            &[
                ("title", "rust programming"),
                ("body", "a systems language"),
            ],
        ),
    )
    .unwrap();
    // doc2: body 包含 "rust"，title 不包含
    fts.index_doc(
        "mf1",
        &make_doc(
            "2",
            &[("title", "hello world"), ("body", "rust is fast and safe")],
        ),
    )
    .unwrap();
    // title 权重 3.0，body 权重 1.0 → doc1 应排在前面
    let q = MultiFieldQuery {
        query: "rust".into(),
        field_weights: [("title".into(), 3.0), ("body".into(), 1.0)]
            .into_iter()
            .collect(),
    };
    let hits = fts.search_multi_field("mf1", &q, 10).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0].doc_id, "1",
        "title-match should rank first with higher weight"
    );
    assert!(hits[0].score > hits[1].score);
}

#[test]
fn fts_multi_field_empty_weights_fallback() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf2", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "mf2",
        &make_doc(
            "1",
            &[("title", "rust async"), ("body", "concurrency model")],
        ),
    )
    .unwrap();
    // 空 field_weights → 退化为普通搜索
    let q = MultiFieldQuery {
        query: "rust".into(),
        field_weights: BTreeMap::new(),
    };
    let hits = fts.search_multi_field("mf2", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
    assert!(hits[0].score > 0.0);
}

#[test]
fn fts_multi_field_no_match() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf3", &FtsConfig::default()).unwrap();
    fts.index_doc("mf3", &make_doc("1", &[("title", "hello world")]))
        .unwrap();
    let q = MultiFieldQuery {
        query: "nonexistent".into(),
        field_weights: [("title".into(), 1.0)].into_iter().collect(),
    };
    let hits = fts.search_multi_field("mf3", &q, 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_multi_field_highlights() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf4", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "mf4",
        &make_doc(
            "1",
            &[("title", "rust language"), ("body", "rust is great")],
        ),
    )
    .unwrap();
    let q = MultiFieldQuery {
        query: "rust".into(),
        field_weights: [("title".into(), 2.0), ("body".into(), 1.0)]
            .into_iter()
            .collect(),
    };
    let hits = fts.search_multi_field("mf4", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    // 高亮应包含 <em>rust</em>
    assert!(hits[0].highlights.get("title").unwrap().contains("<em>"));
    assert!(hits[0].highlights.get("body").unwrap().contains("<em>"));
}

#[test]
fn fts_multi_field_single_field_only() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf5", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "mf5",
        &make_doc("1", &[("title", "rust"), ("body", "python java rust")]),
    )
    .unwrap();
    // 只搜索 title 字段（body 权重 0 → 不参与）
    let q = MultiFieldQuery {
        query: "rust".into(),
        field_weights: [("title".into(), 1.0)].into_iter().collect(),
    };
    let hits = fts.search_multi_field("mf5", &q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    // body 中也有 rust 但不影响评分（只有 title 参与）
    assert!(hits[0].score > 0.0);
}

#[test]
fn fts_multi_field_empty_query() {
    use super::multi_field::MultiFieldQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("mf6", &FtsConfig::default()).unwrap();
    let q = MultiFieldQuery {
        query: "".into(),
        field_weights: BTreeMap::new(),
    };
    let hits = fts.search_multi_field("mf6", &q, 10).unwrap();
    assert!(hits.is_empty());
}

// ── M134: search_term / search_terms 精确匹配搜索 ──

#[test]
fn fts_term_exact_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term1", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "term1",
        &make_doc(
            "1",
            &[("category", "programming"), ("body", "Rust is great")],
        ),
    )
    .unwrap();
    fts.index_doc(
        "term1",
        &make_doc("2", &[("category", "science"), ("body", "Physics is fun")]),
    )
    .unwrap();
    fts.index_doc(
        "term1",
        &make_doc("3", &[("category", "programming"), ("body", "Go is fast")]),
    )
    .unwrap();

    // 精确匹配 "programming" — 应命中 doc1 和 doc3
    let hits = fts
        .search_term("term1", "category", "programming", 10)
        .unwrap();
    assert_eq!(hits.len(), 2);
    let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert!(ids.contains(&"1"));
    assert!(ids.contains(&"3"));

    // 精确匹配 "science" — 应命中 doc2
    let hits = fts.search_term("term1", "category", "science", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "2");
}

#[test]
fn fts_term_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term2", &FtsConfig::default()).unwrap();
    fts.index_doc("term2", &make_doc("1", &[("title", "Rust Guide")]))
        .unwrap();

    // 不存在的 term
    let hits = fts
        .search_term("term2", "title", "nonexistent", 10)
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_term_case_insensitive() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term3", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "term3",
        &make_doc("1", &[("body", "Rust Programming Language")]),
    )
    .unwrap();

    // 大写输入应匹配小写化后的倒排索引
    let hits = fts.search_term("term3", "body", "RUST", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");

    // 混合大小写
    let hits = fts.search_term("term3", "body", "Programming", 10).unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn fts_term_empty_field_matches_all() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term4", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "term4",
        &make_doc("1", &[("title", "Rust"), ("body", "Systems language")]),
    )
    .unwrap();

    // field 为空字符串 → 不做字段过滤，只要倒排索引命中即返回
    let hits = fts.search_term("term4", "", "rust", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "1");
}

#[test]
fn fts_terms_multi_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term5", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "term5",
        &make_doc("1", &[("lang", "rust"), ("body", "Rust is great")]),
    )
    .unwrap();
    fts.index_doc(
        "term5",
        &make_doc("2", &[("lang", "python"), ("body", "Python is popular")]),
    )
    .unwrap();
    fts.index_doc(
        "term5",
        &make_doc("3", &[("lang", "go"), ("body", "Go is fast")]),
    )
    .unwrap();

    // terms 查询：匹配 "rust" 或 "python"
    let hits = fts
        .search_terms("term5", "lang", &["rust", "python"], 10)
        .unwrap();
    assert_eq!(hits.len(), 2);
    let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert!(ids.contains(&"1"));
    assert!(ids.contains(&"2"));
    assert!(!ids.contains(&"3"));
}

#[test]
fn fts_term_highlights() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("term6", &FtsConfig::default()).unwrap();
    fts.index_doc("term6", &make_doc("1", &[("category", "programming")]))
        .unwrap();

    let hits = fts
        .search_term("term6", "category", "programming", 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    // 高亮应包含 <em> 标记
    assert!(hits[0].highlights.contains_key("category"));
    let hl = &hits[0].highlights["category"];
    assert!(hl.contains("<em>"));
}

// ── M135: search_wildcard 通配符搜索 ──

#[test]
fn fts_wildcard_star() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("wc1", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "wc1",
        &make_doc("1", &[("body", "Rust programming language")]),
    )
    .unwrap();
    fts.index_doc("wc1", &make_doc("2", &[("body", "Python is popular")]))
        .unwrap();

    // "rust*" 应匹配 doc1（倒排索引中有 "rust" token）
    let hits = fts.search_wildcard("wc1", "rust*", 10).unwrap();
    assert!(!hits.is_empty());
    assert!(hits.iter().any(|h| h.doc_id == "1"));
}

#[test]
fn fts_wildcard_question() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("wc2", &FtsConfig::default()).unwrap();
    fts.index_doc("wc2", &make_doc("1", &[("body", "test data")]))
        .unwrap();
    fts.index_doc("wc2", &make_doc("2", &[("body", "text data")]))
        .unwrap();

    // "te?t" 应匹配 "test" 和 "text"
    let hits = fts.search_wildcard("wc2", "te?t", 10).unwrap();
    assert_eq!(hits.len(), 2);
}

#[test]
fn fts_wildcard_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("wc3", &FtsConfig::default()).unwrap();
    fts.index_doc("wc3", &make_doc("1", &[("body", "hello world")]))
        .unwrap();

    let hits = fts.search_wildcard("wc3", "xyz*", 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_wildcard_empty_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("wc4", &FtsConfig::default()).unwrap();

    let hits = fts.search_wildcard("wc4", "", 10).unwrap();
    assert!(hits.is_empty());
}

// ── M136: search_regexp 正则表达式搜索 ──

#[test]
fn fts_regexp_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("re1", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "re1",
        &make_doc("1", &[("body", "Rust programming language")]),
    )
    .unwrap();
    fts.index_doc("re1", &make_doc("2", &[("body", "Python is popular")]))
        .unwrap();

    // "rust.*" 应匹配 "rust" token
    let hits = fts.search_regexp("re1", "rust.*", 10).unwrap();
    assert!(!hits.is_empty());
    assert!(hits.iter().any(|h| h.doc_id == "1"));
}

#[test]
fn fts_regexp_char_class() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("re2", &FtsConfig::default()).unwrap();
    fts.index_doc("re2", &make_doc("1", &[("body", "test data")]))
        .unwrap();
    fts.index_doc("re2", &make_doc("2", &[("body", "text data")]))
        .unwrap();

    // "te[sx]t" 应匹配 "test" 和 "text"
    let hits = fts.search_regexp("re2", "te[sx]t", 10).unwrap();
    assert_eq!(hits.len(), 2);
}

#[test]
fn fts_regexp_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("re3", &FtsConfig::default()).unwrap();
    fts.index_doc("re3", &make_doc("1", &[("body", "hello world")]))
        .unwrap();

    let hits = fts.search_regexp("re3", "xyz\\d+", 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_regexp_invalid_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("re4", &FtsConfig::default()).unwrap();

    // 无效正则应返回错误
    let result = fts.search_regexp("re4", "[invalid", 10);
    assert!(result.is_err());
}

#[test]
fn fts_regexp_empty_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("re5", &FtsConfig::default()).unwrap();

    let hits = fts.search_regexp("re5", "", 10).unwrap();
    assert!(hits.is_empty());
}

// ── M137: search_range 范围查询 ──

#[test]
fn fts_range_numeric() {
    use super::range::RangeQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rng1", &FtsConfig::default()).unwrap();
    fts.index_doc(
        "rng1",
        &make_doc("1", &[("score", "0.5"), ("title", "low")]),
    )
    .unwrap();
    fts.index_doc(
        "rng1",
        &make_doc("2", &[("score", "0.8"), ("title", "mid")]),
    )
    .unwrap();
    fts.index_doc(
        "rng1",
        &make_doc("3", &[("score", "0.95"), ("title", "high")]),
    )
    .unwrap();
    fts.index_doc(
        "rng1",
        &make_doc("4", &[("score", "0.3"), ("title", "very low")]),
    )
    .unwrap();

    // gte=0.8 → doc2, doc3
    let hits = fts
        .search_range(
            "rng1",
            &RangeQuery {
                field: "score".into(),
                gte: Some("0.8".into()),
                ..Default::default()
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 2);
    let ids: Vec<&str> = hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert!(ids.contains(&"2"));
    assert!(ids.contains(&"3"));
}

#[test]
fn fts_range_string() {
    use super::range::RangeQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rng2", &FtsConfig::default()).unwrap();
    fts.index_doc("rng2", &make_doc("1", &[("date", "2024-01-01")]))
        .unwrap();
    fts.index_doc("rng2", &make_doc("2", &[("date", "2024-06-15")]))
        .unwrap();
    fts.index_doc("rng2", &make_doc("3", &[("date", "2025-01-01")]))
        .unwrap();

    // gte=2024-06-01, lt=2025-01-01 → doc2
    let hits = fts
        .search_range(
            "rng2",
            &RangeQuery {
                field: "date".into(),
                gte: Some("2024-06-01".into()),
                lt: Some("2025-01-01".into()),
                ..Default::default()
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "2");
}

#[test]
fn fts_range_gt_lt_exclusive() {
    use super::range::RangeQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rng3", &FtsConfig::default()).unwrap();
    fts.index_doc("rng3", &make_doc("1", &[("val", "10")]))
        .unwrap();
    fts.index_doc("rng3", &make_doc("2", &[("val", "20")]))
        .unwrap();
    fts.index_doc("rng3", &make_doc("3", &[("val", "30")]))
        .unwrap();

    // gt=10, lt=30 → doc2 only (exclusive bounds)
    let hits = fts
        .search_range(
            "rng3",
            &RangeQuery {
                field: "val".into(),
                gt: Some("10".into()),
                lt: Some("30".into()),
                ..Default::default()
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "2");
}

#[test]
fn fts_range_no_bounds() {
    use super::range::RangeQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rng4", &FtsConfig::default()).unwrap();

    // 无边界 → 空结果
    let hits = fts
        .search_range(
            "rng4",
            &RangeQuery {
                field: "val".into(),
                ..Default::default()
            },
            10,
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_range_no_match() {
    use super::range::RangeQuery;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("rng5", &FtsConfig::default()).unwrap();
    fts.index_doc("rng5", &make_doc("1", &[("score", "0.5")]))
        .unwrap();

    let hits = fts
        .search_range(
            "rng5",
            &RangeQuery {
                field: "score".into(),
                gte: Some("0.9".into()),
                ..Default::default()
            },
            10,
        )
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fts_delete_by_query_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc(
        "docs",
        &make_doc("d1", &[("body", "rust programming language")]),
    )
    .unwrap();
    fts.index_doc(
        "docs",
        &make_doc("d2", &[("body", "python programming language")]),
    )
    .unwrap();
    fts.index_doc("docs", &make_doc("d3", &[("body", "rust systems design")]))
        .unwrap();

    // 删除包含 "rust" 的文档
    let deleted = fts.delete_by_query("docs", "rust", 100).unwrap();
    assert_eq!(deleted, 2);

    // 验证只剩 python 文档
    let hits = fts.search("docs", "programming", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id, "d2");
}

#[test]
fn fts_delete_by_query_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc("docs", &make_doc("d1", &[("body", "hello world")]))
        .unwrap();

    let deleted = fts.delete_by_query("docs", "nonexistent", 100).unwrap();
    assert_eq!(deleted, 0);

    // 原文档仍在
    let hits = fts.search("docs", "hello", 10).unwrap();
    assert_eq!(hits.len(), 1);
}

#[test]
fn fts_delete_by_query_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    for i in 0..5 {
        fts.index_doc(
            "docs",
            &make_doc(&format!("d{}", i), &[("body", "common keyword here")]),
        )
        .unwrap();
    }

    // limit=2，只删除 2 个
    let deleted = fts.delete_by_query("docs", "common", 2).unwrap();
    assert_eq!(deleted, 2);

    // 还剩 3 个
    let hits = fts.search("docs", "common", 10).unwrap();
    assert_eq!(hits.len(), 3);
}

#[test]
fn fts_update_by_query_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc(
        "docs",
        &make_doc("d1", &[("body", "rust language"), ("tag", "old")]),
    )
    .unwrap();
    fts.index_doc(
        "docs",
        &make_doc("d2", &[("body", "python language"), ("tag", "old")]),
    )
    .unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("tag".into(), "updated".into());

    let count = fts.update_by_query("docs", "rust", &updates, 100).unwrap();
    assert_eq!(count, 1);

    // 验证 d1 的 tag 已更新
    let fields = fts.get_doc("docs", "d1").unwrap().unwrap();
    assert_eq!(fields["tag"], "updated");
    assert_eq!(fields["body"], "rust language"); // body 保持不变

    // d2 未受影响
    let fields2 = fts.get_doc("docs", "d2").unwrap().unwrap();
    assert_eq!(fields2["tag"], "old");
}

#[test]
fn fts_update_by_query_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc("docs", &make_doc("d1", &[("body", "hello")]))
        .unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("tag".into(), "new".into());

    let count = fts
        .update_by_query("docs", "nonexistent", &updates, 100)
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn fts_update_by_query_adds_new_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc("docs", &make_doc("d1", &[("body", "rust programming")]))
        .unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("category".into(), "systems".into());

    fts.update_by_query("docs", "rust", &updates, 100).unwrap();

    let fields = fts.get_doc("docs", "d1").unwrap().unwrap();
    assert_eq!(fields["category"], "systems");
    assert_eq!(fields["body"], "rust programming");
}

#[test]
fn fts_update_doc_partial() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc(
        "docs",
        &make_doc("d1", &[("body", "rust language"), ("tag", "old")]),
    )
    .unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("tag".into(), "new".into());

    assert!(fts.update_doc("docs", "d1", &updates).unwrap());

    let fields = fts.get_doc("docs", "d1").unwrap().unwrap();
    assert_eq!(fields["tag"], "new");
    assert_eq!(fields["body"], "rust language");
}

#[test]
fn fts_update_doc_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("tag".into(), "val".into());

    assert!(!fts.update_doc("docs", "missing", &updates).unwrap());
}

#[test]
fn fts_update_doc_add_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();
    fts.create_index("docs", &FtsConfig::default()).unwrap();

    fts.index_doc("docs", &make_doc("d1", &[("body", "hello world")]))
        .unwrap();

    let mut updates = std::collections::BTreeMap::new();
    updates.insert("source".into(), "web".into());

    fts.update_doc("docs", "d1", &updates).unwrap();

    let fields = fts.get_doc("docs", "d1").unwrap().unwrap();
    assert_eq!(fields["source"], "web");
    assert_eq!(fields["body"], "hello world");
}

#[test]
fn fts_aggregate_terms_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("agg_test", &FtsConfig::default()).unwrap();
    for (id, cat) in &[
        ("1", "rust"),
        ("2", "rust"),
        ("3", "go"),
        ("4", "rust"),
        ("5", "go"),
    ] {
        fts.index_doc(
            "agg_test",
            &make_doc(id, &[("title", "doc"), ("category", cat)]),
        )
        .unwrap();
    }

    let buckets = fts.aggregate_terms("agg_test", "category", None).unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].key, "rust");
    assert_eq!(buckets[0].doc_count, 3);
    assert_eq!(buckets[1].key, "go");
    assert_eq!(buckets[1].doc_count, 2);
}

#[test]
fn fts_aggregate_terms_top_n() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("agg_topn", &FtsConfig::default()).unwrap();
    for (id, cat) in &[
        ("1", "a"),
        ("2", "a"),
        ("3", "b"),
        ("4", "c"),
        ("5", "c"),
        ("6", "c"),
    ] {
        fts.index_doc("agg_topn", &make_doc(id, &[("category", cat)]))
            .unwrap();
    }

    let buckets = fts
        .aggregate_terms("agg_topn", "category", Some(2))
        .unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].key, "c"); // 3 docs
    assert_eq!(buckets[1].key, "a"); // 2 docs
}

#[test]
fn fts_aggregate_terms_no_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("agg_nof", &FtsConfig::default()).unwrap();
    fts.index_doc("agg_nof", &make_doc("1", &[("title", "hello")]))
        .unwrap();

    // 聚合不存在的字段 → 空桶
    let buckets = fts.aggregate_terms("agg_nof", "nonexistent", None).unwrap();
    assert!(buckets.is_empty());
}

#[test]
fn fts_aggregate_terms_empty_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("agg_empty", &FtsConfig::default())
        .unwrap();

    let buckets = fts.aggregate_terms("agg_empty", "category", None).unwrap();
    assert!(buckets.is_empty());
}

#[test]
fn fts_suggest_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sug", &FtsConfig::default()).unwrap();
    fts.index_doc("sug", &make_doc("1", &[("title", "Rust Programming")]))
        .unwrap();
    fts.index_doc("sug", &make_doc("2", &[("title", "Rust Async Runtime")]))
        .unwrap();
    fts.index_doc("sug", &make_doc("3", &[("title", "Ruby on Rails")]))
        .unwrap();

    let items = fts.suggest("sug", "rus", 10).unwrap();
    assert!(!items.is_empty());
    assert!(items.iter().all(|i| i.term.starts_with("rus")));
    // "rust" 出现在 2 个文档中
    assert_eq!(items[0].term, "rust");
    assert_eq!(items[0].doc_freq, 2);
}

#[test]
fn fts_suggest_top_n() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sug_topn", &FtsConfig::default()).unwrap();
    fts.index_doc("sug_topn", &make_doc("1", &[("body", "alpha beta gamma")]))
        .unwrap();
    fts.index_doc("sug_topn", &make_doc("2", &[("body", "alpha bravo")]))
        .unwrap();
    fts.index_doc("sug_topn", &make_doc("3", &[("body", "alpha beta")]))
        .unwrap();

    // 前缀 "a" 匹配 "alpha"；前缀 "b" 匹配 "beta", "bravo"
    let items = fts.suggest("sug_topn", "b", 1).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].term, "beta"); // beta 出现 2 次 > bravo 1 次
}

#[test]
fn fts_suggest_empty_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sug_empty", &FtsConfig::default())
        .unwrap();
    fts.index_doc("sug_empty", &make_doc("1", &[("title", "hello")]))
        .unwrap();

    let items = fts.suggest("sug_empty", "", 10).unwrap();
    assert!(items.is_empty());
}

#[test]
fn fts_suggest_no_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sug_no", &FtsConfig::default()).unwrap();
    fts.index_doc("sug_no", &make_doc("1", &[("title", "hello world")]))
        .unwrap();

    let items = fts.suggest("sug_no", "xyz", 10).unwrap();
    assert!(items.is_empty());
}

#[test]
fn fts_search_sorted_by_field_asc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sort_test", &FtsConfig::default())
        .unwrap();
    fts.index_doc(
        "sort_test",
        &make_doc("1", &[("title", "rust guide"), ("priority", "3")]),
    )
    .unwrap();
    fts.index_doc(
        "sort_test",
        &make_doc("2", &[("title", "rust tutorial"), ("priority", "1")]),
    )
    .unwrap();
    fts.index_doc(
        "sort_test",
        &make_doc("3", &[("title", "rust book"), ("priority", "2")]),
    )
    .unwrap();

    let hits = fts
        .search_sorted(
            "sort_test",
            "rust",
            &FtsSortBy::Field {
                name: "priority".into(),
                desc: false,
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].doc_id, "2"); // priority=1
    assert_eq!(hits[1].doc_id, "3"); // priority=2
    assert_eq!(hits[2].doc_id, "1"); // priority=3
}

#[test]
fn fts_search_sorted_by_field_desc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sort_desc", &FtsConfig::default())
        .unwrap();
    fts.index_doc(
        "sort_desc",
        &make_doc("a", &[("body", "rust lang"), ("date", "2024-01-01")]),
    )
    .unwrap();
    fts.index_doc(
        "sort_desc",
        &make_doc("b", &[("body", "rust async"), ("date", "2024-03-01")]),
    )
    .unwrap();
    fts.index_doc(
        "sort_desc",
        &make_doc("c", &[("body", "rust web"), ("date", "2024-02-01")]),
    )
    .unwrap();

    let hits = fts
        .search_sorted(
            "sort_desc",
            "rust",
            &FtsSortBy::Field {
                name: "date".into(),
                desc: true,
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].doc_id, "b"); // 2024-03-01
    assert_eq!(hits[1].doc_id, "c"); // 2024-02-01
    assert_eq!(hits[2].doc_id, "a"); // 2024-01-01
}

#[test]
fn fts_search_sorted_by_score() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let fts = FtsEngine::open(&store).unwrap();

    fts.create_index("sort_score", &FtsConfig::default())
        .unwrap();
    fts.index_doc("sort_score", &make_doc("1", &[("title", "rust")]))
        .unwrap();
    fts.index_doc("sort_score", &make_doc("2", &[("title", "rust rust rust")]))
        .unwrap();

    let hits = fts
        .search_sorted("sort_score", "rust", &FtsSortBy::Score, 10)
        .unwrap();
    assert_eq!(hits.len(), 2);
    // Score 排序与普通 search 一致
    assert!(hits[0].score >= hits[1].score);
}
