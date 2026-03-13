/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 全文搜索引擎：倒排索引 + BM25 评分，对标 Elasticsearch 核心能力。
//!
//! P0：倒排索引、BM25 评分、Unicode 分词、全文搜索 API。
//! 存储：`fts_{name}_inv`（倒排）、`fts_{name}_doc`（文档）、`fts_{name}_meta`（统计）。
//! 零外部依赖，自研轻量实现。

pub mod admin;
mod aggregate;
mod bm25;
pub mod bool_query;
pub mod es_bulk;
mod fuzzy;
pub mod hybrid;
pub(crate) mod hmm;
pub(crate) mod jieba;
pub mod multi_field;
pub mod phrase;
pub(crate) mod posting;
pub mod range;
pub mod regexp;
pub mod term;
pub mod tokenizer;
pub mod wildcard;

use crate::error::Error;
use crate::storage::{Keyspace, Store};
use std::collections::{BTreeMap, HashMap};

pub use aggregate::{AggBucketFts, FtsSortBy, SuggestItem};
pub use tokenizer::Analyzer;

const FTS_META_KEYSPACE: &str = "fts_meta";

pub(super) fn inv_ks_name(name: &str) -> String {
    format!("fts_{}_inv", name)
}
pub(super) fn doc_ks_name(name: &str) -> String {
    format!("fts_{}_doc", name)
}
pub(super) fn meta_ks_name(name: &str) -> String {
    format!("fts_{}_stat", name)
}

/// 全文索引配置。
#[derive(Debug, Clone)]
pub struct FtsConfig {
    /// 分词器类型。
    pub analyzer: Analyzer,
}

impl Default for FtsConfig {
    fn default() -> Self {
        FtsConfig {
            analyzer: Analyzer::Standard,
        }
    }
}

/// 待索引文档。
#[derive(Debug, Clone)]
pub struct FtsDoc {
    /// 文档唯一 ID。
    pub doc_id: String,
    /// 字段名 → 文本内容。
    pub fields: BTreeMap<String, String>,
}

/// 搜索命中结果。
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// 文档 ID。
    pub doc_id: String,
    /// BM25 相关性分数。
    pub score: f64,
    /// 文档原始字段。
    pub fields: BTreeMap<String, String>,
    /// 高亮片段：field → 带 `<em>` 标记的文本。搜索时自动填充。
    pub highlights: BTreeMap<String, String>,
}

/// 全文搜索引擎。
pub struct FtsEngine {
    store: Store,
    meta_ks: Keyspace,
}

/// 位置列表最大长度（防止高频词在长文档中占用过多空间）。
pub(super) const MAX_POSITIONS: usize = 256;

/// 倒排索引 entry v1 编码: tf(u32) + doc_len(u32) + pos_count(u32) + positions(u32×N)。
///
/// v2 已切换为 VInt+Delta（见 posting.rs），此函数保留供测试基准对照。
#[allow(dead_code)]
fn encode_inv_entry(tf: u32, doc_len: u32, positions: &[u32]) -> Vec<u8> {
    let pos_count = positions.len().min(MAX_POSITIONS);
    let mut buf = Vec::with_capacity(12 + pos_count * 4);
    buf.extend_from_slice(&tf.to_le_bytes());
    buf.extend_from_slice(&doc_len.to_le_bytes());
    buf.extend_from_slice(&(pos_count as u32).to_le_bytes());
    for &p in positions.iter().take(MAX_POSITIONS) {
        buf.extend_from_slice(&p.to_le_bytes());
    }
    buf
}

/// 解码倒排 entry：返回 (tf, doc_len, positions)。
///
/// 自动检测 v1（固定宽度 u32 LE）或 v2（VInt+Delta）格式。
/// v2 格式以 magic byte 0xFE 开头。
pub(super) fn decode_inv_entry(data: &[u8]) -> Option<(u32, u32, Vec<u32>)> {
    posting::decode_inv_entry_auto(data)
}

/// 轻量解码：只返回 (tf, doc_len)，跳过 positions，零堆分配。
/// 搜索 BM25 评分只需 tf + dl，不需要 positions。
/// 对标 Tantivy BlockSegmentPostings: tf-only iterator。
#[inline]
pub(super) fn decode_inv_tf_dl(data: &[u8]) -> Option<(u32, u32)> {
    if data.is_empty() {
        return None;
    }
    if data[0] == 0xFE {
        // v2 format: [magic] [tf: vint] [doc_len: vint] ...
        let mut pos = 1;
        let tf = posting::decode_vint(data, &mut pos)?;
        let dl = posting::decode_vint(data, &mut pos)?;
        Some((tf, dl))
    } else {
        // v1 format: [tf: u32 LE] [dl: u32 LE] ...
        if data.len() < 8 {
            return None;
        }
        let tf = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let dl = u32::from_le_bytes(data[4..8].try_into().ok()?);
        Some((tf, dl))
    }
}

/// v1 格式解码（固定宽度 u32 LE），供 posting 模块向后兼容调用。
///
/// 向后兼容：8 字节旧格式返回空位置列表。
pub(super) fn decode_inv_entry_v1(data: &[u8]) -> Option<(u32, u32, Vec<u32>)> {
    if data.len() < 8 {
        return None;
    }
    let tf = u32::from_le_bytes(data[0..4].try_into().ok()?);
    let dl = u32::from_le_bytes(data[4..8].try_into().ok()?);
    // 旧格式（8 字节）：无位置信息
    if data.len() < 12 {
        return Some((tf, dl, vec![]));
    }
    let pos_count = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
    let mut positions = Vec::with_capacity(pos_count.min(MAX_POSITIONS));
    let mut offset = 12;
    for _ in 0..pos_count.min(MAX_POSITIONS) {
        if offset + 4 > data.len() {
            break;
        }
        positions.push(u32::from_le_bytes(
            data[offset..offset + 4].try_into().ok()?,
        ));
        offset += 4;
    }
    Some((tf, dl, positions))
}

/// 倒排 key: term_hash(8B) + doc_id_hash(8B)。
fn inv_key(term: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&hash_bytes(term));
    key.extend_from_slice(&hash_bytes(doc_id));
    key
}

/// 倒排 prefix: term_hash(8B)，用于 prefix scan 某个 term 的所有文档。
pub(super) fn inv_prefix(term: &str) -> [u8; 8] {
    hash_bytes(term)
}

/// FNV-1a 64-bit 确定性哈希。
///
/// 替代 `DefaultHasher`（SipHash），保证跨 Rust 版本产生相同哈希值。
/// 已有索引数据需要迁移（或重建索引）才能使用新哈希。
pub(super) fn hash_bytes(s: &str) -> [u8; 8] {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut h = FNV_OFFSET;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h.to_be_bytes()
}

/// 统计 key 名。
pub(super) const META_DOC_COUNT: &[u8] = b"__doc_count__";
pub(super) const META_TOTAL_LEN: &[u8] = b"__total_len__";

/// df key: `df:{term_hash_hex}`。
pub(super) fn df_key(term: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(19);
    k.extend_from_slice(b"df:");
    let h = hash_bytes(term);
    for b in &h {
        k.push(HEX[(*b >> 4) as usize]);
        k.push(HEX[(*b & 0x0f) as usize]);
    }
    k
}

const HEX: &[u8; 16] = b"0123456789abcdef";

impl FtsEngine {
    /// 打开全文搜索引擎。
    pub fn open(store: &Store) -> Result<Self, Error> {
        let meta_ks = store.open_keyspace(FTS_META_KEYSPACE)?;
        Ok(FtsEngine {
            store: store.clone(),
            meta_ks,
        })
    }

    /// 创建全文索引。
    pub fn create_index(&self, name: &str, config: &FtsConfig) -> Result<(), Error> {
        let cfg_json =
            serde_json::to_vec(&serde_json::json!({"analyzer": format!("{:?}", config.analyzer)}))
                .map_err(|e| Error::Serialization(e.to_string()))?;
        self.meta_ks.set(name.as_bytes(), &cfg_json)?;
        let _ = self.store.open_keyspace(&inv_ks_name(name))?;
        let _ = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        stat_ks.set(META_DOC_COUNT, 0u64.to_le_bytes())?;
        stat_ks.set(META_TOTAL_LEN, 0u64.to_le_bytes())?;
        Ok(())
    }

    /// 删除全文索引。
    pub fn drop_index(&self, name: &str) -> Result<(), Error> {
        self.meta_ks.delete(name.as_bytes())
    }

    /// 索引单个文档。已存在则覆盖（先删旧再写新）。
    ///
    /// 支持别名（M180）。索引已关闭时返回错误（M178）。
    /// Bug 38：删旧+写新合入同一 WriteBatch 原子提交，防止崩溃后文档丢失。
    pub fn index_doc(&self, name: &str, doc: &FtsDoc) -> Result<(), Error> {
        let name = self.resolve_alias(name);
        let name = name.as_str();
        if self.is_closed(name) {
            return Err(Error::SqlExec(format!("FTS 索引已关闭: {}", name)));
        }
        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let analyzer = Analyzer::Standard;
        let mut batch = self.store.batch();
        // 删旧文档操作追加到 batch（不独立 commit）
        let old_existed =
            self.remove_doc_into_batch(&mut batch, &inv_ks, &doc_ks, &stat_ks, &doc.doc_id, analyzer)?;
        // 合并所有字段文本（直接迭代拼接，避免中间 Vec 分配）
        let full_text: String = doc
            .fields
            .values()
            .enumerate()
            .fold(String::new(), |mut acc, (i, v)| {
                if i > 0 {
                    acc.push(' ');
                }
                acc.push_str(v);
                acc
            });
        let tokens = tokenizer::tokenize(&full_text, analyzer);
        let doc_len = tokens.len() as u32;
        // 统计 term frequency + 位置列表（短语搜索需要）
        let mut tf_map: HashMap<String, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = tf_map
                .entry(token.clone())
                .or_insert_with(|| (0, Vec::new()));
            entry.0 += 1;
            if entry.1.len() < MAX_POSITIONS {
                entry.1.push(pos as u32);
            }
        }
        // 写入倒排索引 + 更新 df + term 注册表
        for (term, (tf, positions)) in &tf_map {
            let ik = inv_key(term, &doc.doc_id);
            // v2 编码：VInt + Delta 压缩，平均节省 50-75% 存储空间
            batch.insert(&inv_ks, ik, posting::encode_inv_entry_v2(*tf, doc_len, positions))?;
            // 增量更新 df（注意：旧文档的 df 已在 remove_doc_into_batch 中递减）
            let dk = df_key(term);
            let old_df = stat_ks
                .get(&dk)?
                .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
                .unwrap_or(0);
            batch.insert(&stat_ks, dk, (old_df + 1).to_le_bytes().to_vec())?;
            // term 注册表（fuzzy 搜索需要遍历所有 term 文本）
            batch.insert(&stat_ks, fuzzy::term_reg_key(term), vec![])?;
        }
        // 写入文档存储
        let doc_json =
            serde_json::to_vec(&doc.fields).map_err(|e| Error::Serialization(e.to_string()))?;
        batch.insert(&doc_ks, doc.doc_id.as_bytes().to_vec(), doc_json)?;
        // 写入 doc_id_hash → doc_id 反向映射（搜索时 O(k) 查找用）
        let mut hash_key = Vec::with_capacity(10);
        hash_key.extend_from_slice(b"h:");
        hash_key.extend_from_slice(&hash_bytes(&doc.doc_id));
        batch.insert(&doc_ks, hash_key, doc.doc_id.as_bytes().to_vec())?;
        // 更新全局统计
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);
        let total_len = read_u64(&stat_ks, META_TOTAL_LEN);
        let new_count = if old_existed {
            doc_count
        } else {
            doc_count + 1
        };
        batch.insert(
            &stat_ks,
            META_DOC_COUNT.to_vec(),
            new_count.to_le_bytes().to_vec(),
        )?;
        batch.insert(
            &stat_ks,
            META_TOTAL_LEN.to_vec(),
            (total_len + doc_len as u64).to_le_bytes().to_vec(),
        )?;
        batch.commit()
    }

    /// 批量索引文档。
    pub fn index_doc_batch(&self, name: &str, docs: &[FtsDoc]) -> Result<(), Error> {
        for doc in docs {
            self.index_doc(name, doc)?;
        }
        Ok(())
    }

    /// 按搜索条件批量更新文档字段（对标 ES `_update_by_query`）。
    ///
    /// 搜索匹配 `query` 的文档（最多 `limit` 个），对每个文档合并 `updates` 字段后重新索引。
    /// 返回实际更新的文档数。
    ///
    /// AI 场景：RAG 知识库批量打标签、更新分类元数据。
    pub fn update_by_query(
        &self,
        name: &str,
        query: &str,
        updates: &BTreeMap<String, String>,
        limit: usize,
    ) -> Result<u64, Error> {
        let hits = self.search(name, query, limit)?;
        let mut updated = 0u64;
        for hit in &hits {
            let mut fields = hit.fields.clone();
            for (k, v) in updates {
                fields.insert(k.clone(), v.clone());
            }
            let doc = FtsDoc {
                doc_id: hit.doc_id.clone(),
                fields,
            };
            self.index_doc(name, &doc)?;
            updated += 1;
        }
        Ok(updated)
    }

    /// 按搜索条件批量删除文档（对标 ES `_delete_by_query`）。
    ///
    /// 先搜索匹配 `query` 的文档（最多 `limit` 个），然后逐个删除。
    /// 返回实际删除的文档数。
    ///
    /// AI 场景：RAG 知识库批量清理过期/低质量文档。
    pub fn delete_by_query(&self, name: &str, query: &str, limit: usize) -> Result<u64, Error> {
        let hits = self.search(name, query, limit)?;
        let mut deleted = 0u64;
        for hit in &hits {
            if self.delete_doc(name, &hit.doc_id)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// 删除文档。
    pub fn delete_doc(&self, name: &str, doc_id: &str) -> Result<bool, Error> {
        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        self.remove_doc_internal(&inv_ks, &doc_ks, &stat_ks, doc_id, Analyzer::Standard)
    }

    /// 部分更新文档字段（对标 ES `POST /_update/{id}`）。
    ///
    /// 读取现有文档，合并 `updates` 字段后重新索引。
    /// 文档不存在返回 `false`，成功更新返回 `true`。
    ///
    /// AI 场景：RAG 知识库更新单个文档的元数据（标签、分类）而不影响正文。
    pub fn update_doc(
        &self,
        name: &str,
        doc_id: &str,
        updates: &BTreeMap<String, String>,
    ) -> Result<bool, Error> {
        let existing = match self.get_doc(name, doc_id)? {
            Some(fields) => fields,
            None => return Ok(false),
        };
        let mut fields = existing;
        for (k, v) in updates {
            fields.insert(k.clone(), v.clone());
        }
        let doc = FtsDoc {
            doc_id: doc_id.to_string(),
            fields,
        };
        self.index_doc(name, &doc)?;
        Ok(true)
    }

    /// 获取文档。
    pub fn get_doc(
        &self,
        name: &str,
        doc_id: &str,
    ) -> Result<Option<BTreeMap<String, String>>, Error> {
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        match doc_ks.get(doc_id.as_bytes())? {
            Some(raw) => {
                let fields: BTreeMap<String, String> = serde_json::from_slice(&raw)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok(Some(fields))
            }
            None => Ok(None),
        }
    }

    /// BM25 全文搜索。
    ///
    /// 支持别名（M180）。索引已关闭时返回错误（M178）。
    pub fn search(&self, name: &str, query: &str, limit: usize) -> Result<Vec<SearchHit>, Error> {
        let name = self.resolve_alias(name);
        let name = name.as_str();
        if self.is_closed(name) {
            return Err(Error::SqlExec(format!("FTS 索引已关闭: {}", name)));
        }
        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);
        let total_len = read_u64(&stat_ks, META_TOTAL_LEN);
        if doc_count == 0 {
            return Ok(vec![]);
        }
        let avgdl = total_len as f64 / doc_count as f64;
        let tokens = tokenizer::tokenize(query, Analyzer::Standard);
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        // 逐 term 扫描倒排索引，按 doc_id 聚合分数（零堆分配：[u8;8] 栈数组）
        // M202：跳过超高频词（df > 80% 文档数），IDF 接近 0 对排序无贡献
        let df_skip_threshold = (doc_count as f64 * 0.8) as u64;
        // 预分配容量：典型场景每个 term 命中 doc_count/10 个文档
        let est_cap = (doc_count as usize / 10).max(256).min(100_000);
        let mut scores: HashMap<[u8; 8], f64> = HashMap::with_capacity(est_cap);
        for term in &tokens {
            let dk = df_key(term);
            let df = stat_ks
                .get(&dk)?
                .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
                .unwrap_or(0);
            if df == 0 {
                continue;
            }
            // M202：多词查询时跳过超高频词 — df>80% 的词 IDF≈0，扫描代价大但贡献极低
            // 条件：1) 多词查询 2) 已有至少一个词被搜索过（保证不会全部跳过）
            if df > df_skip_threshold && tokens.len() > 1 && !scores.is_empty() {
                continue;
            }
            let idf_val = bm25::idf(doc_count, df);
            let prefix = inv_prefix(term);
            inv_ks.for_each_kv_prefix(&prefix, |key, val| {
                if key.len() != 16 {
                    return true;
                }
                // 轻量解码：只提取 tf + dl，跳过 positions（零堆分配）
                if let Some((tf, dl)) = decode_inv_tf_dl(val) {
                    let mut doc_hash = [0u8; 8];
                    doc_hash.copy_from_slice(&key[8..16]);
                    let s = bm25::term_score(tf, dl, avgdl, idf_val);
                    *scores.entry(doc_hash).or_insert(0.0) += s;
                }
                true
            })?;
        }
        // Top-K 选取：用 BinaryHeap 而非全量排序
        // 当匹配文档数远大于 limit 时，BinaryHeap O(N log K) 远优于 sort O(N log N)
        use std::collections::BinaryHeap;
        use std::cmp::Ordering;
        #[derive(PartialEq)]
        struct ScoreItem([u8; 8], f64);
        impl Eq for ScoreItem {}
        impl PartialOrd for ScoreItem {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
        }
        impl Ord for ScoreItem {
            fn cmp(&self, other: &Self) -> Ordering {
                // 小顶堆：分数低的在堆顶，用于维护 Top-K 高分
                self.1.partial_cmp(&other.1).unwrap_or(Ordering::Equal)
            }
        }
        let mut heap: BinaryHeap<std::cmp::Reverse<ScoreItem>> = BinaryHeap::with_capacity(limit + 1);
        for (doc_hash, score) in scores {
            if heap.len() < limit {
                heap.push(std::cmp::Reverse(ScoreItem(doc_hash, score)));
            } else if let Some(min) = heap.peek() {
                if score > min.0 .1 {
                    heap.pop();
                    heap.push(std::cmp::Reverse(ScoreItem(doc_hash, score)));
                }
            }
        }
        let mut scored: Vec<([u8; 8], f64)> = heap.into_iter().map(|std::cmp::Reverse(ScoreItem(h, s))| (h, s)).collect();
        scored.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        // 根据 doc_id_hash 反查文档（O(k) 直接查找）
        let mut results = Vec::with_capacity(scored.len());
        let mut hash_key_buf = [0u8; 10];
        hash_key_buf[0] = b'h';
        hash_key_buf[1] = b':';
        for (doc_id_hash, score) in &scored {
            hash_key_buf[2..10].copy_from_slice(doc_id_hash);
            let doc_id = match doc_ks.get(&hash_key_buf)? {
                Some(raw) => String::from_utf8_lossy(&raw).to_string(),
                None => continue,
            };
            let fields = match doc_ks.get(doc_id.as_bytes())? {
                Some(raw) => {
                    serde_json::from_slice::<BTreeMap<String, String>>(&raw).unwrap_or_default()
                }
                None => continue,
            };
            let highlights = highlight_fields(&fields, &tokens);
            results.push(SearchHit {
                doc_id,
                score: *score,
                fields,
                highlights,
            });
        }
        Ok(results)
    }

    /// Fuzzy 搜索：对查询 token 做编辑距离 ≤ max_dist 的模糊匹配。
    pub fn search_fuzzy(
        &self,
        name: &str,
        query: &str,
        max_dist: u32,
        limit: usize,
    ) -> Result<Vec<SearchHit>, Error> {
        let inv_ks = self.store.open_keyspace(&inv_ks_name(name))?;
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        fuzzy::search_fuzzy_impl(
            &self.store,
            &inv_ks,
            &doc_ks,
            &stat_ks,
            query,
            max_dist,
            limit,
        )
    }

    /// 内部删除文档（从倒排+文档存储中移除），独立 commit。
    fn remove_doc_internal(
        &self,
        inv_ks: &Keyspace,
        doc_ks: &Keyspace,
        stat_ks: &Keyspace,
        doc_id: &str,
        analyzer: Analyzer,
    ) -> Result<bool, Error> {
        let mut batch = self.store.batch();
        let existed = self.remove_doc_into_batch(&mut batch, inv_ks, doc_ks, stat_ks, doc_id, analyzer)?;
        if existed {
            batch.commit()?;
        }
        Ok(existed)
    }

    /// 将文档删除操作追加到已有 WriteBatch（不 commit），供 index_doc 原子覆写使用。
    fn remove_doc_into_batch(
        &self,
        batch: &mut crate::storage::Batch,
        inv_ks: &Keyspace,
        doc_ks: &Keyspace,
        stat_ks: &Keyspace,
        doc_id: &str,
        analyzer: Analyzer,
    ) -> Result<bool, Error> {
        let raw = match doc_ks.get(doc_id.as_bytes())? {
            Some(r) => r,
            None => return Ok(false),
        };
        let fields: BTreeMap<String, String> =
            serde_json::from_slice(&raw).map_err(|e| Error::Serialization(e.to_string()))?;
        let full_text: String = fields.values().cloned().collect::<Vec<_>>().join(" ");
        let tokens = tokenizer::tokenize(&full_text, analyzer);
        let doc_len = tokens.len() as u32;
        let mut tf_map: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *tf_map.entry(token.clone()).or_insert(0) += 1;
        }
        for term in tf_map.keys() {
            batch.remove(inv_ks, inv_key(term, doc_id));
            let dk = df_key(term);
            let old_df = stat_ks
                .get(&dk)?
                .map(|v| u64::from_le_bytes(v[..8].try_into().unwrap_or([0; 8])))
                .unwrap_or(0);
            if old_df > 1 {
                batch.insert(stat_ks, dk, (old_df - 1).to_le_bytes().to_vec())?;
            } else {
                batch.remove(stat_ks, dk);
            }
        }
        batch.remove(doc_ks, doc_id.as_bytes().to_vec());
        // 清理 doc_id_hash 反向映射
        let mut hash_key = Vec::with_capacity(10);
        hash_key.extend_from_slice(b"h:");
        hash_key.extend_from_slice(&hash_bytes(doc_id));
        batch.remove(doc_ks, hash_key);
        let doc_count = read_u64(stat_ks, META_DOC_COUNT);
        let total_len = read_u64(stat_ks, META_TOTAL_LEN);
        batch.insert(
            stat_ks,
            META_DOC_COUNT.to_vec(),
            doc_count.saturating_sub(1).to_le_bytes().to_vec(),
        )?;
        batch.insert(
            stat_ks,
            META_TOTAL_LEN.to_vec(),
            total_len
                .saturating_sub(doc_len as u64)
                .to_le_bytes()
                .to_vec(),
        )?;
        Ok(true)
    }
}

/// 对文档字段生成高亮片段：匹配的 token 用 `<em>...</em>` 包裹。
pub(super) fn highlight_fields(
    fields: &BTreeMap<String, String>,
    query_tokens: &[String],
) -> BTreeMap<String, String> {
    let mut highlights = BTreeMap::new();
    for (field, text) in fields {
        let hl = highlight_text(text, query_tokens);
        if hl != *text {
            highlights.insert(field.clone(), hl);
        }
    }
    highlights
}

/// 对单个文本生成高亮：逐词匹配，命中的词用 `<em>` 包裹。
fn highlight_text(text: &str, query_tokens: &[String]) -> String {
    let mut result = String::with_capacity(text.len() + 32);
    let mut i = 0;
    let chars: Vec<char> = text.chars().collect();
    let n = chars.len();
    while i < n {
        if chars[i].is_alphanumeric() || chars[i] == '_' {
            let start = i;
            while i < n && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let word_lower = word.to_lowercase();
            if query_tokens.iter().any(|t| t == &word_lower) {
                result.push_str("<em>");
                result.push_str(&word);
                result.push_str("</em>");
            } else {
                result.push_str(&word);
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

pub(super) fn read_u64(ks: &Keyspace, key: &[u8]) -> u64 {
    ks.get(key)
        .ok()
        .flatten()
        .and_then(|v| v[..8].try_into().ok().map(u64::from_le_bytes))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
