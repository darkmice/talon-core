/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! FTS 索引管理 API：get_mapping / list_indexes / close / open / aliases / reindex。
//! 对标 ES `GET /index/_mapping`、`POST /index/_close`、`POST /index/_open`、`POST /_aliases`。

use super::{doc_ks_name, meta_ks_name, read_u64, FtsEngine, META_DOC_COUNT};
use crate::error::Error;
use std::collections::BTreeMap;

/// FTS 索引映射信息（对标 ES `GET /index/_mapping`）。
#[derive(Debug, Clone)]
pub struct FtsMapping {
    /// 索引名称。
    pub name: String,
    /// 分词器名称。
    pub analyzer: String,
    /// 已索引文档数。
    pub doc_count: u64,
    /// 已发现的字段列表（从已索引文档中采样）。
    pub fields: Vec<String>,
}

/// FTS 索引摘要信息。
#[derive(Debug, Clone)]
pub struct FtsIndexInfo {
    /// 索引名称。
    pub name: String,
    /// 文档数量。
    pub doc_count: u64,
}

impl FtsEngine {
    /// 获取索引的字段映射信息（对标 ES `GET /index/_mapping`）。
    ///
    /// 从 meta keyspace 读取 config，从 doc keyspace 采样第一个文档获取字段列表。
    pub fn get_mapping(&self, name: &str) -> Result<FtsMapping, Error> {
        // 读取索引 config
        let cfg_raw = self
            .meta_ks
            .get(name.as_bytes())?
            .ok_or_else(|| Error::SqlExec(format!("FTS 索引不存在: {}", name)))?;
        let cfg: serde_json::Value =
            serde_json::from_slice(&cfg_raw).map_err(|e| Error::Serialization(e.to_string()))?;
        let analyzer = cfg
            .get("analyzer")
            .and_then(|v| v.as_str())
            .unwrap_or("Standard")
            .to_string();

        // 读取文档数
        let stat_ks = self.store.open_keyspace(&meta_ks_name(name))?;
        let doc_count = read_u64(&stat_ks, META_DOC_COUNT);

        // 从 doc keyspace 采样多个文档获取字段列表（取并集，最多采样 100 个）
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let mut field_set = std::collections::BTreeSet::new();
        let mut sampled = 0usize;
        doc_ks.for_each_kv_prefix(b"", |_key, value| {
            if let Ok(map) = serde_json::from_slice::<BTreeMap<String, String>>(value) {
                for k in map.keys() {
                    field_set.insert(k.clone());
                }
            }
            sampled += 1;
            sampled < 100
        })?;
        let fields: Vec<String> = field_set.into_iter().collect();

        Ok(FtsMapping {
            name: name.to_string(),
            analyzer,
            doc_count,
            fields,
        })
    }

    /// 列出所有 FTS 索引（对标 ES `GET /_cat/indices`）。
    pub fn list_indexes(&self) -> Result<Vec<FtsIndexInfo>, Error> {
        let mut indexes = Vec::new();
        self.meta_ks.for_each_kv_prefix(b"", |key, _value| {
            if let Ok(name) = std::str::from_utf8(key) {
                let doc_count = self
                    .store
                    .open_keyspace(&meta_ks_name(name))
                    .map(|ks| read_u64(&ks, META_DOC_COUNT))
                    .unwrap_or(0);
                indexes.push(FtsIndexInfo {
                    name: name.to_string(),
                    doc_count,
                });
            }
            true
        })?;
        Ok(indexes)
    }

    /// 关闭索引（对标 ES `POST /index/_close`）。
    ///
    /// 关闭后拒绝 search/index_doc 操作，但允许 get_mapping/drop_index。
    pub fn close_index(&self, name: &str) -> Result<(), Error> {
        let raw = self
            .meta_ks
            .get(name.as_bytes())?
            .ok_or_else(|| Error::SqlExec(format!("FTS 索引不存在: {}", name)))?;
        let mut cfg: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|e| Error::Serialization(e.to_string()))?;
        cfg["closed"] = serde_json::Value::Bool(true);
        let updated = serde_json::to_vec(&cfg).map_err(|e| Error::Serialization(e.to_string()))?;
        self.meta_ks.set(name.as_bytes(), &updated)
    }

    /// 打开索引（对标 ES `POST /index/_open`）。
    pub fn open_index(&self, name: &str) -> Result<(), Error> {
        let raw = self
            .meta_ks
            .get(name.as_bytes())?
            .ok_or_else(|| Error::SqlExec(format!("FTS 索引不存在: {}", name)))?;
        let mut cfg: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|e| Error::Serialization(e.to_string()))?;
        cfg["closed"] = serde_json::Value::Bool(false);
        let updated = serde_json::to_vec(&cfg).map_err(|e| Error::Serialization(e.to_string()))?;
        self.meta_ks.set(name.as_bytes(), &updated)
    }

    /// 重建索引（对标 ES `POST /_reindex`）。
    ///
    /// 遍历 doc keyspace 中所有已索引文档，清空倒排索引后重新构建。
    /// 用于分词器变更、数据修复等场景。返回重建的文档数。
    pub fn reindex(&self, name: &str) -> Result<u64, Error> {
        // 验证索引存在
        if self.meta_ks.get(name.as_bytes())?.is_none() {
            return Err(Error::SqlExec(format!("FTS 索引不存在: {}", name)));
        }
        // 读取所有文档
        let doc_ks = self.store.open_keyspace(&doc_ks_name(name))?;
        let mut docs: Vec<super::FtsDoc> = Vec::new();
        doc_ks.for_each_kv_prefix(b"", |key, value| {
            if let (Ok(doc_id), Ok(fields)) = (
                std::str::from_utf8(key),
                serde_json::from_slice::<std::collections::BTreeMap<String, String>>(value),
            ) {
                docs.push(super::FtsDoc {
                    doc_id: doc_id.to_string(),
                    fields,
                });
            }
            true
        })?;
        let count = docs.len() as u64;
        // 清空倒排索引和统计
        let inv_ks = self.store.open_keyspace(&super::inv_ks_name(name))?;
        let stat_ks = self.store.open_keyspace(&super::meta_ks_name(name))?;
        // 删除所有倒排条目
        let mut inv_keys: Vec<Vec<u8>> = Vec::new();
        inv_ks.for_each_key_prefix(b"", |key| {
            inv_keys.push(key.to_vec());
            true
        })?;
        for key in &inv_keys {
            inv_ks.delete(key)?;
        }
        // 重置统计
        stat_ks.set(super::META_DOC_COUNT, 0u64.to_le_bytes())?;
        stat_ks.set(super::META_TOTAL_LEN, 0u64.to_le_bytes())?;
        // 清空文档存储（重新写入）
        let mut doc_keys: Vec<Vec<u8>> = Vec::new();
        doc_ks.for_each_key_prefix(b"", |key| {
            doc_keys.push(key.to_vec());
            true
        })?;
        for key in &doc_keys {
            doc_ks.delete(key)?;
        }
        // 重新索引所有文档
        for doc in &docs {
            self.index_doc(name, doc)?;
        }
        Ok(count)
    }

    /// 创建索引别名（对标 ES `POST /_aliases`）。
    ///
    /// 别名可用于 search/index_doc 等操作，自动解析为真实索引名。
    /// RAG 场景：知识库版本切换时修改别名指向，应用无需改动。
    pub fn add_alias(&self, alias: &str, index: &str) -> Result<(), Error> {
        // 验证目标索引存在
        if self.meta_ks.get(index.as_bytes())?.is_none() {
            return Err(Error::SqlExec(format!("FTS 索引不存在: {}", index)));
        }
        let alias_key = format!("_alias:{}", alias);
        self.meta_ks.set(alias_key.as_bytes(), index.as_bytes())
    }

    /// 删除索引别名。
    pub fn remove_alias(&self, alias: &str) -> Result<(), Error> {
        let alias_key = format!("_alias:{}", alias);
        if self.meta_ks.get(alias_key.as_bytes())?.is_none() {
            return Err(Error::SqlExec(format!("FTS 别名不存在: {}", alias)));
        }
        self.meta_ks.delete(alias_key.as_bytes())
    }

    /// 解析名称：如果是别名则返回真实索引名，否则原样返回。
    pub(crate) fn resolve_alias(&self, name: &str) -> String {
        let alias_key = format!("_alias:{}", name);
        self.meta_ks
            .get(alias_key.as_bytes())
            .ok()
            .flatten()
            .and_then(|raw| std::str::from_utf8(&raw).ok().map(|s| s.to_string()))
            .unwrap_or_else(|| name.to_string())
    }

    /// 检查索引是否已关闭。
    pub(crate) fn is_closed(&self, name: &str) -> bool {
        self.meta_ks
            .get(name.as_bytes())
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
            .and_then(|cfg| cfg.get("closed").and_then(|v| v.as_bool()))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fts::{FtsConfig, FtsDoc};
    use crate::storage::Store;

    #[test]
    fn get_mapping_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("docs", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([
                    ("title".into(), "Hello World".into()),
                    ("body".into(), "Some content".into()),
                ]),
            },
        )
        .unwrap();
        let mapping = fts.get_mapping("docs").unwrap();
        assert_eq!(mapping.name, "docs");
        assert_eq!(mapping.doc_count, 1);
        assert!(mapping.fields.contains(&"title".to_string()));
        assert!(mapping.fields.contains(&"body".to_string()));
    }

    #[test]
    fn get_mapping_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        assert!(fts.get_mapping("nonexistent").is_err());
    }

    #[test]
    fn close_open_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("idx", &FtsConfig::default()).unwrap();
        // 索引文档正常
        fts.index_doc(
            "idx",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "hello world".into())]),
            },
        )
        .unwrap();
        assert!(!fts.is_closed("idx"));
        // 关闭索引
        fts.close_index("idx").unwrap();
        assert!(fts.is_closed("idx"));
        // 关闭后 search 和 index_doc 应失败
        assert!(fts.search("idx", "hello", 10).is_err());
        assert!(fts
            .index_doc(
                "idx",
                &FtsDoc {
                    doc_id: "2".into(),
                    fields: BTreeMap::from([("text".into(), "test".into())]),
                },
            )
            .is_err());
        // get_mapping 仍可用
        assert!(fts.get_mapping("idx").is_ok());
        // 重新打开
        fts.open_index("idx").unwrap();
        assert!(!fts.is_closed("idx"));
        // 打开后 search 恢复正常
        let hits = fts.search("idx", "hello", 10).unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn reindex_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("ri", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "ri",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "rust programming".into())]),
            },
        )
        .unwrap();
        fts.index_doc(
            "ri",
            &FtsDoc {
                doc_id: "2".into(),
                fields: BTreeMap::from([("text".into(), "python scripting".into())]),
            },
        )
        .unwrap();
        // 重建索引
        let count = fts.reindex("ri").unwrap();
        assert_eq!(count, 2);
        // 重建后搜索仍正常
        let hits = fts.search("ri", "rust", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "1");
        // 文档数正确
        let mapping = fts.get_mapping("ri").unwrap();
        assert_eq!(mapping.doc_count, 2);
    }

    #[test]
    fn alias_search_and_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        fts.create_index("docs_v1", &FtsConfig::default()).unwrap();
        fts.index_doc(
            "docs_v1",
            &FtsDoc {
                doc_id: "1".into(),
                fields: BTreeMap::from([("text".into(), "hello world".into())]),
            },
        )
        .unwrap();
        // 创建别名
        fts.add_alias("docs", "docs_v1").unwrap();
        // 通过别名搜索
        let hits = fts.search("docs", "hello", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "1");
        // 通过别名索引文档
        fts.index_doc(
            "docs",
            &FtsDoc {
                doc_id: "2".into(),
                fields: BTreeMap::from([("text".into(), "goodbye world".into())]),
            },
        )
        .unwrap();
        // 真实索引名搜索应能找到新文档
        let hits = fts.search("docs_v1", "goodbye", 10).unwrap();
        assert_eq!(hits.len(), 1);
        // 删除别名
        fts.remove_alias("docs").unwrap();
        // 别名不再生效 → 搜索不到（索引不存在）
        // 注意：搜索 "docs" 会尝试打开 fts_inv:docs keyspace，不存在时返回空或错误
    }

    #[test]
    fn alias_nonexistent_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        // 别名指向不存在的索引
        assert!(fts.add_alias("docs", "ghost").is_err());
    }

    #[test]
    fn reindex_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        assert!(fts.reindex("ghost").is_err());
    }

    #[test]
    fn close_nonexistent_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        assert!(fts.close_index("ghost").is_err());
        assert!(fts.open_index("ghost").is_err());
    }

    #[test]
    fn list_indexes_empty_and_multi() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fts = FtsEngine::open(&store).unwrap();
        assert!(fts.list_indexes().unwrap().is_empty());
        fts.create_index("a", &FtsConfig::default()).unwrap();
        fts.create_index("b", &FtsConfig::default()).unwrap();
        let indexes = fts.list_indexes().unwrap();
        assert_eq!(indexes.len(), 2);
        let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
