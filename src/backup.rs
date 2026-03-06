/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 数据备份/导出：将指定 keyspace 的数据导出为 JSON Lines 格式，或从文件导入。
//!
//! M3 实现；依赖 storage。

use crate::error::Error;
use crate::storage::{Keyspace, Store};

use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// 导出一个 keyspace 的全部数据到 JSON Lines 文件。
/// 每行格式：{"k":"<base64_key>","v":"<base64_value>"}
/// M86：for_each_kv_prefix 消除 N+1 双重查找。
pub fn export_keyspace(ks: &Keyspace, path: impl AsRef<Path>) -> Result<u64, Error> {
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);
    let mut count = 0u64;
    let mut io_err: Option<Error> = None;
    ks.for_each_kv_prefix(b"", |key, val| {
        let line = serde_json::json!({
            "k": base64_encode(key),
            "v": base64_encode(val),
        });
        if let Err(e) = serde_json::to_writer(&mut writer, &line) {
            io_err = Some(Error::Serialization(e.to_string()));
            return false;
        }
        if let Err(e) = writer.write_all(b"\n") {
            io_err = Some(Error::Io(e));
            return false;
        }
        count += 1;
        true
    })?;
    if let Some(e) = io_err {
        return Err(e);
    }
    writer.flush()?;
    Ok(count)
}

/// 从 JSON Lines 文件导入数据到 keyspace。
pub fn import_keyspace(ks: &Keyspace, path: impl AsRef<Path>) -> Result<u64, Error> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0u64;
    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let obj: serde_json::Value =
            serde_json::from_str(line).map_err(|e| Error::Serialization(e.to_string()))?;
        let k = obj
            .get("k")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Serialization("缺少 k 字段".to_string()))?;
        let v = obj
            .get("v")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Serialization("缺少 v 字段".to_string()))?;
        let key = base64_decode(k)?;
        let val = base64_decode(v)?;
        ks.set(&key, &val)?;
        count += 1;
    }
    Ok(count)
}

/// 导出整个数据库的所有已知 keyspace 到目录。
/// 每个 keyspace 导出为 `{dir}/{keyspace_name}.jsonl`。
pub fn export_db(
    store: &Store,
    dir: impl AsRef<Path>,
    keyspace_names: &[&str],
) -> Result<u64, Error> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir)?;
    let mut total = 0u64;
    for name in keyspace_names {
        if let Ok(ks) = store.open_keyspace(name) {
            let path = dir.join(format!("{}.jsonl", name));
            total += export_keyspace(&ks, path)?;
        }
    }
    Ok(total)
}

/// 从目录导入所有 .jsonl 文件到对应 keyspace。
pub fn import_db(store: &Store, dir: impl AsRef<Path>) -> Result<u64, Error> {
    let dir = dir.as_ref();
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| Error::Serialization("无效文件名".to_string()))?;
            let ks = store.open_keyspace(name)?;
            total += import_keyspace(&ks, &path)?;
        }
    }
    Ok(total)
}

// 简单 base64 编码/解码（不引入外部依赖）
pub(crate) fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

pub(crate) fn base64_decode(s: &str) -> Result<Vec<u8>, Error> {
    fn char_val(c: u8) -> Result<u8, Error> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            b'=' => Ok(0),
            _ => Err(Error::Serialization(format!(
                "无效 base64 字符: {}",
                c as char
            ))),
        }
    }
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 4 {
            break;
        }
        let a = char_val(chunk[0])? as u32;
        let b = char_val(chunk[1])? as u32;
        let c = char_val(chunk[2])? as u32;
        let d = char_val(chunk[3])? as u32;
        let triple = (a << 18) | (b << 12) | (c << 6) | d;
        result.push((triple >> 16) as u8);
        if chunk[2] != b'=' {
            result.push((triple >> 8) as u8);
        }
        if chunk[3] != b'=' {
            result.push(triple as u8);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Store;

    #[test]
    fn base64_roundtrip() {
        let cases: &[&[u8]] = &[b"", b"a", b"ab", b"abc", b"hello world", &[0, 1, 255, 128]];
        for data in cases {
            let encoded = base64_encode(data);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(&decoded, data, "failed for {:?}", data);
        }
    }

    #[test]
    fn export_import_keyspace_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let ks = store.open_keyspace("test").unwrap();
        ks.set(b"key1", b"value1").unwrap();
        ks.set(b"key2", b"value2").unwrap();
        ks.set(b"binary\x00key", b"\xff\x00\x01").unwrap();

        let export_path = dir.path().join("export.jsonl");
        let count = export_keyspace(&ks, &export_path).unwrap();
        assert_eq!(count, 3);

        // 导入到新 keyspace
        let ks2 = store.open_keyspace("test2").unwrap();
        let imported = import_keyspace(&ks2, &export_path).unwrap();
        assert_eq!(imported, 3);
        assert_eq!(
            ks2.get(b"key1").unwrap().as_deref(),
            Some(b"value1" as &[u8])
        );
        assert_eq!(
            ks2.get(b"key2").unwrap().as_deref(),
            Some(b"value2" as &[u8])
        );
        assert_eq!(
            ks2.get(b"binary\x00key").unwrap().as_deref(),
            Some(b"\xff\x00\x01" as &[u8])
        );
    }

    #[test]
    fn export_import_db_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let ks1 = store.open_keyspace("kv").unwrap();
        ks1.set(b"a", b"1").unwrap();
        let ks2 = store.open_keyspace("sql_meta").unwrap();
        ks2.set(b"t", b"schema").unwrap();

        let backup_dir = dir.path().join("backup");
        let total = export_db(&store, &backup_dir, &["kv", "sql_meta"]).unwrap();
        assert_eq!(total, 2);

        // 导入到新 store
        let store2 = Store::open(dir.path().join("db2")).unwrap();
        let imported = import_db(&store2, &backup_dir).unwrap();
        assert_eq!(imported, 2);
        let ks1b = store2.open_keyspace("kv").unwrap();
        assert_eq!(ks1b.get(b"a").unwrap().as_deref(), Some(b"1" as &[u8]));
    }
}
