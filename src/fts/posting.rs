/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Posting 编码优化：VInt 变长整数 + Delta 编码。
//!
//! 参考 Lucene/Tantivy 的 Posting List 压缩策略：
//! - VInt: 小值 1 字节 (0-127)，大值按需 2-5 字节
//! - Delta: 位置列表存差值而非绝对值，差值更小压缩更好
//!
//! 格式 v2（magic=0xFE 开头）:
//! ```text
//! [0xFE] [tf: vint] [doc_len: vint] [pos_count: vint] [delta_positions: vint×N]
//! ```
//!
//! 与 v1 格式（固定宽度 u32 LE）完全向后兼容：
//! 解码时自动检测 magic byte 选择解码路径。

/// v2 格式 magic byte。
const V2_MAGIC: u8 = 0xFE;

/// VInt 编码：小值 1 字节，7-bit 分组 + 高位续接标志。
/// 对标 Lucene 的 VInt / Protocol Buffers 的 varint 编码。
#[inline]
pub fn encode_vint(buf: &mut Vec<u8>, mut val: u32) {
    loop {
        if val < 0x80 {
            buf.push(val as u8);
            return;
        }
        buf.push((val & 0x7F) as u8 | 0x80);
        val >>= 7;
    }
}

/// VInt 解码。返回 (value, bytes_consumed)。
#[inline]
pub fn decode_vint(data: &[u8], pos: &mut usize) -> Option<u32> {
    let mut result = 0u32;
    let mut shift = 0;
    loop {
        if *pos >= data.len() {
            return None;
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 35 {
            return None; // 溢出保护
        }
    }
}

/// v2 编码：VInt + Delta positions。
/// 对于典型的 positions [0, 5, 12, 30]，delta = [0, 5, 7, 18]，
/// 每个 delta 平均 1-2 字节（VInt），比 v1 的固定 4 字节节省 50-75%。
pub fn encode_inv_entry_v2(tf: u32, doc_len: u32, positions: &[u32]) -> Vec<u8> {
    let pos_count = positions.len().min(super::MAX_POSITIONS);
    // 预估容量：magic(1) + 3个vint(~9) + positions(~2×N)
    let mut buf = Vec::with_capacity(10 + pos_count * 2);
    buf.push(V2_MAGIC);
    encode_vint(&mut buf, tf);
    encode_vint(&mut buf, doc_len);
    encode_vint(&mut buf, pos_count as u32);
    // Delta 编码：存差值
    let mut prev = 0u32;
    for &p in positions.iter().take(super::MAX_POSITIONS) {
        let delta = p.saturating_sub(prev);
        encode_vint(&mut buf, delta);
        prev = p;
    }
    buf
}

/// 统一解码：自动检测 v1（固定宽度）或 v2（VInt+Delta）格式。
///
/// v1 格式兼容：首字节不是 0xFE 时走旧路径。
/// v2 格式（0xFE 开头）走 VInt+Delta 解码。
pub fn decode_inv_entry_auto(data: &[u8]) -> Option<(u32, u32, Vec<u32>)> {
    if data.is_empty() {
        return None;
    }
    if data[0] == V2_MAGIC {
        // v2 format
        let mut pos = 1;
        let tf = decode_vint(data, &mut pos)?;
        let doc_len = decode_vint(data, &mut pos)?;
        let count = decode_vint(data, &mut pos)? as usize;
        let mut positions = Vec::with_capacity(count.min(super::MAX_POSITIONS));
        let mut prev = 0u32;
        for _ in 0..count.min(super::MAX_POSITIONS) {
            let delta = decode_vint(data, &mut pos)?;
            prev = prev.wrapping_add(delta);
            positions.push(prev);
        }
        Some((tf, doc_len, positions))
    } else {
        // v1 format: 固定宽度 u32 LE（向后兼容）
        super::decode_inv_entry_v1(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vint_roundtrip() {
        let test_values = [0u32, 1, 127, 128, 255, 16383, 16384, u32::MAX];
        for &v in &test_values {
            let mut buf = Vec::new();
            encode_vint(&mut buf, v);
            let mut pos = 0;
            let decoded = decode_vint(&buf, &mut pos).unwrap();
            assert_eq!(v, decoded, "VInt roundtrip failed for {}", v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn vint_small_values_one_byte() {
        for v in 0..128u32 {
            let mut buf = Vec::new();
            encode_vint(&mut buf, v);
            assert_eq!(buf.len(), 1, "value {} should be 1 byte", v);
        }
        // 128 should be 2 bytes
        let mut buf = Vec::new();
        encode_vint(&mut buf, 128);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn v2_roundtrip() {
        let positions = vec![0, 5, 12, 30, 100, 255, 1000];
        let encoded = encode_inv_entry_v2(7, 500, &positions);
        let (tf, dl, decoded_pos) = decode_inv_entry_auto(&encoded).unwrap();
        assert_eq!(tf, 7);
        assert_eq!(dl, 500);
        assert_eq!(decoded_pos, positions);
    }

    #[test]
    fn v2_empty_positions() {
        let encoded = encode_inv_entry_v2(1, 10, &[]);
        let (tf, dl, pos) = decode_inv_entry_auto(&encoded).unwrap();
        assert_eq!(tf, 1);
        assert_eq!(dl, 10);
        assert!(pos.is_empty());
    }

    #[test]
    fn v2_smaller_than_v1() {
        let positions: Vec<u32> = (0..50).map(|i| i * 3).collect(); // [0, 3, 6, ..., 147]
        let v1 = super::super::encode_inv_entry(50, 200, &positions);
        let v2 = encode_inv_entry_v2(50, 200, &positions);
        assert!(
            v2.len() < v1.len(),
            "v2 ({} bytes) should be smaller than v1 ({} bytes)",
            v2.len(),
            v1.len()
        );
    }

    #[test]
    fn v1_backward_compat() {
        // v1 encoded data (fixed width)
        let v1_data = super::super::encode_inv_entry(3, 100, &[1, 5, 10]);
        // Should decode correctly through auto-detect
        let (tf, dl, pos) = decode_inv_entry_auto(&v1_data).unwrap();
        assert_eq!(tf, 3);
        assert_eq!(dl, 100);
        assert_eq!(pos, vec![1, 5, 10]);
    }
}
