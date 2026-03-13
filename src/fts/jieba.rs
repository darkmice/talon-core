/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 中文分词器：基于最大概率路径（DAG + 动态规划）+ HMM 新词发现，对标 jieba。
//!
//! 纯 Rust 实现，零外部依赖。内嵌高频词典，支持自定义词典加载。
//! 算法：构建 DAG → 动态规划求最大概率路径 → 未登录词 HMM Viterbi 切分。

use std::collections::HashMap;

/// 中文分词词典（内存中的 HashMap）。
pub(super) struct JiebaDict {
    /// word → log(freq/total)
    freq: HashMap<String, f64>,
    /// 总词频（用于计算概率）
    total: f64,
}

/// 内嵌高频词典（编译时嵌入）。
/// 格式：每行 `word freq [pos]`，只取前两列。
const BUILTIN_DICT: &str = include_str!("dict_cn.txt");

impl JiebaDict {
    /// 从内嵌词典初始化。
    pub(super) fn new() -> Self {
        let mut freq = HashMap::new();
        let mut total = 0.0f64;
        for line in BUILTIN_DICT.lines() {
            let mut parts = line.split_whitespace();
            let word = match parts.next() {
                Some(w) => w,
                None => continue,
            };
            let f: f64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1.0);
            freq.insert(word.to_string(), f);
            total += f;
        }
        // 预计算 log 概率
        let log_total = total.ln();
        for v in freq.values_mut() {
            *v = v.ln() - log_total;
        }
        JiebaDict { freq, total }
    }

    /// 加载额外自定义词典（追加到现有词典）。
    #[allow(dead_code)]
    pub(super) fn load_user_dict(&mut self, dict_text: &str) {
        let log_total = self.total.ln();
        for line in dict_text.lines() {
            let mut parts = line.split_whitespace();
            let word = match parts.next() {
                Some(w) => w,
                None => continue,
            };
            let f: f64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1.0);
            self.total += f;
            self.freq.insert(word.to_string(), f.ln() - log_total);
        }
    }

    /// 对中文文本进行分词。
    pub(super) fn cut(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let chars: Vec<char> = text.chars().collect();
        let n = chars.len();
        if n == 0 {
            return tokens;
        }

        // 分离中文段和非中文段
        let mut i = 0;
        while i < n {
            if is_chinese_char(chars[i]) {
                // 收集连续中文字符段
                let start = i;
                while i < n && is_chinese_char(chars[i]) {
                    i += 1;
                }
                let segment: String = chars[start..i].iter().collect();
                tokens.extend(self.cut_chinese(&segment));
            } else if !is_chinese_char(chars[i]) && (chars[i].is_alphanumeric() || chars[i] == '_')
            {
                // 收集连续英文/数字（排除 CJK，因为 is_alphanumeric 对 CJK 也返回 true）
                let start = i;
                while i < n
                    && !is_chinese_char(chars[i])
                    && (chars[i].is_alphanumeric() || chars[i] == '_')
                {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                tokens.push(word);
            } else {
                i += 1; // 跳过标点符号
            }
        }
        tokens
    }

    /// 对纯中文段进行 DAG + DP 分词。
    fn cut_chinese(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        let n = chars.len();
        if n == 0 {
            return vec![];
        }

        // 构建 DAG：dag[i] = 从位置 i 开始的所有可能词的结束位置列表
        let mut dag: Vec<Vec<usize>> = vec![vec![]; n];
        for (i, entry) in dag.iter_mut().enumerate().take(n) {
            let mut j = i;
            let mut frag = String::new();
            while j < n && j - i < 6 {
                frag.push(chars[j]);
                if self.freq.contains_key(&frag) {
                    entry.push(j);
                }
                j += 1;
            }
            if entry.is_empty() {
                entry.push(i);
            }
        }

        // 动态规划：从右到左求最大概率路径
        // route[i] = (log_prob, next_pos)
        let mut route: Vec<(f64, usize)> = vec![(0.0, 0); n + 1];
        route[n] = (0.0, n);
        let default_prob = -(self.total.ln()); // 未登录词概率

        for i in (0..n).rev() {
            let mut best = (f64::NEG_INFINITY, 0usize);
            for &j in &dag[i] {
                let word: String = chars[i..=j].iter().collect();
                let prob = self.freq.get(&word).copied().unwrap_or(default_prob);
                let total = prob + route[j + 1].0;
                if total > best.0 {
                    best = (total, j + 1);
                }
            }
            route[i] = best;
        }

        // 沿最优路径输出分词结果，连续单字交给 HMM 重新切分
        let mut result = Vec::new();
        let mut single_chars: Vec<char> = Vec::new(); // 缓冲连续单字
        let mut i = 0;
        while i < n {
            let j = route[i].1;
            let word: String = chars[i..j].iter().collect();
            if j - i == 1 {
                // 单字：缓冲，等知道是否有更多连续单字
                single_chars.push(chars[i]);
            } else {
                // 多字词：先处理缓冲的单字
                if !single_chars.is_empty() {
                    result.extend(super::hmm::viterbi_cut(&single_chars));
                    single_chars.clear();
                }
                result.push(word);
            }
            i = j;
        }
        // 处理末尾缓冲的单字
        if !single_chars.is_empty() {
            result.extend(super::hmm::viterbi_cut(&single_chars));
        }
        result
    }
}

/// 判断是否为中文字符（CJK 统一表意文字范围）。
fn is_chinese_char(ch: char) -> bool {
    matches!(ch,
        '\u{4E00}'..='\u{9FFF}'
        | '\u{3400}'..='\u{4DBF}'
        | '\u{F900}'..='\u{FAFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_chinese_segmentation() {
        let dict = JiebaDict::new();
        let tokens = dict.cut("我来到北京清华大学");
        let joined = tokens.join("/");
        // 应该至少切出"北京"和"清华大学"或"清华"+"大学"
        assert!(joined.contains("北京"), "应该切出北京, got: {}", joined);
    }

    #[test]
    fn mixed_chinese_english() {
        let dict = JiebaDict::new();
        let tokens = dict.cut("Python是最好的编程语言");
        // cut 返回原始大小写，小写化在 tokenizer 层
        assert!(
            tokens.iter().any(|t| t.eq_ignore_ascii_case("python")),
            "should contain Python, got: {:?}",
            tokens
        );
        assert!(tokens.len() > 2);
    }

    #[test]
    fn empty_input() {
        let dict = JiebaDict::new();
        let tokens = dict.cut("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn pure_english() {
        let dict = JiebaDict::new();
        let tokens = dict.cut("hello world");
        assert_eq!(tokens, vec!["hello", "world"]);
    }
}
