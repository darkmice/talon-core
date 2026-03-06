/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 分词器：Unicode word boundary 分割 + 小写化 + 停用词过滤。
//!
//! P0 实现 Standard Analyzer（对标 ES standard analyzer）。

/// 分词器类型。
#[derive(Debug, Clone, Copy)]
pub enum Analyzer {
    /// Unicode word boundary + 小写化 + 停用词（默认）。
    Standard,
    /// 仅按空白字符分割。
    Whitespace,
    /// 中文 jieba 分词 + 英文 word boundary。
    Chinese,
}

/// 英文停用词表（对标 ES standard analyzer 默认停用词）。
const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

/// 对输入文本进行分词，返回 token 列表（已小写化、去停用词）。
pub(super) fn tokenize(text: &str, analyzer: Analyzer) -> Vec<String> {
    let raw_tokens = match analyzer {
        Analyzer::Standard => tokenize_unicode(text),
        Analyzer::Whitespace => text.split_whitespace().map(|s| s.to_string()).collect(),
        Analyzer::Chinese => {
            let dict = super::jieba::JiebaDict::new();
            dict.cut(text)
        }
    };
    raw_tokens
        .into_iter()
        .map(|t| t.to_lowercase())
        .filter(|t| !t.is_empty() && t.len() <= 256)
        .filter(|t| !STOP_WORDS.contains(&t.as_str()))
        .collect()
}

/// Unicode word boundary 分词：按非字母数字字符分割。
/// 支持中文（每个字一个 token）、英文（连续字母数字为一个 token）。
fn tokenize_unicode(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            // CJK 字符：每个字独立为一个 token
            if is_cjk(ch) {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            } else {
                current.push(ch);
            }
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// 判断是否为 CJK 统一表意文字。
fn is_cjk(ch: char) -> bool {
    matches!(ch,
        '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}' // CJK Extension A
        | '\u{F900}'..='\u{FAFF}' // CJK Compatibility Ideographs
        | '\u{2E80}'..='\u{2EFF}' // CJK Radicals Supplement
        | '\u{3000}'..='\u{303F}' // CJK Symbols and Punctuation
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_english() {
        let tokens = tokenize("Hello World! This is a test.", Analyzer::Standard);
        assert_eq!(tokens, vec!["hello", "world", "test"]);
    }

    #[test]
    fn standard_chinese() {
        let tokens = tokenize("你好世界", Analyzer::Standard);
        assert_eq!(tokens, vec!["你", "好", "世", "界"]);
    }

    #[test]
    fn chinese_analyzer_segmentation() {
        let tokens = tokenize("我来到北京清华大学", Analyzer::Chinese);
        let joined = tokens.join("/");
        assert!(
            joined.contains("北京"),
            "should segment 北京, got: {}",
            joined
        );
    }

    #[test]
    fn chinese_analyzer_ai_terms() {
        let tokens = tokenize("人工智能和机器学习", Analyzer::Chinese);
        let joined = tokens.join("/");
        assert!(
            joined.contains("人工智能"),
            "should segment 人工智能, got: {}",
            joined
        );
        assert!(
            joined.contains("机器学习"),
            "should segment 机器学习, got: {}",
            joined
        );
    }

    #[test]
    fn standard_mixed() {
        let tokens = tokenize("Python异步编程 async/await", Analyzer::Standard);
        assert!(tokens.contains(&"python".to_string()));
        assert!(tokens.contains(&"异".to_string()));
        assert!(tokens.contains(&"async".to_string()));
        assert!(tokens.contains(&"await".to_string()));
    }

    #[test]
    fn whitespace_analyzer() {
        let tokens = tokenize("Hello World", Analyzer::Whitespace);
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn stop_words_filtered() {
        let tokens = tokenize("the quick brown fox", Analyzer::Standard);
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }
}
