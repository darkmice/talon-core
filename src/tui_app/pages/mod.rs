/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TUI 页面模块 — 8 大引擎 + AI-Native 层，共 9 页。
//!
//! 快捷键方案（业界标准，跨平台）：
//! - NAV: `1-9` 切页、`j/k` 导航、`g/G` 跳顶/跳底、`i` 进入输入、`r` 刷新、`q` 退出、`?` 帮助
//! - INPUT: `Esc` 退出输入 → NAV、`Ctrl+Enter` 执行 SQL、`Enter` 提交
//! - GLOBAL: `Ctrl+C` 强制退出、`Ctrl+R` 重连、`F1-F9` 兼容切页

pub mod ai_chat;
pub mod dashboard;
pub mod fts;
pub mod geo;
pub mod graph;
pub mod mq;
pub mod sql_editor;
pub mod ts;
pub mod vector_search;

/// 全部页面有序列表。
const ALL_PAGES: [Page; 9] = [
    Page::Dashboard,
    Page::SqlEditor,
    Page::VectorSearch,
    Page::Fts,
    Page::TimeSeries,
    Page::MessageQueue,
    Page::Geo,
    Page::Graph,
    Page::AiChat,
];

/// 页面枚举 — 8 引擎 + AI-Native。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    /// 1: KV 缓存引擎。
    Dashboard,
    /// 2: SQL 关系型引擎。
    SqlEditor,
    /// 3: 向量索引引擎。
    VectorSearch,
    /// 4: 全文搜索引擎。
    Fts,
    /// 5: 时序存储引擎。
    TimeSeries,
    /// 6: 消息队列引擎。
    MessageQueue,
    /// 7: GEO 地理引擎。
    Geo,
    /// 8: 图引擎。
    Graph,
    /// 9: AI-Native 层。
    AiChat,
}

impl Page {
    /// 全部页面有序列表。
    pub fn all() -> &'static [Page; 9] {
        &ALL_PAGES
    }

    /// 从 1-based 索引创建页面（数字键 / F 键）。
    pub fn from_index(n: usize) -> Option<Page> {
        ALL_PAGES.get(n.wrapping_sub(1)).copied()
    }

    /// 页面在列表中的 0-based 位置。
    fn ordinal(self) -> usize {
        ALL_PAGES.iter().position(|p| *p == self).unwrap_or(0)
    }

    /// 下一页（循环）。
    pub fn next(self) -> Page {
        ALL_PAGES[(self.ordinal() + 1) % ALL_PAGES.len()]
    }

    /// 上一页（循环）。
    pub fn prev(self) -> Page {
        ALL_PAGES[(self.ordinal() + ALL_PAGES.len() - 1) % ALL_PAGES.len()]
    }

    /// 页面标签（显示在标题栏徽章中）。
    pub fn label(&self) -> &str {
        match self {
            Page::Dashboard => "KV ENGINE",
            Page::SqlEditor => "SQL ENGINE",
            Page::VectorSearch => "VECTOR ENGINE",
            Page::Fts => "FTS ENGINE",
            Page::TimeSeries => "TS ENGINE",
            Page::MessageQueue => "MQ ENGINE",
            Page::Geo => "GEO ENGINE",
            Page::Graph => "GRAPH ENGINE",
            Page::AiChat => "AI ENGINE",
        }
    }

    /// 页面快捷键提示（数字键）。
    pub fn hotkey(&self) -> &str {
        match self {
            Page::Dashboard => "1",
            Page::SqlEditor => "2",
            Page::VectorSearch => "3",
            Page::Fts => "4",
            Page::TimeSeries => "5",
            Page::MessageQueue => "6",
            Page::Geo => "7",
            Page::Graph => "8",
            Page::AiChat => "9",
        }
    }

    /// 页面短名（标题栏缩写）。
    pub fn short_name(&self) -> &str {
        match self {
            Page::Dashboard => "KV",
            Page::SqlEditor => "SQL",
            Page::VectorSearch => "Vec",
            Page::Fts => "FTS",
            Page::TimeSeries => "TS",
            Page::MessageQueue => "MQ",
            Page::Geo => "GEO",
            Page::Graph => "Graph",
            Page::AiChat => "AI",
        }
    }
}

/// 字符索引转字节偏移（Unicode 安全）。
pub(crate) fn char_to_byte(s: &str, char_idx: usize) -> usize {
    s.char_indices()
        .nth(char_idx)
        .map(|(i, _)| i)
        .unwrap_or(s.len())
}

/// 字符串的字符数。
pub(crate) fn char_len(s: &str) -> usize {
    s.chars().count()
}
