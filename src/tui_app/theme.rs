/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TUI 主题 — 颜色和样式常量，匹配设计稿暗色主题。

use ratatui::style::{Color, Modifier, Style};

// ── 主色调 ──
/// 品牌主色 #3713ec
pub const PRIMARY: Color = Color::Rgb(55, 19, 236);
/// 强调色 #00f0ff
pub const ACCENT: Color = Color::Rgb(0, 240, 255);

// ── 背景色 ──
/// 最深背景 #0a0a0f
#[allow(dead_code)]
pub const BG_DARK: Color = Color::Rgb(10, 10, 15);
/// 面板背景 #13111c
pub const SURFACE_DARK: Color = Color::Rgb(19, 17, 28);
/// 较浅面板 #1c1a29
pub const SURFACE_LIGHTER: Color = Color::Rgb(28, 26, 41);
/// 终端区域 #0d0d12
pub const TERMINAL_BG: Color = Color::Rgb(13, 13, 18);

// ── 功能色 ──
/// 成功/在线 #4ade80
pub const GREEN: Color = Color::Rgb(74, 222, 128);
/// 信息/链接 #60a5fa
pub const BLUE: Color = Color::Rgb(96, 165, 250);
/// 警告/高亮 #facc15
pub const YELLOW: Color = Color::Rgb(250, 204, 21);
/// 错误/危险 #f87171
pub const RED: Color = Color::Rgb(248, 113, 113);
/// 次要强调 #a78bfa
pub const PURPLE: Color = Color::Rgb(167, 139, 250);
/// 禁用/辅助 #475569
pub const GRAY: Color = Color::Rgb(71, 85, 105);

// ── 文本色 ──
/// 主要文本 slate-200
pub const TEXT: Color = Color::Rgb(226, 232, 240);
/// 次要文本 slate-400
pub const TEXT_SECONDARY: Color = Color::Rgb(148, 163, 184);
/// 弱化文本 slate-500
pub const TEXT_MUTED: Color = Color::Rgb(100, 116, 139);
/// 最暗文本 slate-600
pub const TEXT_DIM: Color = Color::Rgb(71, 85, 105);

// ── 边框色 ──
/// 默认边框 slate-700
pub const BORDER: Color = Color::Rgb(51, 65, 85);
/// 暗边框 slate-800
pub const BORDER_DIM: Color = Color::Rgb(30, 41, 59);

// ── 常用样式 ──

/// 标题栏背景样式。
pub fn title_bar() -> Style {
    Style::default().bg(SURFACE_DARK).fg(TEXT)
}

/// 普通边框样式。
pub fn border() -> Style {
    Style::default().fg(BORDER)
}

/// 焦点边框样式。
pub fn border_focus() -> Style {
    Style::default().fg(BLUE)
}

/// 列表选中项高亮样式。
pub fn list_highlight() -> Style {
    Style::default()
        .fg(TERMINAL_BG)
        .bg(BLUE)
        .add_modifier(Modifier::BOLD)
}

/// 统计卡片值样式。
pub fn stat_value(color: Color) -> Style {
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

/// 日志级别样式。
pub fn log_level(level: &str) -> Style {
    let color = match level {
        "INFO" => BLUE,
        "SUCCESS" => GREEN,
        "WARN" => YELLOW,
        "ERROR" => RED,
        "DEBUG" => TEXT_DIM,
        _ => TEXT_MUTED,
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}
