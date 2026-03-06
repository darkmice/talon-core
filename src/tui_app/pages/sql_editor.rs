/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! 设计稿 2 — SQL 编辑器。
//!
//! 布局：数据库树 | 连接栏 + 查询编辑器 + 结果表格 + 快捷键栏。

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::*;

use super::{char_len, char_to_byte};
use crate::tui_app::{net, theme};

/// SQL 编辑器页面状态。
pub struct SqlEditorState {
    pub query_input: String,
    pub cursor_pos: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub exec_time: String,
    pub tables: Vec<TableInfo>,
    pub selected_table: usize,
    pub history: Vec<String>,
    pub focus: SqlFocus,
    pub error_msg: Option<String>,
}

/// 数据库表信息。
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<(String, String)>,
}

/// 焦点区域。
#[derive(PartialEq, Eq)]
pub enum SqlFocus {
    Editor,
    Explorer,
    Results,
}

impl SqlEditorState {
    /// 创建初始状态（demo 数据）。
    pub fn new() -> Self {
        Self {
            query_input: "SELECT * FROM users WHERE status = 'active';".into(),
            cursor_pos: 44,
            columns: vec![
                "id".into(),
                "email".into(),
                "status".into(),
                "role".into(),
                "created_at".into(),
            ],
            rows: vec![
                vec![
                    "1001".into(),
                    "alice@talon.cli".into(),
                    "active".into(),
                    "admin".into(),
                    "2023-10-12 09:00".into(),
                ],
                vec![
                    "1005".into(),
                    "bob@dev.net".into(),
                    "active".into(),
                    "dev".into(),
                    "2023-10-14 11:20".into(),
                ],
                vec![
                    "1042".into(),
                    "charlie@ops.io".into(),
                    "active".into(),
                    "ops".into(),
                    "2023-11-01 14:15".into(),
                ],
                vec![
                    "1089".into(),
                    "david@hr.org".into(),
                    "active".into(),
                    "user".into(),
                    "2023-11-05 08:30".into(),
                ],
                vec![
                    "1102".into(),
                    "eve@sec.grp".into(),
                    "active".into(),
                    "audit".into(),
                    "2023-11-10 16:45".into(),
                ],
            ],
            row_count: 5,
            exec_time: "0.04s".into(),
            tables: vec![
                TableInfo {
                    name: "users".into(),
                    columns: vec![
                        ("id".into(), "uint".into()),
                        ("email".into(), "varchar".into()),
                        ("status".into(), "enum".into()),
                        ("created_at".into(), "timestamp".into()),
                    ],
                },
                TableInfo {
                    name: "audit_logs".into(),
                    columns: vec![],
                },
                TableInfo {
                    name: "permissions".into(),
                    columns: vec![],
                },
            ],
            selected_table: 0,
            history: vec![],
            focus: SqlFocus::Explorer,
            error_msg: None,
        }
    }

    /// 当前是否处于文本输入模式。
    pub fn is_input_active(&self) -> bool {
        self.focus == SqlFocus::Editor
    }

    /// 进入输入模式（聚焦编辑器）。
    pub fn enter_input_mode(&mut self) {
        self.focus = SqlFocus::Editor;
    }

    /// 刷新数据（重新加载表列表）。
    pub fn refresh(&mut self, client: &mut Option<net::TuiClient>) {
        self.refresh_tables(client);
    }

    /// 处理键盘事件。
    pub fn handle_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match self.focus {
            SqlFocus::Editor => self.handle_editor_key(key, client),
            SqlFocus::Explorer => self.handle_explorer_key(key),
            SqlFocus::Results => {
                if key.code == KeyCode::Tab {
                    self.focus = SqlFocus::Explorer;
                }
            }
        }
    }

    fn handle_editor_key(&mut self, key: KeyEvent, client: &mut Option<net::TuiClient>) {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Enter) | (_, KeyCode::F(5)) => {
                self.execute_query(client);
            }
            (KeyModifiers::CONTROL, KeyCode::Char('l')) => {
                self.query_input.clear();
                self.cursor_pos = 0;
            }
            (_, KeyCode::Tab) => self.focus = SqlFocus::Results,
            (_, KeyCode::Esc) => {
                self.focus = SqlFocus::Explorer;
            }
            (_, KeyCode::Char(c)) => {
                let byte_pos = char_to_byte(&self.query_input, self.cursor_pos);
                self.query_input.insert(byte_pos, c);
                self.cursor_pos += 1;
            }
            (_, KeyCode::Backspace) => {
                if self.cursor_pos > 0 {
                    self.cursor_pos -= 1;
                    let start = char_to_byte(&self.query_input, self.cursor_pos);
                    let end = char_to_byte(&self.query_input, self.cursor_pos + 1);
                    self.query_input.drain(start..end);
                }
            }
            (_, KeyCode::Left) => {
                self.cursor_pos = self.cursor_pos.saturating_sub(1);
            }
            (_, KeyCode::Right) => {
                if self.cursor_pos < char_len(&self.query_input) {
                    self.cursor_pos += 1;
                }
            }
            (_, KeyCode::Home) => self.cursor_pos = 0,
            (_, KeyCode::End) => self.cursor_pos = char_len(&self.query_input),
            _ => {}
        }
    }

    fn handle_explorer_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab => self.focus = SqlFocus::Editor,
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected_table = self.selected_table.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected_table < self.tables.len().saturating_sub(1) {
                    self.selected_table += 1;
                }
            }
            _ => {}
        }
    }

    fn execute_query(&mut self, client: &mut Option<net::TuiClient>) {
        let sql = self.query_input.trim().trim_end_matches(';');
        if sql.is_empty() {
            return;
        }
        self.history.push(self.query_input.clone());
        self.error_msg = None;

        if let Some(ref mut c) = client {
            match c.sql_query(sql) {
                Ok(resp) => self.parse_response(&resp),
                Err(e) => {
                    self.error_msg = Some(e.clone());
                    self.columns = vec!["Error".into()];
                    self.rows = vec![vec![e]];
                    self.row_count = 0;
                }
            }
        } else {
            self.error_msg = Some("未连接到服务端".into());
        }
    }

    fn parse_response(&mut self, resp: &serde_json::Value) {
        if let Some(false) = resp.get("ok").and_then(|o| o.as_bool()) {
            let msg = resp
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("未知错误");
            self.error_msg = Some(msg.to_string());
            self.rows.clear();
            self.row_count = 0;
            return;
        }
        if let Some(data) = resp.get("data") {
            if let Some(cols) = data.get("columns").and_then(|c| c.as_array()) {
                self.columns = cols
                    .iter()
                    .filter_map(|c| c.as_str().map(String::from))
                    .collect();
            }
            if let Some(rows) = data.get("rows").and_then(|r| r.as_array()) {
                self.rows = rows
                    .iter()
                    .map(|row| {
                        row.as_array()
                            .map(|arr| arr.iter().map(format_cell).collect())
                            .unwrap_or_else(|| vec![row.to_string()])
                    })
                    .collect();
                self.row_count = self.rows.len();
                self.exec_time = "OK".into();
            }
        }
    }

    /// 从服务端加载表列表。
    pub fn refresh_tables(&mut self, client: &mut Option<net::TuiClient>) {
        if let Some(ref mut c) = client {
            if let Ok(resp) = c.sql_query("SHOW TABLES") {
                if let Some(data) = resp.get("data") {
                    if let Some(rows) = data.get("rows").and_then(|r| r.as_array()) {
                        self.tables = rows
                            .iter()
                            .filter_map(|r| {
                                r.as_array().and_then(|a| a.first()).map(|v| TableInfo {
                                    name: format_cell(v),
                                    columns: vec![],
                                })
                            })
                            .collect();
                    }
                }
            }
        }
    }

    /// 绘制 SQL 编辑器页面。
    pub fn draw(&self, frame: &mut Frame, area: Rect) {
        let main_cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(24), Constraint::Min(1)])
            .split(area);

        self.draw_explorer(frame, main_cols[0]);

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // 连接信息
                Constraint::Length(5), // 编辑器
                Constraint::Min(1),    // 结果
                Constraint::Length(1), // 快捷键
            ])
            .split(main_cols[1]);

        self.draw_conn_bar(frame, right[0]);
        self.draw_editor(frame, right[1]);
        self.draw_results(frame, right[2]);
        self.draw_key_bar(frame, right[3]);
    }

    fn draw_explorer(&self, frame: &mut Frame, area: Rect) {
        let block = Block::bordered()
            .title(Span::styled(
                " DATABASE EXPLORER ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(if self.focus == SqlFocus::Explorer {
                theme::border_focus()
            } else {
                theme::border()
            })
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(Span::styled(
            " ▸ public",
            Style::default().fg(theme::BLUE),
        )));

        for (i, table) in self.tables.iter().enumerate() {
            let sel = i == self.selected_table;
            let style = if sel {
                Style::default()
                    .fg(theme::YELLOW)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::TEXT_SECONDARY)
            };
            let prefix = if sel { "   ▾ " } else { "   ▸ " };
            lines.push(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled("⊞ ", Style::default().fg(theme::PURPLE)),
                Span::styled(&table.name, style),
            ]));
            if sel {
                for (cn, ct) in &table.columns {
                    lines.push(Line::from(vec![
                        Span::raw("       "),
                        Span::styled(cn, Style::default().fg(theme::TEXT_MUTED)),
                        Span::raw("  "),
                        Span::styled(ct, Style::default().fg(theme::TEXT_DIM)),
                    ]));
                }
            }
        }

        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_conn_bar(&self, frame: &mut Frame, area: Rect) {
        let bar = Line::from(vec![
            Span::styled(" ● ", Style::default().fg(theme::GREEN)),
            Span::styled("SQL ENGINE: CONNECTED", Style::default().fg(theme::GREEN)),
            Span::styled("  │  ", Style::default().fg(theme::BORDER)),
            Span::styled("TABLES: ", Style::default().fg(theme::TEXT_DIM)),
            Span::styled(
                format!("{}", self.tables.len()),
                Style::default().fg(theme::TEXT),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }

    fn draw_editor(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focus == SqlFocus::Editor {
            theme::border_focus()
        } else {
            theme::border()
        };
        let block = Block::bordered()
            .title(Span::styled(
                " QUERY EDITOR [SQL] ",
                Style::default().fg(theme::TEXT_MUTED),
            ))
            .border_style(border_style)
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let sql_line = highlight_sql(&self.query_input);
        frame.render_widget(Paragraph::new(sql_line).wrap(Wrap { trim: false }), inner);
    }

    fn draw_results(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focus == SqlFocus::Results {
            theme::border_focus()
        } else {
            theme::border()
        };
        let count_text = format!("{} rows in {}", self.row_count, self.exec_time);
        let title = format!(" QUERY RESULTS ");
        let block = Block::bordered()
            .title(Span::styled(title, Style::default().fg(theme::TEXT_MUTED)))
            .title(
                block::Title::from(Span::styled(count_text, Style::default().fg(theme::GREEN)))
                    .alignment(Alignment::Right),
            )
            .border_style(border_style)
            .style(Style::default().bg(theme::TERMINAL_BG));

        if self.rows.is_empty() {
            let msg = self.error_msg.as_deref().unwrap_or("(0 行)");
            frame.render_widget(
                Paragraph::new(msg)
                    .style(Style::default().fg(theme::TEXT_DIM))
                    .block(block),
                area,
            );
            return;
        }

        let header_cells: Vec<Cell> = self
            .columns
            .iter()
            .map(|c| Cell::from(c.as_str()).style(Style::default().fg(theme::TEXT_MUTED).bold()))
            .collect();
        let header = Row::new(header_cells)
            .style(Style::default().bg(theme::SURFACE_DARK))
            .height(1);

        let data_rows: Vec<Row> = self
            .rows
            .iter()
            .map(|row| {
                let cells: Vec<Cell> = row
                    .iter()
                    .enumerate()
                    .map(|(_i, cell)| {
                        Cell::from(cell.as_str()).style(Style::default().fg(theme::TEXT))
                    })
                    .collect();
                Row::new(cells)
            })
            .collect();

        let col_count = self.columns.len().max(1);
        let widths: Vec<Constraint> = (0..col_count)
            .map(|_| Constraint::Ratio(1, col_count as u32))
            .collect();

        let table = Table::new(data_rows, widths)
            .header(header)
            .block(block)
            .highlight_style(Style::default().bg(theme::SURFACE_LIGHTER));

        frame.render_widget(table, area);
    }

    fn draw_key_bar(&self, frame: &mut Frame, area: Rect) {
        let bar = if self.focus == SqlFocus::Editor {
            Line::from(vec![
                Span::styled(" Ctrl+Enter", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Execute  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Ctrl+L", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Clear  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Esc", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("Tab", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Focus  ", Style::default().fg(theme::TEXT_SECONDARY)),
            ])
        } else {
            Line::from(vec![
                Span::styled(" i", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Edit  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("j/k", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Nav  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("g/G", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Top/Bot  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("r", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Refresh  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("1-9", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Pages  ", Style::default().fg(theme::TEXT_SECONDARY)),
                Span::styled("?", Style::default().fg(theme::BLUE).bold()),
                Span::styled(" Help", Style::default().fg(theme::TEXT_SECONDARY)),
            ])
        };
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }
}

/// 格式化 Talon Value JSON 为可读字符串。
fn format_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Object(map) => {
            if let Some(v) = map.get("Integer").and_then(|v| v.as_i64()) {
                v.to_string()
            } else if let Some(v) = map.get("Float").and_then(|v| v.as_f64()) {
                format!("{}", v)
            } else if let Some(v) = map.get("Text").and_then(|v| v.as_str()) {
                v.to_string()
            } else if map.contains_key("Null") {
                "NULL".to_string()
            } else {
                serde_json::to_string(v).unwrap_or_default()
            }
        }
        _ => v.to_string(),
    }
}

/// 简易 SQL 关键字高亮。
fn highlight_sql(sql: &str) -> Line<'static> {
    const KEYWORDS: &[&str] = &[
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "SET", "DELETE", "CREATE", "TABLE",
        "DROP", "ALTER", "JOIN", "LEFT", "RIGHT", "ON", "AND", "OR", "NOT", "IN", "LIKE", "LIMIT",
        "ORDER", "BY", "GROUP", "HAVING", "AS", "VALUES", "NULL", "SHOW", "TABLES", "DESC", "ASC",
        "INNER", "OUTER",
    ];
    let mut spans: Vec<Span<'static>> = Vec::new();
    for (i, part) in sql.split(' ').enumerate() {
        if i > 0 {
            spans.push(Span::raw(" "));
        }
        let upper = part.trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_');
        let suffix = &part[upper.len()..];
        if KEYWORDS.contains(&upper.to_uppercase().as_str()) {
            spans.push(Span::styled(
                upper.to_string(),
                Style::default()
                    .fg(theme::BLUE)
                    .add_modifier(Modifier::BOLD),
            ));
            if !suffix.is_empty() {
                spans.push(Span::styled(
                    suffix.to_string(),
                    Style::default().fg(theme::TEXT),
                ));
            }
        } else if part.starts_with('\'') {
            spans.push(Span::styled(
                part.to_string(),
                Style::default().fg(theme::GREEN),
            ));
        } else if part == "*" {
            spans.push(Span::styled(
                part.to_string(),
                Style::default().fg(theme::ACCENT),
            ));
        } else {
            spans.push(Span::styled(
                part.to_string(),
                Style::default().fg(theme::TEXT),
            ));
        }
    }
    Line::from(spans)
}
