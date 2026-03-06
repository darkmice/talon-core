/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! TUI 应用核心 — 状态机 + 事件循环 + 全局布局。
//!
//! 生产级：自动重连、8 引擎 + AI 共 9 页全接入。

pub mod net;
pub mod pages;
pub mod theme;

use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::*;

use pages::Page;

/// 自动重连间隔（秒）。
const RECONNECT_INTERVAL_SECS: u64 = 10;

/// TUI 应用主结构。
pub struct App {
    url: String,
    client: Option<net::TuiClient>,
    current_page: Page,
    should_quit: bool,
    connected: bool,
    status_msg: String,
    last_reconnect: Instant,
    tick_count: u64,
    show_help: bool,
    // 8 引擎 + AI 页面状态
    dashboard: pages::dashboard::DashboardState,
    sql_editor: pages::sql_editor::SqlEditorState,
    vector_search: pages::vector_search::VectorSearchState,
    fts: pages::fts::FtsState,
    ts: pages::ts::TsState,
    mq: pages::mq::MqState,
    geo: pages::geo::GeoState,
    graph: pages::graph::GraphState,
    ai_chat: pages::ai_chat::AiChatState,
}

impl App {
    /// 创建新的 TUI 应用实例。
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: None,
            current_page: Page::Dashboard,
            should_quit: false,
            connected: false,
            status_msg: "正在连接...".into(),
            last_reconnect: Instant::now(),
            tick_count: 0,
            show_help: false,
            dashboard: pages::dashboard::DashboardState::new(),
            sql_editor: pages::sql_editor::SqlEditorState::new(),
            vector_search: pages::vector_search::VectorSearchState::new(),
            fts: pages::fts::FtsState::new(),
            ts: pages::ts::TsState::new(),
            mq: pages::mq::MqState::new(),
            geo: pages::geo::GeoState::new(),
            graph: pages::graph::GraphState::new(),
            ai_chat: pages::ai_chat::AiChatState::new(),
        }
    }

    /// 运行主事件循环。
    pub fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.try_connect();
        let tick_rate = Duration::from_millis(200);
        let mut last_tick = Instant::now();
        loop {
            terminal.draw(|frame| self.draw(frame))?;
            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_default();
            if event::poll(timeout)? {
                match event::read()? {
                    Event::Key(key) => self.handle_key(key),
                    _ => {}
                }
            }
            if last_tick.elapsed() >= tick_rate {
                self.on_tick();
                last_tick = Instant::now();
            }
            if self.should_quit {
                break;
            }
        }
        Ok(())
    }

    fn try_connect(&mut self) {
        match net::TuiClient::connect(&self.url) {
            Ok(client) => {
                let addr = client.addr.clone();
                self.client = Some(client);
                self.connected = true;
                self.status_msg = format!("已连接 {}", addr);
                // 连接成功后刷新所有页面数据
                self.dashboard.refresh(&mut self.client);
                self.sql_editor.refresh(&mut self.client);
                self.vector_search.refresh(&mut self.client);
                self.fts.refresh(&mut self.client);
                self.ts.refresh(&mut self.client);
                self.mq.refresh(&mut self.client);
                self.geo.refresh(&mut self.client);
                self.graph.refresh(&mut self.client);
                self.ai_chat.refresh(&mut self.client);
            }
            Err(e) => {
                self.connected = false;
                self.status_msg = format!("离线 ({})", e);
            }
        }
        self.last_reconnect = Instant::now();
    }

    /// 当前页面是否正在文本输入（输入模式）。
    fn is_input_active(&self) -> bool {
        match self.current_page {
            Page::Dashboard => self.dashboard.is_input_active(),
            Page::SqlEditor => self.sql_editor.is_input_active(),
            Page::VectorSearch => self.vector_search.is_input_active(),
            Page::Fts => self.fts.is_input_active(),
            Page::TimeSeries => self.ts.is_input_active(),
            Page::MessageQueue => self.mq.is_input_active(),
            Page::Geo => self.geo.is_input_active(),
            Page::Graph => self.graph.is_input_active(),
            Page::AiChat => self.ai_chat.is_input_active(),
        }
    }

    fn handle_key(&mut self, key: event::KeyEvent) {
        // ── 0. 帮助弹窗打开时，任意键关闭 ──
        if self.show_help {
            self.show_help = false;
            return;
        }

        // ── 1. 全局快捷键（任何模式下均生效） ──
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                self.should_quit = true;
                return;
            }
            (KeyModifiers::CONTROL, KeyCode::Char('r')) => {
                self.try_connect();
                return;
            }
            // F1-F9 保留兼容（跨平台备选）
            (_, KeyCode::F(n @ 1..=9)) => {
                if let Some(p) = Page::from_index(n as usize) {
                    self.current_page = p;
                }
                return;
            }
            _ => {}
        }

        // ── 2. 导航模式快捷键（仅在非文本输入时生效） ──
        //    Tab 始终送给页面（切换面板焦点），不在此拦截。
        if !self.is_input_active() {
            match key.code {
                // q — 退出（htop/lazygit/btop 标准）
                KeyCode::Char('q') => {
                    self.should_quit = true;
                    return;
                }
                // ? — 帮助弹窗（lazygit/htop 标准）
                KeyCode::Char('?') => {
                    self.show_help = true;
                    return;
                }
                // i — 进入输入模式（vim 标准）
                KeyCode::Char('i') => {
                    self.enter_input_mode();
                    return;
                }
                // 数字键 1-9 — 直跳页面（htop/lazygit 标准）
                KeyCode::Char(c @ '1'..='9') => {
                    let idx = (c as usize) - ('0' as usize);
                    if let Some(p) = Page::from_index(idx) {
                        self.current_page = p;
                    }
                    return;
                }
                // ] — 下一页（lazygit 风格）
                KeyCode::Char(']') => {
                    self.current_page = self.current_page.next();
                    return;
                }
                // [ — 上一页
                KeyCode::Char('[') => {
                    self.current_page = self.current_page.prev();
                    return;
                }
                // r — 刷新当前页面数据（htop/k9s 标准）
                KeyCode::Char('r') => {
                    self.refresh_current_page();
                    return;
                }
                // g — 跳到列表顶部（vim/lazygit 标准）
                KeyCode::Char('g') => {
                    self.jump_top();
                    return;
                }
                // G — 跳到列表底部（vim/lazygit 标准）
                KeyCode::Char('G') => {
                    self.jump_bottom();
                    return;
                }
                _ => {}
            }
        }

        // ── 3. 页面级键盘事件 ──
        match self.current_page {
            Page::Dashboard => self.dashboard.handle_key(key, &mut self.client),
            Page::SqlEditor => self.sql_editor.handle_key(key, &mut self.client),
            Page::VectorSearch => self.vector_search.handle_key(key, &mut self.client),
            Page::Fts => self.fts.handle_key(key, &mut self.client),
            Page::TimeSeries => self.ts.handle_key(key, &mut self.client),
            Page::MessageQueue => self.mq.handle_key(key, &mut self.client),
            Page::Geo => self.geo.handle_key(key, &mut self.client),
            Page::Graph => self.graph.handle_key(key, &mut self.client),
            Page::AiChat => self.ai_chat.handle_key(key, &mut self.client),
        }
    }

    /// 进入当前页面的输入模式。
    fn enter_input_mode(&mut self) {
        match self.current_page {
            Page::Dashboard => self.dashboard.enter_input_mode(),
            Page::SqlEditor => self.sql_editor.enter_input_mode(),
            Page::VectorSearch => self.vector_search.enter_input_mode(),
            Page::Fts => self.fts.enter_input_mode(),
            Page::TimeSeries => self.ts.enter_input_mode(),
            Page::MessageQueue => self.mq.enter_input_mode(),
            Page::Geo => self.geo.enter_input_mode(),
            Page::Graph => self.graph.enter_input_mode(),
            Page::AiChat => self.ai_chat.enter_input_mode(),
        }
    }

    /// 跳到当前页面列表顶部。
    fn jump_top(&mut self) {
        match self.current_page {
            Page::Dashboard => self.dashboard.kv_selected = 0,
            Page::SqlEditor => self.sql_editor.selected_table = 0,
            Page::VectorSearch => self.vector_search.selected_col = 0,
            Page::Fts => self.fts.selected_idx = 0,
            Page::TimeSeries => self.ts.selected = 0,
            Page::MessageQueue => self.mq.selected = 0,
            Page::Geo => self.geo.selected = 0,
            Page::Graph => self.graph.selected_node = 0,
            Page::AiChat => self.ai_chat.selected_session = 0,
        }
    }

    /// 跳到当前页面列表底部。
    fn jump_bottom(&mut self) {
        match self.current_page {
            Page::Dashboard => {
                self.dashboard.kv_selected = self.dashboard.kv_keys.len().saturating_sub(1);
            }
            Page::SqlEditor => {
                self.sql_editor.selected_table = self.sql_editor.tables.len().saturating_sub(1);
            }
            Page::VectorSearch => {
                self.vector_search.selected_col =
                    self.vector_search.collections.len().saturating_sub(1);
            }
            Page::Fts => {
                self.fts.selected_idx = self.fts.indices.len().saturating_sub(1);
            }
            Page::TimeSeries => {
                self.ts.selected = self.ts.metrics.len().saturating_sub(1);
            }
            Page::MessageQueue => {
                self.mq.selected = self.mq.topics.len().saturating_sub(1);
            }
            Page::Geo => {
                self.geo.selected = self.geo.points.len().saturating_sub(1);
            }
            Page::Graph => {
                self.graph.selected_node = self.graph.nodes.len().saturating_sub(1);
            }
            Page::AiChat => {
                self.ai_chat.selected_session = self.ai_chat.sessions.len().saturating_sub(1);
            }
        }
    }

    /// 刷新当前页面数据。
    fn refresh_current_page(&mut self) {
        match self.current_page {
            Page::Dashboard => self.dashboard.refresh(&mut self.client),
            Page::SqlEditor => self.sql_editor.refresh(&mut self.client),
            Page::VectorSearch => self.vector_search.refresh(&mut self.client),
            Page::Fts => self.fts.refresh(&mut self.client),
            Page::TimeSeries => self.ts.refresh(&mut self.client),
            Page::MessageQueue => self.mq.refresh(&mut self.client),
            Page::Geo => self.geo.refresh(&mut self.client),
            Page::Graph => self.graph.refresh(&mut self.client),
            Page::AiChat => self.ai_chat.refresh(&mut self.client),
        }
        if self.connected {
            self.status_msg = format!("已刷新 {}", self.current_page.label());
        }
    }

    fn on_tick(&mut self) {
        self.tick_count += 1;
        if !self.connected
            && self.last_reconnect.elapsed() > Duration::from_secs(RECONNECT_INTERVAL_SECS)
        {
            self.try_connect();
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(area);

        self.draw_title_bar(frame, layout[0]);
        match self.current_page {
            Page::Dashboard => self.dashboard.draw(frame, layout[1]),
            Page::SqlEditor => self.sql_editor.draw(frame, layout[1]),
            Page::VectorSearch => self.vector_search.draw(frame, layout[1]),
            Page::Fts => self.fts.draw(frame, layout[1]),
            Page::TimeSeries => self.ts.draw(frame, layout[1]),
            Page::MessageQueue => self.mq.draw(frame, layout[1]),
            Page::Geo => self.geo.draw(frame, layout[1]),
            Page::Graph => self.graph.draw(frame, layout[1]),
            Page::AiChat => self.ai_chat.draw(frame, layout[1]),
        }
        self.draw_status_bar(frame, layout[2]);

        // 帮助弹窗覆盖层
        if self.show_help {
            self.draw_help_popup(frame, area);
        }
    }

    fn draw_help_popup(&self, frame: &mut Frame, area: Rect) {
        let w = 52u16.min(area.width.saturating_sub(4));
        let h = 22u16.min(area.height.saturating_sub(4));
        let x = area.x + (area.width.saturating_sub(w)) / 2;
        let y = area.y + (area.height.saturating_sub(h)) / 2;
        let popup = Rect::new(x, y, w, h);

        frame.render_widget(Clear, popup);
        let block = Block::bordered()
            .title(Span::styled(
                " KEYBINDINGS ",
                Style::default().fg(theme::ACCENT).bold(),
            ))
            .title(
                block::Title::from(Span::styled(
                    " press any key to close ",
                    Style::default().fg(theme::TEXT_DIM),
                ))
                .alignment(Alignment::Right),
            )
            .border_style(Style::default().fg(theme::PRIMARY))
            .style(Style::default().bg(theme::TERMINAL_BG));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);

        let kc = theme::BLUE;
        let dc = theme::TEXT_SECONDARY;
        let hc = theme::YELLOW;
        let lines = vec![
            Line::from(Span::styled(
                " NAVIGATION MODE",
                Style::default().fg(hc).bold(),
            )),
            Line::from(vec![
                Span::styled("   1-9       ", Style::default().fg(kc).bold()),
                Span::styled("switch to page 1-9", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   [ / ]     ", Style::default().fg(kc).bold()),
                Span::styled("prev / next page", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   j/k ↑/↓   ", Style::default().fg(kc).bold()),
                Span::styled("navigate list", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   g / G     ", Style::default().fg(kc).bold()),
                Span::styled("jump to top / bottom", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   Tab       ", Style::default().fg(kc).bold()),
                Span::styled("cycle panel focus", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   i         ", Style::default().fg(kc).bold()),
                Span::styled("enter input mode (vim)", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   r         ", Style::default().fg(kc).bold()),
                Span::styled("refresh current page", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   q         ", Style::default().fg(kc).bold()),
                Span::styled("quit", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   ?         ", Style::default().fg(kc).bold()),
                Span::styled("this help", Style::default().fg(dc)),
            ]),
            Line::from(""),
            Line::from(Span::styled(" INPUT MODE", Style::default().fg(hc).bold())),
            Line::from(vec![
                Span::styled("   Esc       ", Style::default().fg(kc).bold()),
                Span::styled("back to navigation", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   Ctrl+Enter", Style::default().fg(kc).bold()),
                Span::styled("execute (SQL editor)", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   Enter     ", Style::default().fg(kc).bold()),
                Span::styled("submit / search / send", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   Ctrl+L    ", Style::default().fg(kc).bold()),
                Span::styled("clear input (SQL editor)", Style::default().fg(dc)),
            ]),
            Line::from(""),
            Line::from(Span::styled(" GLOBAL", Style::default().fg(hc).bold())),
            Line::from(vec![
                Span::styled("   Ctrl+C    ", Style::default().fg(kc).bold()),
                Span::styled("force quit (any mode)", Style::default().fg(dc)),
            ]),
            Line::from(vec![
                Span::styled("   Ctrl+R    ", Style::default().fg(kc).bold()),
                Span::styled("reconnect to server", Style::default().fg(dc)),
            ]),
        ];
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_title_bar(&self, frame: &mut Frame, area: Rect) {
        let bar_bg = Block::default().style(theme::title_bar());
        frame.render_widget(bar_bg, area);

        let mut spans = vec![
            Span::styled(" ✦ ", Style::default().fg(theme::PRIMARY)),
            Span::styled("Talon", Style::default().fg(Color::White).bold()),
            Span::raw(" "),
            Span::styled(
                format!(" {} ", self.current_page.label()),
                Style::default().fg(Color::White).bg(theme::PRIMARY),
            ),
            Span::raw(" "),
        ];
        for p in Page::all() {
            let active = *p == self.current_page;
            spans.push(Span::styled(
                p.hotkey(),
                Style::default().fg(if active { theme::ACCENT } else { theme::GRAY }),
            ));
            spans.push(Span::styled(
                format!("{} ", p.short_name()),
                Style::default().fg(if active { Color::White } else { theme::GRAY }),
            ));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), area);

        // 在线状态
        let (color, label) = if self.connected {
            (theme::GREEN, "● ONLINE ")
        } else {
            let blink = self.tick_count % 4 < 2;
            if blink {
                (theme::RED, "● OFFLINE")
            } else {
                (theme::RED, "  OFFLINE")
            }
        };
        let rw: u16 = 10;
        if area.width > rw + 2 {
            let ra = Rect::new(area.x + area.width - rw - 1, area.y, rw, 1);
            frame.render_widget(
                Paragraph::new(Span::styled(label, Style::default().fg(color)))
                    .alignment(Alignment::Right),
                ra,
            );
        }
    }

    fn draw_status_bar(&self, frame: &mut Frame, area: Rect) {
        let input_mode = self.is_input_active();
        let mut spans = vec![Span::styled(" ➜ ", Style::default().fg(theme::GREEN))];
        if input_mode {
            spans.push(Span::styled(
                "INPUT ",
                Style::default().fg(theme::YELLOW).bold(),
            ));
            spans.push(Span::styled("Esc", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " back  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled(
                "Ctrl+C",
                Style::default().fg(theme::BLUE).bold(),
            ));
            spans.push(Span::styled(
                " quit  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
        } else {
            spans.push(Span::styled(
                "NAV ",
                Style::default().fg(theme::GREEN).bold(),
            ));
            spans.push(Span::styled("1-9", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " page  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled("j/k", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(" nav  ", Style::default().fg(theme::TEXT_DIM)));
            spans.push(Span::styled("g/G", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " top/bot  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled("i", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " input  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled("r", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " refresh  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled("q", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " quit  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
            spans.push(Span::styled("?", Style::default().fg(theme::BLUE).bold()));
            spans.push(Span::styled(
                " help  ",
                Style::default().fg(theme::TEXT_DIM),
            ));
        }
        spans.push(Span::styled("│ ", Style::default().fg(theme::BORDER)));
        spans.push(Span::styled(
            &self.status_msg,
            Style::default().fg(theme::TEXT_MUTED),
        ));
        frame.render_widget(
            Paragraph::new(Line::from(spans)).style(Style::default().bg(theme::SURFACE_DARK)),
            area,
        );
    }
}
