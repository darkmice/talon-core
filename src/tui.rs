/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
//! Talon TUI — 全屏终端管理界面。
//!
//! 用法：
//!   talon-tui "talon://:token@host:port"
//!   talon-tui --url "talon://localhost:7720"
//!   TALON_URL="talon://localhost:7720" talon-tui

mod tui_app;

use std::io;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url_str = parse_args(&args);

    // 初始化终端
    if let Err(e) = enable_raw_mode() {
        eprintln!("终端 raw mode 启用失败: {}", e);
        std::process::exit(1);
    }
    let mut stdout = io::stdout();
    if let Err(e) = execute!(stdout, EnterAlternateScreen, EnableMouseCapture) {
        let _ = disable_raw_mode();
        eprintln!("终端初始化失败: {}", e);
        std::process::exit(1);
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = match Terminal::new(backend) {
        Ok(t) => t,
        Err(e) => {
            let _ = disable_raw_mode();
            eprintln!("Terminal 创建失败: {}", e);
            std::process::exit(1);
        }
    };

    // 运行应用
    let mut app = tui_app::App::new(url_str);
    let result = app.run(&mut terminal);

    // 恢复终端
    let _ = disable_raw_mode();
    let _ = execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    );
    let _ = terminal.show_cursor();

    if let Err(e) = result {
        eprintln!("错误: {}", e);
        std::process::exit(1);
    }
}

/// 解析命令行参数，返回连接 URL。
fn parse_args(args: &[String]) -> String {
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                i += 1;
                if i < args.len() {
                    return args[i].clone();
                }
            }
            "--help" | "-h" => {
                println!("Talon TUI — 全屏终端管理界面");
                println!();
                println!("用法:");
                println!("  talon-tui \"talon://:token@host:port\"");
                println!("  talon-tui --url \"talon://localhost:7720\"");
                println!("  TALON_URL=\"talon://localhost:7720\" talon-tui");
                println!();
                println!("快捷键（跨平台，兼容 macOS）:");
                println!();
                println!("  导航模式 (NAV):");
                println!("    1-9      切换引擎页面 (KV/SQL/Vec/FTS/TS/MQ/GEO/Graph/AI)");
                println!("    [ / ]    上/下一页");
                println!("    j/k ↑/↓  列表导航");
                println!("    g / G    跳到列表顶部/底部");
                println!("    Tab      切换面板焦点");
                println!("    i        进入输入模式 (vim 风格)");
                println!("    r        刷新当前页面");
                println!("    q        退出");
                println!("    ?        帮助弹窗");
                println!();
                println!("  输入模式 (INPUT):");
                println!("    Esc      退出输入 → 回到导航");
                println!("    Ctrl+Enter  执行 SQL");
                println!("    Enter    提交/搜索/发送");
                println!("    Ctrl+L   清空输入 (SQL)");
                println!();
                println!("  全局:");
                println!("    Ctrl+C   强制退出（任何模式）");
                println!("    Ctrl+R   手动重连");
                println!("    F1-F9    备选切页（兼容传统终端）");
                std::process::exit(0);
            }
            s if s.starts_with("talon://") => return s.to_string(),
            _ => {}
        }
        i += 1;
    }
    if let Ok(url) = std::env::var("TALON_URL") {
        return url;
    }
    "talon://localhost:7720".to_string()
}
