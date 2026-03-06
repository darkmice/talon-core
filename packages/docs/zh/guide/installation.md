# 安装

## 系统要求

- **操作系统** Linux / macOS / Windows
- **内存** 建议 ≥ 512 MB
- **磁盘** SSD 推荐（LSM-Tree 写入密集型）

## 作为 Rust 依赖使用

```toml
[dependencies]
talon = { git = "https://github.com/darkmice/talon-bin.git", tag = "v0.1.8", package = "talon-sys" }
```

### 功能开关（Feature Flags）

```toml
[dependencies]
talon = { git = "https://github.com/darkmice/talon-bin.git", tag = "v0.1.8", package = "talon-sys", features = ["server"] }
```

| Feature | 说明 |
|---------|------|
| `default` | 嵌入式模式，包含所有 9 引擎 |
| `server` | 启用 HTTP + TCP + Redis 服务端 |
| `cluster` | 启用 Primary-Replica 集群 |

## 配置

```rust
use talon::{Talon, StorageConfig};

let config = StorageConfig {
    cache_size: 256 * 1024 * 1024,  // 256 MB 块缓存
    ..Default::default()
};
let db = Talon::open_with_config("./data", config)?;
```

### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `cache_size` | `64 MB` | LSM-Tree 块缓存大小 |
| `max_write_buffer_size` | `64 MB` | 写入缓冲区上限 |
| `compaction_style` | `Leveled` | 压缩策略 |

## 验证安装

```rust
use talon::Talon;

let db = Talon::open("./test-data")?;
db.run_sql("SELECT 1 + 1")?;
println!("Talon 安装成功!");
```
