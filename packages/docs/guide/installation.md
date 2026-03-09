# Installation

## System Requirements

- **OS**: Linux, macOS, Windows
- **Architecture**: x86_64, aarch64 (Apple Silicon)

## As a Rust Library (Embedded Mode)

Add to your `Cargo.toml`:

```toml
[dependencies]
talon = { git = "https://github.com/darkmice/talon-bin.git", tag = "v0.1.10", package = "talon-sys" }
```

## Binary Size

| Build | Size |
|-------|------|
| Release | ~15 MB |
| Release + LTO | ~12 MB |
| Release + strip | ~10 MB |

## Configuration

### Storage Configuration

```rust
use talon::{StorageConfig, Talon};

let config = StorageConfig {
    cache_size_mb: 256,      // Block cache size
    ..Default::default()
};
let db = Talon::open_with_config("./data", config)?;
```

### Cluster Configuration

```rust
use talon::{ClusterConfig, ClusterRole, Talon, StorageConfig};

let cluster = ClusterConfig {
    role: ClusterRole::Primary,
    ..Default::default()
};
let db = Talon::open_with_cluster("./data", StorageConfig::default(), cluster)?;
```
