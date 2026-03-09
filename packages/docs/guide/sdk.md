# SDK Overview

Talon provides official SDKs for **5 languages**, all achieving 100% API coverage across 10 engine modules.

Every SDK communicates with the Talon engine through a unified `talon_execute` C ABI — the same JSON command protocol, consistent behavior across all languages.

## Supported Languages

| Language | Install | Binding | Native Lib |
|----------|---------|---------|------------|
| [Go](/guide/sdk-go) | `go get` | cgo (static link) | Compiled into binary |
| [Python](/guide/sdk-python) | `pip install talon-db` | ctypes | Auto-download on first use |
| [Node.js](/guide/sdk-nodejs) | `npm install talon-db` | koffi | Auto-download on install |
| [Java](/guide/sdk-java) | Source build | JNA | From `lib/` directory |
| [.NET](/guide/sdk-dotnet) | `dotnet add package TalonDb` | P/Invoke | Included in NuGet package |

## Module Coverage

All SDKs cover the complete set of 10 engine modules:

| Module | Methods | Description |
|--------|---------|-------------|
| SQL | 1 | Relational queries |
| KV | 17 | Key-value with TTL, atomic ops, pagination |
| Vector | 7 | HNSW vector index & search |
| TS | 7 | Time-series engine |
| MQ | 9 | Message queue |
| AI | 30+ | Session / Context / Memory / RAG / Agent / Trace / LLM Config / Auto-Embed |
| FTS | 16 | Full-text search (BM25 + fuzzy + hybrid) |
| Geo | 10 | Geospatial (radius / box / fence) |
| Graph | 19 | Property graph (CRUD + BFS + shortest path + PageRank) |
| Cluster + Ops | 10 | Cluster management / stats / backup |

## Pre-compiled Libraries

The `lib/` directory in `talon-sdk` contains pre-compiled libraries for 4 platforms, automatically built and pushed by CI:

| OS | Arch | Static | Dynamic |
|----|------|--------|--------|
| macOS | arm64 (Apple Silicon) | `.a` ✅ | `.dylib` ✅ |
| macOS | amd64 (Intel) | `.a` ✅ | `.dylib` ✅ |
| Linux | amd64 (海光/兆芯) | `.a` ✅ | `.so` ✅ |
| Linux | arm64 (鲲鹏/飞腾) | `.a` ✅ | `.so` ✅ |
| Windows | amd64 | `.lib` ✅ | `.dll` ✅ |
| Windows | arm64 | `.lib` ✅ | `.dll` ✅ |
| Linux | loongarch64 (龙芯) | `.a` ✅ | `.so` ✅ |
| Linux | riscv64 | `.a` ✅ | `.so` ✅ |

### Library Lookup Priority

1. `TALON_LIB_PATH` environment variable (direct file path)
2. `talon-sdk/lib/{platform}/` bundled library (auto-detect platform)
3. System search paths

## Architecture

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Go SDK     │  │ Python SDK   │  │ Node.js SDK  │
│   (cgo)      │  │  (ctypes)    │  │  (koffi)     │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       ▼                 ▼                 ▼
  ┌─────────────────────────────────────────────┐
  │          talon_execute(db, json_cmd)         │
  │              C ABI (FFI Layer)               │
  └──────────────────┬──────────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────────┐
  │              Talon Engine (Rust)             │
  │  SQL │ KV │ TS │ MQ │ Vec │ FTS │ Geo │ Graph │
  └─────────────────────────────────────────────┘
```
