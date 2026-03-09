# AI Engine

Native Session, Context, Memory, RAG, Agent, Trace, Intent, Embedding Cache, LLM Provider, Auto-Embedding, Auto-Summarize, and Token Count abstractions for LLM applications.

## Overview

The AI Engine is Talon's 9th engine — a first-class semantic abstraction layer purpose-built for LLM application development. It eliminates the need for external frameworks (LangChain, LlamaIndex) by providing native primitives for session management, conversation context, semantic memory, RAG document management, agent orchestration, execution tracing, intent recognition, embedding caching, LLM provider configuration, automatic embedding generation, smart context compression, and precise token counting.

::: tip Enterprise Feature
The AI Engine is provided by `talon-ai`, a commercially licensed extension distributed as pre-compiled libraries. SDK users get AI features built into the `talon-bin` binary — no separate installation required.
:::

## Quick Start

```rust
use talon::Talon;
use talon_ai::TalonAiExt;

let db = Talon::open("./data")?;
let ai = db.ai()?;

// Create a session
ai.create_session("chat-001", BTreeMap::new(), None)?;

// Append messages
ai.append_message("chat-001", &ContextMessage {
    role: "user".into(),
    content: "What is Talon?".into(),
    token_count: Some(5),
    ..Default::default()
})?;

// Store semantic memory
ai.store_memory(&entry, &embedding)?;

// Search memories
let memories = ai.search_memory(&query_embedding, 5)?;
```

## API Reference

### Session Management

#### `create_session`
```rust
pub fn create_session(&self, id: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<Session, Error>
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `&str` | Unique session identifier |
| `metadata` | `BTreeMap<String, String>` | Custom key-value metadata |
| `ttl_secs` | `Option<u64>` | Auto-expire after N seconds |

#### `create_session_if_not_exists`
```rust
pub fn create_session_if_not_exists(&self, id: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<(Session, bool), Error>
```
Idempotent session creation — returns existing session if already present, otherwise creates a new one. Returns `(session, is_new)` where `is_new=true` indicates a newly created session. Ideal for concurrent scenarios like group chats where multiple requests may simultaneously detect a missing session.

#### `get_session`
```rust
pub fn get_session(&self, id: &str) -> Result<Option<Session>, Error>
```
Lazy expiration: expired sessions return `None`.

#### `list_sessions`
```rust
pub fn list_sessions(&self) -> Result<Vec<Session>, Error>
```
Lists all active sessions (excludes archived and expired).

#### `delete_session`
```rust
pub fn delete_session(&self, id: &str) -> Result<(), Error>
```
Cascade delete: removes session + all context messages + traces.

#### `update_session`
```rust
pub fn update_session(&self, id: &str, metadata: BTreeMap<String, String>) -> Result<Session, Error>
```
Merge new metadata into existing session. Existing keys are overwritten, new keys are added.

#### Tags
```rust
pub fn add_session_tags(&self, id: &str, tags: &[String]) -> Result<(), Error>
pub fn remove_session_tags(&self, id: &str, tags: &[String]) -> Result<(), Error>
pub fn get_session_tags(&self, session_id: &str) -> Result<Vec<String>, Error>
pub fn search_sessions_by_tag(&self, tag: &str) -> Result<Vec<Session>, Error>
```

#### Archive
```rust
pub fn archive_session(&self, id: &str) -> Result<(), Error>
pub fn unarchive_session(&self, id: &str) -> Result<(), Error>
pub fn list_archived_sessions(&self) -> Result<Vec<Session>, Error>
pub fn search_sessions_by_metadata(&self, key: &str, value: &str) -> Result<Vec<Session>, Error>
```

#### Export & Stats
```rust
pub fn export_session(&self, session_id: &str) -> Result<ExportedSession, Error>
pub fn export_sessions(&self, session_ids: &[&str]) -> Result<Vec<ExportedSession>, Error>
pub fn session_stats(&self, session_id: &str) -> Result<SessionStats, Error>
pub fn sessions_stats(&self, session_ids: &[&str]) -> Result<Vec<SessionStats>, Error>
```

#### `cleanup_expired_sessions`
```rust
pub fn cleanup_expired_sessions(&self) -> Result<usize, Error>
```
Batch purge all expired sessions (cascade delete context + trace). Returns count deleted.

### Conversation Context

#### `append_message`
```rust
pub fn append_message(&self, session_id: &str, msg: &ContextMessage) -> Result<(), Error>
```

```rust
pub struct ContextMessage {
    pub role: String,        // "user" | "assistant" | "system"
    pub content: String,     // Message text
    pub timestamp: i64,      // Auto-set if 0
    pub token_count: Option<u32>, // Token count for window management
}
```

#### `get_history`
```rust
pub fn get_history(&self, session_id: &str, limit: Option<usize>) -> Result<Vec<ContextMessage>, Error>
```
Get conversation history in chronological order.

#### `get_recent_messages`
```rust
pub fn get_recent_messages(&self, session_id: &str, n: usize) -> Result<Vec<ContextMessage>, Error>
```
Get the most recent N messages in chronological order.

#### `get_context_window`
```rust
pub fn get_context_window(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
```
Get messages fitting within a token budget (auto-truncation for LLM context length).

#### `get_context_window_with_prompt`
```rust
pub fn get_context_window_with_prompt(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
```
Build complete LLM input context: `system_prompt` + `summary` + recent messages. Token budget allocation:
1. Deduct system_prompt tokens (if set)
2. Deduct context_summary tokens (if set)
3. Fill remaining budget with most recent messages

#### `get_context_window_smart`
```rust
pub fn get_context_window_smart(&self, session_id: &str, max_tokens: u32) -> Result<Vec<ContextMessage>, Error>
```
Smart context window with automatic summarization:
- If a summary exists or conversation fits: returns directly (like `get_context_window_with_prompt`)
- If total tokens > `max_tokens × 2` and Chat Provider is configured: auto-summarizes old messages, then returns
- If no Chat Provider configured: falls back to simple truncation

#### System Prompt & Summary
```rust
pub fn set_system_prompt(&self, session_id: &str, prompt: &str, token_count: u32) -> Result<(), Error>
pub fn get_system_prompt(&self, session_id: &str) -> Result<Option<String>, Error>
pub fn set_context_summary(&self, session_id: &str, summary: &str, token_count: u32) -> Result<(), Error>
pub fn get_context_summary(&self, session_id: &str) -> Result<Option<String>, Error>
```

#### `compact_context`
```rust
pub fn compact_context(&self, session_id: &str, keep_recent_n: usize) -> Result<u64, Error>
```
Compress context: keep only the most recent N messages, delete the rest. Returns count deleted. Typical usage with `set_context_summary`:
```rust
// 1. Persist summary first (crash-safe: summary is saved even if step 2 fails)
ai.set_context_summary(sid, &summary_text, summary_tokens)?;
// 2. Then compact messages
ai.compact_context(sid, 6)?;
```

#### `clear_context`
```rust
pub fn clear_context(&self, session_id: &str) -> Result<u64, Error>
```
Clear all messages while preserving the session. Returns count deleted.

### LLM Provider Configuration

Configure external LLM providers for auto-summarize and auto-embed features.
Chat and Embedding providers can be configured independently (e.g., Chat via DeepSeek, Embedding via local Ollama).

#### `configure_llm`
```rust
pub fn configure_llm(&self, config: AiLlmConfig) -> Result<(), Error>
```

```rust
pub struct AiLlmConfig {
    pub chat: Option<LlmEndpoint>,     // For auto_summarize, etc.
    pub embed: Option<EmbedEndpoint>,   // For auto_store_memory, auto_search_memory, etc.
}

pub struct LlmEndpoint {
    pub base_url: String,      // e.g. "https://api.openai.com/v1", "http://localhost:11434/v1"
    pub api_key: Option<String>,
    pub model: String,         // e.g. "gpt-4o-mini", "deepseek-chat", "qwen-turbo"
    pub max_retries: u8,       // Default: 2
    pub timeout_secs: u32,     // Default: 60
}

pub struct EmbedEndpoint {
    pub base_url: String,      // e.g. "https://api.openai.com/v1", "https://api.jina.ai/v1"
    pub api_key: Option<String>,
    pub model: String,         // e.g. "text-embedding-3-small", "bge-m3"
    pub dimensions: u32,       // Required: embedding dimension
    pub timeout_secs: u32,     // Default: 30
}
```

All providers use the OpenAI-compatible API format, supporting OpenAI, DeepSeek, Ollama, Tongyi Qianwen, Jina, and any OpenAI-compatible endpoint.

#### `get_llm_config`
```rust
pub fn get_llm_config(&self) -> Result<Option<AiLlmConfig>, Error>
```

#### `clear_llm_config`
```rust
pub fn clear_llm_config(&self) -> Result<(), Error>
```
Clear LLM configuration (revert to manual mode).

### Auto-Embedding

Automatically generate embeddings via configured Embed Provider — no need to manage vector computation manually.

#### `auto_store_memory`
```rust
pub fn auto_store_memory(&self, content: &str, metadata: BTreeMap<String, String>, ttl_secs: Option<u64>) -> Result<u64, Error>
```
Store memory with auto-generated embedding. Requires configured Embed Provider.

#### `auto_search_memory`
```rust
pub fn auto_search_memory(&self, query: &str, k: usize) -> Result<Vec<MemorySearchResult>, Error>
```
Semantic memory search with auto-generated query embedding. Requires configured Embed Provider.

### Auto-Summarize

Automatically generate context summaries using the configured Chat Provider.

#### `auto_summarize`
```rust
pub fn auto_summarize(&self, session_id: &str, opts: SummarizeOptions) -> Result<String, Error>
```

```rust
pub struct SummarizeOptions {
    pub max_summary_tokens: u32,     // Max tokens for summary (default: 200)
    pub purge_old: bool,             // Delete old messages after summarizing (default: false)
    pub custom_prompt: Option<String>, // Custom summarization prompt (None = built-in template)
}
```

Workflow:
1. Retrieve all history messages for the session
2. Concatenate into conversation text, call LLM to generate summary
3. Compute precise token count using tiktoken BPE
4. Store summary via `set_context_summary`
5. If `purge_old=true`, clear all existing messages

### Token Count

Precise BPE token counting using tiktoken — results are 100% consistent with OpenAI's tokenizer. No network required; vocabulary data is embedded at compile time.

#### `count_tokens`
```rust
pub fn count_tokens(text: &str, encoding: TokenEncoding) -> Result<u32, Error>
pub fn count_tokens_default(text: &str) -> Result<u32, Error>  // Uses o200k_base
pub fn count_tokens_batch(texts: &[&str], encoding: TokenEncoding) -> Result<Vec<u32>, Error>
```

```rust
pub enum TokenEncoding {
    Cl100kBase,  // GPT-4 / GPT-3.5-turbo / text-embedding-3-* series
    O200kBase,   // GPT-4o / o1 / o3 series (default)
}
```

### Semantic Memory

#### `store_memory`
```rust
pub fn store_memory(&self, entry: &MemoryEntry, embedding: &[f32]) -> Result<(), Error>
```
Store a vectorized long-term memory.

```rust
pub struct MemoryEntry {
    pub id: u64,
    pub content: String,
    pub metadata: BTreeMap<String, String>,
    pub created_at: i64,
    pub expires_at: Option<i64>,  // TTL-based expiration
}
```

#### `search_memory`
```rust
pub fn search_memory(&self, query_embedding: &[f32], k: usize) -> Result<Vec<MemorySearchResult>, Error>
```
Semantic similarity search over memories. Automatically skips expired entries (lazy expiration).

#### `update_memory`
```rust
pub fn update_memory(&self, id: u64, content: Option<&str>, metadata: Option<BTreeMap<String, String>>) -> Result<(), Error>
```
Update text content and/or metadata without changing the vector. To update the vector, delete and re-store.

#### `delete_memory`
```rust
pub fn delete_memory(&self, id: u64) -> Result<(), Error>
```

#### `memory_count`
```rust
pub fn memory_count(&self) -> Result<u64, Error>
```

#### Advanced Memory Operations
```rust
pub fn search_memory_with_filter(&self, query_embedding: &[f32], k: usize, filter: impl Fn(&MemoryEntry) -> bool) -> Result<Vec<MemoryEntry>, Error>
pub fn list_memories(&self, offset: usize, limit: usize) -> Result<Vec<MemoryEntry>, Error>
pub fn store_memory_with_ttl(&self, entry: &MemoryEntry, embedding: &[f32], ttl_secs: u64) -> Result<(), Error>
pub fn store_memories_batch(&self, entries: &[(&MemoryEntry, &[f32])]) -> Result<(), Error>
```

#### Deduplication & Cleanup
```rust
pub fn find_duplicate_memories(&self, threshold: f32) -> Result<Vec<DuplicatePair>, Error>
pub fn deduplicate_memories(&self, threshold: f32) -> Result<usize, Error>
pub fn cleanup_expired_memories(&self) -> Result<usize, Error>
pub fn memory_stats(&self) -> Result<MemoryStats, Error>
```

### RAG Document Management

#### Document CRUD
```rust
pub fn store_document(&self, doc: &RagDocumentWithChunks) -> Result<u64, Error>
pub fn store_document_with_ttl(&self, doc: &RagDocumentWithChunks, ttl_secs: u64) -> Result<u64, Error>
pub fn get_document(&self, doc_id: u64) -> Result<Option<RagDocumentWithChunks>, Error>
pub fn list_documents(&self) -> Result<Vec<RagDocument>, Error>
pub fn delete_document(&self, doc_id: u64) -> Result<(), Error>
pub fn document_count(&self) -> Result<usize, Error>
pub fn replace_document(&self, doc_id: u64, doc: &RagDocumentWithChunks) -> Result<(), Error>
pub fn replace_document_with_ttl(&self, doc_id: u64, doc: &RagDocumentWithChunks, ttl_secs: u64) -> Result<(), Error>
pub fn store_documents_batch(&self, docs: &[RagDocumentWithChunks]) -> Result<Vec<u64>, Error>
pub fn delete_documents_batch(&self, doc_ids: &[u64]) -> Result<usize, Error>
pub fn cleanup_expired_documents(&self) -> Result<usize, Error>
```

#### Chunk-Level Operations
```rust
pub fn get_chunk(&self, chunk_id: u64) -> Result<Option<RagChunk>, Error>
pub fn update_chunk(&self, chunk_id: u64, text: &str, embedding: &[f32]) -> Result<(), Error>
pub fn delete_chunk(&self, chunk_id: u64) -> Result<(), Error>
```

#### Search
```rust
pub fn search_chunks(&self, query_embedding: &[f32], k: usize) -> Result<Vec<RagSearchResult>, Error>
pub fn search_chunks_hybrid(&self, query_text: &str, query_embedding: &[f32], k: usize) -> Result<Vec<RagSearchResult>, Error>
pub fn search_chunks_by_keyword(&self, keyword: &str, limit: usize) -> Result<Vec<RagSearchResult>, Error>
```

#### Versioning & Metadata
```rust
pub fn get_document_version(&self, doc_id: u64) -> Result<Option<u64>, Error>
pub fn list_document_versions(&self, doc_id: u64) -> Result<Vec<u64>, Error>
pub fn get_document_at_version(&self, doc_id: u64, version: u64) -> Result<Option<RagDocumentWithChunks>, Error>
pub fn document_stats(&self) -> Result<(usize, usize, usize), Error>   // (doc_count, chunk_count, total_bytes)
pub fn search_documents_by_metadata(&self, key: &str, value: &str) -> Result<Vec<RagDocument>, Error>
```

#### Data Types
```rust
pub struct RagDocumentWithChunks {
    pub document: RagDocument,
    pub chunks: Vec<RagChunkInput>,
}
pub struct RagDocument {
    pub id: u64,
    pub title: String,
    pub source: Option<String>,
    pub metadata: Option<serde_json::Value>,
}
pub struct RagChunkInput {
    pub text: String,
    pub embedding: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}
```

### Agent Primitives

#### Tool Call Caching
```rust
pub fn cache_tool_result(&self, tool_name: &str, input_hash: &str, result: &str, ttl_secs: Option<u64>) -> Result<(), Error>
pub fn get_cached_tool_result(&self, tool_name: &str, input_hash: &str) -> Result<Option<ToolCacheEntry>, Error>
pub fn invalidate_tool_cache(&self, tool_name: &str) -> Result<u64, Error>
```
Cache expensive tool call results with TTL. `invalidate_tool_cache` removes all cached results for a tool.

#### Agent State Persistence
```rust
pub fn save_agent_state(&self, agent_id: &str, step_id: &str, state: &str, metadata: BTreeMap<String, String>) -> Result<AgentStep, Error>
pub fn get_agent_state(&self, agent_id: &str) -> Result<Option<AgentStep>, Error>
pub fn list_agent_steps(&self, agent_id: &str) -> Result<Vec<AgentStep>, Error>
pub fn get_agent_step_count(&self, agent_id: &str) -> Result<usize, Error>
pub fn rollback_agent_to_step(&self, agent_id: &str, step_id: &str) -> Result<usize, Error>
pub fn clear_agent_steps(&self, agent_id: &str) -> Result<usize, Error>
pub fn delete_agent_state(&self, agent_id: &str) -> Result<(), Error>
```
Persist agent execution steps for checkpointing.
- `rollback_agent_to_step` — removes all steps after the specified checkpoint, returns count removed
- `clear_agent_steps` — removes all steps for an agent, returns count removed
- `delete_agent_state` — deletes all agent data including steps

```rust
pub struct AgentStep {
    pub step_id: String,
    pub state: String,       // JSON state, structure defined by caller
    pub metadata: BTreeMap<String, String>,
    pub timestamp_ms: i64,
}
```

### Execution Trace

#### Logging
```rust
pub fn log_trace(&self, record: &TraceRecord) -> Result<(), Error>
```

```rust
pub struct TraceRecord {
    pub run_id: String,
    pub session_id: Option<String>,
    pub operation: String,   // "llm_call", "tool_call", "embedding", etc.
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub latency_ms: u64,
    pub token_usage: Option<u32>,
}
```

#### Querying
```rust
pub fn query_traces_by_session(&self, session_id: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_run(&self, run_id: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_operation(&self, operation: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_by_session_and_operation(&self, session_id: &str, operation: &str) -> Result<Vec<TraceRecord>, Error>
pub fn query_traces_in_range(&self, start_ms: i64, end_ms: i64) -> Result<Vec<TraceRecord>, Error>
pub fn export_traces(&self, session_id: Option<&str>) -> Result<Vec<TraceRecord>, Error>
```

#### Analytics
```rust
pub fn get_token_usage(&self, session_id: &str) -> Result<i64, Error>
pub fn get_token_usage_by_run(&self, run_id: &str) -> Result<i64, Error>
pub fn trace_stats(&self, session_id: Option<&str>) -> Result<TraceStats, Error>
pub fn trace_performance_report(&self, session_id: Option<&str>) -> Result<serde_json::Value, Error>
```
- `trace_stats` — aggregate count, token usage, grouped by operation type
- `trace_performance_report` — latency statistics, slow operation detection

### Embedding Cache

#### `cache_embedding`
```rust
pub fn cache_embedding(&self, text_hash: &str, embedding: &[f32]) -> Result<(), Error>
```
Cache an embedding vector to avoid redundant API calls to external embedding services.

#### `get_cached_embedding`
```rust
pub fn get_cached_embedding(&self, content_hash: &str) -> Result<Option<Vec<f32>>, Error>
```

#### `invalidate_embedding_cache`
```rust
pub fn invalidate_embedding_cache(&self) -> Result<u64, Error>
```
Clear all cached embeddings. Returns count deleted.

#### `embedding_cache_count`
```rust
pub fn embedding_cache_count(&self) -> Result<usize, Error>
```

### Intent Recognition

#### `query_by_intent`
```rust
pub fn query_by_intent(&self, query: &IntentQuery) -> Result<IntentResult, Error>
```

```rust
pub struct IntentQuery {
    pub text: String,
    pub context: Option<Vec<ContextMessage>>,
}

pub struct IntentResult {
    pub kind: IntentKind,        // Sql, Kv, Vector, Fts, Geo, Graph, Ts, Mq, Ai, Unknown
    pub confidence: f32,
    pub suggested_action: Option<String>,
}

pub enum IntentKind {
    Sql, Kv, Vector, Fts, Geo, Graph, Ts, Mq, Ai, Unknown,
}
```
Routes natural language queries to the appropriate engine.

## Accessing the AI Engine

```rust
use talon_ai::TalonAiExt;

// Write mode (Replica nodes return Error::ReadOnly)
let ai = db.ai()?;

// Read-only mode (available on Replica nodes)
let ai = db.ai_read()?;
```

## Best Practices

1. **Session lifecycle**: Use TTL for automatic cleanup of stale sessions
2. **Idempotent creation**: Use `create_session_if_not_exists()` in concurrent scenarios
3. **Token management**: Use `get_context_window()` to fit within LLM limits
4. **Smart context**: Use `get_context_window_smart()` for auto-summarization of long conversations
5. **Context compression**: Use `compact_context()` with `set_context_summary()` for safe context pruning
6. **Auto-embedding**: Configure an Embed Provider and use `auto_store_memory()` / `auto_search_memory()` to skip manual embedding computation
7. **Precise token count**: Use `count_tokens()` for exact token counting compatible with OpenAI
8. **Memory dedup**: Periodically call `deduplicate_memories()` to avoid redundancy
9. **Tool caching**: Cache expensive API calls with appropriate TTL
10. **Tracing**: Record all LLM calls for debugging and cost tracking
11. **Embedding cache**: Hash text content and cache embeddings to reduce API costs
