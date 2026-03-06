# AI-Consumable Documentation

Talon provides machine-readable documentation formats for AI tools, LLM assistants, and automation pipelines.

## Available Formats

| Format | File | Purpose |
|--------|------|---------|
| **llms.txt** | `/llms.txt` | Concise API reference for LLM context windows |
| **llms-full.txt** | `/llms-full.txt` | Complete API reference with all signatures |
| **Markdown** | `/engines/*.md` | Structured docs for RAG indexing |
| **Cursor Rules** | `.cursorrules` | AI IDE integration rules |

## llms.txt Standard

Following the [llms.txt](https://llmstxt.org/) standard, Talon provides a concise text file summarizing all APIs for direct LLM consumption.

**Usage in AI tools:**
```
# In system prompt or context
Read the Talon API docs at: https://docs.talon.dev/llms.txt
```

## For RAG Pipelines

All documentation pages are written in clean Markdown, making them ideal for chunking and indexing into vector databases:

```rust
// Index Talon docs into Talon itself!
let ai = db.ai()?;
for doc_file in glob("docs/engines/*.md") {
    let content = std::fs::read_to_string(doc_file)?;
    let chunks = chunk_markdown(&content, 512);
    for chunk in chunks {
        let embedding = embed(&chunk.text);
        ai.store_rag_document(&RagDocumentWithChunks {
            document: RagDocument {
                id: doc_file.to_string(),
                title: chunk.heading.clone(),
                ..Default::default()
            },
            chunks: vec![RagChunkInput {
                text: chunk.text,
                embedding,
                ..Default::default()
            }],
        })?;
    }
}
```

## For AI IDEs (Cursor / Windsurf)

Place the following in your project's `.cursorrules` or `.windsurfrules`:

```
# Talon Database
This project uses Talon, an AI-native multi-model data engine.
- 9 engines: SQL, KV, TimeSeries, MQ, Vector, FTS, GEO, Graph, AI
- Single binary, zero dependencies, embedded Rust library
- All engines accessed via `Talon::open("path")` then engine-specific methods
- AI Engine: db.ai()? for Session/Context/Memory/RAG/Agent/Trace
- Vector: db.vector("name")? for HNSW ANN search
- FTS: db.fts()? for full-text search with BM25
- All pub APIs return Result<T, talon::Error>
```

## For MCP Servers

Talon documentation can be served via MCP (Model Context Protocol) for real-time AI tool access:

```typescript
// MCP server providing Talon API knowledge
server.addResource({
  uri: "talon://api-reference",
  name: "Talon API Reference",
  mimeType: "text/markdown",
  async read() {
    return fs.readFileSync("llms-full.txt", "utf-8");
  }
});
```
