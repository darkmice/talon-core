# AI 可消费文档

Talon 提供机器可读的文档格式，供 AI 工具、LLM 助手和自动化管道使用。

## 可用格式

| 格式 | 文件 | 用途 |
|------|------|------|
| **llms.txt** | `/llms.txt` | 精简 API 参考，适配 LLM 上下文窗口 |
| **llms-full.txt** | `/llms-full.txt` | 完整 API 参考，包含所有签名 |
| **Markdown** | `/engines/*.md` | 结构化文档，适合 RAG 索引 |
| **Cursor Rules** | `.cursorrules` | AI IDE 集成规则 |

## llms.txt 标准

遵循 [llms.txt](https://llmstxt.org/) 标准，Talon 提供精简文本文件，概述所有 API，供 LLM 直接消费。

**在 AI 工具中使用：**
```
# 在系统提示或上下文中
读取 Talon API 文档：https://docs.talon.dev/llms.txt
```

## 用于 RAG 管道

所有文档页面使用干净的 Markdown 编写，非常适合分块和索引到向量数据库：

```rust
// 将 Talon 文档索引到 Talon 自身！
let ai = db.ai()?;
for doc_file in glob("docs/engines/*.md") {
    let content = std::fs::read_to_string(doc_file)?;
    let chunks = chunk_markdown(&content, 512);
    for chunk in chunks {
        let embedding = embed(&chunk.text);
        ai.store_rag_document(&RagDocumentWithChunks {
            document: RagDocument { id: doc_file.to_string(), title: chunk.heading.clone(), ..Default::default() },
            chunks: vec![RagChunkInput { text: chunk.text, embedding, ..Default::default() }],
        })?;
    }
}
```

## 用于 AI IDE（Cursor / Windsurf）

在项目的 `.cursorrules` 或 `.windsurfrules` 中添加：

```
# Talon Database
本项目使用 Talon，一个 AI 原生多模融合数据引擎。
- 9 大引擎：SQL、KV、TimeSeries、MQ、Vector、FTS、GEO、Graph、AI
- 单二进制、零依赖、嵌入式 Rust 库
- 通过 Talon::open("path") 打开，然后调用各引擎方法
- AI 引擎：db.ai()? 用于 Session/Context/Memory/RAG/Agent/Trace
- 向量：db.vector("name")? 用于 HNSW ANN 搜索
- 全文搜索：db.fts()? 用于 BM25 搜索
- 所有 pub API 返回 Result<T, talon::Error>
```

## 用于 MCP 服务器

Talon 文档可通过 MCP（Model Context Protocol）提供，实现实时 AI 工具访问：

```typescript
server.resource(
  "api-reference",
  "talon://api-reference",
  async (uri) => ({
    contents: [{ uri: uri.href, mimeType: "text/plain", text: fs.readFileSync("llms-full.txt", "utf-8") }],
  })
);
```
