# MCP 集成

Talon 文档可通过 Model Context Protocol (MCP) 提供，实现实时 AI 工具访问。

## 概述

MCP（Model Context Protocol）是连接 AI 系统与外部工具和数据的标准。Talon MCP 服务器可以提供：

1. **文档资源** — AI 按需读取 Talon API 文档
2. **查询工具** — AI 直接执行 SQL/KV/向量查询
3. **Schema 发现** — AI 检查表结构、索引和引擎状态

## MCP 服务器示例

```typescript
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import fs from "fs";

const server = new McpServer({
  name: "talon-docs",
  version: "0.1.0",
});

// 提供 API 文档作为资源
server.resource(
  "api-reference",
  "talon://api-reference",
  async (uri) => ({
    contents: [{
      uri: uri.href,
      mimeType: "text/plain",
      text: fs.readFileSync("llms-full.txt", "utf-8"),
    }],
  })
);

// 提供各引擎文档
const engines = ["sql", "kv", "timeseries", "message-queue", "vector", "full-text-search", "geo", "graph", "ai"];
for (const engine of engines) {
  server.resource(
    `engine-${engine}`,
    `talon://engines/${engine}`,
    async (uri) => ({
      contents: [{
        uri: uri.href,
        mimeType: "text/markdown",
        text: fs.readFileSync(`engines/${engine}.md`, "utf-8"),
      }],
    })
  );
}

const transport = new StdioServerTransport();
await server.connect(transport);
```

## 与 AI IDE 集成

在 MCP 配置中添加（如 `.cursor/mcp.json`）：

```json
{
  "mcpServers": {
    "talon-docs": {
      "command": "node",
      "args": ["./talon-mcp-server.js"]
    }
  }
}
```

AI 助手在编写代码时即可访问 Talon API 文档。
