# MCP Integration

Talon documentation can be served via the Model Context Protocol (MCP) for real-time AI tool access.

## Overview

MCP (Model Context Protocol) is a standard for connecting AI systems with external tools and data. A Talon MCP server can provide:

1. **Documentation Resources** — AI reads Talon API docs on demand
2. **Query Tools** — AI executes SQL/KV/Vector queries directly
3. **Schema Discovery** — AI inspects table schemas, indexes, and engine status

## Example MCP Server

```typescript
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import fs from "fs";

const server = new McpServer({
  name: "talon-docs",
  version: "0.1.0",
});

// Serve API documentation as a resource
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

// Serve individual engine docs
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

## Integration with AI IDEs

Add to your MCP configuration (e.g., `.cursor/mcp.json`):

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

The AI assistant will then have access to Talon API documentation when writing code.
