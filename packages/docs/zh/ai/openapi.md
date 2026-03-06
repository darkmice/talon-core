# OpenAPI 规范

Talon 的 HTTP API 可使用 OpenAPI 3.0 规范描述，用于集成 API 工具、代码生成器和 AI Agent。

## 状态

> OpenAPI YAML 规范计划在未来版本中发布。HTTP API 端点已在 [HTTP API](/zh/server/http-api) 中文档化。

## 与 AI Agent 集成

支持 OpenAPI/函数调用的 AI Agent 可使用规范自动发现并调用 Talon HTTP 端点。

```yaml
# OpenAPI 片段示例（计划中）
openapi: 3.0.0
info:
  title: Talon API
  version: 0.1.0
paths:
  /api/sql:
    post:
      summary: 执行 SQL
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                sql:
                  type: string
                params:
                  type: array
      responses:
        '200':
          description: 查询结果
```
