# OpenAPI Specification

Talon's HTTP API can be described using the OpenAPI 3.0 specification for integration with API tools, code generators, and AI agents.

## Status

> OpenAPI YAML specification is planned for a future release. The HTTP API endpoints are documented in the [HTTP API](/server/http-api) section.

## Usage with AI Agents

AI agents that support OpenAPI/function calling can use the specification to automatically discover and invoke Talon HTTP endpoints.

```yaml
# Example OpenAPI snippet (planned)
openapi: 3.0.0
info:
  title: Talon API
  version: 0.1.0
paths:
  /api/sql:
    post:
      summary: Execute SQL
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
          description: Query results
```
