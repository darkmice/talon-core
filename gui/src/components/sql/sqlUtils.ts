/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
type TFunction = (key: string) => string;

const SQL_KEYWORDS = new Set([
  "SELECT","FROM","WHERE","AND","OR","NOT","IN","BETWEEN","LIKE","IS","NULL","AS",
  "ORDER","BY","ASC","DESC","LIMIT","OFFSET","DISTINCT","JOIN","INNER","LEFT","RIGHT",
  "ON","INSERT","INTO","VALUES","UPDATE","SET","DELETE","CREATE","TABLE","DROP","ALTER",
  "INDEX","SHOW","TABLES","DESCRIBE","EXPLAIN","TRUNCATE","BEGIN","COMMIT","ROLLBACK",
  "IF","EXISTS","ADD","COLUMN","RENAME","TO","DEFAULT","PRIMARY","KEY","VECTOR","INTEGER",
  "TEXT","FLOAT","BOOLEAN","JSONB","TIMESTAMP","GEOPOINT","BLOB","USING","HNSW","WITH",
  "COUNT","SUM","AVG","MIN","MAX","UNION","EXCEPT","INTERSECT","GROUP","HAVING",
]);

const FORMAT_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY", "HAVING",
  "LIMIT", "OFFSET", "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM",
  "CREATE TABLE", "CREATE VECTOR INDEX", "DROP", "ALTER", "JOIN", "LEFT JOIN",
  "INNER JOIN", "ON", "UNION", "EXCEPT", "INTERSECT",
];

function escapeHtml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function highlightSql(input: string): string {
  const re = /('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|--[^\n]*|\b\d+(?:\.\d+)?\b|\b\w+\b|.)/g;
  let m: RegExpExecArray | null;
  let out = "";
  while ((m = re.exec(input)) !== null) {
    const tok = m[0];
    if (tok.startsWith("'") || tok.startsWith('"')) {
      out += `<span class="text-emerald-400">${escapeHtml(tok)}</span>`;
    } else if (tok.startsWith("--")) {
      out += `<span class="text-slate-600 italic">${escapeHtml(tok)}</span>`;
    } else if (/^\d/.test(tok)) {
      out += `<span class="text-blue-400">${escapeHtml(tok)}</span>`;
    } else if (SQL_KEYWORDS.has(tok.toUpperCase())) {
      out += `<span class="text-purple-400">${escapeHtml(tok)}</span>`;
    } else {
      out += escapeHtml(tok);
    }
  }
  return out;
}

export function formatSqlString(sql: string): string {
  let s = sql.trim().replace(/\s+/g, " ");
  for (const kw of FORMAT_KEYWORDS) {
    const re = new RegExp(`\\b${kw}\\b`, "gi");
    s = s.replace(re, `\n${kw}`);
  }
  return s.replace(/^\n/, "").replace(/\n\s*\n/g, "\n");
}

export function fmtTime(ts: number, t: TFunction): string {
  const d = new Date(ts);
  const now = new Date();
  if (d.toDateString() === now.toDateString()) return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  if (now.getTime() - d.getTime() < 86400000 * 2) return t("common.yesterday");
  return d.toLocaleDateString();
}
