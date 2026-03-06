/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
export function fmtSize(b: number): string {
  if (b >= 1048576) return `${(b / 1048576).toFixed(1)} MB`;
  if (b >= 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${b} B`;
}

export function guessType(val: string): string {
  if (!val || val === "-") return "STRING";
  try { const p = JSON.parse(val); if (typeof p === "object" && p !== null) return Array.isArray(p) ? "LIST" : "HASH"; } catch {}
  return "STRING";
}

export function fmtJson(v: string): string {
  try { return JSON.stringify(JSON.parse(v), null, 2); } catch { return v; }
}

export const typeBadgeVariant: Record<string, string> = {
  HASH: "primary", STRING: "success", LIST: "info", SET: "warning",
};
