/// 将 Talon 返回的各种值类型格式化为显示字符串。
export function formatValue(v: any): string {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
  if (typeof v === "object") {
    if ("Integer" in v) return String(v.Integer);
    if ("Float" in v) return String(v.Float);
    if ("Text" in v) return v.Text;
    if ("Timestamp" in v) return String(v.Timestamp);
    if ("Boolean" in v) return v.Boolean ? "TRUE" : "FALSE";
    if ("Blob" in v) return `<BLOB ${Array.isArray(v.Blob) ? v.Blob.length : 0} bytes>`;
    if ("Null" in v) return "NULL";
    return JSON.stringify(v);
  }
  return String(v);
}
