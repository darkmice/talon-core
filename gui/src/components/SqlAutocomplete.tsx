import { useState, useEffect, useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
  "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "SHOW", "TABLES",
  "DESCRIBE", "DESC", "EXPLAIN", "TRUNCATE", "BEGIN", "COMMIT", "ROLLBACK",
  "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "IS", "NULL", "AS",
  "ORDER", "BY", "ASC", "LIMIT", "OFFSET", "DISTINCT", "JOIN",
  "INNER", "LEFT", "ON", "IF", "EXISTS", "ADD", "COLUMN", "RENAME", "TO",
  "DEFAULT", "NOT NULL", "PRIMARY KEY", "VECTOR", "INTEGER", "TEXT", "FLOAT",
  "BOOLEAN", "JSONB", "TIMESTAMP", "GEOPOINT", "BLOB",
  "COUNT", "SUM", "AVG", "MIN", "MAX",
  "INSERT OR REPLACE", "ON CONFLICT", "DO UPDATE",
  "CREATE VECTOR INDEX", "DROP VECTOR INDEX", "USING HNSW", "WITH",
  "vec_distance", "vec_cosine", "vec_l2", "vec_dot",
  "ST_DISTANCE", "ST_WITHIN", "NOW()",
];

const SQL_TEMPLATES = [
  { trigger: "sel", text: "SELECT * FROM ", label: "SELECT * FROM ..." },
  { trigger: "selw", text: "SELECT * FROM  WHERE ", label: "SELECT ... WHERE ..." },
  { trigger: "ins", text: "INSERT INTO  () VALUES ()", label: "INSERT INTO ... VALUES ..." },
  { trigger: "upd", text: "UPDATE  SET  WHERE ", label: "UPDATE ... SET ... WHERE ..." },
  { trigger: "del", text: "DELETE FROM  WHERE ", label: "DELETE FROM ... WHERE ..." },
  { trigger: "crt", text: "CREATE TABLE  (\n  id INTEGER NOT NULL,\n  \n)", label: "CREATE TABLE ..." },
  { trigger: "desc", text: "DESCRIBE ", label: "DESCRIBE table" },
  { trigger: "show", text: "SHOW TABLES", label: "SHOW TABLES" },
  { trigger: "cnt", text: "SELECT COUNT(*) FROM ", label: "SELECT COUNT(*) FROM ..." },
  { trigger: "cvi", text: "CREATE VECTOR INDEX  ON () WITH (metric='cosine', m=16, ef_construction=200)", label: "CREATE VECTOR INDEX ..." },
  { trigger: "expl", text: "EXPLAIN SELECT * FROM ", label: "EXPLAIN SELECT ..." },
];

function getContext(text: string, cursorPos: number) {
  const before = text.slice(0, cursorPos);
  const lines = before.split("\n");
  const currentLine = lines[lines.length - 1] || "";
  const words = currentLine.trimStart().split(/\s+/);
  const lastWord = words[words.length - 1] || "";
  const prevWord = (words.length >= 2 ? words[words.length - 2] : "").toUpperCase();
  const allUpper = before.toUpperCase();
  const needsTable = ["FROM", "INTO", "TABLE", "UPDATE", "JOIN", "DESCRIBE", "DESC"].includes(prevWord);
  let activeTable: string | null = null;
  const fromMatch = allUpper.match(/FROM\s+`?(\w+)`?/);
  const updateMatch = allUpper.match(/UPDATE\s+`?(\w+)`?/);
  const intoMatch = allUpper.match(/INTO\s+`?(\w+)`?/);
  if (fromMatch) activeTable = fromMatch[1].toLowerCase();
  else if (updateMatch) activeTable = updateMatch[1].toLowerCase();
  else if (intoMatch) activeTable = intoMatch[1].toLowerCase();
  const needsColumn =
    ["SELECT", "SET", "WHERE", "BY", "ON"].includes(prevWord) ||
    prevWord === "," ||
    (prevWord === "AND" && allUpper.includes("WHERE")) ||
    (prevWord === "OR" && allUpper.includes("WHERE"));
  return { lastWord, prevWord, needsTable, needsColumn, activeTable, currentLine };
}

function scoreMatch(input: string, candidate: string) {
  const inp = input.toLowerCase();
  const cand = candidate.toLowerCase();
  if (cand === inp) return -1;
  if (cand.startsWith(inp)) return 100 - cand.length;
  if (cand.includes(inp)) return 50 - cand.length;
  return -1;
}

export function getSuggestions(text: string, cursorPos: number, schema: any) {
  const ctx = getContext(text, cursorPos);
  const { lastWord, needsTable, needsColumn, activeTable } = ctx;
  if (!lastWord && !needsTable && !needsColumn) return [];
  const input = lastWord;
  const results = [];
  if (needsTable && schema?.tables) {
    for (const t of schema.tables) {
      const s = input ? scoreMatch(input, t.name) : 90;
      if (s >= 0 || !input) results.push({ text: t.name, label: t.name, type: "table", score: s });
    }
  } else if (needsColumn && activeTable && schema?.tables) {
    const tbl = schema.tables.find((t: any) => t.name.toLowerCase() === activeTable);
    if (tbl) {
      for (const col of tbl.columns) {
        const s = input ? scoreMatch(input, col.name) : 90;
        if (s >= 0 || !input) results.push({ text: col.name, label: `${col.name}  ${col.type}`, type: "column", score: s });
      }
    }
    results.push({ text: "*", label: "* (all columns)", type: "column", score: input ? -1 : 95 });
  }
  if (input && input.length >= 1) {
    for (const tpl of SQL_TEMPLATES) {
      if (tpl.trigger.startsWith(input.toLowerCase())) {
        results.push({ text: tpl.text, label: tpl.label, type: "template", score: 200, replaceWord: true });
      }
    }
    for (const kw of SQL_KEYWORDS) {
      const s = scoreMatch(input, kw);
      if (s >= 0) results.push({ text: kw, label: kw, type: "keyword", score: s });
    }
    if (schema?.tables && !needsTable) {
      for (const t of schema.tables) {
        const s = scoreMatch(input, t.name);
        if (s >= 0) results.push({ text: t.name, label: t.name, type: "table", score: s - 10 });
      }
    }
  }
  results.sort((a, b) => b.score - a.score);
  const seen = new Set();
  return results.filter(r => {
    const key = r.text + r.type;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, 12);
}

const TYPE_COLORS: Record<string, string> = {
  keyword: "text-purple-400",
  table: "text-teal-400",
  column: "text-blue-400",
  template: "text-yellow-400",
};
const TYPE_LABELS: Record<string, string> = {
  keyword: "KW",
  table: "TBL",
  column: "COL",
  template: "TPL",
};

interface SqlAutocompleteProps {
  textareaRef: React.RefObject<HTMLTextAreaElement | null>;
  sql: string;
  setSql: (v: string) => void;
  schema: any;
}

export default function SqlAutocomplete({ textareaRef, sql, setSql, schema }: SqlAutocompleteProps) {
  const { t } = useTranslation();
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [visible, setVisible] = useState(false);
  const [position, setPosition] = useState({ top: 0, left: 0 });
  const menuRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const updateSuggestions = useCallback(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    const pos = ta.selectionStart;
    const items = getSuggestions(sql, pos, schema);
    if (items.length > 0) {
      setSuggestions(items);
      setSelectedIdx(0);
      setVisible(true);
      const lines = sql.slice(0, pos).split("\n");
      const lineNum = lines.length;
      const colNum = (lines[lines.length - 1] || "").length;
      setPosition({ top: lineNum * 22 + 44, left: Math.min(colNum * 8.4 + 16, 500) });
    } else {
      setVisible(false);
    }
  }, [sql, schema, textareaRef]);

  const applySuggestion = useCallback((item: any) => {
    const ta = textareaRef.current;
    if (!ta) return;
    const pos = ta.selectionStart;
    const before = sql.slice(0, pos);
    const after = sql.slice(pos);
    let newBefore;
    if (item.replaceWord) {
      const wordStart = before.search(/\S+$/);
      newBefore = before.slice(0, wordStart >= 0 ? wordStart : pos) + item.text;
    } else {
      const wordStart = before.search(/\S+$/);
      newBefore = before.slice(0, wordStart >= 0 ? wordStart : pos) + item.text;
    }
    const newSql = newBefore + after;
    setSql(newSql);
    setVisible(false);
    setTimeout(() => {
      ta.focus();
      ta.selectionStart = ta.selectionEnd = newBefore.length;
    }, 0);
  }, [sql, setSql, textareaRef]);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (!visible || suggestions.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIdx(i => (i + 1) % suggestions.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIdx(i => (i - 1 + suggestions.length) % suggestions.length);
    } else if (e.key === "Tab" || e.key === "Enter") {
      if (visible && suggestions.length > 0) {
        if (e.key === "Tab" || (!e.ctrlKey && !e.metaKey && e.key === "Enter")) {
          e.preventDefault();
          applySuggestion(suggestions[selectedIdx]);
        }
      }
    } else if (e.key === "Escape") {
      setVisible(false);
    }
  }, [visible, suggestions, selectedIdx, applySuggestion]);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(updateSuggestions, 120);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [sql, updateSuggestions]);

  useEffect(() => {
    const ta = textareaRef.current;
    if (!ta) return;
    ta.addEventListener("keydown", handleKeyDown);
    return () => ta.removeEventListener("keydown", handleKeyDown);
  }, [handleKeyDown, textareaRef]);

  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setVisible(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, []);

  if (!visible || suggestions.length === 0) return null;

  return (
    <div ref={menuRef} className="absolute z-50 bg-surface border border-border-dark rounded-lg shadow-2xl shadow-black/50 py-1 min-w-[240px] max-w-[400px] max-h-[280px] overflow-y-auto"
      style={{ top: position.top, left: position.left }}>
      {suggestions.map((item, i) => (
        <button key={i} onClick={() => applySuggestion(item)}
          onMouseEnter={() => setSelectedIdx(i)}
          className={`w-full text-left flex items-center gap-2 px-3 py-1.5 text-[12px] transition-colors
            ${i === selectedIdx ? "bg-accent/20 text-white" : "text-slate-300 hover:bg-white/5"}`}>
          <span className={`text-[10px] font-mono w-6 shrink-0 ${TYPE_COLORS[item.type] || "text-slate-500"}`}>
            {TYPE_LABELS[item.type] || "?"}
          </span>
          <span className="font-mono truncate flex-1" title={item.label}>{item.label}</span>
        </button>
      ))}
      <div className="px-3 py-1 border-t border-border-dark text-[10px] text-slate-500">
        {t("sql.autocompleteHint")}
      </div>
    </div>
  );
}
