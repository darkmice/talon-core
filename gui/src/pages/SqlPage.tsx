import { useState, useEffect, useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { execSql, getSchemaInfo } from "../lib/tauri";
import { formatValue } from "../lib/formatValue";
import { useSqlStore } from "../stores/sqlStore";
import { useToastStore } from "../stores/toastStore";
import { formatSqlString } from "../components/sql/sqlUtils";
import {
  SqlTabBar,
  SqlEditor,
  SqlActionBar,
  SqlResultPanel,
  SqlHistoryPanel,
} from "../components/sql";

export default function SqlPage() {
  const { t } = useTranslation();
  const { tabs, activeTab, setSql: storeSql, setResult: storeResult, addHistory } = useSqlStore();
  const addToast = useToastStore((s) => s.addToast);
  const [loading, setLoading] = useState(false);
  const [schema, setSchema] = useState<any>(null);
  const [cursorPos, setCursorPos] = useState({ ln: 1, col: 1 });
  const [editorRatio, setEditorRatio] = useState(0.4);
  const [hasSelection, setHasSelection] = useState(false);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc" | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const draggingRef = useRef(false);

  const currentTab = tabs.find(tb => tb.id === activeTab) || tabs[0];
  const sql = currentTab?.sql || "";
  const { rows, columns, error, elapsed } = currentTab?.result || { rows: null, columns: null, error: null, elapsed: null };
  const setSql = (v: string) => storeSql(activeTab, v);
  const setResult = (r: { rows: any[] | null; columns: string[] | null; error: string | null; elapsed: number | null }) => storeResult(activeTab, r);

  useEffect(() => {
    getSchemaInfo().then(res => { if (res.ok) setSchema((res as any).data); else addToast("error", (res as any).error ?? t("explorer.loadSchemaFailed")); }).catch(e => {
      addToast("error", String(e));
    });
  }, []);

  const getSelectedText = useCallback(() => {
    const ta = textareaRef.current;
    if (!ta) return "";
    return ta.value.substring(ta.selectionStart, ta.selectionEnd);
  }, []);

  const handleSelectionChange = useCallback((pos: { ln: number; col: number }) => {
    setCursorPos(pos);
    const ta = textareaRef.current;
    setHasSelection(!!ta && ta.selectionStart !== ta.selectionEnd);
  }, []);

  const startDrag = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const onMove = (ev: MouseEvent) => {
      if (!draggingRef.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const ratio = Math.min(0.8, Math.max(0.15, (ev.clientY - rect.top) / rect.height));
      setEditorRatio(ratio);
    };
    const onUp = () => {
      draggingRef.current = false;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const run = async (sqlOverride?: string) => {
    const override = typeof sqlOverride === "string" ? sqlOverride : "";
    const selected = override || getSelectedText().trim();
    const s = (selected || sql.trim()).replace(/;$/, "");
    if (!s) return;
    setLoading(true);
    const t0 = performance.now();
    try {
      const data = await execSql(s);
      const ms = Math.round(performance.now() - t0);
      if (!data.ok) {
        setResult({ rows: null, columns: null, error: data.error ?? t("common.unknownError"), elapsed: ms });
        addHistory({ sql: s, time: Date.now(), ok: false, ms, rowCount: 0 });
      } else {
        const d = data.data as any;
        const r = d?.rows || [];
        const cols = d?.columns && d.columns.length > 0 ? d.columns : null;
        setResult({ rows: r, columns: cols, error: null, elapsed: ms });
        addHistory({ sql: s, time: Date.now(), ok: true, ms, rowCount: r.length });
      }
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      setResult({ rows: null, columns: null, error: String(e), elapsed: ms });
      addHistory({ sql: sql.trim(), time: Date.now(), ok: false, ms, rowCount: 0 });
    }
    setLoading(false);
  };

  const copyJson = async () => {
    if (!rows) return;
    const data = rows.map((r: any) => {
      const cells = Array.isArray(r) ? r : Object.values(r);
      if (columns && columns.length === cells.length) {
        const obj: Record<string, any> = {};
        cells.forEach((c: any, i: number) => { obj[columns[i]] = formatValue(c); });
        return obj;
      }
      return cells.map((c: any) => formatValue(c));
    });
    try {
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2));
      addToast("success", t("sql.jsonCopied"));
    } catch (e) { addToast("error", String(e)); }
  };

  const copyCsv = async () => {
    if (!rows) return;
    const header = columns ? columns.join(",") + "\n" : "";
    const text = header + rows.map((r: any) => {
      const cells = Array.isArray(r) ? r : Object.values(r);
      return cells.map((c: any) => formatValue(c)).join(",");
    }).join("\n");
    try {
      await navigator.clipboard.writeText(text);
      addToast("success", t("sql.csvCopied"));
    } catch (e) { addToast("error", String(e)); }
  };

  const handleClear = () => { setSql(""); setResult({ rows: null, columns: null, error: null, elapsed: null }); setSortCol(null); setSortDir(null); };
  const handleFormat = () => setSql(formatSqlString(sql));

  const handleSort = useCallback((colName: string, dir: "asc" | "desc" | null) => {
    const baseSql = sql.trim().replace(/;$/, "");
    const orderByRe = /\s+ORDER\s+BY\s+[\s\S]*?(?=\s+LIMIT\b|\s+OFFSET\b|\s*$)/i;
    const stripped = baseSql.replace(orderByRe, "");
    if (!dir) {
      setSortCol(null);
      setSortDir(null);
      setSql(stripped);
      run(stripped);
      return;
    }
    const limitMatch = stripped.match(/(\s+(?:LIMIT|OFFSET)\b[\s\S]*)$/i);
    const beforeLimit = limitMatch ? stripped.slice(0, -limitMatch[1].length) : stripped;
    const limitPart = limitMatch ? limitMatch[1] : "";
    const newSql = `${beforeLimit} ORDER BY "${colName}" ${dir.toUpperCase()}${limitPart}`;
    setSortCol(colName);
    setSortDir(dir);
    setSql(newSql);
    run(newSql);
  }, [sql, run]);

  return (
    <div className="flex h-full">
      <div className="flex-1 flex flex-col min-w-0">
        <SqlTabBar />
        <div ref={containerRef} className="flex-1 flex flex-col min-h-0">
          <div style={{ height: `${editorRatio * 100}%` }} className="flex flex-col min-h-[80px] shrink-0">
            <SqlEditor sql={sql} setSql={setSql} schema={schema} onRun={run} onFormat={handleFormat} onCursorChange={handleSelectionChange} textareaRef={textareaRef} />
          </div>
          <SqlActionBar loading={loading} hasSql={!!sql.trim()} hasSelection={hasSelection} onRun={run} onClear={handleClear} onFormat={handleFormat} cursorPos={cursorPos} onDragStart={startDrag} />
          <SqlResultPanel rows={rows} columns={columns} error={error} elapsed={elapsed} onCopyJson={copyJson} onCopyCsv={copyCsv} onSort={handleSort} sortCol={sortCol} sortDir={sortDir} />
        </div>
      </div>
      <SqlHistoryPanel onSelectSql={setSql} onRunSql={(s) => { setSql(s); run(s); }} />
    </div>
  );
}
