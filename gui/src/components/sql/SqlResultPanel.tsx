import { useState, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { formatValue } from "../../lib/formatValue";
import { Button, Badge } from "../ui";

const MAX_DISPLAY_ROWS = 500;

interface SqlResultPanelProps {
  rows: any[] | null;
  columns: string[] | null;
  error: string | null;
  elapsed: number | null;
  onCopyJson: () => void;
  onCopyCsv: () => void;
  onSort?: (colName: string, dir: "asc" | "desc" | null) => void;
  sortCol?: string | null;
  sortDir?: "asc" | "desc" | null;
}

export default function SqlResultPanel({ rows, columns, error, elapsed, onCopyJson, onCopyCsv, onSort, sortCol, sortDir }: SqlResultPanelProps) {
  const { t } = useTranslation();
  const [copiedCell, setCopiedCell] = useState<string | null>(null);

  const copyCell = useCallback(async (cellKey: string, value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedCell(cellKey);
      setTimeout(() => setCopiedCell(null), 1200);
    } catch (_) {}
  }, []);

  const handleSort = useCallback((colIdx: number) => {
    if (!columns || !columns[colIdx] || !onSort) return;
    const colName = columns[colIdx];
    if (sortCol === colName) {
      if (sortDir === "asc") onSort(colName, "desc");
      else onSort(colName, null);
    } else {
      onSort(colName, "asc");
    }
  }, [columns, sortCol, sortDir, onSort]);

  const displayRows = rows && rows.length > MAX_DISPLAY_ROWS ? rows.slice(0, MAX_DISPLAY_ROWS) : rows;
  const isTruncated = rows !== null && rows.length > MAX_DISPLAY_ROWS;

  return (
    <div className="flex-1 border-t border-border-dark flex flex-col min-h-0">
      {/* Result header */}
      <div className="flex items-center justify-between px-4 py-2 shrink-0 border-b border-border-dark">
        <div className="flex items-center gap-3 text-xs">
          <span className="font-semibold text-white">{t("sql.result", { n: 1 })}</span>
          {rows !== null && !error && (
            <>
              <Badge variant="success">
                <span className="material-symbols-outlined text-[10px] mr-0.5">check_circle</span>
                {rows.length} {t("common.rows")}
              </Badge>
              {elapsed !== null && <span className="text-slate-400">{elapsed}ms</span>}
            </>
          )}
          {error && (
            <>
              <Badge variant="danger">{t("sql.error")}</Badge>
              {elapsed !== null && <span className="text-slate-400">{elapsed}ms</span>}
            </>
          )}
        </div>
        {rows && rows.length > 0 && (
          <div className="flex gap-1 items-center">
            {sortCol && onSort && (
              <button onClick={() => onSort(sortCol, null)}
                className="text-[10px] text-slate-400 hover:text-white transition flex items-center gap-0.5 mr-1">
                <span className="material-symbols-outlined text-[12px]">close</span>
                Sort
              </button>
            )}
            <Button variant="secondary" size="sm" icon="data_object" onClick={onCopyJson}>
              {t("sql.copyJson")}
            </Button>
            <Button variant="secondary" size="sm" icon="download" onClick={onCopyCsv}>
              {t("sql.csv")}
            </Button>
          </div>
        )}
      </div>

      {/* Result table */}
      <div className="flex-1 overflow-auto">
        {error && (
          <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">
            {error}
          </div>
        )}
        {rows !== null && !error && rows.length === 0 && (
          <p className="text-slate-400 text-sm text-center py-10">{t("common.noData")}</p>
        )}
        {displayRows !== null && !error && displayRows.length > 0 && (
          <>
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr>
                  <th className="bg-dark-700 px-2 py-2.5 text-center text-[10px] text-slate-600 border-b border-border-dark sticky top-0 z-20 w-10">#</th>
                  {(Array.isArray(displayRows[0]) ? displayRows[0] : Object.keys(displayRows[0])).map((_: any, i: number) => (
                    <th key={i}
                      onClick={() => handleSort(i)}
                      className="bg-dark-700 px-3 py-2.5 text-left sticky top-0 z-20 whitespace-nowrap border-b border-border-dark cursor-pointer hover:bg-dark-600 transition-colors select-none">
                      <span className="text-[11px] uppercase tracking-wider text-slate-400 font-semibold inline-flex items-center gap-1">
                        {columns && columns[i] ? columns[i] : t("common.colN", { n: i + 1 })}
                        {columns && columns[i] && sortCol === columns[i] && sortDir && (
                          <span className="material-symbols-outlined text-[12px] text-primary">
                            {sortDir === "asc" ? "arrow_upward" : "arrow_downward"}
                          </span>
                        )}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {displayRows.map((row, ri) => {
                  const cells = Array.isArray(row) ? row : Object.values(row);
                  return (
                    <tr key={ri} className="hover:bg-white/[0.02] transition-colors">
                      <td className="px-2 py-2 border-b border-border-dark text-center text-[11px] text-slate-600">{ri + 1}</td>
                      {cells.map((cell, ci) => {
                        const v = formatValue(cell);
                        const isNull = v === "NULL";
                        const isNum = !isNull && !isNaN(Number(v)) && v !== "";
                        const cellKey = `${ri}:${ci}`;
                        const isCopied = copiedCell === cellKey;
                        return (
                          <td key={ci}
                            onClick={() => !isNull && copyCell(cellKey, v)}
                            className={`px-3 py-2 border-b border-border-dark max-w-[300px] truncate font-mono text-[13px] ${!isNull ? "cursor-pointer hover:bg-white/[0.04]" : ""}`}
                            title={isNull ? "NULL" : `${v}\n(click to copy)`}>
                            {isCopied ? <span className="text-emerald-400 text-[11px]">✓ copied</span>
                              : isNull ? <span className="text-slate-600 italic">NULL</span>
                              : isNum ? <span className="text-blue-400">{v}</span>
                              : <span className="text-slate-300">{v}</span>}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {isTruncated && (
              <p className="text-center text-xs text-slate-500 py-3">
                {t("sql.truncatedHint", { shown: MAX_DISPLAY_ROWS, total: rows!.length })}
              </p>
            )}
          </>
        )}
        {rows === null && !error && (
          <div className="text-center py-16">
            <span className="material-symbols-outlined text-[40px] text-slate-700">terminal</span>
            <p className="text-slate-400 text-sm mt-3">{t("sql.runHint")}</p>
          </div>
        )}
      </div>
    </div>
  );
}
