import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useExplorerStore, PAGE_SIZE_CONST } from "../../stores/explorerStore";
import { useToastStore } from "../../stores/toastStore";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { formatValue } from "../../lib/formatValue";
import { Input, Select, Checkbox } from "../ui";

export default function DataTab() {
  const { t } = useTranslation();
  const {
    selectedTable, tableSchema, tableData, dataLoading, page, totalCount,
    searchText, setSearchText, goPage, refresh,
  } = useExplorerStore();
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"ASC" | "DESC">("ASC");
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
  const [deleting, setDeleting] = useState(false);

  useEffect(() => { setSelectedRows(new Set()); }, [page, searchText, selectedTable]);

  const totalPages = totalCount !== null ? Math.max(1, Math.ceil(totalCount / PAGE_SIZE_CONST)) : 1;

  const filteredData = (() => {
    if (!tableData || !searchText.trim()) return tableData;
    const q = searchText.toLowerCase();
    return tableData.filter((row: any) => {
      const cells = Array.isArray(row) ? row : Object.values(row);
      return cells.some((c: any) => formatValue(c).toLowerCase().includes(q));
    });
  })();

  const exportCsv = () => {
    if (!tableData || tableData.length === 0) return;
    const headers = tableSchema
      ? tableSchema.map((col: any) => formatValue(Array.isArray(col) ? col[0] : col))
      : Array.from({ length: (tableData[0] as any[]).length }, (_, i) => `col_${i + 1}`);
    const csvRows = [headers.join(",")];
    for (const row of tableData) {
      const cells = Array.isArray(row) ? row : Object.values(row);
      csvRows.push(cells.map((c: any) => {
        const v = formatValue(c);
        return v.includes(",") || v.includes('"') || v.includes("\n") ? `"${v.replace(/"/g, '""')}"` : v;
      }).join(","));
    }
    const blob = new Blob([csvRows.join("\n")], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${selectedTable || "export"}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    useToastStore.getState().addToast("success", t("explorer.csvExported"));
  };

  return (
    <>
      {/* Toolbar */}
      <div className="flex items-center justify-between p-3 border-b border-border-dark bg-dark-800/50 shrink-0">
        <div className="flex items-center gap-2">
          <button
            onClick={() => {
              if (!sortCol && tableSchema && tableSchema.length > 0) {
                const firstCol = Array.isArray(tableSchema[0]) ? formatValue(tableSchema[0][0]) : "1";
                setSortCol(firstCol);
              } else if (sortCol) {
                setSortCol(null);
              }
            }}
            className={`flex items-center gap-2 px-3 py-1.5 text-xs font-medium border border-border-dark rounded transition-colors ${
              sortCol ? "bg-primary text-white" : "text-slate-300 bg-surface hover:bg-slate-700 hover:text-white"
            }`}
          >
            <span className="material-symbols-outlined text-[16px]">sort</span>
            {t("explorer.sort")}
          </button>
          <div className="h-6 w-px bg-slate-700 mx-1" />
          <button
            onClick={refresh}
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-slate-300 bg-transparent hover:bg-surface rounded transition-colors"
            title={t("explorer.refresh")}
          >
            <span className="material-symbols-outlined text-[16px]">refresh</span>
          </button>
        </div>
        <div className="flex items-center gap-2">
          <Input
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            icon="search"
            size="sm"
            placeholder={t("explorer.search")}
            className="w-64"
          />
          <button
            onClick={exportCsv}
            disabled={!tableData || tableData.length === 0}
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-slate-300 bg-surface border border-border-dark rounded hover:bg-slate-700 hover:text-white transition-colors disabled:opacity-30"
          >
            <span className="material-symbols-outlined text-[16px]">download</span>
            {t("explorer.export")}
          </button>
        </div>
      </div>

      {/* Sort bar */}
      {sortCol && tableSchema && (
        <div className="flex items-center gap-3 px-4 py-2.5 border-b border-border-dark bg-dark-800/30 shrink-0">
          <span className="material-symbols-outlined text-[16px] text-slate-400">sort</span>
          <span className="text-xs text-slate-400">{t("explorer.sortBy")}</span>
          <Select
            value={sortCol ?? ""}
            onValueChange={v => setSortCol(v)}
            size="sm"
            options={tableSchema.map((col: any, i: number) => {
              const name = Array.isArray(col) ? formatValue(col[0]) : t("common.colN", { n: i + 1 });
              return { value: name, label: name };
            })}
            className="w-40"
          />
          <button
            onClick={() => setSortDir(sortDir === "ASC" ? "DESC" : "ASC")}
            className="flex items-center gap-1 text-xs text-slate-300 bg-surface border border-border-dark rounded px-2 py-1 hover:bg-slate-700 transition-colors"
          >
            <span className="material-symbols-outlined text-[14px]">
              {sortDir === "ASC" ? "arrow_upward" : "arrow_downward"}
            </span>
            {sortDir}
          </button>
          <button
            onClick={async () => {
              if (!selectedTable || !sortCol) return;
              const tbl = escapeSqlIdent(selectedTable);
              const col = escapeSqlIdent(sortCol);
              try {
                const data = await execSql(`SELECT * FROM ${tbl} ORDER BY ${col} ${sortDir} LIMIT ${PAGE_SIZE_CONST} OFFSET 0`);
                if (data.ok) {
                  useExplorerStore.setState({ tableData: (data.data as any)?.rows || [], page: 0 });
                } else {
                  useToastStore.getState().addToast("error", data.error ?? t("common.unknownError"));
                }
              } catch (e) {
                useToastStore.getState().addToast("error", String(e));
              }
            }}
            className="text-xs text-primary hover:underline"
          >
            {t("explorer.apply")}
          </button>
          <button
            onClick={() => { setSortCol(null); refresh(); }}
            className="text-xs text-slate-400 hover:text-white transition"
          >
            {t("explorer.clear")}
          </button>
        </div>
      )}

      {/* Data Grid */}
      <div className="flex-1 overflow-auto relative bg-dark-900">
        {dataLoading && (
          <div className="flex items-center justify-center py-16">
            <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
            <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
          </div>
        )}
        {!dataLoading && filteredData && filteredData.length === 0 && (
          <p className="text-slate-400 text-sm text-center py-10">
            {searchText ? t("common.noData") : t("explorer.empty")}
          </p>
        )}
        {!dataLoading && filteredData && filteredData.length > 0 && (
          <table className="w-full text-left border-separate border-spacing-0">
            <thead className="bg-surface sticky top-0 z-20 shadow-sm">
              <tr>
                <th className="sticky left-0 top-0 z-30 w-12 bg-surface border-b border-r border-border-dark p-0 text-center">
                  <div className="flex items-center justify-center h-full w-full py-3">
                    <Checkbox
                      checked={filteredData!.length > 0 && selectedRows.size === filteredData!.length}
                      onChange={v => {
                        if (v) {
                          setSelectedRows(new Set(filteredData!.map((_, i) => i)));
                        } else {
                          setSelectedRows(new Set());
                        }
                      }}
                      className="w-3.5 h-3.5"
                    />
                  </div>
                </th>
                {tableSchema && tableSchema.map((col, i) => {
                  const colName = Array.isArray(col) ? formatValue(col[0]) : t("common.colN", { n: i + 1 });
                  const colType = Array.isArray(col) && col.length > 1 ? formatValue(col[1]) : "";
                  return (
                    <th key={i} className="min-w-[120px] bg-surface border-b border-r border-border-dark px-4 py-2">
                      <div className="flex flex-col gap-0.5">
                        <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{colName}</span>
                        {colType && <span className="text-[9px] font-mono text-slate-400">{colType}</span>}
                      </div>
                    </th>
                  );
                })}
                {!tableSchema && filteredData[0] && Array.from(
                  { length: Array.isArray(filteredData[0]) ? filteredData[0].length : Object.keys(filteredData[0]).length },
                  (_, i) => (
                    <th key={i} className="min-w-[120px] bg-surface border-b border-r border-border-dark px-4 py-2">
                      <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("common.colN", { n: i + 1 })}</span>
                    </th>
                  )
                )}
              </tr>
            </thead>
            <tbody className="text-xs font-mono text-slate-300 divide-y divide-border-dark">
              {filteredData.map((row: any, ri: number) => {
                const cells = Array.isArray(row) ? row : Object.values(row);
                const stripeBg = ri % 2 === 1 ? "bg-white/[0.01]" : "";
                return (
                  <tr key={ri} className={`hover:bg-white/[0.02] group transition-colors ${stripeBg}`}>
                    <td className={`sticky left-0 z-10 ${ri % 2 === 1 ? "bg-dark-800" : "bg-dark-900"} group-hover:bg-dark-800 border-r border-border-dark text-center`}>
                      <div className="flex items-center justify-center">
                        <Checkbox
                          checked={selectedRows.has(ri)}
                          onChange={v => {
                            const next = new Set(selectedRows);
                            if (v) next.add(ri); else next.delete(ri);
                            setSelectedRows(next);
                          }}
                          className="w-3.5 h-3.5"
                        />
                      </div>
                    </td>
                    {cells.map((cell: any, ci: number) => {
                      const v = formatValue(cell);
                      const isNull = v === "NULL";
                      const isNum = !isNull && !isNaN(Number(v)) && v !== "";
                      const isBool = !isNull && (v === "TRUE" || v === "FALSE" || v === "true" || v === "false");
                      const isEmail = !isNull && v.includes("@") && v.includes(".");
                      return (
                        <td key={ci} className="px-4 py-2.5 border-r border-border-dark max-w-[300px] truncate" title={v}>
                          {isNull ? <span className="text-slate-600 italic">NULL</span>
                            : isBool ? <span className={v.toUpperCase() === "TRUE" ? "text-green-400" : "text-red-400"}>{v.toUpperCase()}</span>
                            : isNum ? <span className="text-blue-400">{v}</span>
                            : isEmail ? <span className="text-blue-400">{v}</span>
                            : <span className="text-white">{v}</span>}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Selection action bar */}
      {selectedRows.size > 0 && filteredData && (
        <div className="absolute bottom-20 left-1/2 -translate-x-1/2 bg-slate-800 border border-border-dark shadow-2xl rounded-xl p-2 pl-4 flex items-center gap-4 z-50">
          <span className="text-xs font-bold text-white">{t("explorer.selectedRows", { count: selectedRows.size })}</span>
          <div className="h-6 w-px bg-slate-700" />
          <button
            onClick={async () => {
              const rows = [...selectedRows].map(i => filteredData![i]);
              const headers = tableSchema
                ? tableSchema.map((c: any) => formatValue(Array.isArray(c) ? c[0] : c))
                : Array.from({ length: (rows[0] as any[]).length }, (_, i) => `col_${i + 1}`);
              const jsonRows = rows.map((row: any) => {
                const cells = Array.isArray(row) ? row : Object.values(row);
                const obj: Record<string, any> = {};
                cells.forEach((c: any, ci: number) => { obj[headers[ci] || `col_${ci}`] = c; });
                return obj;
              });
              try {
                await navigator.clipboard.writeText(JSON.stringify(jsonRows, null, 2));
                useToastStore.getState().addToast("success", t("explorer.copiedJson"));
              } catch (e) {
                useToastStore.getState().addToast("error", String(e));
              }
            }}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-300 hover:text-white hover:bg-white/5 rounded transition-colors"
          >
            <span className="material-symbols-outlined text-[14px]">data_object</span>
            {t("explorer.copyJson")}
          </button>
          <button
            onClick={async () => {
              const rows = [...selectedRows].map(i => filteredData![i]);
              const headers = tableSchema
                ? tableSchema.map((c: any) => formatValue(Array.isArray(c) ? c[0] : c))
                : Array.from({ length: (rows[0] as any[]).length }, (_, i) => `col_${i + 1}`);
              const csvLines = [headers.join(",")];
              for (const row of rows) {
                const cells = Array.isArray(row) ? row : Object.values(row);
                csvLines.push(cells.map((c: any) => {
                  const v = formatValue(c);
                  return v.includes(",") || v.includes('"') || v.includes("\n") ? `"${v.replace(/"/g, '""')}"` : v;
                }).join(","));
              }
              try {
                await navigator.clipboard.writeText(csvLines.join("\n"));
                useToastStore.getState().addToast("success", t("explorer.copiedCsv"));
              } catch (e) {
                useToastStore.getState().addToast("error", String(e));
              }
            }}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-300 hover:text-white hover:bg-white/5 rounded transition-colors"
          >
            <span className="material-symbols-outlined text-[14px]">table_chart</span>
            {t("explorer.copyCsv")}
          </button>
          <div className="h-6 w-px bg-slate-700" />
          <button
            disabled={deleting}
            onClick={async () => {
              if (!selectedTable || !tableSchema || selectedRows.size === 0 || deleting) return;
              setDeleting(true);
              const pkIdx = tableSchema.findIndex((c: any) => {
                const cells = Array.isArray(c) ? c : Object.values(c);
                return formatValue(cells[2] ?? "").toUpperCase() === "YES";
              });
              if (pkIdx < 0) {
                useToastStore.getState().addToast("error", t("explorer.noPkForDelete"));
                setDeleting(false);
                return;
              }
              const pkCol = formatValue(Array.isArray(tableSchema[pkIdx]) ? tableSchema[pkIdx][0] : tableSchema[pkIdx]);
              const pkValues = [...selectedRows].map(i => {
                const row = filteredData![i];
                const cells = Array.isArray(row) ? row : Object.values(row);
                return cells[pkIdx];
              });
              const inList = pkValues.map(v => {
                const fv = formatValue(v);
                return isNaN(Number(fv)) ? `'${fv.replace(/'/g, "''")}'` : fv;
              }).join(", ");
              const sql = `DELETE FROM ${escapeSqlIdent(selectedTable)} WHERE ${escapeSqlIdent(pkCol)} IN (${inList})`;
              if (!confirm(t("explorer.confirmDeleteRows", { count: selectedRows.size }))) { setDeleting(false); return; }
              try {
                const res = await execSql(sql);
                if (res.ok) {
                  useToastStore.getState().addToast("success", t("explorer.rowsDeleted", { count: selectedRows.size }));
                  setSelectedRows(new Set());
                  refresh();
                } else {
                  useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
                }
              } catch (e) {
                useToastStore.getState().addToast("error", String(e));
              }
              setDeleting(false);
            }}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-red-400 hover:text-red-300 hover:bg-red-400/10 rounded transition-colors ${deleting ? "opacity-50 pointer-events-none" : ""}`}
          >
            <span className="material-symbols-outlined text-[14px]">delete</span>
            {t("explorer.deleteSelected")}
          </button>
          <button
            onClick={() => setSelectedRows(new Set())}
            className="text-slate-500 hover:text-white transition-colors p-1"
            title={t("common.close")}
          >
            <span className="material-symbols-outlined text-[16px]">close</span>
          </button>
        </div>
      )}

      {/* Pagination */}
      {tableData && tableData.length > 0 && (
        <footer className="h-12 border-t border-border-dark bg-dark-800 flex items-center justify-between px-4 shrink-0">
          <div className="text-xs text-slate-400">
            {t("explorer.showing", { from: page * PAGE_SIZE_CONST + 1, to: page * PAGE_SIZE_CONST + tableData.length, total: totalCount !== null ? totalCount.toLocaleString() : "?" })}
          </div>
          <div className="flex items-center gap-1">
            <button onClick={() => goPage(0)} disabled={page === 0}
              className="w-8 h-8 flex items-center justify-center rounded hover:bg-white/5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
              title={t("common.firstPage")}>
              <span className="material-symbols-outlined text-[18px]">first_page</span>
            </button>
            <button onClick={() => goPage(page - 1)} disabled={page === 0}
              className="w-8 h-8 flex items-center justify-center rounded hover:bg-white/5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
              title={t("common.previousPage")}>
              <span className="material-symbols-outlined text-[18px]">chevron_left</span>
            </button>
            <div className="flex items-center gap-1 mx-2">
              <span className="w-10 h-7 flex items-center justify-center bg-dark-900 border border-border-dark rounded text-xs text-white tabular-nums">{page + 1}</span>
              <span className="text-xs text-slate-400">/ {totalPages}</span>
            </div>
            <button onClick={() => goPage(page + 1)} disabled={totalCount !== null && (page + 1) * PAGE_SIZE_CONST >= totalCount}
              className="w-8 h-8 flex items-center justify-center rounded hover:bg-white/5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
              title={t("common.nextPage")}>
              <span className="material-symbols-outlined text-[18px]">chevron_right</span>
            </button>
            <button onClick={() => goPage(totalPages - 1)} disabled={totalCount !== null && (page + 1) * PAGE_SIZE_CONST >= totalCount}
              className="w-8 h-8 flex items-center justify-center rounded hover:bg-white/5 text-slate-400 hover:text-white transition-colors disabled:opacity-50"
              title={t("common.lastPage")}>
              <span className="material-symbols-outlined text-[18px]">last_page</span>
            </button>
          </div>
        </footer>
      )}
    </>
  );
}
