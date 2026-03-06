import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useExplorerStore, fmtCount } from "../../stores/explorerStore";
import { useToastStore } from "../../stores/toastStore";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { Input } from "../ui";

export default function TableListPanel() {
  const { t } = useTranslation();
  const {
    tables, tableCounts, filterText, loading, selectedTable, showCreateForm,
    setFilterText, setShowCreateForm, loadTables, selectTable,
  } = useExplorerStore();
  const [droppingTable, setDroppingTable] = useState(false);

  const dropTable = async (name: string) => {
    if (droppingTable) return;
    if (!confirm(t("explorer.confirmDropTable", { name }))) return;
    setDroppingTable(true);
    try {
      const res = await execSql(`DROP TABLE ${escapeSqlIdent(name)}`);
      if (res.ok) {
        useToastStore.getState().addToast("success", t("explorer.tableDropped", { name }));
        if (selectedTable === name) {
          useExplorerStore.setState({ selectedTable: null, tableSchema: null, tableData: null, totalCount: null });
        }
        loadTables();
      } else {
        useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
    setDroppingTable(false);
  };

  const filteredTables = filterText
    ? tables.filter(n => n.toLowerCase().includes(filterText.toLowerCase()))
    : tables;

  return (
    <div className="w-[260px] flex-shrink-0 border-r border-border-dark flex flex-col bg-surface">
      {/* Header */}
      <div className="h-14 flex items-center justify-between px-4 border-b border-border-dark shrink-0">
        <h2 className="text-sm font-semibold text-white">{t("explorer.tables")}</h2>
        <div className="flex gap-1">
          <button
            onClick={() => setShowCreateForm(!showCreateForm)}
            className={`w-7 h-7 flex items-center justify-center rounded transition-colors ${
              showCreateForm
                ? "text-white bg-white/10 hover:bg-white/20"
                : "text-slate-400 hover:text-white hover:bg-white/5"
            }`}
            title={showCreateForm ? t("common.close") : t("explorer.createTable")}
          >
            <span className="material-symbols-outlined text-[18px]">
              {showCreateForm ? "close" : "add"}
            </span>
          </button>
          <button
            onClick={loadTables}
            disabled={loading}
            className="w-7 h-7 flex items-center justify-center text-slate-400 hover:text-white rounded hover:bg-white/5 transition-colors"
            title={t("explorer.refresh")}
          >
            <span className={`material-symbols-outlined text-[18px] ${loading ? "animate-spin" : ""}`}>
              refresh
            </span>
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="p-3 border-b border-border-dark/50">
        <Input
          value={filterText}
          onChange={e => setFilterText(e.target.value)}
          placeholder={t("explorer.filterTables")}
          icon="search"
          size="sm"
          className="w-full"
        />
      </div>

      {/* Table List */}
      <div className="flex-1 overflow-y-auto p-2 space-y-0.5 no-scrollbar">
        <div className="px-3 py-2 text-xs font-bold text-slate-400 uppercase tracking-wider">
          {t("explorer.public")}
        </div>

        {filteredTables.length === 0 && (
          <p className="text-slate-400 text-xs text-center py-6">{t("explorer.noTables")}</p>
        )}

        {filteredTables.map(name => {
          const isSystem = name.startsWith("_");
          // Separate system tables rendering handled below
          if (isSystem) return null;
          return (
            <button
              key={name}
              onClick={() => selectTable(name)}
              className={`w-full flex items-center gap-2 px-3 py-2 text-sm rounded transition-colors mb-0.5 group ${
                selectedTable === name
                  ? "bg-primary text-white"
                  : "text-slate-400 hover:text-slate-200 hover:bg-white/5"
              }`}
            >
              <span className="material-symbols-outlined text-[18px] opacity-80">table_chart</span>
              <span className="flex-1 text-left font-mono truncate" title={name}>{name}</span>
              {tableCounts[name] !== undefined && (
                <span className={`ml-auto text-xs tabular-nums ${
                  selectedTable === name ? "opacity-70" : "text-slate-600 group-hover:text-slate-500"
                }`}>
                  {fmtCount(tableCounts[name])}
                </span>
              )}
              <span role="button" tabIndex={0}
                onClick={e => { e.stopPropagation(); dropTable(name); }}
                onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.stopPropagation(); dropTable(name); } }}
                className="material-symbols-outlined text-[16px] opacity-0 group-hover:opacity-60 hover:!opacity-100 hover:text-red-400 transition-all ml-1 cursor-pointer"
                title={t("explorer.dropTable")}
              >delete</span>
            </button>
          );
        })}

        {/* System tables */}
        {filteredTables.some(n => n.startsWith("_")) && (
          <>
            <div className="px-3 py-2 text-xs font-bold text-slate-400 uppercase tracking-wider mt-4">
              {t("explorer.system")}
            </div>
            {filteredTables.filter(n => n.startsWith("_")).map(name => (
              <button
                key={name}
                onClick={() => selectTable(name)}
                className={`w-full flex items-center gap-2 px-3 py-2 text-sm rounded transition-colors mb-0.5 group ${
                  selectedTable === name
                    ? "bg-primary text-white"
                    : "text-slate-400 hover:text-slate-200 hover:bg-white/5"
                }`}
              >
                <span className="material-symbols-outlined text-[18px] opacity-60">settings_suggest</span>
                <span className="flex-1 text-left font-mono truncate" title={name}>{name}</span>
              </button>
            ))}
          </>
        )}
      </div>
    </div>
  );
}
