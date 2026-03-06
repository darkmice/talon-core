import { useTranslation } from "react-i18next";
import { useExplorerStore, type ExplorerTab } from "../../stores/explorerStore";

interface Props {}

const tabs: ExplorerTab[] = ["data", "structure", "indexes"];

export default function ExplorerHeader({}: Props) {
  const { t } = useTranslation();
  const { selectedTable, totalCount, tab, setTab, triggerAddColumn } = useExplorerStore();

  if (!selectedTable) return null;

  return (
    <header data-tauri-drag-region className="h-14 border-b border-border-dark flex items-center justify-between px-5 bg-dark-800 shrink-0">
      {/* Left: table name + row count */}
      <div className="flex items-center gap-2">
        <h2 className="text-lg font-bold text-white tracking-tight max-w-[300px] truncate" title={selectedTable}>{selectedTable}</h2>
        {totalCount !== null && (
          <span className="px-2 py-0.5 rounded-full bg-slate-800 border border-border-dark text-xs text-slate-400 font-mono">
            {totalCount.toLocaleString()} {t("common.rows")}
          </span>
        )}
      </div>

      {/* Center: tabs */}
      <div className="flex h-full">
        {tabs.map(id => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`h-full px-4 text-sm font-medium border-b-2 transition-colors ${
              tab === id
                ? "border-primary text-primary"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
          >
            {t(`explorer.${id}`)}
          </button>
        ))}
      </div>

      {/* Right: actions */}
      <div className="flex items-center gap-2">
        {tab === "structure" && (
          <button
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium bg-surface hover:bg-slate-700 text-white rounded transition-colors border border-border-dark"
            title={t("explorer.addColumnBtn")}
            onClick={triggerAddColumn}
          >
            <span className="material-symbols-outlined text-[16px]">add</span>
            {t("explorer.addColumnBtn")}
          </button>
        )}
      </div>
    </header>
  );
}
