import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSqlStore } from "../../stores/sqlStore";

export default function SqlTabBar() {
  const { t } = useTranslation();
  const { tabs, activeTab, setActiveTab, addTab, closeTab, renameTab } = useSqlStore();
  const [editingTabId, setEditingTabId] = useState<number | null>(null);
  const [editingName, setEditingName] = useState("");

  return (
    <div data-tauri-drag-region className="flex items-center border-b border-border-dark shrink-0 bg-dark-900 overflow-x-auto no-scrollbar">
      {tabs.map(tab => (
        <button key={tab.id} onClick={() => setActiveTab(tab.id)}
          onDoubleClick={() => { setEditingTabId(tab.id); setEditingName(tab.name); }}
          className={`flex items-center gap-2 px-4 py-2.5 text-xs font-medium border-r border-border-dark transition group
            ${activeTab === tab.id ? "bg-dark-800 text-white" : "bg-dark-900 text-slate-500 hover:text-slate-300"}`}>
          <span className="material-symbols-outlined text-[14px] text-primary">code</span>
          {tab.result?.rows !== null && tab.result?.rows !== undefined && (
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0" title={`${tab.result.rows.length} rows`} />
          )}
          {tab.result?.error && (
            <span className="w-1.5 h-1.5 rounded-full bg-red-400 shrink-0" />
          )}
          {editingTabId === tab.id ? (
            <input
              autoFocus
              value={editingName}
              onChange={e => setEditingName(e.target.value)}
              onBlur={() => { if (editingName.trim()) renameTab(tab.id, editingName.trim()); setEditingTabId(null); }}
              onKeyDown={e => { if (e.key === "Enter") { if (editingName.trim()) renameTab(tab.id, editingName.trim()); setEditingTabId(null); } if (e.key === "Escape") setEditingTabId(null); }}
              onClick={e => e.stopPropagation()}
              className="bg-transparent border-b border-primary text-white text-xs outline-none w-24"
            />
          ) : tab.name}
          {tabs.length > 1 && editingTabId !== tab.id && (
            <span role="button" tabIndex={0} onClick={e => { e.stopPropagation(); closeTab(tab.id); }}
              onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.stopPropagation(); closeTab(tab.id); } }}
              className="ml-1 text-slate-600 hover:text-red-400 transition opacity-0 group-hover:opacity-100 cursor-pointer text-xs"
              title={t("sql.closeTab")}>×</span>
          )}
        </button>
      ))}
      <button onClick={addTab}
        className="px-3 py-2.5 text-slate-600 hover:text-slate-300 transition"
        title={t("sql.newTab")}>
        <span className="material-symbols-outlined text-[16px]">add</span>
      </button>
    </div>
  );
}
