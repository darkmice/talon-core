import { useTranslation } from "react-i18next";
import { useAppStore, type PageId } from "../stores/appStore";

const navItems: { id: PageId; icon: string; i18n: string; shortcut: string }[] = [
  { id: "connect", icon: "link", i18n: "nav.connect", shortcut: "⌘1" },
  { id: "explorer", icon: "database", i18n: "nav.explorer", shortcut: "⌘2" },
  { id: "sql", icon: "code", i18n: "nav.sql", shortcut: "⌘3" },
  { id: "kv", icon: "vpn_key", i18n: "nav.kv", shortcut: "⌘4" },
  { id: "mq", icon: "forum", i18n: "nav.mq", shortcut: "⌘5" },
  { id: "vector", icon: "explore", i18n: "nav.vector", shortcut: "⌘6" },
  { id: "geo", icon: "location_on", i18n: "nav.geo", shortcut: "⌘7" },
  { id: "fts", icon: "search", i18n: "nav.fts", shortcut: "" },
  { id: "graph", icon: "hub", i18n: "nav.graph", shortcut: "" },
  { id: "ai", icon: "smart_toy", i18n: "nav.ai", shortcut: "⌘8" },
  { id: "ts", icon: "schedule", i18n: "nav.ts", shortcut: "⌘9" },
  { id: "stats", icon: "bar_chart", i18n: "nav.stats", shortcut: "⌘0" },
];

export default function Sidebar() {
  const { t } = useTranslation();
  const { page, setPage, connected, connLabel, connMode } = useAppStore();
  return (
    <aside className="w-[240px] flex-shrink-0 bg-sidebar border-r border-border-dark flex flex-col justify-between z-20">
      {/* Drag region for macOS traffic lights area */}
      <div data-tauri-drag-region className="h-[52px] shrink-0" />
      {/* Logo */}
      <div className="px-5 pb-5">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white font-bold text-lg">T</div>
          <div>
            <h1 className="text-white text-base font-bold tracking-tight">{t("app.name")}</h1>
            <p className="text-slate-400 text-xs font-medium">{t("app.subtitle")}</p>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto px-3 py-2 space-y-1 no-scrollbar">
        {navItems.map(({ id, icon, i18n: key, shortcut }) => {
          const disabled = id !== "connect" && !connected;
          const active = page === id;
          return (
            <button
              key={id}
              onClick={() => !disabled && setPage(id)}
              disabled={disabled}
              className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors relative group
                ${active
                  ? "bg-primary/20 text-primary border border-primary/20"
                  : "text-slate-400 hover:bg-white/5 hover:text-white border border-transparent"}
                ${disabled ? "opacity-30 cursor-not-allowed" : "cursor-pointer"}`}
            >
              {active && <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-5 bg-primary rounded-r-full" />}
              <span className="material-symbols-outlined text-[20px]">{icon}</span>
              <span className="flex-1 text-left">{t(key)}</span>
              <span className="text-[10px] text-slate-600 font-mono opacity-0 group-hover:opacity-100 transition-opacity">{shortcut}</span>
            </button>
          );
        })}
      </nav>

      {/* Bottom: Connection + Settings */}
      <div className="p-4 border-t border-border-dark">
        <div className="flex items-center gap-3 mb-4 px-2">
          <div className="relative flex items-center justify-center w-2 h-2">
            {connected && <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />}
            <span className={`relative inline-flex rounded-full h-2 w-2 ${connected ? "bg-green-500" : "bg-red-400"}`} />
          </div>
          <div>
            <p className="text-xs font-semibold text-white">
              {connected ? t("common.connected") : t("common.disconnected")}
            </p>
            {connected && connLabel && (
              <p className="text-[10px] text-slate-400 font-mono flex items-center gap-1 max-w-[180px]">
                <span className="material-symbols-outlined text-[10px] shrink-0">{connMode === "embedded" ? "folder" : "dns"}</span>
                <span className="truncate" title={connLabel}>{connLabel}</span>
              </p>
            )}
          </div>
        </div>
        <button
          onClick={() => setPage("settings")}
          className="w-full flex items-center justify-center gap-2 h-9 rounded-lg bg-surface hover:bg-slate-700 text-slate-300 text-sm font-medium transition-colors border border-border-dark"
        >
          <span className="material-symbols-outlined text-[18px]">settings</span>
          <span>{t("nav.settings")}</span>
        </button>
      </div>
    </aside>
  );
}
