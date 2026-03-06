import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSqlStore } from "../../stores/sqlStore";
import { Badge, Input } from "../ui";
import { fmtTime } from "./sqlUtils";

interface SqlHistoryPanelProps {
  onSelectSql: (sql: string) => void;
  onRunSql?: (sql: string) => void;
}

export default function SqlHistoryPanel({ onSelectSql, onRunSql }: SqlHistoryPanelProps) {
  const { t } = useTranslation();
  const { history, clearHistory, removeHistory } = useSqlStore();
  const [showSearch, setShowSearch] = useState(false);
  const [searchQ, setSearchQ] = useState("");
  const [collapsed, setCollapsed] = useState(false);

  if (collapsed) {
    return (
      <div className="w-8 border-l border-border-dark bg-sidebar flex flex-col items-center shrink-0">
        <button onClick={() => setCollapsed(false)} className="py-3 text-slate-500 hover:text-white transition" title={t("sql.history")}>
          <span className="material-symbols-outlined text-[16px]">chevron_left</span>
        </button>
        <span className="text-[9px] text-slate-600 writing-vertical-lr tracking-widest mt-1">{t("sql.history")}</span>
      </div>
    );
  }

  return (
    <div className="w-56 border-l border-border-dark bg-sidebar flex flex-col shrink-0">
      <div className="flex items-center justify-between px-3 py-3 border-b border-border-dark">
        <span className="text-xs font-semibold text-white uppercase tracking-wider">{t("sql.history")}</span>
        <div className="flex gap-1">
          <button onClick={() => setCollapsed(true)} className="text-slate-500 hover:text-white transition" title={t("common.collapse")}>
            <span className="material-symbols-outlined text-[16px]">chevron_right</span>
          </button>
          <button onClick={() => { setShowSearch(!showSearch); if (showSearch) setSearchQ(""); }} className="text-slate-500 hover:text-white transition" title={t("common.search")}>
            <span className="material-symbols-outlined text-[16px]">{showSearch ? "close" : "search"}</span>
          </button>
          <button onClick={() => { if (history.length === 0 || confirm(t("sql.confirmClearHistory"))) clearHistory(); }}
            className="text-slate-500 hover:text-red-400 transition" title={t("sql.clearHistory")}>
            <span className="material-symbols-outlined text-[16px]">delete</span>
          </button>
        </div>
      </div>
      {showSearch && (
        <div className="px-3 py-2 border-b border-border-dark">
          <Input
            value={searchQ}
            onChange={e => setSearchQ(e.target.value)}
            placeholder={t("sql.filterHistory")}
            size="sm"
            icon="search"
            autoFocus
          />
        </div>
      )}
      <div className="flex-1 overflow-y-auto no-scrollbar">
        {history.length === 0 && <p className="text-slate-400 text-xs text-center py-8">{t("common.noData")}</p>}
        {history.filter(h => !searchQ.trim() || h.sql.toLowerCase().includes(searchQ.toLowerCase())).map((h, i) => (
          <button key={h.time} onClick={() => onSelectSql(h.sql)} onDoubleClick={() => onRunSql?.(h.sql)}
            className="w-full text-left px-3 py-3 border-b border-border-dark hover:bg-white/[0.03] transition group">
            <div className="flex items-center justify-between mb-1">
              <Badge variant={h.ok ? "success" : "danger"} size="sm">
                {h.ok ? t("sql.success") : t("sql.error")}
              </Badge>
              <div className="flex items-center gap-1">
                <span className="text-[10px] text-slate-500">{fmtTime(h.time, t)}</span>
                <span role="button" tabIndex={0}
                  onClick={e => { e.stopPropagation(); removeHistory(h.time); }}
                  onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.stopPropagation(); removeHistory(h.time); } }}
                  className="text-slate-600 hover:text-red-400 transition opacity-0 group-hover:opacity-100 cursor-pointer"
                  title={t("common.delete")}>
                  <span className="material-symbols-outlined text-[13px]">close</span>
                </span>
              </div>
            </div>
            <p className="font-mono text-[11px] text-slate-400 group-hover:text-white transition leading-relaxed line-clamp-2" title={h.sql}>
              {h.sql}
            </p>
            {h.ok && <p className="text-[10px] text-slate-500 mt-1">{h.ms}ms · {h.rowCount} {t("common.rows")}</p>}
          </button>
        ))}
      </div>
    </div>
  );
}
