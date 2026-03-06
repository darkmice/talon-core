import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "../ui";

interface SqlActionBarProps {
  loading: boolean;
  hasSql: boolean;
  hasSelection?: boolean;
  onRun: () => void;
  onClear: () => void;
  onFormat: () => void;
  cursorPos?: { ln: number; col: number };
  onDragStart?: (e: React.MouseEvent) => void;
}

export default function SqlActionBar({ loading, hasSql, hasSelection, onRun, onClear, onFormat, cursorPos, onDragStart }: SqlActionBarProps) {
  const { t } = useTranslation();
  const [showShortcuts, setShowShortcuts] = useState(false);

  return (
    <div className="shrink-0">
      {onDragStart && (
        <div
          onMouseDown={onDragStart}
          className="h-1 cursor-row-resize hover:bg-primary/30 active:bg-primary/50 transition-colors border-t border-border-dark"
        />
      )}
      <div className="flex items-center gap-3 px-4 py-2.5">
      <Button variant="primary" icon="play_arrow" loading={loading} disabled={!hasSql} onClick={onRun}>
        {hasSelection ? t("sql.runSelected") : t("sql.runQuery")} <span className="text-[10px] opacity-60 ml-1 bg-white/10 px-1.5 py-0.5 rounded">{t("sql.cmdEnter")}</span>
      </Button>
      <Button variant="secondary" icon="delete" disabled={!hasSql} onClick={onClear}>
        {t("sql.clear")}
      </Button>
      <Button variant="secondary" icon="format_align_left" disabled={!hasSql} onClick={onFormat}>
        {t("sql.format")} <span className="text-[10px] opacity-60 ml-1 bg-white/10 px-1.5 py-0.5 rounded">⇧⌘F</span>
      </Button>
      <div className="flex-1" />
      {cursorPos && (
        <span className="text-[11px] text-slate-500 font-mono">
          Ln {cursorPos.ln}, Col {cursorPos.col}
        </span>
      )}
      <div className="relative">
        <button
          onClick={() => setShowShortcuts(!showShortcuts)}
          className="text-slate-600 hover:text-slate-300 transition"
          title={t("sql.shortcuts")}>
          <span className="material-symbols-outlined text-[16px]">keyboard</span>
        </button>
        {showShortcuts && (
          <div className="absolute bottom-8 right-0 bg-dark-700 border border-border-dark rounded-lg shadow-xl p-3 z-50 w-56">
            <p className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold mb-2">{t("sql.shortcuts")}</p>
            {[
              ["⌘ Enter", t("sql.runQuery")],
              ["⇧⌘F", t("sql.format")],
              ["⌘/", t("sql.toggleComment")],
              ["⌘D", t("sql.deleteLine")],
              ["Tab", t("sql.indent")],
            ].map(([key, label]) => (
              <div key={key} className="flex items-center justify-between py-1">
                <span className="text-[11px] text-slate-300">{label}</span>
                <kbd className="text-[10px] bg-dark-900 text-slate-400 px-1.5 py-0.5 rounded font-mono">{key}</kbd>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
    </div>
  );
}
