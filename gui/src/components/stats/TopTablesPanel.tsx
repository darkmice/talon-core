import { useTranslation } from "react-i18next";

interface TopTablesPanelProps {
  tableDetails: { name: string; count: number }[];
  maxCount: number;
}

export default function TopTablesPanel({ tableDetails, maxCount }: TopTablesPanelProps) {
  const { t } = useTranslation();

  return (
    <div className="lg:col-span-2 bg-surface border border-border-dark rounded-xl">
      <div className="flex items-center justify-between px-5 py-4 border-b border-border-dark">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
          <span className="material-symbols-outlined text-primary text-[18px]">bar_chart</span>
          {t("stats.topTables")}
        </h3>
      </div>
      <div className="p-5 space-y-3">
        {tableDetails.length === 0 && <p className="text-slate-400 text-sm text-center py-4">{t("common.noData")}</p>}
        {tableDetails.map(({ name, count }) => (
          <div key={name} className="flex items-center gap-3">
            <span className="font-mono text-[13px] text-white w-48 truncate" title={name}>{name}</span>
            <div className="flex-1 h-3 bg-dark-800 rounded-full overflow-hidden">
              <div className="h-full bg-gradient-to-r from-blue-500 to-purple-500 rounded-full transition-all"
                style={{ width: `${Math.max(2, (count / maxCount) * 100)}%` }} />
            </div>
            <span className="text-xs text-slate-400 font-mono w-28 text-right tabular-nums">
              {typeof count === "number" ? count.toLocaleString() : count} {t("common.rows")}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
