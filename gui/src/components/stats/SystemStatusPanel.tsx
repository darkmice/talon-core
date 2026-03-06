import { useTranslation } from "react-i18next";
import { useAppStore } from "../../stores/appStore";

export default function SystemStatusPanel() {
  const { t } = useTranslation();
  const { connected, connMode, connLabel } = useAppStore();

  return (
    <div className="bg-surface border border-border-dark rounded-xl">
      <div className="px-5 py-4 border-b border-border-dark">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
          <span className={`material-symbols-outlined text-[18px] ${connected ? "text-emerald-400" : "text-red-400"}`}>monitor_heart</span>
          {t("stats.systemStatus")}
        </h3>
      </div>
      <div className="p-5 flex flex-col items-center">
        {/* Status ring */}
        <div className="relative w-32 h-32 mb-4">
          <svg className="w-full h-full -rotate-90" viewBox="0 0 36 36">
            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
              fill="none" stroke="#1e293b" strokeWidth="3" />
            <path d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
              fill="none" stroke={connected ? "#10b981" : "#f87171"} strokeWidth="3" strokeDasharray={connected ? "100, 100" : "0, 100"} strokeLinecap="round" />
          </svg>
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            <span className={`text-lg font-bold ${connected ? "text-emerald-400" : "text-red-400"}`}>
              {connected ? t("common.connected") : t("common.disconnected")}
            </span>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4 w-full">
          <div className="text-center">
            <p className="text-xs text-slate-400">{t("stats.mode")}</p>
            <p className="text-sm font-semibold text-white mt-0.5">
              {connected ? (connMode === "embedded" ? t("stats.embedded") : t("stats.network")) : "—"}
            </p>
          </div>
          <div className="text-center">
            <p className="text-xs text-slate-400">{t("stats.endpoint")}</p>
            <p className="text-sm font-semibold text-white mt-0.5 truncate" title={connLabel || ""}>
              {connected && connLabel ? connLabel : "—"}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
