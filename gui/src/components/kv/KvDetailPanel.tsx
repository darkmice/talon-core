import { useTranslation } from "react-i18next";
import { Button, Badge } from "../ui";
import { useToastStore } from "../../stores/toastStore";
import { fmtSize, guessType, fmtJson, typeBadgeVariant } from "./kvUtils";

interface KvDetailPanelProps {
  item: { key: string; value: string; size: number };
  onClose: () => void;
  onDelete: () => void;
}

export default function KvDetailPanel({ item, onClose, onDelete }: KvDetailPanelProps) {
  const { t } = useTranslation();
  const tp = guessType(item.value);

  const copyValue = async () => {
    try {
      await navigator.clipboard.writeText(item.value);
      useToastStore.getState().addToast("success", t("kv.valueCopied"));
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
  };

  return (
    <div className="w-80 border-l border-border-dark bg-sidebar flex flex-col shrink-0">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border-dark">
        <span className="text-xs font-semibold text-white uppercase tracking-wider">{t("kv.details")}</span>
        <button onClick={onClose} className="text-slate-500 hover:text-white transition" title={t("common.close")}>
          <span className="material-symbols-outlined text-[16px]">close</span>
        </button>
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        <div>
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">{t("kv.key")}</p>
          <p className="font-mono text-sm text-emerald-400 break-all">{item.key}</p>
        </div>
        <div>
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">{t("kv.type")}</p>
          <Badge variant={(typeBadgeVariant[tp] || "default") as any}>{tp}</Badge>
        </div>
        <div>
          <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1">{t("kv.size")}</p>
          <p className="text-sm text-slate-300">{fmtSize(item.size)}</p>
        </div>
        <div>
          <div className="flex items-center justify-between mb-1">
            <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">{t("kv.value")}</p>
            <button onClick={copyValue} className="text-[10px] text-primary hover:underline">{t("kv.copy")}</button>
          </div>
          <pre className="bg-dark-800 border border-border-dark rounded-lg p-3 text-[12px] font-mono text-slate-300 whitespace-pre-wrap break-all max-h-[300px] overflow-y-auto">
            {fmtJson(item.value)}
          </pre>
        </div>
      </div>
      <div className="px-4 py-3 border-t border-border-dark flex gap-2">
        <Button variant="danger" icon="delete" size="sm" className="flex-1" onClick={onDelete}>
          {t("kv.delete")}
        </Button>
      </div>
    </div>
  );
}
