import { useTranslation } from "react-i18next";
import { useToastStore } from "../../stores/toastStore";
import { Button, Input, Textarea, Label } from "../ui";

interface KvCreateFormProps {
  newKey: string;
  newVal: string;
  newTtl: string;
  saving?: boolean;
  onKeyChange: (v: string) => void;
  onValChange: (v: string) => void;
  onTtlChange: (v: string) => void;
  onSave: () => void;
}

export default function KvCreateForm({ newKey, newVal, newTtl, saving, onKeyChange, onValChange, onTtlChange, onSave }: KvCreateFormProps) {
  const { t } = useTranslation();

  return (
    <div className="px-5 py-4 border-b border-border-dark bg-dark-900/50 shrink-0">
      <div className="flex items-center gap-2 mb-3">
        <span className="material-symbols-outlined text-emerald-400 text-[16px]">add_circle</span>
        <span className="text-sm font-semibold text-white">{t("kv.createTitle")}</span>
      </div>
      <div className="flex gap-4">
        <div className="flex-1">
          <div className="flex gap-3 mb-3">
            <div className="flex-1">
              <Label>{t("kv.keyLabel")}</Label>
              <Input mono value={newKey} onChange={e => onKeyChange(e.target.value)} placeholder={t("kv.keyPlaceholder")} className="w-full" />
            </div>
            <div className="w-40">
              <Label>{t("kv.ttlLabel")}</Label>
              <Input mono value={newTtl} onChange={e => onTtlChange(e.target.value)} placeholder={t("kv.ttlPlaceholder")} className="w-full" />
            </div>
          </div>
          <Label>{t("kv.valueLabel")}</Label>
          <Textarea mono value={newVal} onChange={e => onValChange(e.target.value)}
            placeholder={t("kv.valuePlaceholder")} className="h-20" />
          <div className="flex items-center justify-between mt-2">
            <span className="text-[10px] text-slate-400">{t("kv.formatHint")}</span>
            <button type="button" onClick={() => {
              try { onValChange(JSON.stringify(JSON.parse(newVal), null, 2)); } catch {
                useToastStore.getState().addToast("error", t("common.invalidJson"));
              }
            }} className="text-xs text-primary cursor-pointer hover:underline">{t("kv.formatJson")}</button>
          </div>
        </div>
      </div>
      <Button variant="primary" icon="save" onClick={onSave} loading={saving} disabled={!newKey.trim()} className="mt-3 w-full" size="lg">
        {t("kv.saveKey")}
      </Button>
    </div>
  );
}
