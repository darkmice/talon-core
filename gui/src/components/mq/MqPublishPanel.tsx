import { useTranslation } from "react-i18next";
import { useToastStore } from "../../stores/toastStore";
import { Button, Input, Select, Textarea, Label } from "../ui";

interface MqPublishPanelProps {
  topics: { name: string }[];
  selectedTopic: string;
  msgKey: string;
  payload: string;
  format: string;
  publishing: boolean;
  onSelectTopic: (name: string) => void;
  onMsgKeyChange: (v: string) => void;
  onPayloadChange: (v: string) => void;
  onFormatChange: (f: string) => void;
  onPublish: () => void;
}

export default function MqPublishPanel({
  topics, selectedTopic, msgKey, payload, format, publishing,
  onSelectTopic, onMsgKeyChange, onPayloadChange, onFormatChange, onPublish,
}: MqPublishPanelProps) {
  const { t } = useTranslation();

  return (
    <div className="w-80 border-l border-border-dark bg-sidebar flex flex-col shrink-0">
      <div className="flex items-center justify-between px-4 py-4 border-b border-border-dark">
        <div className="flex items-center gap-2">
          <span className="material-symbols-outlined text-orange-400 text-[18px]">send</span>
          <span className="text-sm font-semibold text-white">{t("mq.publishTitle")}</span>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-4 no-scrollbar">
        <div>
          <Label>{t("mq.targetTopic")}</Label>
          <Select value={selectedTopic} onValueChange={onSelectTopic}
            options={topics.map(({ name }) => ({ value: name, label: name }))}
            className="w-full" />
        </div>
        <div>
          <Label>{t("mq.messageKey")}</Label>
          <Input mono size="sm" value={msgKey} onChange={e => onMsgKeyChange(e.target.value)} placeholder={t("mq.messageKeyPlaceholder")} className="w-full" />
        </div>
        <div>
          <Label>{t("mq.format")}</Label>
          <div className="flex bg-dark-800 rounded-lg border border-border-dark p-0.5">
            {["JSON", "Text", "Binary"].map(f => (
              <button key={f} onClick={() => onFormatChange(f)}
                className={`flex-1 px-3 py-1.5 rounded-md text-xs font-medium transition ${format === f ? "bg-primary text-white" : "text-slate-500 hover:text-slate-300"}`}>
                {f}
              </button>
            ))}
          </div>
        </div>
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <Label className="mb-0">{t("mq.payload")}</Label>
            <button type="button" onClick={() => {
              try { onPayloadChange(JSON.stringify(JSON.parse(payload), null, 2)); } catch {
                useToastStore.getState().addToast("error", t("common.invalidJson"));
              }
            }} className="text-xs text-primary cursor-pointer hover:underline">{t("mq.prettyJson")}</button>
          </div>
          <Textarea mono value={payload} onChange={e => onPayloadChange(e.target.value)} className="h-40" />
        </div>
      </div>
      <div className="p-4 border-t border-border-dark">
        <Button variant="primary" icon="send" size="lg" loading={publishing} disabled={!selectedTopic || !payload.trim()} onClick={onPublish} className="w-full">
          {t("mq.publishBtn")}
        </Button>
      </div>
    </div>
  );
}
