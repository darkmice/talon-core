import { useState } from "react";
import { useTranslation } from "react-i18next";
import { execTs } from "../../lib/tauri";
import { Button, Input, Textarea, Label } from "../ui";

export default function WriteTab() {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [tagsStr, setTagsStr] = useState('{"host": "server-1"}');
  const [fieldsStr, setFieldsStr] = useState('{"cpu": 72.5, "mem": 4096}');
  const [msg, setMsg] = useState<{ type: string; text: string } | null>(null);
  const [writing, setWriting] = useState(false);

  const doWrite = async () => {
    if (!name.trim()) return;
    setMsg(null);
    setWriting(true);
    try {
      let tags: Record<string, string> = {};
      let fields: Record<string, number> = {};
      try { tags = JSON.parse(tagsStr); } catch { setMsg({ type: "error", text: t("ts.invalidTagsJson") }); setWriting(false); return; }
      try { fields = JSON.parse(fieldsStr); } catch { setMsg({ type: "error", text: t("ts.invalidFieldsJson") }); setWriting(false); return; }
      const data = await execTs("write", { name: name.trim(), tags, fields, timestamp: Date.now() * 1000 });
      if (data.ok) {
        setMsg({ type: "success", text: t("ts.writeSuccess") });
      } else {
        setMsg({ type: "error", text: data.error ?? t("ts.writeFailed") });
      }
    } catch (e) { setMsg({ type: "error", text: String(e) }); }
    setWriting(false);
  };

  return (
    <div className="max-w-lg">
      <div className="flex flex-col gap-4">
        <div>
          <Label>{t("ts.metricName")}</Label>
          <Input mono value={name} onChange={e => setName(e.target.value)} placeholder={t("ts.metricPlaceholder")} className="w-full" />
        </div>
        <div>
          <Label>{t("ts.tagsJson")}</Label>
          <Textarea mono value={tagsStr} onChange={e => setTagsStr(e.target.value)} className="h-20" />
        </div>
        <div>
          <Label>{t("ts.fieldsJson")}</Label>
          <Textarea mono value={fieldsStr} onChange={e => setFieldsStr(e.target.value)} className="h-20" />
        </div>
        <Button variant="primary" icon="send" onClick={doWrite} loading={writing} disabled={!name.trim()} className="self-start">
          {t("ts.writePoint")}
        </Button>
      </div>
      {msg && (
        <div className={`mt-3 px-4 py-2.5 rounded-lg text-sm border ${
          msg.type === "success" ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400" : "bg-red-500/10 border-red-500/30 text-red-400"
        }`}>{msg.text}</div>
      )}
    </div>
  );
}
