import { useState } from "react";
import { useTranslation } from "react-i18next";
import { execTs } from "../../lib/tauri";
import { Button, Input, Label } from "../ui";

export default function CreateTab() {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [tags, setTags] = useState("host");
  const [fields, setFields] = useState("cpu,mem");
  const [msg, setMsg] = useState<{ type: string; text: string } | null>(null);
  const [creating, setCreating] = useState(false);

  const doCreate = async () => {
    if (!name.trim() || creating) return;
    setMsg(null);
    setCreating(true);
    try {
      const tagArr = tags.split(",").map(s => s.trim()).filter(Boolean);
      const fieldArr = fields.split(",").map(s => s.trim()).filter(Boolean);
      const data = await execTs("create", { name: name.trim(), tags: tagArr, fields: fieldArr });
      if (data.ok) {
        setMsg({ type: "success", text: t("ts.createSuccess", { name }) });
        setName("");
      } else {
        setMsg({ type: "error", text: data.error ?? t("common.unknownError") });
      }
    } catch (e) {
      setMsg({ type: "error", text: String(e) });
    }
    setCreating(false);
  };

  return (
    <div className="max-w-lg">
      <div className="flex flex-col gap-4">
        <div>
          <Label>{t("ts.metricName")}</Label>
          <Input mono value={name} onChange={e => setName(e.target.value)} placeholder={t("ts.metricPlaceholder")} className="w-full" />
        </div>
        <div>
          <Label>{t("ts.tags")}</Label>
          <Input mono value={tags} onChange={e => setTags(e.target.value)} placeholder={t("ts.tagsPlaceholder")} className="w-full" />
        </div>
        <div>
          <Label>{t("ts.fields")}</Label>
          <Input mono value={fields} onChange={e => setFields(e.target.value)} placeholder={t("ts.fieldsPlaceholder")} className="w-full" />
        </div>
        <Button variant="primary" icon="add" onClick={doCreate} loading={creating} disabled={!name.trim()} className="self-start">
          {t("ts.createMetric")}
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
