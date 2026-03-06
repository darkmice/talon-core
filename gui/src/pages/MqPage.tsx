import { useState, useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { execMq } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { Button, Input, PageHeader } from "../components/ui";
import { MqTopicTable, MqPublishPanel } from "../components/mq";

export default function MqPage() {
  const { t } = useTranslation();
  const addToast = useToastStore(s => s.addToast);
  const [topics, setTopics] = useState<{ name: string; len: number }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [newTopic, setNewTopic] = useState("");
  const [selectedTopic, setSelectedTopic] = useState("");
  const [msgKey, setMsgKey] = useState("");
  const [payload, setPayload] = useState('{\n  "key": "value"\n}');
  const [format, setFormat] = useState("JSON");
  const [publishing, setPublishing] = useState(false);
  const [peeked, setPeeked] = useState<any[] | null>(null);
  const [peekLoading, setPeekLoading] = useState(false);
  const [filterTopicText, setFilterTopicText] = useState("");
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const peekTopicRef = useRef("");

  const loadTopics = async (autoSelect = false) => {
    if (topicsLoading) return;
    setTopicsLoading(true);
    setError(null);
    try {
      const data = await execMq("topics", {});
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setTopicsLoading(false); return; }
      const list = (data.data as any)?.topics || [];
      const results = [];
      for (const tp of list) {
        const name: string = typeof tp === "string" ? tp : tp.name || JSON.stringify(tp);
        try {
          const ld = await execMq("len", { topic: name });
          results.push({ name, len: (ld.data as any)?.len ?? 0 });
        } catch { results.push({ name, len: 0 }); }
      }
      setTopics(results);
      if (results.length > 0 && (autoSelect || !selectedTopic)) setSelectedTopic(results[0].name);
    } catch (e) { setError(String(e)); }
    setTopicsLoading(false);
  };

  const createTopic = async () => {
    if (!newTopic.trim() || creating) return;
    setCreating(true);
    try {
      const data = await execMq("create", { topic: newTopic.trim(), max_len: 0 });
      if (data.ok) { addToast("success", t("mq.topicCreated", { name: newTopic.trim() })); setNewTopic(""); loadTopics(); }
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setCreating(false);
  };

  const publish = async () => {
    if (!selectedTopic || !payload.trim()) return;
    setPublishing(true);
    try {
      const data = await execMq("publish", { topic: selectedTopic, payload: payload.trim() });
      if (data.ok) { addToast("success", t("mq.messagePublished")); loadTopics(); }
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setPublishing(false);
  };

  const deleteTopic = async (name: string) => {
    if (deleting) return;
    if (!confirm(t("mq.deleteConfirm", { name }))) return;
    setDeleting(true);
    try {
      const data = await execMq("drop", { topic: name });
      if (data.ok) {
        const wasSelected = selectedTopic === name;
        if (wasSelected) { setSelectedTopic(""); setPeeked(null); }
        loadTopics(wasSelected);
      }
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setDeleting(false);
  };

  const peekMessages = async (topic: string, count = 5) => {
    peekTopicRef.current = topic;
    setPeekLoading(true); setPeeked(null);
    try {
      const data = await execMq("peek", { topic, count });
      if (peekTopicRef.current !== topic) return;
      if (data.ok) setPeeked((data.data as any)?.messages || []);
      else { setPeeked([]); addToast("error", data.error ?? t("common.unknownError")); }
    } catch (e) {
      if (peekTopicRef.current !== topic) return;
      setPeeked([]); addToast("error", String(e));
    }
    if (peekTopicRef.current !== topic) return;
    setPeekLoading(false);
  };

  const handleSelectTopic = (name: string) => {
    setSelectedTopic(name);
    peekMessages(name);
  };

  useEffect(() => { loadTopics(); }, []);

  return (
    <div className="flex h-full">
      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <PageHeader icon="forum" title={t("mq.title")} subtitle={t("mq.subtitle")}>
          <div className="flex-1 mx-4">
            <Input icon="search" placeholder={t("mq.filterTopics")} size="sm" className="max-w-xs"
              value={filterTopicText} onChange={e => setFilterTopicText(e.target.value)} />
          </div>
          <div className="flex items-center gap-2">
            <Input mono size="sm" value={newTopic} onChange={e => setNewTopic(e.target.value)}
              onKeyDown={e => e.key === "Enter" && createTopic()}
              placeholder={t("mq.newTopicPlaceholder")} className="w-40" />
            <Button variant="primary" icon="add" size="sm" onClick={createTopic} loading={creating} disabled={!newTopic.trim()}>
              {t("mq.create")}
            </Button>
          </div>
          <Button variant="ghost" icon="refresh" size="sm" onClick={() => loadTopics()} loading={topicsLoading} title={t("common.refresh")} />
        </PageHeader>

        <MqTopicTable topics={topics && filterTopicText ? topics.filter(tp => tp.name.toLowerCase().includes(filterTopicText.toLowerCase())) : topics} error={error} selectedTopic={selectedTopic}
          peeked={peeked} peekLoading={peekLoading}
          onSelectTopic={handleSelectTopic} onDeleteTopic={deleteTopic}
          onPeekRefresh={() => peekMessages(selectedTopic)} />
      </div>

      <MqPublishPanel topics={topics || []} selectedTopic={selectedTopic}
        msgKey={msgKey} payload={payload} format={format} publishing={publishing}
        onSelectTopic={setSelectedTopic} onMsgKeyChange={setMsgKey}
        onPayloadChange={setPayload} onFormatChange={setFormat} onPublish={publish} />
    </div>
  );
}
