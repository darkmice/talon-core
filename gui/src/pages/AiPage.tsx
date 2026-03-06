import { useState, useRef } from "react";
import { useTranslation } from "react-i18next";
import { execAi } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { Tabs, Button, PageHeader } from "../components/ui";
import { SessionsTab, MemoryTab, RagTab } from "../components/ai";
import type { SessionsTabHandle } from "../components/ai/SessionsTab";

export default function AiPage() {
  const { t } = useTranslation();
  const addToast = useToastStore(s => s.addToast);
  const [tab, setTab] = useState("sessions");
  const [creatingSession, setCreatingSession] = useState(false);
  const sessionsRef = useRef<SessionsTabHandle>(null);

  const createSession = async () => {
    if (creatingSession) return;
    setCreatingSession(true);
    try {
      const data = await execAi("create_session");
      if (data.ok) {
        addToast("success", t("ai.sessionCreated"));
        setTab("sessions");
        sessionsRef.current?.reload();
      } else {
        addToast("error", data.error ?? t("common.unknownError"));
      }
    } catch (e) {
      addToast("error", String(e));
    }
    setCreatingSession(false);
  };
  const tabItems = [
    { id: "sessions", label: t("ai.sessions") },
    { id: "memory", label: t("ai.memory") },
    { id: "rag", label: t("ai.ragDocuments") },
  ];
  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="smart_toy" title={t("ai.title")} subtitle={t("ai.subtitle")}>
        <Tabs items={tabItems} active={tab} onChange={setTab} />
        <Button variant="primary" icon="add" size="sm" onClick={createSession} loading={creatingSession}>{t("ai.newSession")}</Button>
      </PageHeader>
      <div className="flex-1 overflow-y-auto p-5">
        {tab === "sessions" && <SessionsTab ref={sessionsRef} />}
        {tab === "memory" && <MemoryTab />}
        {tab === "rag" && <RagTab />}
      </div>
    </div>
  );
}
