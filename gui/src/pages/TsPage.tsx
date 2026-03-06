import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Tabs, PageHeader } from "../components/ui";
import { QueryTab, WriteTab, CreateTab } from "../components/ts";

export default function TsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState("query");
  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="schedule" title={t("ts.title")} subtitle={t("ts.subtitle")}>
        <Tabs items={[
          { id: "query", label: t("ts.query") },
          { id: "write", label: t("ts.write") },
          { id: "create", label: t("ts.create") },
        ]} active={tab} onChange={setTab} />
      </PageHeader>
      <div className="flex-1 overflow-y-auto p-5">
        {tab === "query" && <QueryTab />}
        {tab === "write" && <WriteTab />}
        {tab === "create" && <CreateTab />}
      </div>
    </div>
  );
}
