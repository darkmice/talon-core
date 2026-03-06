import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { getSchemaInfo } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { Tabs, PageHeader } from "../components/ui";
import { NearbyTab, DistanceTab } from "../components/geo";

export default function GeoPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState("nearby");
  const [schema, setSchema] = useState<any>(null);

  useEffect(() => {
    getSchemaInfo().then(res => {
      if (res.ok) setSchema((res as any).data);
      else useToastStore.getState().addToast("error", (res as any).error ?? t("explorer.loadSchemaFailed"));
    }).catch(e => {
      useToastStore.getState().addToast("error", String(e));
    });
  }, []);

  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="location_on" title={t("geo.title")} subtitle={t("geo.subtitle")}>
        <Tabs items={[
          { id: "nearby", label: t("geo.nearbySearch") },
          { id: "distance", label: t("geo.distanceCalc") },
        ]} active={tab} onChange={setTab} />
      </PageHeader>
      <div className="flex-1 overflow-y-auto p-5">
        {tab === "nearby" && <NearbyTab schema={schema} />}
        {tab === "distance" && <DistanceTab />}
      </div>
    </div>
  );
}
