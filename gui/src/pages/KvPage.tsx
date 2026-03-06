import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { execKv } from "../lib/tauri";
import { useToastStore } from "../stores/toastStore";
import { Button, Input, PageHeader } from "../components/ui";
import { KvCreateForm, KvTable, KvDetailPanel } from "../components/kv";

export default function KvPage() {
  const { t } = useTranslation();
  const addToast = useToastStore(s => s.addToast);
  const [prefix, setPrefix] = useState("");
  const [items, setItems] = useState<{ key: string; value: string; size: number }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [newVal, setNewVal] = useState("");
  const [newTtl, setNewTtl] = useState("-1");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [deletingKey, setDeletingKey] = useState(false);

  const doSearch = async () => {
    if (loading) return;
    setError(null);
    setLoading(true);
    try {
      const data = await execKv("keys", { prefix });
      if (!data.ok) { setError(data.error ?? t("common.unknownError")); setLoading(false); return; }
      const keys: string[] = (data.data as any)?.keys || [];
      const results = [];
      let failCount = 0;
      for (const key of keys.slice(0, 200)) {
        try {
          const vd = await execKv("get", { key });
          const val: string = (vd.data as any)?.value ?? "";
          const size = new Blob([val]).size;
          results.push({ key, value: val, size });
        } catch {
          results.push({ key, value: "-", size: 0 });
          failCount++;
        }
      }
      if (failCount > 0) addToast("warning", t("kv.partialLoadFailed", { count: failCount }));
      setItems(results);
    } catch (e) { setError(String(e)); }
    setLoading(false);
  };

  useEffect(() => { doSearch(); }, []);

  const doSet = async () => {
    if (!newKey.trim() || saving) return;
    setSaving(true);
    const params: Record<string, any> = { key: newKey, value: newVal };
    const ttl = parseInt(newTtl);
    if (ttl > 0) params.ttl = ttl;
    try {
      const data = await execKv("set", params);
      if (data.ok) {
        addToast("success", t("kv.keySaved", { key: newKey }));
        setShowForm(false); setNewKey(""); setNewVal(""); setNewTtl("-1"); doSearch();
      } else {
        addToast("error", data.error ?? t("common.unknownError"));
      }
    } catch (e) { addToast("error", String(e)); }
    setSaving(false);
  };

  const doDel = async (key: string) => {
    if (deletingKey) return;
    if (!confirm(t("kv.deleteConfirm", { key }))) return;
    setDeletingKey(true);
    try {
      const data = await execKv("del", { key });
      if (data.ok) {
        if (selectedKey === key) setSelectedKey(null);
        doSearch();
      }
      else addToast("error", data.error ?? t("common.unknownError"));
    } catch (e) { addToast("error", String(e)); }
    setDeletingKey(false);
  };

  const selectedItem = selectedKey ? items?.find(i => i.key === selectedKey) : null;

  return (
    <div className="flex h-full">
      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <PageHeader icon="vpn_key" title={t("kv.title")} subtitle={t("kv.subtitle")}>
          <div className="flex-1 mx-4">
            <Input icon="search" value={prefix} onChange={e => setPrefix(e.target.value)}
              onKeyDown={e => { if (e.key === "Enter") doSearch(); if (e.key === "Escape") { (e.target as HTMLInputElement).value = ""; setPrefix(""); } }}
              placeholder={t("kv.searchPlaceholder")} className="max-w-lg" />
          </div>
          {items && (
            <div className="flex items-center gap-2 bg-surface border border-border-dark rounded-lg px-3 py-1.5">
              <span className="material-symbols-outlined text-emerald-400 text-[16px]">database</span>
              <span className="text-sm font-semibold text-white">{items.length.toLocaleString()}</span>
              <span className="text-xs text-slate-400">{t("kv.keysLabel")}</span>
            </div>
          )}
          <Button variant="primary" icon="add" size="sm" onClick={() => setShowForm(!showForm)}>
            {t("kv.addNewKey")}
          </Button>
        </PageHeader>

        {showForm && (
          <KvCreateForm newKey={newKey} newVal={newVal} newTtl={newTtl} saving={saving}
            onKeyChange={setNewKey} onValChange={setNewVal} onTtlChange={setNewTtl} onSave={doSet} />
        )}

        <KvTable items={items} error={error} selectedKey={selectedKey} loading={loading}
          onSelectKey={setSelectedKey} onDeleteKey={doDel} />

        {items && items.length > 0 && (
          <div className="flex items-center justify-between px-5 py-2.5 border-t border-border-dark shrink-0">
            <span className="text-xs text-slate-400">
              {t("kv.showing", { from: 1, to: items.length, total: items.length })}
            </span>
          </div>
        )}
      </div>

      {selectedItem && (
        <KvDetailPanel item={selectedItem} onClose={() => setSelectedKey(null)}
          onDelete={() => { doDel(selectedItem.key); setSelectedKey(null); }} />
      )}
    </div>
  );
}
