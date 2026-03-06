import { useTranslation } from "react-i18next";
import { Toggle, Select, Input, Button, Label, PageHeader } from "../components/ui";
import { useSettingsStore } from "../stores/settingsStore";
import { useToastStore } from "../stores/toastStore";

export default function SettingsPage() {
  const { t } = useTranslation();
  const s = useSettingsStore();

  const Section = ({ icon, title, children }: { icon: string; title: string; children: React.ReactNode }) => (
    <div className="mb-8">
      <h3 className="flex items-center gap-2 text-base font-semibold text-white mb-4">
        <span className="material-symbols-outlined text-primary text-[20px]">{icon}</span>
        {title}
      </h3>
      <div className="bg-surface rounded-xl border border-border-dark divide-y divide-border-dark">
        {children}
      </div>
    </div>
  );

  const Row = ({ label, desc, children }: { label: string; desc?: string; children: React.ReactNode }) => (
    <div className="flex items-center justify-between px-5 py-4">
      <div>
        <p className="text-sm font-medium text-white">{label}</p>
        {desc && <p className="text-xs text-slate-400 mt-0.5">{desc}</p>}
      </div>
      {children}
    </div>
  );

  return (
    <div className="flex flex-col h-full">
      <PageHeader icon="settings" title={t("settings.title")} subtitle={t("settings.subtitle")}>
        <Button variant="secondary" icon="restart_alt" size="sm" onClick={() => {
          if (!confirm(t("settings.confirmReset"))) return;
          s.resetDefaults();
          useToastStore.getState().addToast("success", t("settings.resetDone"));
        }}>
          {t("settings.resetDefaults")}
        </Button>
      </PageHeader>
      <div className="flex-1 overflow-y-auto">
      <div className="max-w-3xl mx-auto px-6 py-6">

        {/* General */}
        <Section icon="tune" title={t("settings.general")}>
          <Row label={t("settings.appearance")} desc={t("settings.appearanceDesc")}>
            <div className="flex bg-dark-800 rounded-lg border border-border-dark p-0.5">
              {["light", "dark", "system"].map(v => (
                <button key={v} onClick={() => s.setTheme(v as "light" | "dark" | "system")}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition
                    ${s.theme === v ? "bg-surface text-white" : "text-slate-500 hover:text-slate-300"}`}>
                  <span className="material-symbols-outlined text-[16px]">
                    {v === "light" ? "light_mode" : v === "dark" ? "dark_mode" : "desktop_windows"}
                  </span>
                  {t(`settings.${v}`)}
                </button>
              ))}
            </div>
          </Row>
          <Row label={t("settings.language")} desc={t("settings.languageDesc")}>
            <Select value={s.language} onValueChange={s.setLanguage}
              options={[{ value: "en", label: "English (US)" }, { value: "zh", label: "中文（简体）" }]}
              className="w-44" />
          </Row>
          <Row label={t("settings.startup")} desc={t("settings.startupDesc")}>
            <Toggle value={s.autoConnect} onChange={s.setAutoConnect} />
          </Row>
        </Section>

        {/* Editor */}
        <Section icon="code" title={t("settings.editor")}>
          <Row label={t("settings.fontFamily")} desc={t("settings.fontFamilyDesc")}>
            <Select value={s.fontFamily} onValueChange={s.setFontFamily}
              options={[
                { value: "JetBrains Mono", label: "JetBrains Mono" },
                { value: "Fira Code", label: "Fira Code" },
                { value: "SF Mono", label: "SF Mono" },
                { value: "Consolas", label: "Consolas" },
              ]}
              className="w-44" />
          </Row>
          <Row label={t("settings.fontSize")} desc={t("settings.fontSizeDesc")}>
            <div className="flex items-center gap-3">
              <span className="text-xs text-slate-400 w-8">{s.fontSize}px</span>
              <input type="range" min="10" max="24" value={s.fontSize} onChange={e => s.setFontSize(Number(e.target.value))}
                className="w-40 accent-primary" />
            </div>
          </Row>
          <Row label={t("settings.autoSave")} desc={t("settings.autoSaveDesc")}>
            <Toggle value={s.autoSave} onChange={s.setAutoSave} />
          </Row>
          <Row label={t("settings.lineWrap")} desc={t("settings.lineWrapDesc")}>
            <Toggle value={s.lineWrap} onChange={s.setLineWrap} />
          </Row>
        </Section>

        {/* Network */}
        <Section icon="language" title={t("settings.network")}>
          <Row label={t("settings.proxy")} desc={t("settings.proxyDesc")}>
            <Toggle value={s.proxyEnabled} onChange={s.setProxyEnabled} />
          </Row>
          {s.proxyEnabled && (
            <div className="px-5 py-4 flex gap-3">
              <div className="flex-1">
                <Label>{t("settings.host")}</Label>
                <Input value={s.proxyHost} onChange={e => s.setProxyHost(e.target.value)}
                  placeholder={t("settings.hostPlaceholder")} className="w-full" />
              </div>
              <div className="w-28">
                <Label>{t("settings.port")}</Label>
                <Input value={s.proxyPort} onChange={e => s.setProxyPort(e.target.value)}
                  placeholder={t("settings.portPlaceholder")} className="w-full" />
              </div>
            </div>
          )}
        </Section>
      </div>
      </div>
    </div>
  );
}
