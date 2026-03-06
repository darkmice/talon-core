/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { create } from "zustand";
import { persist } from "zustand/middleware";
import i18n from "../i18n";

interface SettingsState {
  theme: "light" | "dark" | "system";
  language: string;
  autoConnect: boolean;
  fontFamily: string;
  fontSize: number;
  autoSave: boolean;
  lineWrap: boolean;
  proxyEnabled: boolean;
  proxyHost: string;
  proxyPort: string;

  setTheme: (v: SettingsState["theme"]) => void;
  setLanguage: (v: string) => void;
  setAutoConnect: (v: boolean) => void;
  setFontFamily: (v: string) => void;
  setFontSize: (v: number) => void;
  setAutoSave: (v: boolean) => void;
  setLineWrap: (v: boolean) => void;
  setProxyEnabled: (v: boolean) => void;
  setProxyHost: (v: string) => void;
  setProxyPort: (v: string) => void;
  resetDefaults: () => void;
}

const defaults = {
  theme: "dark" as const,
  language: "en",
  autoConnect: false,
  fontFamily: "JetBrains Mono",
  fontSize: 14,
  autoSave: true,
  lineWrap: false,
  proxyEnabled: false,
  proxyHost: "",
  proxyPort: "",
};

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set) => ({
      ...defaults,

      setTheme: (v) => set({ theme: v }),
      setLanguage: (v) => {
        i18n.changeLanguage(v);
        set({ language: v });
      },
      setAutoConnect: (v) => set({ autoConnect: v }),
      setFontFamily: (v) => set({ fontFamily: v }),
      setFontSize: (v) => set({ fontSize: v }),
      setAutoSave: (v) => set({ autoSave: v }),
      setLineWrap: (v) => set({ lineWrap: v }),
      setProxyEnabled: (v) => set({ proxyEnabled: v }),
      setProxyHost: (v) => set({ proxyHost: v }),
      setProxyPort: (v) => set({ proxyPort: v }),
      resetDefaults: () => {
        i18n.changeLanguage(defaults.language);
        set(defaults);
      },
    }),
    {
      name: "talon-settings",
    }
  )
);
