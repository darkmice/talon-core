/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import en from "./en.json";
import zh from "./zh.json";

function getSavedLang(): string {
  try {
    const raw = localStorage.getItem("talon-settings");
    if (raw) {
      const parsed = JSON.parse(raw);
      return parsed?.state?.language || "en";
    }
  } catch {}
  return "en";
}

const savedLang = getSavedLang();

i18n.use(initReactI18next).init({
  resources: {
    en: { translation: en },
    zh: { translation: zh },
  },
  lng: savedLang,
  fallbackLng: "en",
  interpolation: {
    escapeValue: false,
  },
});

export default i18n;
