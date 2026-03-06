/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface ConnectionEntry {
  url: string;
  label: string;
  lastUsed: number;
  mode: "tcp" | "embedded";
}

interface ConnectionState {
  history: ConnectionEntry[];

  addToHistory: (url: string, label: string, mode?: "tcp" | "embedded") => void;
  removeFromHistory: (url: string) => void;
  clearHistory: () => void;
}

export const useConnectionStore = create<ConnectionState>()(
  persist(
    (set) => ({
      history: [],

      addToHistory: (url, label, mode = "tcp") =>
        set((s) => ({
          history: [
            { url, label, lastUsed: Date.now(), mode },
            ...s.history.filter((h) => h.url !== url),
          ].slice(0, 10),
        })),

      removeFromHistory: (url) =>
        set((s) => ({
          history: s.history.filter((h) => h.url !== url),
        })),

      clearHistory: () => set({ history: [] }),
    }),
    {
      name: "talon-connections",
    }
  )
);
