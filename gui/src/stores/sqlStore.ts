/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface TabResult {
  rows: any[] | null;
  columns: string[] | null;
  error: string | null;
  elapsed: number | null;
}

export interface SqlTab {
  id: number;
  name: string;
  sql: string;
  result: TabResult;
}

export interface SqlHistoryEntry {
  sql: string;
  time: number;
  ok: boolean;
  ms: number;
  rowCount: number;
}

interface SqlState {
  tabs: SqlTab[];
  activeTab: number;
  nextTabId: number;
  history: SqlHistoryEntry[];

  setActiveTab: (id: number) => void;
  setSql: (tabId: number, sql: string) => void;
  setResult: (tabId: number, result: TabResult) => void;
  addTab: () => void;
  closeTab: (id: number) => void;
  renameTab: (id: number, name: string) => void;
  addHistory: (entry: SqlHistoryEntry) => void;
  removeHistory: (time: number) => void;
  clearHistory: () => void;
}

export const useSqlStore = create<SqlState>()(
  persist(
    (set, get) => ({
      tabs: [{ id: 1, name: "Query 1", sql: "", result: { rows: null, columns: null, error: null, elapsed: null } }],
      activeTab: 1,
      nextTabId: 2,
      history: [],

      setActiveTab: (id) => set({ activeTab: id }),

      setSql: (tabId, sql) =>
        set((s) => ({
          tabs: s.tabs.map((t) => (t.id === tabId ? { ...t, sql } : t)),
        })),

      setResult: (tabId, result) =>
        set((s) => ({
          tabs: s.tabs.map((t) => (t.id === tabId ? { ...t, result } : t)),
        })),

      addTab: () =>
        set((s) => {
          const id = s.nextTabId;
          return {
            tabs: [...s.tabs, { id, name: `Query ${id}`, sql: "", result: { rows: null, columns: null, error: null, elapsed: null } }],
            activeTab: id,
            nextTabId: id + 1,
          };
        }),

      closeTab: (id) =>
        set((s) => {
          if (s.tabs.length <= 1) return s;
          const next = s.tabs.filter((t) => t.id !== id);
          return {
            tabs: next,
            activeTab: s.activeTab === id ? next[next.length - 1].id : s.activeTab,
          };
        }),

      renameTab: (id, name) =>
        set((s) => ({
          tabs: s.tabs.map((t) => (t.id === id ? { ...t, name } : t)),
        })),

      addHistory: (entry) =>
        set((s) => ({
          history: [entry, ...s.history.filter((h) => h.sql !== entry.sql)].slice(0, 50),
        })),

      removeHistory: (time) =>
        set((s) => ({
          history: s.history.filter((h) => h.time !== time),
        })),

      clearHistory: () => set({ history: [] }),
    }),
    {
      name: "talon-sql",
      partialize: (state) => ({
        ...state,
        tabs: state.tabs.map((t) => ({ ...t, result: { rows: null, columns: null, error: null, elapsed: null } })),
      }),
    }
  )
);
