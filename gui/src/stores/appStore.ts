/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { create } from "zustand";

export type PageId =
  | "connect" | "explorer" | "sql" | "kv" | "mq"
  | "vector" | "geo" | "fts" | "graph" | "ai" | "ts" | "stats" | "settings";

export type ConnMode = "tcp" | "embedded";

interface AppState {
  page: PageId;
  connected: boolean;
  connLabel: string;
  connMode: ConnMode | null;

  setPage: (page: PageId) => void;
  setConnected: (label: string, mode: ConnMode) => void;
  setDisconnected: () => void;
}

export const useAppStore = create<AppState>((set) => ({
  page: "connect",
  connected: false,
  connLabel: "",
  connMode: null,

  setPage: (page) => set({ page }),

  setConnected: (label, mode) =>
    set({ connected: true, connLabel: label, connMode: mode, page: "explorer" }),

  setDisconnected: () =>
    set({ connected: false, connLabel: "", connMode: null, page: "connect" }),
}));
