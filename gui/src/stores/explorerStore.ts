/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 * See the LICENSE file in the project root for full license information.
 */
import { create } from "zustand";
import { execSql, escapeSqlIdent } from "../lib/tauri";
import { formatValue } from "../lib/formatValue";
import { useToastStore } from "./toastStore";
import i18n from "../i18n";

const PAGE_SIZE = 10;

let _gen = 0;

export type ExplorerTab = "data" | "structure" | "indexes";

export interface ColumnDef {
  name: string;
  type: string;
  pk: boolean;
  nn: boolean;
  defaultValue: string;
}

interface ExplorerState {
  // Table list
  tables: string[];
  tableCounts: Record<string, number>;
  filterText: string;
  loading: boolean;

  // Selected table
  selectedTable: string | null;
  tab: ExplorerTab;
  showCreateForm: boolean;

  // Data tab
  tableSchema: any[] | null;
  tableData: any[] | null;
  dataLoading: boolean;
  page: number;
  totalCount: number | null;
  searchText: string;

  // Indexes tab
  indexData: any[] | null;

  // Structure tab triggers
  addColumnTrigger: number;
  triggerAddColumn: () => void;

  // Create table form
  newTableName: string;
  newTableDesc: string;
  newColumns: ColumnDef[];

  // Actions
  resetAll: () => void;
  setFilterText: (v: string) => void;
  setTab: (tab: ExplorerTab) => void;
  setShowCreateForm: (v: boolean) => void;
  setSearchText: (v: string) => void;
  setPage: (p: number) => void;
  setNewTableName: (v: string) => void;
  setNewTableDesc: (v: string) => void;
  setNewColumns: (cols: ColumnDef[]) => void;

  // Async actions
  loadTables: () => Promise<void>;
  selectTable: (name: string) => Promise<void>;
  loadSchema: (name: string) => Promise<void>;
  loadData: (name: string, pg: number) => Promise<void>;
  loadCount: (name: string) => Promise<void>;
  loadIndexes: (name: string) => Promise<void>;
  goPage: (np: number) => void;
  refresh: () => void;
}

export const useExplorerStore = create<ExplorerState>((set, get) => ({
  tables: [],
  tableCounts: {},
  filterText: "",
  loading: false,
  selectedTable: null,
  tab: "data",
  showCreateForm: false,
  tableSchema: null,
  tableData: null,
  dataLoading: false,
  page: 0,
  totalCount: null,
  searchText: "",
  indexData: null,
  addColumnTrigger: 0,
  triggerAddColumn: () => set(s => ({ addColumnTrigger: s.addColumnTrigger + 1 })),
  newTableName: "",
  newTableDesc: "",
  newColumns: [
    { name: "id", type: "INTEGER", pk: true, nn: true, defaultValue: "" },
    { name: "", type: "TEXT", pk: false, nn: false, defaultValue: "" },
  ],

  resetAll: () => { _gen++; set({
    tables: [], tableCounts: {}, filterText: "", loading: false,
    selectedTable: null, tab: "data", showCreateForm: false,
    tableSchema: null, tableData: null, dataLoading: false,
    page: 0, totalCount: null, searchText: "", indexData: null,
    newTableName: "", newTableDesc: "",
    newColumns: [
      { name: "id", type: "INTEGER", pk: true, nn: true, defaultValue: "" },
      { name: "", type: "TEXT", pk: false, nn: false, defaultValue: "" },
    ],
  }); },

  setFilterText: (v) => set({ filterText: v }),
  setTab: (tab) => {
    set({ tab });
    const { selectedTable, indexData } = get();
    if (tab === "indexes" && indexData === null && selectedTable) {
      get().loadIndexes(selectedTable);
    }
  },
  setShowCreateForm: (v) => set({ showCreateForm: v }),
  setSearchText: (v) => set({ searchText: v }),
  setPage: (p) => set({ page: p }),
  setNewTableName: (v) => set({ newTableName: v }),
  setNewTableDesc: (v) => set({ newTableDesc: v }),
  setNewColumns: (cols) => set({ newColumns: cols }),

  loadTables: async () => {
    const gen = ++_gen;
    set({ loading: true, selectedTable: null, tableSchema: null, tableData: null, totalCount: null, indexData: null });
    try {
      const data = await execSql("SHOW TABLES");
      if (gen !== _gen) return;
      if (!data.ok) { useToastStore.getState().addToast("error", data.error ?? i18n.t("common.unknownError")); set({ loading: false }); return; }
      const d = data.data as any;
      const rows = d?.rows || [];
      const names: string[] = rows.map((r: any) =>
        Array.isArray(r) ? formatValue(r[0]) : formatValue(r)
      );
      set({ tables: names });
      const { selectedTable } = get();
      if (names.length > 0 && !selectedTable) {
        get().selectTable(names[0]);
      }
      const counts: Record<string, number> = {};
      for (const name of names.slice(0, 30)) {
        if (gen !== _gen) return;
        try {
          const cnt = await execSql(`SELECT COUNT(*) FROM ${escapeSqlIdent(name)}`);
          if (gen !== _gen) return;
          const cd = cnt.data as any;
          if (cnt.ok && cd?.rows?.length > 0) {
            const row = cd.rows[0];
            const val = Array.isArray(row) ? row[0] : row;
            counts[name] = Number(formatValue(val));
          }
        } catch {}
      }
      if (gen !== _gen) return;
      set({ tableCounts: counts });
    } catch (e) {
      if (gen !== _gen) return;
      useToastStore.getState().addToast("error", String(e));
    }
    if (gen !== _gen) return;
    set({ loading: false });
  },

  selectTable: async (name) => {
    set({
      selectedTable: name,
      page: 0,
      tab: "data",
      tableSchema: null,
      tableData: null,
      totalCount: null,
      searchText: "",
      indexData: null,
      showCreateForm: false,
    });
    const s = get();
    await Promise.all([s.loadSchema(name), s.loadData(name, 0), s.loadCount(name)]);
  },

  loadSchema: async (name) => {
    const gen = _gen;
    try {
      const data = await execSql(`DESCRIBE ${escapeSqlIdent(name)}`);
      if (gen !== _gen) return;
      if (data.ok) set({ tableSchema: (data.data as any)?.rows || [] });
      else useToastStore.getState().addToast("error", data.error ?? i18n.t("explorer.loadSchemaFailed"));
    } catch (e) {
      if (gen !== _gen) return;
      useToastStore.getState().addToast("error", String(e));
    }
  },

  loadData: async (name, pg) => {
    const gen = _gen;
    set({ dataLoading: true });
    try {
      const offset = pg * PAGE_SIZE;
      const data = await execSql(
        `SELECT * FROM ${escapeSqlIdent(name)} LIMIT ${PAGE_SIZE} OFFSET ${offset}`
      );
      if (gen !== _gen) return;
      if (data.ok) set({ tableData: (data.data as any)?.rows || [] });
      else { set({ tableData: [] }); useToastStore.getState().addToast("error", data.error ?? i18n.t("common.unknownError")); }
    } catch (e) {
      if (gen !== _gen) return;
      set({ tableData: [] });
      useToastStore.getState().addToast("error", String(e));
    }
    if (gen !== _gen) return;
    set({ dataLoading: false });
  },

  loadCount: async (name) => {
    const gen = _gen;
    try {
      const data = await execSql(`SELECT COUNT(*) FROM ${escapeSqlIdent(name)}`);
      if (gen !== _gen) return;
      const d = data.data as any;
      if (data.ok && d?.rows?.length > 0) {
        const row = d.rows[0];
        set({ totalCount: Number(formatValue(Array.isArray(row) ? row[0] : row)) });
      }
    } catch (e) {
      if (gen !== _gen) return;
      useToastStore.getState().addToast("error", String(e));
    }
  },

  loadIndexes: async (name) => {
    const gen = _gen;
    try {
      const data = await execSql(`SHOW INDEXES FROM ${escapeSqlIdent(name)}`);
      if (gen !== _gen) return;
      if (data.ok) set({ indexData: (data.data as any)?.rows || [] });
      else { set({ indexData: [] }); useToastStore.getState().addToast("error", data.error ?? i18n.t("common.unknownError")); }
    } catch (e) {
      if (gen !== _gen) return;
      set({ indexData: [] });
      useToastStore.getState().addToast("error", String(e));
    }
  },

  goPage: (np) => {
    const { totalCount, selectedTable } = get();
    if (np < 0) return;
    if (totalCount !== null && np * PAGE_SIZE >= totalCount) return;
    set({ page: np });
    if (selectedTable) get().loadData(selectedTable, np);
  },

  refresh: () => {
    const { selectedTable, page } = get();
    if (selectedTable) {
      get().loadData(selectedTable, page);
      get().loadCount(selectedTable);
    }
  },
}));

export const PAGE_SIZE_CONST = PAGE_SIZE;

export function fmtCount(n: number): string {
  if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return String(n);
}
