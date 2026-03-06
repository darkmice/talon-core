import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useExplorerStore } from "../../stores/explorerStore";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { useToastStore } from "../../stores/toastStore";
import { formatValue } from "../../lib/formatValue";
import { Checkbox, Select } from "../ui";

const TYPE_COLORS: Record<string, string> = {
  INTEGER: "text-blue-400",
  BIGINT: "text-blue-400",
  FLOAT: "text-blue-400",
  VARCHAR: "text-green-400",
  TEXT: "text-green-400",
  BOOLEAN: "text-orange-400",
  TIMESTAMP: "text-cyan-400",
  ENUM: "text-purple-400",
  JSON: "text-yellow-400",
  JSONB: "text-yellow-400",
  VECTOR: "text-pink-400",
  GEOPOINT: "text-emerald-400",
  BLOB: "text-slate-400",
};

function getTypeColor(type: string): string {
  const upper = type.toUpperCase().replace(/\(.*\)/, "").trim();
  return TYPE_COLORS[upper] || "text-slate-400";
}

const ALL_TYPES = [
  "INTEGER", "BIGINT", "FLOAT", "VARCHAR", "TEXT",
  "BOOLEAN", "TIMESTAMP", "ENUM", "JSON", "JSONB",
  "VECTOR", "GEOPOINT", "BLOB",
];

interface ColumnEdit {
  name: string;
  type: string;
  length: string;
  nullable: boolean;
  defaultValue: string;
  comment: string;
  isPk: boolean;
  modified: boolean;
  originalName?: string;
  originalType?: string;
}

export default function StructureTab() {
  const { t } = useTranslation();
  const { tableSchema, selectedTable, addColumnTrigger } = useExplorerStore();
  const [columns, setColumns] = useState<ColumnEdit[] | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [originalCount, setOriginalCount] = useState(0);
  const [deletedColumns, setDeletedColumns] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);

  // 切换表时重置本地编辑状态
  useEffect(() => {
    setColumns(null);
    setHasChanges(false);
    setDeletedColumns([]);
  }, [selectedTable]);

  // ExplorerHeader "Add Column" 按钮触发
  useEffect(() => {
    if (addColumnTrigger > 0 && columns) {
      setColumns([...columns, {
        name: "", type: "TEXT", length: "", nullable: true,
        defaultValue: "NULL", comment: "", isPk: false, modified: true,
      }]);
      setHasChanges(true);
    }
  }, [addColumnTrigger]);

  // Initialize columns from schema on first render or schema change
  if (tableSchema && columns === null) {
    // DESCRIBE 返回 7 列: [name, type, isPK, nullable, default, FK, comment]
    const cols: ColumnEdit[] = tableSchema.map((col) => {
      const cells = Array.isArray(col) ? col : Object.values(col);
      const colName = formatValue(cells[0]);
      const colType = formatValue(cells[1] ?? "");
      const isPk = formatValue(cells[2] ?? "").toUpperCase() === "YES";
      const nullable = formatValue(cells[3] ?? "").toUpperCase() === "YES";
      const defVal = formatValue(cells[4] ?? "");
      const comment = formatValue(cells[6] ?? "");
      const typeMatch = colType.match(/^(\w+)(?:\((\d+)\))?/);
      const parsedType = typeMatch ? typeMatch[1].toUpperCase() : colType.toUpperCase();
      const parsedLen = typeMatch && typeMatch[2] ? typeMatch[2] : "";
      return {
        name: colName,
        type: parsedType,
        length: parsedLen,
        nullable,
        defaultValue: defVal === "NULL" ? "NULL" : defVal || "NULL",
        comment,
        isPk,
        modified: false,
        originalName: colName,
        originalType: parsedType + (parsedLen ? `(${parsedLen})` : ""),
      };
    });
    setColumns(cols);
    setOriginalCount(cols.length);
  }

  const updateColumn = (idx: number, patch: Partial<ColumnEdit>) => {
    if (!columns) return;
    const next = [...columns];
    next[idx] = { ...next[idx], ...patch, modified: true };
    setColumns(next);
    setHasChanges(true);
  };

  const addColumn = () => {
    if (!columns) return;
    setColumns([...columns, {
      name: "", type: "TEXT", length: "", nullable: true,
      defaultValue: "NULL", comment: "", isPk: false, modified: true,
    }]);
    setHasChanges(true);
  };

  const removeColumn = (idx: number) => {
    if (!columns || columns.length <= 1) return;
    if (columns[idx].isPk) return;
    const col = columns[idx];
    if (idx < originalCount && col.originalName) {
      setDeletedColumns(prev => [...prev, col.originalName!]);
      setOriginalCount(prev => prev - 1);
    }
    const next = columns.filter((_, i) => i !== idx);
    setColumns(next);
    setHasChanges(true);
  };

  const discardChanges = () => {
    setColumns(null);
    setHasChanges(false);
    setDeletedColumns([]);
  };

  const saveChanges = async () => {
    if (!columns || !selectedTable || saving) return;
    setSaving(true);
    const { addToast } = useToastStore.getState();
    const tbl = escapeSqlIdent(selectedTable);
    try {
      // 1. DROP deleted columns
      for (const colName of deletedColumns) {
        const sql = `ALTER TABLE ${tbl} DROP COLUMN ${escapeSqlIdent(colName)}`;
        const res = await execSql(sql);
        if (!res.ok) { addToast("error", res.error ?? t("explorer.dropColumnFailed", { name: colName })); setColumns(null); setHasChanges(false); setDeletedColumns([]); useExplorerStore.getState().loadSchema(selectedTable); setSaving(false); return; }
      }
      // 2. Process existing columns (rename / alter type)
      for (let i = 0; i < Math.min(columns.length, originalCount); i++) {
        const col = columns[i];
        if (!col.modified || !col.originalName) continue;
        // Rename
        if (col.name.trim() && col.name !== col.originalName) {
          const sql = `ALTER TABLE ${tbl} RENAME COLUMN ${escapeSqlIdent(col.originalName)} TO ${escapeSqlIdent(col.name)}`;
          const res = await execSql(sql);
          if (!res.ok) { addToast("error", res.error ?? t("explorer.renameColumnFailed", { name: col.originalName })); setColumns(null); setHasChanges(false); setDeletedColumns([]); useExplorerStore.getState().loadSchema(selectedTable); setSaving(false); return; }
        }
        // Alter type
        const currentType = col.length ? `${col.type}(${col.length})` : col.type;
        if (col.originalType && currentType !== col.originalType) {
          const colIdent = escapeSqlIdent(col.name.trim() || col.originalName);
          const sql = `ALTER TABLE ${tbl} ALTER COLUMN ${colIdent} TYPE ${currentType}`;
          const res = await execSql(sql);
          if (!res.ok) { addToast("error", res.error ?? t("explorer.alterTypeFailed", { name: col.name })); setColumns(null); setHasChanges(false); setDeletedColumns([]); useExplorerStore.getState().loadSchema(selectedTable); setSaving(false); return; }
        }
      }
      // 3. ADD new columns
      for (let i = originalCount; i < columns.length; i++) {
        const col = columns[i];
        if (!col.name.trim()) continue;
        const colIdent = escapeSqlIdent(col.name);
        const typePart = col.length ? `${col.type}(${col.length})` : col.type;
        const nnPart = col.nullable ? "" : " NOT NULL";
        const defPart = col.defaultValue && col.defaultValue !== "NULL" ? ` DEFAULT ${col.defaultValue}` : "";
        const sql = `ALTER TABLE ${tbl} ADD COLUMN ${colIdent} ${typePart}${nnPart}${defPart}`;
        const res = await execSql(sql);
        if (!res.ok) { addToast("error", res.error ?? t("explorer.addColumnFailed", { name: col.name })); setColumns(null); setHasChanges(false); setDeletedColumns([]); useExplorerStore.getState().loadSchema(selectedTable); setSaving(false); return; }
      }
      addToast("success", t("explorer.schemaApplied", { name: selectedTable }));
      setColumns(null);
      setHasChanges(false);
      setDeletedColumns([]);
      useExplorerStore.getState().loadSchema(selectedTable);
    } catch (e) {
      addToast("error", String(e));
      setColumns(null); setHasChanges(false); setDeletedColumns([]);
      useExplorerStore.getState().loadSchema(selectedTable);
    }
    setSaving(false);
  };

  if (!tableSchema) {
    return (
      <div className="flex items-center justify-center py-16">
        <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
        <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
      </div>
    );
  }

  if (!columns) return null;

  const modifiedCount = columns.filter(c => c.modified).length;

  return (
    <>
      {/* Info bar */}
      <div className="flex items-center justify-between p-3 border-b border-border-dark bg-dark-800/50 shrink-0">
        <div className="flex items-center gap-2 text-xs text-slate-400">
          <span className="material-symbols-outlined text-[16px]">info</span>
          <span>{t("explorer.modifyingSchema", { name: selectedTable })}</span>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={discardChanges} className="text-slate-400 hover:text-white transition-colors p-1" title={t("explorer.discard")}>
            <span className="material-symbols-outlined text-[18px]">undo</span>
          </button>
        </div>
      </div>

      {/* Structure Table */}
      <div className="flex-1 overflow-auto relative bg-dark-900 pb-20">
        <table className="w-full text-left border-separate border-spacing-0">
          <thead className="bg-surface sticky top-0 z-20 shadow-sm">
            <tr>
              <th className="sticky left-0 z-30 w-10 bg-surface border-b border-r border-border-dark p-0 text-center">
                <span className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">#</span>
              </th>
              <th className="w-12 bg-surface border-b border-r border-border-dark p-0 text-center">
                <span className="material-symbols-outlined text-[16px] text-slate-400 pt-2">key</span>
              </th>
              <th className="min-w-[200px] bg-surface border-b border-r border-border-dark px-4 py-2">
                <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("explorer.name")}</span>
              </th>
              <th className="min-w-[160px] bg-surface border-b border-r border-border-dark px-4 py-2">
                <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("explorer.type")}</span>
              </th>
              <th className="min-w-[100px] bg-surface border-b border-r border-border-dark px-4 py-2">
                <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("explorer.length")}</span>
              </th>
              <th className="min-w-[100px] bg-surface border-b border-r border-border-dark px-4 py-2 text-center">
                <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("explorer.allowNull")}</span>
              </th>
              <th className="min-w-[180px] bg-surface border-b border-r border-border-dark px-4 py-2">
                <span className="text-[11px] font-bold text-slate-200 uppercase tracking-wider">{t("explorer.default")}</span>
              </th>
              <th className="w-12 bg-surface border-b border-border-dark px-2 py-2" />
            </tr>
          </thead>
          <tbody className="text-xs text-slate-300 divide-y divide-border-dark">
            {columns.map((col, i) => {
              const rowBg = col.modified ? "bg-primary/5" : "";
              const borderLeft = col.modified ? "border-l-2 border-l-primary" : "";
              return (
                <tr key={i} className={`hover:bg-white/[0.02] group transition-colors ${rowBg}`}>
                  {/* Row number */}
                  <td className={`sticky left-0 z-10 bg-dark-900 group-hover:bg-dark-800 border-r border-border-dark text-center text-slate-600 ${borderLeft}`}>
                    <span className="text-xs font-mono">{i + 1}</span>
                  </td>
                  {/* PK icon */}
                  <td className="border-r border-border-dark text-center">
                    {col.isPk && <span className="material-symbols-outlined text-[14px] text-yellow-500">vpn_key</span>}
                  </td>
                  {/* Name */}
                  <td className="p-0 border-r border-border-dark relative">
                    <input
                      type="text"
                      value={col.name}
                      onChange={e => updateColumn(i, { name: e.target.value })}
                      className={`w-full h-full bg-transparent border-0 px-4 py-2.5 focus:ring-1 focus:ring-inset focus:ring-primary font-mono text-xs ${col.modified ? "text-primary font-bold" : "text-white"}`}
                    />
                    {col.modified && <div className="absolute top-0 right-0 w-2 h-2 bg-primary rounded-bl-full" />}
                  </td>
                  {/* Type */}
                  <td className="p-0 border-r border-border-dark">
                    <Select
                      value={col.type}
                      onValueChange={v => updateColumn(i, { type: v })}
                      size="sm"
                      options={ALL_TYPES.map(tp => ({ value: tp, label: tp }))}
                      className="min-w-[130px]"
                    />
                  </td>
                  {/* Length */}
                  <td className="p-0 border-r border-border-dark">
                    <input
                      type="text"
                      value={col.length}
                      onChange={e => updateColumn(i, { length: e.target.value })}
                      placeholder="-"
                      className="w-full h-full bg-transparent border-0 px-4 py-2.5 focus:ring-1 focus:ring-inset focus:ring-primary text-white font-mono text-xs text-center placeholder-slate-700"
                    />
                  </td>
                  {/* Nullable */}
                  <td className="border-r border-border-dark text-center bg-white/[0.01]">
                    <Checkbox
                      checked={col.nullable}
                      onChange={v => updateColumn(i, { nullable: v })}
                    />
                  </td>
                  {/* Default */}
                  <td className="p-0 border-r border-border-dark bg-white/[0.01]">
                    <input
                      type="text"
                      value={col.defaultValue}
                      onChange={e => updateColumn(i, { defaultValue: e.target.value })}
                      className="w-full h-full bg-transparent border-0 px-4 py-2.5 focus:ring-1 focus:ring-inset focus:ring-primary text-slate-400 font-mono text-xs italic"
                    />
                  </td>
                  {/* Delete */}
                  <td className="text-center">
                    <button
                      onClick={() => removeColumn(i)}
                      disabled={columns.length <= 1}
                      className="text-slate-600 hover:text-red-400 transition-colors disabled:opacity-30"
                      title={t("common.delete")}
                    >
                      <span className="material-symbols-outlined text-[16px]">delete</span>
                    </button>
                  </td>
                </tr>
              );
            })}
            {/* Add new column row */}
            <tr
              onClick={addColumn}
              className="hover:bg-white/[0.02] group transition-colors opacity-50 hover:opacity-100 cursor-pointer"
            >
              <td className="sticky left-0 z-10 bg-dark-900 border-r border-border-dark text-center text-slate-700" />
              <td className="border-r border-border-dark text-center" />
              <td className="p-0 border-r border-border-dark relative">
                <div className="flex items-center gap-2 px-4 py-2.5 text-slate-400 font-mono text-xs italic">
                  <span className="material-symbols-outlined text-[14px]">add</span>
                  {t("explorer.addNewColumn")}
                </div>
              </td>
              <td className="p-2 text-slate-700 text-xs italic" colSpan={5} />
            </tr>
          </tbody>
        </table>
      </div>

      {/* Floating save bar */}
      {hasChanges && modifiedCount > 0 && (
        <div className="absolute bottom-6 left-1/2 -translate-x-1/2 bg-slate-800 border border-border-dark shadow-2xl rounded-xl p-2 pl-4 flex items-center gap-4 z-50">
          <div className="flex flex-col">
            <span className="text-xs font-bold text-white">{t("explorer.unsavedChanges")}</span>
            <span className="text-[10px] text-slate-400">{t("explorer.modifications", { count: modifiedCount })}</span>
          </div>
          <div className="h-8 w-px bg-slate-700" />
          <div className="flex gap-2">
            <button
              onClick={discardChanges}
              className="px-3 py-1.5 rounded-lg text-xs font-medium text-slate-300 hover:text-white hover:bg-white/5 transition-colors"
            >
              {t("explorer.discard")}
            </button>
            <button
              onClick={saveChanges}
              disabled={saving}
              className={`px-4 py-1.5 rounded-lg text-xs font-bold bg-primary hover:bg-blue-600 text-white shadow-lg shadow-blue-500/20 transition-all flex items-center gap-2 ${saving ? "opacity-50 pointer-events-none" : ""}`}
            >
              <span className="material-symbols-outlined text-[16px]">save</span>
              {saving ? t("common.saving") : t("explorer.saveChanges")}
            </button>
          </div>
        </div>
      )}
    </>
  );
}
