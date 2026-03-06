import { useTranslation } from "react-i18next";
import { useExplorerStore, type ColumnDef } from "../../stores/explorerStore";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { useToastStore } from "../../stores/toastStore";
import { useState } from "react";
import { Button, Input, Select, Label, Checkbox, PageHeader } from "../ui";

const COLUMN_TYPES = [
  "INTEGER", "BIGINT", "FLOAT", "VARCHAR", "TEXT",
  "BOOLEAN", "TIMESTAMP", "JSON", "JSONB",
  "VECTOR", "GEOPOINT", "BLOB",
];

export default function CreateTableView() {
  const { t } = useTranslation();
  const {
    newTableName, newTableDesc, newColumns,
    setNewTableName, setNewTableDesc, setNewColumns,
    setShowCreateForm, loadTables,
  } = useExplorerStore();
  const addToast = useToastStore((s) => s.addToast);
  const [isTemporary, setIsTemporary] = useState(false);
  const [creating, setCreating] = useState(false);
  const [uniqueCols, setUniqueCols] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);

  const updateColumn = (i: number, patch: Partial<ColumnDef>) => {
    const cols = [...newColumns];
    cols[i] = { ...cols[i], ...patch };
    setNewColumns(cols);
  };

  const removeColumn = (i: number) => {
    if (newColumns.length <= 1) return;
    setNewColumns(newColumns.filter((_, j) => j !== i));
  };

  const addColumn = () => {
    setNewColumns([...newColumns, { name: "", type: "TEXT", pk: false, nn: false, defaultValue: "" }]);
  };

  const createTable = async () => {
    if (!newTableName.trim() || creating) return;
    setCreating(true);
    const cols = newColumns.filter(c => c.name.trim());
    if (cols.length === 0) {
      addToast("error", t("explorer.columnRequired"));
      setCreating(false);
      return;
    }
    const colDefs = cols.map(c => {
      let def = `${escapeSqlIdent(c.name)} ${c.type}`;
      if (c.nn) def += " NOT NULL";
      if (c.pk) def += " PRIMARY KEY";
      if (c.defaultValue.trim()) def += ` DEFAULT ${c.defaultValue.trim()}`;
      return def;
    }).join(", ");
    const escapedUniqueCols = uniqueCols.trim()
      ? uniqueCols.split(",").map(s => escapeSqlIdent(s.trim())).filter(s => s !== '``')
      : [];
    const uniquePart = escapedUniqueCols.length > 0 ? `, UNIQUE(${escapedUniqueCols.join(", ")})` : "";
    const tempKw = isTemporary ? "TEMP " : "";
    const sql = `CREATE ${tempKw}TABLE ${escapeSqlIdent(newTableName.trim())} (${colDefs}${uniquePart})`;
    try {
      const data = await execSql(sql);
      if (data.ok) {
        // P4: Send COMMENT ON TABLE if description is provided
        if (newTableDesc.trim()) {
          await execSql(`COMMENT ON TABLE ${escapeSqlIdent(newTableName.trim())} IS '${newTableDesc.trim().replace(/'/g, "''")}'`);
        }
        addToast("success", t("explorer.tableCreated", { name: newTableName.trim() }));
        setShowCreateForm(false);
        setNewTableName("");
        setNewTableDesc("");
        setNewColumns([
          { name: "id", type: "INTEGER", pk: true, nn: true, defaultValue: "" },
          { name: "", type: "TEXT", pk: false, nn: false, defaultValue: "" },
        ]);
        setIsTemporary(false);
        setUniqueCols("");
        loadTables();
      } else {
        addToast("error", data.error ?? t("explorer.createFailed"));
      }
    } catch (e) {
      addToast("error", String(e));
    }
    setCreating(false);
  };

  const cancel = () => {
    setShowCreateForm(false);
  };

  return (
    <div className="flex-1 flex flex-col min-w-0 bg-dark-900">
      <PageHeader icon="add_circle" title={t("explorer.createTableTitle")} subtitle={t("explorer.createTableSubtitle")}>
        <Button variant="ghost" size="sm" onClick={cancel}>{t("explorer.cancel")}</Button>
        <Button variant="primary" icon="save" size="sm" onClick={createTable} loading={creating} disabled={!newTableName.trim()}>{t("explorer.saveTable")}</Button>
      </PageHeader>

      {/* Form Content */}
      <div className="flex-1 overflow-auto p-8">
        <div className="max-w-6xl mx-auto space-y-8">
          {/* Table Information */}
          <div className="bg-surface border border-border-dark rounded-lg p-6">
            <h3 className="text-sm font-bold text-white uppercase tracking-wider mb-4 flex items-center gap-2">
              <span className="material-symbols-outlined text-primary text-[20px]">info</span>
              {t("explorer.tableName")}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <Label required>{t("explorer.tableName")}</Label>
                <Input
                  mono
                  value={newTableName}
                  onChange={e => setNewTableName(e.target.value)}
                  placeholder={t("explorer.tableNamePlaceholder")}
                  className="w-full"
                />
              </div>
              <div>
                <Label>{t("explorer.description")}</Label>
                <Input
                  value={newTableDesc}
                  onChange={e => setNewTableDesc(e.target.value)}
                  placeholder={t("explorer.descPlaceholder")}
                  className="w-full"
                />
              </div>
            </div>
          </div>

          {/* Columns */}
          <div className="bg-surface border border-border-dark rounded-lg flex flex-col">
            <div className="p-4 border-b border-border-dark bg-dark-800/50 flex items-center justify-between">
              <h3 className="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
                <span className="material-symbols-outlined text-primary text-[20px]">view_column</span>
                {t("explorer.columns")}
              </h3>
              <span className="text-xs text-slate-400">{t("explorer.columnsHint")}</span>
            </div>

            {/* Column Header */}
            <div className="grid grid-cols-[30px_1.5fr_1fr_80px_80px_1fr_40px] gap-4 px-4 py-2 border-b border-border-dark bg-dark-800 text-xs font-semibold text-slate-400 uppercase tracking-wider items-center">
              <div className="text-center">#</div>
              <div>{t("explorer.columnName")}</div>
              <div>{t("explorer.dataType")}</div>
              <div className="text-center" title={t("explorer.primaryKey")}>{t("explorer.pk")}</div>
              <div className="text-center" title={t("explorer.notNull")}>{t("explorer.nn")}</div>
              <div>{t("explorer.defaultValue")}</div>
              <div />
            </div>

            {/* Column Rows */}
            <div className="divide-y divide-border-dark bg-dark-900">
              {newColumns.map((col, i) => (
                <div key={i} className="grid grid-cols-[30px_1.5fr_1fr_80px_80px_1fr_40px] gap-4 px-4 py-3 items-center hover:bg-white/[0.02] group transition-colors">
                  <div className="text-center text-slate-600 text-xs font-mono">{i + 1}</div>
                  <div>
                    <Input
                      mono size="sm"
                      value={col.name}
                      onChange={e => updateColumn(i, { name: e.target.value })}
                      placeholder="column_name"
                      className="w-full"
                    />
                  </div>
                  <div>
                    <Select
                      size="sm"
                      value={col.type}
                      onValueChange={v => updateColumn(i, { type: v })}
                      options={COLUMN_TYPES.map(tp => ({ value: tp, label: tp }))}
                      className="w-full"
                    />
                  </div>
                  <div className="flex justify-center">
                    <Checkbox checked={col.pk} onChange={v => updateColumn(i, { pk: v })} />
                  </div>
                  <div className="flex justify-center">
                    <Checkbox checked={col.nn} onChange={v => updateColumn(i, { nn: v })} />
                  </div>
                  <div>
                    <Input
                      mono size="sm"
                      value={col.defaultValue}
                      onChange={e => updateColumn(i, { defaultValue: e.target.value })}
                      placeholder="NULL"
                      className="w-full"
                    />
                  </div>
                  <div className="flex justify-center">
                    <button
                      onClick={() => removeColumn(i)}
                      disabled={newColumns.length <= 1}
                      className="text-slate-600 hover:text-red-400 transition-colors p-1 rounded hover:bg-red-400/10 disabled:opacity-30"
                      title={t("common.delete")}
                    >
                      <span className="material-symbols-outlined text-[18px]">delete</span>
                    </button>
                  </div>
                </div>
              ))}
            </div>

            {/* Add Column Button */}
            <div className="p-3 bg-dark-800/50 border-t border-border-dark">
              <button
                onClick={addColumn}
                className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-primary hover:text-white hover:bg-primary rounded-lg transition-colors w-full justify-center border border-dashed border-primary/30 hover:border-transparent"
              >
                <span className="material-symbols-outlined text-[20px]">add_circle</span>
                {t("explorer.addColumn")}
              </button>
            </div>
          </div>

          {/* Advanced Settings */}
          <div className="border border-border-dark rounded-lg bg-surface/50">
            <div
              className="flex items-center justify-between cursor-pointer p-4"
              role="button"
              tabIndex={0}
              onClick={() => setShowAdvanced(!showAdvanced)}
              onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setShowAdvanced(!showAdvanced); } }}
            >
              <h3 className="text-sm font-medium text-slate-300">{t("explorer.advancedSettings")}</h3>
              <span className={`material-symbols-outlined text-slate-500 transition-transform ${showAdvanced ? "rotate-180" : ""}`}>expand_more</span>
            </div>
            {showAdvanced && (
              <div className="px-4 pb-4 space-y-4 border-t border-border-dark pt-4">
                <div className="flex items-center gap-3">
                  <Checkbox checked={isTemporary} onChange={setIsTemporary} />
                  <div>
                    <span className="text-sm text-slate-200">{t("explorer.temporaryTable")}</span>
                    <p className="text-xs text-slate-400">{t("explorer.temporaryTableHint")}</p>
                  </div>
                </div>
                <div>
                  <Label>{t("explorer.uniqueConstraint")}</Label>
                  <Input
                    mono size="sm"
                    value={uniqueCols}
                    onChange={e => setUniqueCols(e.target.value)}
                    placeholder={t("explorer.uniquePlaceholder")}
                    className="w-full"
                  />
                  <p className="text-xs text-slate-400 mt-1">{t("explorer.uniqueHint")}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
