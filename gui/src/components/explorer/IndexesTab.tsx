import { useState, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useExplorerStore } from "../../stores/explorerStore";
import { useToastStore } from "../../stores/toastStore";
import { execSql, escapeSqlIdent } from "../../lib/tauri";
import { formatValue } from "../../lib/formatValue";
import { DataTable, Input, Button, EmptyState, type ColumnDef } from "../ui";

interface IdxRow {
  name: string;
  type: string;
  columns: string;
  status: string;
}

export default function IndexesTab() {
  const { t } = useTranslation();
  const { indexData, selectedTable, loadIndexes } = useExplorerStore();
  const [searchIdx, setSearchIdx] = useState("");
  const [droppingIndex, setDroppingIndex] = useState(false);

  const dropIndex = async (indexName: string) => {
    if (droppingIndex) return;
    if (!confirm(t("explorer.confirmDropIndex", { name: indexName }))) return;
    setDroppingIndex(true);
    try {
      const res = await execSql(`DROP INDEX ${escapeSqlIdent(indexName)}`);
      if (res.ok) {
        useToastStore.getState().addToast("success", t("explorer.indexDropped", { name: indexName }));
        if (selectedTable) loadIndexes(selectedTable);
      } else {
        useToastStore.getState().addToast("error", res.error ?? t("common.unknownError"));
      }
    } catch (e) {
      useToastStore.getState().addToast("error", String(e));
    }
    setDroppingIndex(false);
  };

  const rows = useMemo<IdxRow[]>(() => {
    if (!indexData) return [];
    return indexData.map((idx: any) => {
      const cells = Array.isArray(idx) ? idx : Object.values(idx);
      return {
        name: formatValue(cells[0] ?? ""),
        type: formatValue(cells[1] ?? ""),
        columns: formatValue(cells[2] ?? ""),
        status: cells.length > 3 ? formatValue(cells[3] ?? "") : "",
      };
    });
  }, [indexData]);

  const columns = useMemo<ColumnDef<IdxRow, any>[]>(() => [
    {
      accessorKey: "name",
      header: t("explorer.indexName"),
      size: 240,
      cell: ({ getValue }) => <span className="text-white font-medium truncate block max-w-[220px]" title={getValue()}>{getValue()}</span>,
    },
    {
      accessorKey: "columns",
      header: t("explorer.indexColumns"),
      size: 280,
      cell: ({ getValue }) => <span className="text-blue-400 truncate block max-w-[260px]" title={getValue()}>{getValue()}</span>,
    },
    {
      accessorKey: "type",
      header: t("explorer.indexType"),
      size: 140,
      cell: ({ getValue }) => <span className="text-slate-400">{getValue()}</span>,
    },
    {
      accessorKey: "status",
      header: t("explorer.indexStatus"),
      size: 140,
      enableSorting: false,
      cell: ({ getValue }) => <StatusBadge value={getValue()} />,
    },
    {
      id: "actions",
      header: "",
      size: 48,
      enableSorting: false,
      cell: ({ row }) => (
        <button
          onClick={(e) => { e.stopPropagation(); dropIndex(row.original.name); }}
          className="opacity-0 group-hover:opacity-100 text-red-400 hover:text-red-300 transition-opacity"
          title={t("explorer.dropIndex")}
        >
          <span className="material-symbols-outlined text-[16px]">delete</span>
        </button>
      ),
    },
  ], [t, dropIndex]);

  if (indexData === null) {
    return (
      <div className="flex items-center justify-center py-16">
        <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
        <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
      </div>
    );
  }

  if (indexData.length === 0) {
    return (
      <EmptyState icon="list_alt" title={t("explorer.noIndexes")} />
    );
  }

  return (
    <>
      {/* Toolbar */}
      <div className="flex items-center justify-between p-3 border-b border-border-dark bg-dark-800/50 shrink-0">
        <Button variant="secondary" icon="refresh" size="sm"
          onClick={() => selectedTable && loadIndexes(selectedTable)}>
          {t("explorer.refresh")}
        </Button>
        <div className="flex items-center gap-2">
          <Input icon="search" size="sm" placeholder={t("common.search")}
            value={searchIdx} onChange={e => setSearchIdx(e.target.value)} className="w-64" />
        </div>
      </div>

      <DataTable
        columns={columns}
        data={rows}
        globalFilter={searchIdx}
        compact
      />

      {/* Footer */}
      <footer className="h-10 border-t border-border-dark bg-dark-800 flex items-center px-4 shrink-0">
        <span className="text-xs text-slate-400">
          {t("explorer.totalIndexes", { count: indexData.length })}
        </span>
      </footer>
    </>
  );
}

function StatusBadge({ value }: { value: string }) {
  const { t } = useTranslation();
  const upper = value.toUpperCase();
  if (upper.includes("BUILDING") || upper.includes("PROGRESS")) {
    return (
      <span className="px-1.5 py-0.5 rounded bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 text-[10px]">
        {t("explorer.indexBuilding")}
      </span>
    );
  }
  if (upper.includes("INVALID") || upper.includes("ERROR")) {
    return (
      <span className="px-1.5 py-0.5 rounded bg-red-500/10 text-red-400 border border-red-500/20 text-[10px]">
        {t("explorer.indexInvalid")}
      </span>
    );
  }
  return (
    <span className="px-1.5 py-0.5 rounded bg-green-500/10 text-green-400 border border-green-500/20 text-[10px]">
      {t("explorer.indexValid")}
    </span>
  );
}
