import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { DataTable, Badge, EmptyState, type ColumnDef } from "../ui";
import { fmtSize, guessType, typeBadgeVariant } from "./kvUtils";

interface KvItem {
  key: string;
  value: string;
  size: number;
}

interface KvTableProps {
  items: KvItem[] | null;
  error: string | null;
  selectedKey: string | null;
  loading?: boolean;
  onSelectKey: (key: string) => void;
  onDeleteKey: (key: string) => void;
}

export default function KvTable({ items, error, selectedKey, loading, onSelectKey, onDeleteKey }: KvTableProps) {
  const { t } = useTranslation();

  const columns = useMemo<ColumnDef<KvItem, any>[]>(() => [
    {
      accessorKey: "key",
      header: t("kv.key"),
      cell: ({ getValue }) => (
        <span className="font-mono text-[13px] text-emerald-400 truncate block max-w-[300px]" title={getValue()}>{getValue()}</span>
      ),
    },
    {
      id: "type",
      header: t("kv.type"),
      size: 80,
      cell: ({ row }) => {
        const tp = guessType(row.original.value);
        return <Badge variant={(typeBadgeVariant[tp] || "default") as any} size="sm">{tp}</Badge>;
      },
      enableSorting: false,
    },
    {
      accessorKey: "size",
      header: t("kv.size"),
      size: 80,
      cell: ({ getValue }) => (
        <span className="text-slate-400 text-xs">{fmtSize(getValue())}</span>
      ),
    },
    {
      accessorKey: "value",
      header: t("kv.valuePreview"),
      cell: ({ getValue }) => (
        <span className="font-mono text-[12px] text-slate-400 max-w-[400px] truncate block" title={getValue()}>{getValue()}</span>
      ),
      enableSorting: false,
    },
    {
      id: "actions",
      header: "",
      size: 48,
      enableSorting: false,
      cell: ({ row }) => (
        <button
          onClick={(e) => { e.stopPropagation(); onDeleteKey(row.original.key); }}
          className="opacity-0 group-hover:opacity-100 text-red-400 hover:text-red-300 transition-opacity"
          title={t("common.delete")}
        >
          <span className="material-symbols-outlined text-[16px]">delete</span>
        </button>
      ),
    },
  ], [t, onDeleteKey]);

  if (error) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="m-4 bg-red-500/10 border border-red-500/30 text-red-400 text-sm rounded-lg px-4 py-3 font-mono">{error}</div>
      </div>
    );
  }

  if (items === null) {
    return (
      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <span className="material-symbols-outlined text-[24px] text-primary animate-spin">progress_activity</span>
            <span className="ml-2 text-sm text-slate-400">{t("common.loading")}</span>
          </div>
        ) : (
          <EmptyState icon="vpn_key" title={t("kv.emptyTitle")} description={t("kv.emptyDesc")} />
        )}
      </div>
    );
  }

  return (
    <DataTable
      columns={columns}
      data={items}
      emptyText={t("kv.noKeys")}
      emptyIcon="vpn_key"
      onRowClick={(row) => onSelectKey(row.key)}
      selectedRowId={selectedKey}
      getRowId={(row) => row.key}
    />
  );
}
