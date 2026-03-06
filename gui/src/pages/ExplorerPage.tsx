import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useExplorerStore } from "../stores/explorerStore";
import { EmptyState } from "../components/ui";
import {
  TableListPanel,
  ExplorerHeader,
  DataTab,
  StructureTab,
  IndexesTab,
  CreateTableView,
} from "../components/explorer";

export default function ExplorerPage() {
  const { t } = useTranslation();
  const { selectedTable, tab, showCreateForm, loadTables, refresh } = useExplorerStore();

  useEffect(() => { loadTables(); }, []);

  return (
    <div className="flex h-full">
      {/* Left: Table List */}
      <TableListPanel />

      {/* Right: Content */}
      {showCreateForm ? (
        <CreateTableView />
      ) : (
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-dark-900 relative">
          {!selectedTable ? (
            <EmptyState icon="database" title={t("explorer.selectTable")} />
          ) : (
            <>
              <ExplorerHeader />
              {tab === "data" && <DataTab />}
              {tab === "structure" && <StructureTab />}
              {tab === "indexes" && <IndexesTab />}
            </>
          )}
        </div>
      )}
    </div>
  );
}
