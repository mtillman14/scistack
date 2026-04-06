/**
 * Root application component.
 *
 * Layout:
 *   ┌─────────────────────────────────────────────┐
 *   │  header: SciStack + db name                 │
 *   ├───────────────────────────────┬─────────────┤
 *   │  PipelineDAG (left 3/4)       │  sidebar    │
 *   │                               │  (right 1/4)│
 *   └───────────────────────────────┴─────────────┘
 */

import { useEffect, useState, useCallback } from "react";
import { ReactFlowProvider } from "@xyflow/react";
import PipelineDAG from "./components/DAG/PipelineDAG";
import Sidebar from "./components/Sidebar/Sidebar";
import { RunLogProvider } from "./context/RunLogContext";
import { SelectedNodeProvider } from "./context/SelectedNodeContext";
import { callBackend } from "./api";

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: "flex",
    flexDirection: "column",
    width: "100%",
    height: "100%",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "10px 18px",
    background: "#1a1a2e",
    color: "#fff",
    fontSize: 14,
    flexShrink: 0,
  },
  title: {
    fontWeight: 700,
    fontSize: 16,
    letterSpacing: 0.5,
  },
  separator: {
    opacity: 0.4,
  },
  dbName: {
    fontFamily: "monospace",
    opacity: 0.8,
  },
  refreshBtn: {
    marginLeft: "auto",
    padding: "4px 12px",
    background: "#2a2a4a",
    color: "#ccc",
    border: "1px solid #3a3a5a",
    borderRadius: 4,
    cursor: "pointer",
    fontSize: 12,
    fontFamily: "inherit",
  },
  schemaKeys: {
    opacity: 0.6,
    fontSize: 12,
  },
  body: {
    display: "flex",
    flexDirection: "row",
    flex: 1,
    minHeight: 0,
  },
  dagArea: {
    flex: 3,
    minWidth: 0,
    minHeight: 0,
  },
  sidebar: {
    flex: 1,
    minWidth: 0,
    borderLeft: "1px solid #2a2a4a",
    background: "#12122a",
  },
};

export default function App() {
  const [schema, setSchema] = useState<{ keys: string[] }>({ keys: [] });
  const [dbName, setDbName] = useState("");
  const [restarting, setRestarting] = useState(false);

  const handleRestart = useCallback(async () => {
    setRestarting(true);
    try {
      // Host-side method handled by the VS Code extension: kills and respawns
      // the Python subprocess so edits to scistack_gui server code AND the
      // user's pipeline module are picked up.
      await callBackend("restart_python");
    } catch (err) {
      console.error("Restart failed:", err);
    } finally {
      setRestarting(false);
    }
  }, []);

  useEffect(() => {
    callBackend("get_schema")
      .then((d) => setSchema(d as { keys: string[] }))
      .catch(console.error);
    callBackend("get_info")
      .then((d) => setDbName((d as { db_name: string }).db_name))
      .catch(console.error);
  }, []);

  return (
    <RunLogProvider>
      <SelectedNodeProvider>
        <div style={styles.root}>
          <header style={styles.header}>
            <span style={styles.title}>SciStack</span>
            <span style={styles.separator}>|</span>
            <span style={styles.dbName}>{dbName || "loading…"}</span>
            <button
              style={styles.refreshBtn}
              onClick={handleRestart}
              disabled={restarting}
              title="Restart the Python process to pick up edits to server or pipeline code"
            >
              {restarting ? "Restarting..." : "Restart"}
            </button>
            {schema.keys.length > 0 && (
              <span style={styles.schemaKeys}>
                schema: [{schema.keys.join(", ")}]
              </span>
            )}
          </header>
          <ReactFlowProvider>
            <div style={styles.body}>
              <div style={styles.dagArea}>
                <PipelineDAG />
              </div>
              <div style={styles.sidebar}>
                <Sidebar />
              </div>
            </div>
          </ReactFlowProvider>
        </div>
      </SelectedNodeProvider>
    </RunLogProvider>
  );
}
