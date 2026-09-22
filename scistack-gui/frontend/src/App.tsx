/**
 * Root application component.
 *
 * Layout:
 *   ┌───────────────────────────────┬─────────────┐
 *   │  header: SciStack + db name   │             │
 *   │  + Restart/Report/📁 Paths    │             │
 *   ├────────────────────────────────┤  sidebar    │
 *   │  HypothesisTabs: tab strip +  │  (right 1/4,│
 *   │  Research Question row        │  full       │
 *   ├────────────────────────────────┤  screen    │
 *   │  PipelineDAG (RunsDock docked │  height)    │
 *   │  bottom-left)                 │             │
 *   └───────────────────────────────┴─────────────┘
 *
 * The header and HypothesisTabs live INSIDE the dagArea column (not
 * spanning the full window width) so the sidebar can run the full height
 * of the screen instead of starting below a full-width header row — the
 * header's buttons end up left-aligned over the (now narrower) canvas
 * column as a natural consequence, not via extra positioning.
 *
 * The 📁 Paths button opens PathsPopup (formerly the sidebar's permanent
 * "Project" tab); RunsDock (formerly the sidebar's "Runs" tab) is now a
 * React Flow Panel inside PipelineDAG so run status stays visible without a
 * tab switch — see components/RunsDock.tsx and components/PathsPopup.tsx.
 * The sidebar itself no longer has its own tab bar either — see
 * components/Sidebar/Sidebar.tsx.
 */

import { useEffect, useState, useCallback } from "react";
import { ReactFlowProvider } from "@xyflow/react";
import PipelineDAG from "./components/DAG/PipelineDAG";
import Breadcrumb from "./components/DAG/Breadcrumb";
import HypothesisTabs from "./components/HypothesisTabs";
import PathsPopup from "./components/PathsPopup";
import ProvenancePanel from "./components/Provenance/ProvenancePanel";
import Sidebar from "./components/Sidebar/Sidebar";
import PipelineRunController from "./components/PipelineRunController";
import { RunLogProvider } from "./context/RunLogContext";
import { SelectedNodeProvider } from "./context/SelectedNodeContext";
import { SidebarSelectionProvider } from "./context/SidebarSelectionContext";
import { ScopeProvider } from "./context/ScopeContext";
import { PlanRunProvider } from "./context/PlanRunContext";
import { ClipboardProvider } from "./context/ClipboardContext";
import { addNotificationHandler, callBackend, isVSCodeMode } from "./api";
import { injectedDbName } from "./session";
import * as modalStyles from "./components/modalStyles";
import ProjectBootstrapWizard from "./components/Bootstrap/ProjectBootstrapWizard";

/**
 * Startup diagnostics reported by the backend's get_info response.
 * Populated by Phase 8 (stale lockfile handling): when a project is opened
 * with an out-of-date uv.lock the backend tries to run `uv sync`, and any
 * failure shows up here as a blocking error so the user never interacts
 * with a broken venv.
 */
interface StartupError {
  kind: string;
  message: string;
  details: string;
  blocking: boolean;
}

interface InfoResponse {
  db_loaded?: boolean;
  db_name?: string;
  startup_errors?: StartupError[];
}

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
  refreshCodeBtn: {
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
  refreshBtn: {
    padding: "4px 12px",
    background: "#2a2a4a",
    color: "#ccc",
    border: "1px solid #3a3a5a",
    borderRadius: 4,
    cursor: "pointer",
    fontSize: 12,
    fontFamily: "inherit",
  },
  reportBtn: {
    padding: "4px 12px",
    background: "#164e63",
    color: "#a5f3fc",
    border: "1px solid #0891b2",
    borderRadius: 4,
    cursor: "pointer",
    fontSize: 12,
    fontFamily: "inherit",
  },
  pathsBtn: {
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
    display: "flex",
    flexDirection: "column",
  },
  canvasWrap: {
    flex: 1,
    minHeight: 0,
  },
  sidebar: {
    flex: 1,
    minWidth: 0,
    borderLeft: "1px solid #2a2a4a",
    background: "#12122a",
  },
  // --- Blocking startup-error dialog (Phase 8: stale lockfile handling) ---
  // Overlay/dialog/title chrome now lives in ./components/modalStyles —
  // these three entries are gone; see the red-accent override in
  // StartupErrorDialog below.
  startupDialogMessage: {
    fontSize: 13,
    lineHeight: 1.5,
    marginBottom: 12,
    whiteSpace: "pre-wrap" as const,
  },
  startupDialogDetails: {
    background: "#0f0f1e",
    border: "1px solid #2a2a4a",
    borderRadius: 4,
    padding: 10,
    fontFamily: "monospace",
    fontSize: 11,
    whiteSpace: "pre-wrap" as const,
    maxHeight: 260,
    overflow: "auto",
    color: "#ccc",
  },
  startupDialogFooter: {
    marginTop: 16,
    fontSize: 12,
    opacity: 0.75,
  },
};

export default function App() {
  const [schema, setSchema] = useState<{ keys: string[] }>({ keys: [] });
  // Seeded from the value the extension injected into this webview, so the
  // header names the right database from the first frame — no "loading…"
  // flash, and no way for it to disagree with the server this tab talks to.
  // The browser build has no injected session and falls back to get_info.
  const [dbName, setDbName] = useState(injectedDbName);
  // null = info not fetched yet; false = no project open (browser wizard);
  // true = normal DAG shell. VS Code always has a project open by the time
  // its webview mounts, so it never observes `false` here.
  const [dbLoaded, setDbLoaded] = useState<boolean | null>(null);
  const [restarting, setRestarting] = useState(false);
  const [refreshingCode, setRefreshingCode] = useState(false);
  const [reporting, setReporting] = useState(false);
  const [startupErrors, setStartupErrors] = useState<StartupError[]>([]);
  const [pathsOpen, setPathsOpen] = useState(false);
  const [provenanceOpen, setProvenanceOpen] = useState(false);

  // Endpoint report: db.inspect.write_report → self-contained index.html
  // (figures embedded). Standalone opens it via the artifacts file route;
  // the VS Code webview can't open new tabs, so it shows the path.
  const handleReport = useCallback(async () => {
    setReporting(true);
    try {
      const res = (await callBackend("write_report")) as {
        index_path: string;
      };
      if (isVSCodeMode) {
        window.alert(`Report written to:\n${res.index_path}`);
      } else {
        window.open(
          `/api/artifacts/file?path=${encodeURIComponent(res.index_path)}`,
          "_blank",
        );
      }
    } catch (err) {
      window.alert(`Report failed: ${(err as Error).message}`);
    } finally {
      setReporting(false);
    }
  }, []);

  // Lightweight alternative to Restart: re-imports the configured Python/
  // MATLAB files in-process (registry.refresh_all/refresh_module, already
  // implemented server-side — this just gives it a UI trigger) instead of
  // killing and respawning the whole Python process. Backend already
  // broadcasts dag_updated on success, which every discovery-consuming
  // component (EditTab, ProjectConfigPanel, etc.) listens for.
  const handleRefreshCode = useCallback(async () => {
    setRefreshingCode(true);
    try {
      await callBackend("refresh_module");
    } catch (err) {
      console.error("Refresh code failed:", err);
    } finally {
      setRefreshingCode(false);
    }
  }, []);

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

  // Fetches /api/info and branches the whole app on db_loaded. Passed to
  // ProjectBootstrapWizard as onReady so a successful create/open flips
  // straight into the normal DAG shell without a page reload — the backend
  // db._db singleton is just a swappable module global (scistack_gui/db.py),
  // so re-fetching here is all that's needed once the wizard's POST lands.
  const refreshInfo = useCallback(() => {
    callBackend("get_info")
      .then((d) => {
        const info = d as InfoResponse;
        if (info.db_loaded === false) {
          setDbLoaded(false);
          return;
        }
        setDbLoaded(true);
        setDbName(info.db_name ?? "");
        setStartupErrors(info.startup_errors ?? []);
      })
      .catch(console.error);
  }, []);

  useEffect(() => {
    refreshInfo();
  }, [refreshInfo]);

  // A restarted server may be a different database (or the same one with a
  // changed schema), and this panel's React state survives the swap because
  // of retainContextWhenHidden. Re-ask rather than keep what we had: a
  // header showing a database the backend is not serving is the 2026-09-22
  // bug, and it was invisible precisely because nothing ever re-fetched.
  useEffect(
    () =>
      addNotificationHandler((msg) => {
        if (msg.method === "db_changed") refreshInfo();
      }),
    [refreshInfo]
  );

  // Only fetch schema/etc. once a project is actually open — before that,
  // every other endpoint 500s on the missing db singleton (harmless, but
  // noisy, and there's nothing to render yet regardless).
  useEffect(() => {
    if (!dbLoaded) return;
    callBackend("get_schema")
      .then((d) => setSchema(d as { keys: string[] }))
      .catch(console.error);
  }, [dbLoaded]);

  // Phase 8: any blocking startup error pauses the whole UI.
  const blockingErrors = startupErrors.filter((e) => e.blocking);

  if (dbLoaded === false) {
    return <ProjectBootstrapWizard onReady={refreshInfo} />;
  }

  if (dbLoaded === null) {
    return null;
  }

  return (
    <RunLogProvider>
      <SelectedNodeProvider>
        <SidebarSelectionProvider>
        <ScopeProvider>
          <PlanRunProvider>
          <ClipboardProvider>
            <div style={styles.root}>
              <ReactFlowProvider>
                <div style={styles.body}>
                  <div style={styles.dagArea}>
                    <header style={styles.header}>
                      <span style={styles.title}>SciStack</span>
                      <span style={styles.separator}>|</span>
                      <span style={styles.dbName}>{dbName || "loading…"}</span>
                      <button
                        style={styles.refreshCodeBtn}
                        onClick={handleRefreshCode}
                        disabled={refreshingCode}
                        title="Re-import your Python/MATLAB files to pick up edits, without restarting the whole process"
                      >
                        {refreshingCode ? "Refreshing…" : "🔄 Refresh Code"}
                      </button>
                      <button
                        style={styles.refreshBtn}
                        onClick={handleRestart}
                        disabled={restarting}
                        title="Restart the Python process to pick up edits to server or pipeline code"
                      >
                        {restarting ? "Restarting..." : "Restart"}
                      </button>
                      <button
                        style={styles.reportBtn}
                        onClick={handleReport}
                        disabled={reporting}
                        title="Write the endpoint report (figures + stats with provenance) and open it"
                      >
                        {reporting ? "Writing…" : "📄 Report"}
                      </button>
                      <button
                        style={styles.pathsBtn}
                        onClick={() => setPathsOpen(true)}
                        title="Configured code paths (Python + MATLAB) and discovered exports"
                      >
                        📁 Paths
                      </button>
                      <button
                        style={styles.pathsBtn}
                        onClick={() => setProvenanceOpen(true)}
                        title="Pick a variable and a variant, and see the functions, versions and runs that produced it (same answer as `scidb trace --variant … --runs`)"
                      >
                        🔍 Provenance
                      </button>
                      {schema.keys.length > 0 && (
                        <span style={styles.schemaKeys}>
                          schema: [{schema.keys.join(", ")}]
                        </span>
                      )}
                    </header>
                    <HypothesisTabs />
                    <Breadcrumb />
                    <div style={styles.canvasWrap}>
                      <PipelineDAG />
                    </div>
                  </div>
                  <div style={styles.sidebar}>
                    <Sidebar />
                  </div>
                </div>
              </ReactFlowProvider>
              <PipelineRunController />
              {pathsOpen && <PathsPopup onClose={() => setPathsOpen(false)} />}
              {provenanceOpen && (
                <ProvenancePanel onClose={() => setProvenanceOpen(false)} />
              )}
              {blockingErrors.length > 0 && (
                <StartupErrorDialog errors={blockingErrors} />
              )}
            </div>
          </ClipboardProvider>
          </PlanRunProvider>
        </ScopeProvider>
        </SidebarSelectionProvider>
      </SelectedNodeProvider>
    </RunLogProvider>
  );
}

/**
 * Blocking modal shown when the backend reports a startup-time error
 * (e.g. failed `uv sync`). There's no dismiss button on purpose — the
 * user needs to fix the problem and restart the project rather than
 * interact with a broken venv.
 */
function StartupErrorDialog({ errors }: { errors: StartupError[] }) {
  return (
    <div style={modalStyles.overlay} role="alertdialog" aria-modal="true">
      <div style={{ ...modalStyles.dialog, border: "1px solid #ff4d4f" }}>
        <div style={{ ...modalStyles.dialogTitle, color: "#ff4d4f" }}>
          Project failed to open cleanly
        </div>
        {errors.map((err, i) => (
          <div key={`${err.kind}-${i}`} style={{ marginBottom: 16 }}>
            <div style={styles.startupDialogMessage}>{err.message}</div>
            {err.details && (
              <pre style={styles.startupDialogDetails}>{err.details}</pre>
            )}
          </div>
        ))}
        <div style={styles.startupDialogFooter}>
          Fix the problem above, then restart the SciStack project to
          continue.
        </div>
      </div>
    </div>
  );
}
