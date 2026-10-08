/**
 * ProjectBootstrapWizard — browser-frontend equivalent of the VS Code
 * extension's "SciStack: Open Pipeline" command (extension/src/extension.ts).
 *
 * Shown by App.tsx whenever GET /api/info reports `db_loaded: false`, i.e.
 * `scistack-gui` was launched with no db_path (or a nonexistent one and no
 * --schema-keys) — see scistack_gui/__main__.py and
 * docs/claude/scistack-gui-project-setup-guide.md §5. Unlike the VS Code
 * wizard, there's no native file-picker to fall back on here: the browser
 * has no API that returns a real filesystem path from a dialog, so every
 * location is a plain text input (server-side directory browsing was
 * considered and deliberately deferred — see
 * .claude/plan-browser-db-creation-wizard.md).
 *
 * Two POST calls do all the work, both wrapping the same backend sequence
 * the CLI runs at startup (scistack_gui.bootstrap.open_or_create_project):
 *   - create_project → POST /api/bootstrap/create
 *   - open_project   → POST /api/bootstrap/open
 * On success, `onReady` re-fetches /api/info (App.tsx's refreshInfo), which
 * flips the app straight into the normal DAG shell — no page reload.
 *
 * Unlike the VS Code wizard, there's no "Pipeline Code" step asking for a
 * project/module path: neither call passes `module`/`project`, so the
 * backend always falls into open_or_create_project()'s auto-discovery
 * branch — the same loose-script/folder-scan mode already used elsewhere
 * in this codebase (scistack_gui/config.py's load_config(None, db_path)),
 * which searches upward for a pyproject.toml first and otherwise scans the
 * database's own directory for .py/.m files. Nothing needs to be typed in
 * up front; "Refresh Code" in the header re-scans later once files exist.
 *
 * Creating a database runs scidb.project.init_project (via POST
 * /api/bootstrap/create): the folder becomes a SciStack project -- a package
 * with pyproject.toml, src/<name>/, scistack.toml and the entities file. The
 * optional "Entities file" field overrides where that file goes; blank means
 * the default, inside the package (2026-10-08).
 */

import { useState } from "react";
import { callBackend } from "../../api";
import * as modalStyles from "../modalStyles";

type DbMode = "open" | "create" | null;

interface Props {
  onReady: () => void;
}

export default function ProjectBootstrapWizard({ onReady }: Props) {
  const [dbMode, setDbMode] = useState<DbMode>(null);
  const [openPath, setOpenPath] = useState("");
  const [createFolder, setCreateFolder] = useState("");
  const [createFilename, setCreateFilename] = useState("");
  const [createSchemaKeys, setCreateSchemaKeys] = useState("");
  const [createEntitiesFile, setCreateEntitiesFile] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const schemaKeysList = createSchemaKeys
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const canSubmit =
    (dbMode === "open"
      ? openPath.trim().length > 0
      : dbMode === "create"
        ? createFolder.trim().length > 0 &&
          createFilename.trim().length > 0 &&
          schemaKeysList.length > 0
        : false) && !submitting;

  const handleSubmit = async () => {
    if (!canSubmit) return;
    setSubmitting(true);
    setError(null);
    try {
      if (dbMode === "create") {
        await callBackend("create_project", {
          folder: createFolder.trim(),
          filename: createFilename.trim(),
          schema_keys: schemaKeysList,
          entities_file: createEntitiesFile.trim() || null,
        });
      } else {
        await callBackend("open_project", {
          db_path: openPath.trim(),
        });
      }
      onReady();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={modalStyles.overlay}>
      <div style={{ ...modalStyles.dialog, width: 520 }}>
        <div style={modalStyles.dialogTitle}>
          SciStack — Open or Create a Project
        </div>
        <p style={styles.intro}>
          No database is open yet. Open an existing .duckdb file, or create a
          new one and define its schema. Pipeline code (Python/MATLAB files
          next to the database, or a pyproject.toml found above it) is
          discovered automatically once the database is open — no need to
          point at it here.
        </p>

        <Section title="Database">
          <RadioRow
            name="dbMode"
            value={dbMode}
            onChange={(v) => setDbMode(v as DbMode)}
            options={[
              { value: "open", label: "Open existing database" },
              { value: "create", label: "Create new database" },
            ]}
          />
          {dbMode === "open" && (
            <Field label="Database path (.duckdb)">
              <input
                style={styles.input}
                value={openPath}
                onChange={(e) => setOpenPath(e.target.value)}
                placeholder="/path/to/experiment.duckdb"
              />
            </Field>
          )}
          {dbMode === "create" && (
            <>
              <Field label="Destination folder (must already exist)">
                <input
                  style={styles.input}
                  value={createFolder}
                  onChange={(e) => setCreateFolder(e.target.value)}
                  placeholder="/path/to/my_study"
                />
              </Field>
              <Field label="Filename">
                <input
                  style={styles.input}
                  value={createFilename}
                  onChange={(e) => setCreateFilename(e.target.value)}
                  placeholder="my_study (.duckdb appended automatically)"
                />
              </Field>
              <Field label="Schema keys (comma-separated, top-down)">
                <input
                  style={styles.input}
                  value={createSchemaKeys}
                  onChange={(e) => setCreateSchemaKeys(e.target.value)}
                  placeholder="subject, session, trial"
                />
              </Field>
              <Field label="Entities file (Variables/Parameters/PathInputs created from the GUI, relative to the project root — not to the database folder)">
                <input
                  style={styles.input}
                  value={createEntitiesFile}
                  onChange={(e) => setCreateEntitiesFile(e.target.value)}
                  placeholder="blank = src/<project>/scistack_entities.toml"
                />
              </Field>
            </>
          )}
        </Section>

        {error && <div style={styles.errorBanner}>{error}</div>}

        <div style={styles.footer}>
          <button
            style={{
              ...styles.submitBtn,
              opacity: canSubmit ? 1 : 0.5,
              cursor: canSubmit ? "pointer" : "not-allowed",
            }}
            disabled={!canSubmit}
            onClick={handleSubmit}
          >
            {submitting
              ? "Working…"
              : dbMode === "create"
                ? "Create Database"
                : "Open Database"}
          </button>
        </div>
      </div>
    </div>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>{title}</div>
      {children}
    </div>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label style={styles.field}>
      <span style={styles.fieldLabel}>{label}</span>
      {children}
    </label>
  );
}

function RadioRow({
  name,
  value,
  onChange,
  options,
}: {
  name: string;
  value: string | null;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <div style={styles.radioRow}>
      {options.map((opt) => (
        <label key={opt.value} style={styles.radioLabel}>
          <input
            type="radio"
            name={name}
            checked={value === opt.value}
            onChange={() => onChange(opt.value)}
          />
          {opt.label}
        </label>
      ))}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  intro: {
    fontSize: 13,
    lineHeight: 1.5,
    opacity: 0.85,
    marginBottom: 16,
  },
  section: {
    marginBottom: 20,
    paddingBottom: 16,
    borderBottom: "1px solid #2a2a4a",
  },
  sectionTitle: {
    fontSize: 13,
    fontWeight: 700,
    marginBottom: 10,
    opacity: 0.9,
  },
  radioRow: {
    display: "flex",
    flexDirection: "column",
    gap: 8,
    marginBottom: 12,
  },
  radioLabel: {
    display: "flex",
    alignItems: "center",
    gap: 8,
    fontSize: 13,
    cursor: "pointer",
  },
  field: {
    display: "flex",
    flexDirection: "column",
    gap: 4,
    marginBottom: 10,
  },
  fieldLabel: {
    fontSize: 12,
    opacity: 0.75,
  },
  input: {
    background: "#0f0f1e",
    border: "1px solid #2a2a4a",
    borderRadius: 4,
    color: "#eee",
    padding: "6px 8px",
    fontSize: 13,
    fontFamily: "monospace",
  },
  errorBanner: {
    background: "rgba(255, 77, 79, 0.12)",
    border: "1px solid #ff4d4f",
    borderRadius: 4,
    padding: "8px 10px",
    fontSize: 12,
    color: "#ff4d4f",
    marginBottom: 12,
    whiteSpace: "pre-wrap",
  },
  footer: {
    display: "flex",
    justifyContent: "flex-end",
  },
  submitBtn: {
    background: "#3a3aff",
    color: "#fff",
    border: "none",
    borderRadius: 4,
    padding: "8px 16px",
    fontSize: 13,
    fontWeight: 600,
  },
};
