/**
 * "🐞 Report bug…" popup (issue #20), opened from the header's ⋯ menu
 * (HeaderMoreMenu).
 *
 * Step "write": title, description, and "Save my project to attach" (on by
 * default) with the export checkboxes — the SAME choices and defaults as
 * "Export project" (backend `get_export_options`, owned by
 * scidb.project.bundle.ExportOptions: history on, data off).
 *
 * Step "attach": the project was written as `<db>-bug-report.scistack.zip`
 * (`export_project_bundle` with `attachable`, a plain zip GitHub accepts) and
 * the popup says where it is, why attaching it helps, and how. Open on GitHub
 * opens the prefilled new-issue page (bugReport.ts owns the link). No token,
 * no backend upload: the user drags the file in and presses Submit.
 */

import { useEffect, useState } from "react";
import { callBackend, isVSCodeMode, openExternalUrl, pickSavePath, revealInOs } from "../api";
import { BUNDLE_BUILD } from "../bundleBuild";
import {
  GITHUB_ATTACHMENT_LIMIT_BYTES,
  bugReportFileName,
  bugReportLink,
  fileNameOf,
} from "./bugReport";
import type { ExportOptionsResponse, ExportResult } from "./ProjectExportButton";
import * as modalStyles from "./modalStyles";

const BODY_PLACEHOLDER = [
  "What happened?",
  "",
  "What did you expect to happen?",
  "",
  "Steps to reproduce:",
  "1. ",
].join("\n");

const field: React.CSSProperties = {
  width: "100%",
  boxSizing: "border-box",
  background: "#0f0f1e",
  color: "#eee",
  border: "1px solid #3a3a5a",
  borderRadius: 4,
  padding: "6px 8px",
  fontFamily: "inherit",
  fontSize: 13,
};

const button: React.CSSProperties = {
  padding: "4px 12px",
  background: "#2a2a4a",
  color: "#ccc",
  border: "1px solid #3a3a5a",
  borderRadius: 4,
  cursor: "pointer",
  fontSize: 12,
  fontFamily: "inherit",
};

const primary: React.CSSProperties = {
  ...button,
  background: "#164e63",
  color: "#a5f3fc",
  borderColor: "#0891b2",
};

const note: React.CSSProperties = { fontSize: 12, opacity: 0.75, marginBottom: 12, lineHeight: 1.5 };

export default function BugReportDialog({ onClose }: { onClose: () => void }) {
  const [title, setTitle] = useState("");
  const [body, setBody] = useState("");
  const [attach, setAttach] = useState(true);
  const [spec, setSpec] = useState<ExportOptionsResponse | null>(null);
  const [values, setValues] = useState<Record<string, boolean>>({});
  const [saved, setSaved] = useState<ExportResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    callBackend("get_export_options")
      .then((d) => {
        const s = d as ExportOptionsResponse;
        setSpec(s);
        setValues(Object.fromEntries(s.choices.map((c) => [c.name, c.default])));
      })
      .catch((err) => {
        console.warn("[SciStack] bug report: get_export_options failed", err);
        setError(`Cannot save the project file: ${(err as Error).message}`);
        setAttach(false);
      });
  }, []);

  const openGitHub = async (attachmentName?: string) => {
    const { url, truncated } = bugReportLink(title, body, BUNDLE_BUILD, attachmentName);
    console.info("[SciStack] bug report link", {
      title_length: title.length,
      body_length: body.length,
      url_length: url.length,
      truncated,
      attachment: attachmentName ?? null,
    });
    setError(null);
    try {
      await openExternalUrl(url);
    } catch (err) {
      setError(`Could not open GitHub: ${(err as Error).message}`);
    }
  };

  const saveProject = async () => {
    if (!spec) return;
    setError(null);
    const path = await pickSavePath({
      defaultName: bugReportFileName(spec.default_name, spec.extension),
      formats: ["zip"],
      filterName: "Zipped SciStack project",
      prompt: "Save the project file for the bug report as (absolute path):",
    }).catch((err) => {
      setError(`Could not choose where to save: ${(err as Error).message}`);
      return null;
    });
    if (!path) return; // cancelled
    setBusy(true);
    try {
      const r = (await callBackend("export_project_bundle", {
        out_path: path,
        options: values,
        attachable: true,
      })) as ExportResult;
      console.info("[SciStack] bug report project file", { path: r.path, bytes: r.bytes, options: r.options });
      setSaved(r);
    } catch (err) {
      setError(`Saving the project failed: ${(err as Error).message}`);
    } finally {
      setBusy(false);
    }
  };

  const titleMissing = title.trim() === "";

  return (
    <div
      style={modalStyles.overlay}
      role="dialog"
      aria-modal="true"
      onClick={(e) => { if (e.target === e.currentTarget && !busy) onClose(); }}
      onKeyDown={(e) => { if (e.key === "Escape" && !busy) onClose(); }}
    >
      <div style={{ ...modalStyles.dialog, width: 560 }} data-testid="bug-report-dialog">
        <div style={modalStyles.dialogTitle}>Report a bug</div>
        {saved ? (
          <AttachStep saved={saved} />
        ) : (
          <>
            <div style={note}>
              This opens GitHub's new-issue page with your text filled in. Sign in
              there and press <b>Submit new issue</b>. The GUI version is added
              automatically.
            </div>
            <label style={{ fontSize: 12, display: "block", marginBottom: 4 }}>Title</label>
            <input
              style={field}
              value={title}
              autoFocus
              maxLength={256}
              placeholder="Short summary of the problem"
              onChange={(e) => setTitle(e.target.value)}
              data-testid="bug-report-title"
            />
            <label style={{ fontSize: 12, display: "block", margin: "12px 0 4px" }}>Description</label>
            <textarea
              style={{ ...field, minHeight: 180, resize: "vertical", fontFamily: "monospace", fontSize: 12 }}
              value={body}
              placeholder={BODY_PLACEHOLDER}
              onChange={(e) => setBody(e.target.value)}
              data-testid="bug-report-body"
            />
            <label style={{ display: "flex", gap: 6, alignItems: "flex-start", fontSize: 12, marginTop: 12, cursor: "pointer" }}>
              <input
                type="checkbox"
                checked={attach}
                disabled={!spec || busy}
                onChange={(e) => setAttach(e.target.checked)}
                data-testid="bug-report-attach"
              />
              <span>
                <b>Save my project to attach to the issue</b> (recommended): a
                <code> .scistack.zip</code> file with your code, canvas and run
                history, so the bug can be reproduced.
              </span>
            </label>
            {attach && spec && (
              <div style={{ marginLeft: 22, marginTop: 4 }}>
                {spec.choices.map((c) => (
                  <label key={c.name} style={{ display: "flex", gap: 6, fontSize: 12, margin: "4px 0", cursor: "pointer" }}>
                    <input
                      type="checkbox"
                      checked={values[c.name] ?? c.default}
                      disabled={busy}
                      onChange={(e) => setValues((v) => ({ ...v, [c.name]: e.target.checked }))}
                    />
                    <span>{c.label}</span>
                  </label>
                ))}
                <div style={{ fontSize: 11, opacity: 0.6, marginTop: 4 }}>
                  GitHub issues are public. Raw input files are never included.
                </div>
              </div>
            )}
          </>
        )}
        {error && (
          <div style={{ color: "#ff4d4f", fontSize: 12, marginTop: 8, wordBreak: "break-word" }}>{error}</div>
        )}
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16 }}>
          {saved ? (
            <>
              <button style={button} onClick={() => setSaved(null)}>Back</button>
              {isVSCodeMode && (
                <button
                  style={button}
                  onClick={() => revealInOs(saved.path).catch((err) => setError((err as Error).message))}
                  data-testid="bug-report-reveal"
                >
                  Show in folder
                </button>
              )}
              <button style={button} onClick={onClose}>Close</button>
              <button
                style={primary}
                onClick={() => openGitHub(fileNameOf(saved.path))}
                data-testid="bug-report-submit"
              >
                Open on GitHub
              </button>
            </>
          ) : (
            <>
              <button style={button} onClick={onClose} disabled={busy}>Cancel</button>
              <button
                style={primary}
                onClick={() => (attach ? saveProject() : openGitHub().then(onClose))}
                disabled={busy || titleMissing || (attach && !spec)}
                title={titleMissing ? "Give the bug a title first" : undefined}
                data-testid="bug-report-next"
              >
                {busy ? "Saving project…" : attach ? "Save project & continue" : "Open on GitHub"}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/** Where the file is, why attaching it matters, and how. */
function AttachStep({ saved }: { saved: ExportResult }) {
  const mb = saved.bytes / (1024 * 1024);
  const size = mb >= 1 ? `${mb.toFixed(1)} MB` : `${Math.max(1, Math.round(saved.bytes / 1024))} KB`;
  const tooBig = saved.bytes > GITHUB_ATTACHMENT_LIMIT_BYTES;
  const name = fileNameOf(saved.path);
  return (
    <div data-testid="bug-report-attach-step">
      <div style={{ fontSize: 12, color: "#9ae6b4", marginBottom: 12, wordBreak: "break-all" }}>
        Project saved: {saved.path} ({size})
      </div>
      <div style={note}>
        <b>Optional, but strongly recommended:</b> attach this file to the issue.
        A bug that can be reproduced from your own pipeline is far more likely
        to be fixed, and fixed quickly.
      </div>
      <ol style={{ fontSize: 12, lineHeight: 1.6, paddingLeft: 20, margin: "0 0 8px" }}>
        <li>Click <b>Open on GitHub</b> below and sign in if asked.</li>
        <li>
          Drag <code>{name}</code> into the description box (or click
          "Paste, drop, or click to add files" under it).
          {isVSCodeMode && <> <b>Show in folder</b> opens its folder.</>}
        </li>
        <li>Wait for the upload to finish, then press <b>Submit new issue</b>.</li>
      </ol>
      <div style={{ fontSize: 11, opacity: 0.6 }}>
        Includes: code, config, canvas, saved plots
        {saved.options.include_history ? ", run history" : ""}
        {saved.options.include_logs ? ", the end of your logs" : ""}
        {saved.options.include_data ? ", saved data" : ", no data"}
        {saved.options.hide_path_locations ? "; data folder paths hidden" : ""}.
      </div>
      {tooBig && (
        <div style={{ fontSize: 12, color: "#fbbf24", marginTop: 8 }}>
          This file is over GitHub's 25 MB attachment limit. Go Back and untick
          data, or share the file another way (e.g. a cloud link in the issue).
        </div>
      )}
    </div>
  );
}
