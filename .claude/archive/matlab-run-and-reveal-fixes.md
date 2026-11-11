# MATLAB Run Routing & reveal_in_editor UNC Path Fixes

## Context

Two bugs surfaced when the user clicked **Run** on a MATLAB function node
(`load10MWTREDCapReport`) in the VS Code extension GUI:

1. The Python server responded with
   `"Function 'load10MWTREDCapReport' not found in registry. Did you pass --module or --project with the script that defines it?"`
   even though the MATLAB registry had successfully registered that
   function at startup (confirmed in the log:
   `Registered MATLAB function: load10MWTREDCapReport`).
2. The `reveal_in_editor` RPC that should have opened
   `…\src\overground\load10MWTREDCapReport.m` in a side editor did nothing
   visible, even though `dagPanel.ts` logged the call.

Both issues are in the GUI layer (per CLAUDE.md, this is the right layer
for these fixes).

---

## Root Cause #1 — MATLAB run routing

`scistack-gui/extension/src/dagPanel.ts:76-82` intercepts `start_run` and
dispatches MATLAB runs to `handleMatlabRun` (which generates a
ready-to-paste MATLAB command) **only when `params.language === 'matlab'`**.
Otherwise the call is forwarded to the Python server, whose
`_h_start_run` → `_run_in_thread` → `registry.get_function()` path only
knows about Python functions and raises `KeyError`.

The backend already tags MATLAB function nodes with
`fn_data["language"] = "matlab"` in
`scistack-gui/scistack_gui/api/pipeline.py:488-489`, so the React node's
`data.language` is `"matlab"` for MATLAB functions. But
`frontend/src/components/DAG/FunctionNode.tsx:111-119` never forwards that
field in the `start_run` payload:

```tsx
await callBackend('start_run', {
  function_name: data.label,
  variants: checkedVariants,
  run_id: newRunId,
  schema_filter: data.schemaFilter ?? null,
  schema_level: data.schemaLevel ?? null,
  run_options: data.runOptions ?? null,
  where_filters: (wf && wf.length > 0) ? wf : null,
  // no language field → dagPanel treats every run as Python
})
```

Every MATLAB Run thus falls through to the Python server and fails.

## Root Cause #2 — reveal_in_editor silent failure

`dagPanel.ts:117-136` calls `vscode.workspace.openTextDocument(uri)` and
then `vscode.window.showTextDocument(…)`. On error, the outer `catch` in
the message handler (`dagPanel.ts:66-71`) sends the error back to the
webview. The webview caller in `FunctionNode.tsx:122-140` only surfaces
errors thrown at the extension boundary — and the error object format from
the webview message can be swallowed, yielding a no-op.

The file paths involved are UNC (`\\fs2.smpp.local\RTO\…`) because our
MATLAB path resolution converts the mapped-drive project root
(`y:\LabMembers\…`) into its canonical UNC form. `vscode.Uri.file()`
handles UNC paths, but if `openTextDocument` throws for any reason
(permissions, timeout, path canonicalization quirk on remote SMB) we lose
the error to the webview channel and the user just sees "nothing
happened".

---

## Fix Plan

### Part A — Forward the `language` field from FunctionNode to start_run

**File:** `scistack-gui/frontend/src/components/DAG/FunctionNode.tsx`

1. Extend `FunctionNodeData` interface (line 18-27) with
   `language?: string`.
2. In `handleRun` (line 111-119), add `language: data.language` to the
   `start_run` payload. No fallback needed — `undefined` means "Python",
   which matches the `!== 'matlab'` check in `dagPanel.ts`.

No backend changes required: `_h_start_run` already ignores unknown
params (it only extracts the fields it cares about).

### Part B — Surface reveal_in_editor errors and add diagnostics

**File:** `scistack-gui/extension/src/dagPanel.ts`

1. In `revealInEditor` (line 117-136), wrap the
   `openTextDocument` + `showTextDocument` sequence with explicit
   try/catch that logs the failure to `this.outputChannel` **with the
   full file path and error message** before re-throwing, so the user
   can see what went wrong in the VS Code Output channel even if the
   webview swallows the response.
2. Normalize the URI: when the file path starts with `\\`, explicitly
   construct the URI via `vscode.Uri.from({ scheme: 'file', authority,
   path })` instead of `vscode.Uri.file()`, which has had edge cases
   with UNC paths in older VS Code versions. Cheap to do and removes a
   known source of flakiness.
3. Log the final URI (`uri.toString()`) to `outputChannel` before the
   `openTextDocument` call so we can see the canonicalized form.

### Part C — (Optional) Convert script files to function warnings less
alarming

Not required for the run/reveal bug, but noted from the same log:
`main_overground.m`, `mainAllSubjects_Aim2.m`, etc. are parsed by
`parse_matlab_function` and warned about. These are scripts, not
functions. We could demote the log from `WARNING` to `INFO` when the
file clearly has no `function` keyword, or add a separate script
category. **Defer unless requested** — not a blocker.

---

## Testing

### Manual reproduction (on Windows)

1. Open the Stroke-R01-Aim-2 project in the GUI.
2. Click **Run** on `load10MWTREDCapReport` MATLAB function node.
3. **Expected after fix:** the extension runs `handleMatlabRun`, which
   generates a MATLAB command and either sends it to the MATLAB
   terminal or copies it to clipboard. No `"Function … not found in
   registry"` error.
4. Double-click the function label.
5. **Expected after fix:** the `.m` file opens in the side editor.
   If it fails, the VS Code Output → "SciStack" channel contains the
   exact error.

### Unit tests

* Add a frontend test (or extend the existing FunctionNode test, if
  any) that asserts the `start_run` payload includes `language` when
  `data.language === 'matlab'`.
* Extension-side: no good way to test VS Code URI opening without a
  full integration harness; defer.

### Regression checks

* Python function run still works (no `language` field means dagPanel
  falls through to `pythonProcess.request('start_run', …)` as before).
* MATLAB function double-click with a normal local path still opens
  the file.

---

## Files Touched (summary)

| File | Change |
| --- | --- |
| `scistack-gui/frontend/src/components/DAG/FunctionNode.tsx` | Add `language` to `FunctionNodeData` + include it in `start_run` payload |
| `scistack-gui/extension/src/dagPanel.ts` | Log errors in `revealInEditor`; prefer explicit UNC URI construction |

Rebuild steps after editing:

```
cd /workspace/scistack-gui/frontend && npm run build
cd /workspace/scistack-gui/extension && npm run build
```

The VS Code extension will need "SciStack: Restart Python Process"
(or full extension reload) to pick up the new assets.

---

## Open Questions / Follow-ups

1. Does the extension currently display `error: { message: … }`
   responses from the extension to the user in any way? Worth
   inspecting `FunctionNode.tsx:138` `window.alert(\`Failed to open
   source: ${err}\`)` — that should trigger, but maybe didn't. Need
   to confirm whether the error propagated or was swallowed between
   webview postMessage and the caller's await.
2. Should we consolidate the Python vs MATLAB dispatch into the Python
   server instead of the extension? Right now knowledge of "MATLAB runs
   go through `generate_matlab_command`" is split across frontend
   (sets `language`), extension (intercepts), and backend (tags nodes).
   A single JSON-RPC method could handle both cases. Deferred — larger
   refactor.
3. The `matlab-path-resolution.md` doc in `docs/claude/` (currently
   untracked in git) should document the y:\ → UNC conversion so
   future debugging is easier.
