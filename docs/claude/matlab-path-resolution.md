# MATLAB Path Resolution — `matlab.functions` vs `matlab.variables`

This note documents how `scistack_gui.project.config.load_config` resolves the two
MATLAB file lists declared in `[tool.scistack.matlab]` (or in the `[matlab]`
table of a top-level `scistack.toml`), and how it handles the common case
where their file sets overlap.

Related reading:
- `scistack-gui/scistack_gui/project/config.py` — implementation
- `docs/claude/matlab-gui-implementation.md` — broader MATLAB support overview
- `scistack-gui/tests/test_config.py` — regression tests

---

## 1. What the two keys mean

| Key | Purpose |
| --- | --- |
| `matlab.functions` | `.m` files that should be parsed as MATLAB *functions* and surfaced as callable nodes in the DAG. |
| `matlab.variables` | `.m` classdef files that subclass `scidb.BaseVariable` and are registered as typed variables. |

Each list accepts:
- Individual `.m` file paths (relative to the project root).
- Directory paths — recursively walked for `.m` files.
- Glob patterns — expanded and filtered to `.m` files only.

All entries are resolved through `_resolve_glob_paths` and returned as
absolute `Path` objects.

---

## 2. The overlap problem

A common real-world layout puts variable classdefs *inside* a broader
source tree that is itself listed as a functions source:

```toml
[matlab]
functions = ["src/"]
variables = ["src/vars/"]
```

Without deduplication, the `src/` recursive walk picks up every file under
`src/vars/` and those files are then parsed as functions. Because they are
classdefs, the parser emits a WARNING per file — noisy on every startup,
and a bigger problem on slow network drives where each file read already
costs real time.

The fix is to treat `matlab.variables` as authoritative: if a file appears
in both resolved lists, it is removed from `matlab_functions`.

---

## 3. The dedupe rule

Applied in `load_config` after both lists have been fully resolved and
before `matlab_addpath` is derived:

```python
var_path_set = {p.resolve() for p in matlab_variables}
matlab_functions = [
    p for p in matlab_functions if p.resolve() not in var_path_set
]
```

Key properties:

- **Comparison uses `Path.resolve()`** so symlinks, `./` fragments, and
  mixed relative/absolute forms all compare equal.
- **Variables win.** If a user lists the same file in both keys, the file
  is treated as a variable and *not* a function. This matches the usual
  intent: `classdef X < scidb.BaseVariable` is not callable as a function,
  so registering it as one would always be wrong.
- **`matlab_addpath` is unaffected.** addpath is derived from parent
  directories of *both* resolved lists (plus `variable_dir`), so dedup
  doesn't shrink the runtime MATLAB path.
- **An INFO log records the count** excluded so users who accidentally
  rely on the implicit deduplication get a visible breadcrumb.

---

## 4. What the user sees

Given the overlapping config above, with `src/func.m` and `src/vars/var.m`
on disk:

```
[scistack] INFO scistack_gui.project.config: Excluded 1 file(s) from matlab.functions
because they are also declared in matlab.variables.
[scistack] INFO scistack_gui.project.config: Loaded config from .../scistack.toml:
0 modules, 0 packages, auto_discover=True, 1 MATLAB functions, 1 MATLAB variables
```

And in the resulting `SciStackConfig`:

```python
[p.name for p in config.matlab_functions] == ["func.m"]
[p.name for p in config.matlab_variables] == ["var.m"]
```

---

## 5. Edge cases

- **No overlap** — `excluded == 0` so no INFO line is emitted and
  `matlab_functions` passes through unchanged.
- **All overlap** — legal but odd; `matlab_functions` becomes empty and a
  single INFO line reports the count. No error is raised because the user
  might have intentionally moved a file from functions to variables and
  forgotten to remove the functions entry.
- **Symlinked dirs** — `Path.resolve()` follows symlinks, so a variable
  listed via its canonical path will still shadow a function listed via
  a symlinked parent.
- **Globs that cross the boundary** — e.g. `functions = ["src/**/*.m"]`
  behaves identically to `functions = ["src/"]` for dedupe purposes,
  because both lists have been fully expanded to file paths before the
  dedupe runs.

---

## 6. Why do it in `config.py` and not in the MATLAB parser?

Two reasons:

1. **Single source of truth.** After `load_config` returns, the
   `SciStackConfig.matlab_functions` and `matlab_variables` lists are
   consumed by multiple modules (`matlab_registry.load_from_config`,
   `matlab_command_format.generate_matlab_command` for `addpath_dirs`, etc.).
   Deduplicating once at config time keeps all downstream consumers in
   sync.

2. **CLAUDE.md guidance.** Scistack layers should own their own data
   invariants. The fact that "a file declared as a variable is never a
   function" is a property of the config, not of the parser — so the fix
   lives in `config.py`, not in `parse_matlab_function`.

---

## 7. Regression test

See `tests/test_config.py::test_matlab_variables_excluded_from_functions`.
It builds the exact `src/` + `src/vars/` layout described above and
asserts that the two resolved lists are disjoint and correctly
partitioned.

---

## 8. Drive-letter preservation on Windows

### Problem

`Path.resolve()` on Windows canonicalizes *mapped drives* to their UNC
target. If the user opens a workspace as `y:\LabMembers\...` where `y:`
maps to `\\fs2.smpp.local\RTO\LabMembers\...`, then
`Path("y:\\foo").resolve()` returns `\\fs2.smpp.local\RTO\foo`.

VS Code 1.75+ **refuses to open UNC paths** unless the host is listed in
the user setting `security.allowedUNCHosts`. With resolved paths, the
GUI's `reveal_in_editor` RPC fails with:

```
Unable to read file '\\fs2.smpp.local\RTO\...\load10MWTREDCapReport.m'
(Unknown (FileSystemError): UNC host 'fs2.smpp.local' access is not
allowed. Please update the 'security.allowedUNCHosts' setting ...)
```

### Fix

`config.py` now uses a helper `_normalize(p)`:

```python
def _normalize(p) -> Path:
    return Path(os.path.normpath(os.path.abspath(str(p))))
```

`os.path.abspath + normpath` produces an absolute, normalized path
*without* following symlinks or rewriting mapped drives. The following
call sites have been converted from `.resolve()` → `_normalize()`:

- `resolve_project_root` / `locate_config_at`: how the project root is decided.
- `load_config`: module, entities_file, variable_file, matlab variable_dir.
- `_resolve_glob_paths`: individual MATLAB/Python file entries.
- `matlab_parser.parse_matlab_function`: `MatlabFunctionInfo.file_path`.
- `matlab_registry.load_from_config`: variable-path storage.

**Dedupe comparisons still use `.resolve()`** because they're
comparison-only and idempotent on both sides of the `==`:

```python
var_path_set = {p.resolve() for p in matlab_variables}
matlab_functions = [p for p in matlab_functions if p.resolve() not in var_path_set]
```

This means two different textual paths that point to the same file are
still treated as equivalent for dedupe purposes, without the stored
paths being canonicalized.

### Invariant

> All `Path` objects stored in a `SciStackConfig` use the same drive/UNC
> form the user supplied on `--project`. `.resolve()` is never called on
> a path that is later stored or returned to the frontend.

### Regression tests

- `tests/test_config.py::test_normalize_is_absolute_and_normpath`
- `tests/test_config.py::test_normalize_does_not_follow_symlinks`
- `tests/test_config.py::test_matlab_functions_not_canonicalized_through_symlink`
  — uses a directory symlink to simulate the Windows mapped-drive case
  and asserts the stored path keeps the symlink prefix.

### Related fixes in the VS Code extension

`scistack-gui/extension/src/dagPanel.ts` now:

- Constructs UNC URIs via `vscode.Uri.from({scheme,authority,path})`
  instead of `vscode.Uri.file()` (the `buildFileUri` helper).
- Wraps `openTextDocument` and `showTextDocument` in separate try/catch
  blocks that log the full error (with the resolved URI) to the SciStack
  Output channel before returning it to the webview. This is what made
  the UNC diagnostic surface-able; without it the error was silently
  swallowed by the webview round-trip.
