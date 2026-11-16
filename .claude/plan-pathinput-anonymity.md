# Plan: hide PathInput locations at export (issue #16)

Status: **draft for approval, 2026-11-15**. Nothing built.

Issue #16: "During export, give the option to preserve or obfuscate the paths
in PathInput. Preserve for internal project transfers, obfuscate for public
release."

## What exists today

- `scidb/project/bundle.py` writes the `.scistack` bundle. `ExportOptions` owns
  the export defaults; `bundle_cli.EXPORT_FLAGS` is the one list of offered
  options (CLI `--X/--no-X` flags and the GUI's export checkboxes both read it).
- Import already replaces locations: `import_project(path_roots={name: folder})`
  -> `_rewrite_path_inputs`, which reads the entities TOML through
  `_declared_path_inputs` and writes it back through
  `entities_file.render_path_input_value` / `upsert_entry`. A PathInput that
  keeps the exporter's root is flagged in the import report.
- Schema keys go through ONE map (`schema_map.KeyMap`): each section applies it
  to its own data and reports into a `MapReport`. It reaches sections as
  `ImportContext.key_map`.
- PathInput identity is the NAME only. `root_folder` and the template are never
  hashed, so changing or removing them forks no record, invocation or call id,
  and `scistack verify` is unaffected.

So today an export is always "preserve": the exporter's folders travel as they are.

## Where a PathInput location sits in a bundle

Found by reading the code; this list is the scope of "obfuscate".

| # | Place | Holds |
|---|---|---|
| L1 | `code/` entities TOML `[path_inputs]` | `root_folder`; an absolute template's leading folders |
| L2 | `code/` `.py` / `.m` files that declare a PathInput in source | the same, as code |
| L3 | `gui/gui.json` `path_inputs` (`portability_service.export_globals` -> `path_input_display`) | `root_folder`, alternates |
| L4 | `gui/verbatim/` `_pipeline_path_input_history` (only with data) | `(name, template, root_folder)` |
| L5 | `history/` `_constant.value_repr` of `__pathinput__` records | the full spec (`to_spec()`), every root ever run |
| L6 | `history/` free text: `_run_outcome.reason`, `_run_summary.settings` / `notes`, `_function_source.unit_source` | resolved file paths in error messages, hard-coded paths |
| L7 | `manifest.json` `project.root` | the exporter's project folder (usually has the user name) |
| L8 | `config/scistack.toml` | absolute in-project paths (import makes them relative using L7) |
| L9 | `config/scistack.toml` entries outside the project, `code/.scistack-external.json` | folders of external code |
| L10 | `data/` variable tables | whatever the user's functions saved |

## Design

### Meaning of the two modes

- **Preserve** (today's behaviour, stays the default): nothing changes.
- **Hide locations**: a PathInput keeps its name, template, aliases and
  key_regex (the structure a recipient needs to point it at their own copy) and
  loses WHERE the files are:
  - `root_folder` is removed (not replaced by a made-up path);
  - an absolute template loses its leading placeholder-free folders
    (`/Volumes/Lab/Study/{subject}/a.csv` -> `{subject}/a.csv`). The split
    reuses `PathInput._root_and_segments`'s anchoring rule, exposed as one
    public scifor method, e.g. `PathInput.without_location()`.

  With no root, a PathInput resolves against the project folder. That is an
  honest default for a public release (data beside the code), and import
  already asks for `--path-root NAME=FOLDER`.

Not hidden, by design: schema key VALUES (subject codes, session names) in
aliases, history and data. Anonymizing those was dropped on 2026-10-08 and
this plan does not reopen it.

### One owner: `scidb/project/path_redaction.py`

Shaped like `schema_map.py`: one object, each section applies it to its own
data, one report.

```
PathRedaction
  .path_input(arm_or_spec) -> the same shape without its location
                              (delegates to scifor PathInput.without_location;
                               remembers each removed folder as a "needle")
  .text(s)                 -> s with every needle replaced by a neutral token
                              ("<PathInput Xsens>", "<project>")
  .needles                 -> removed folders + the exporter's project root,
                              each in its spellings ("/", "\", JSON-escaped "\\")
RedactionReport
  hidden    [(where, PathInput name)]
  replaced  {where: count}
  remaining [(where, which needle BY NAME, count)]   # never the folder itself
```

`ExportContext.redaction` is `None` when preserving, the same way
`ImportContext.key_map` is identity when the schema is kept. No section tests
the option itself.

### The option

`ExportOptions.hide_path_locations: bool = False`, plus one `EXPORT_FLAGS` row:
`--hide-path-locations`, label "Hide PathInput folders (for a public release)".
Being a bool, it needs no new CLI or GUI widget: the existing flag loop and
checkbox list pick it up. The manifest already records `options`, so the
import side knows which mode the bundle was written in. No format-version bump
(old bundles simply lack the field).

### Code is never rewritten; leftovers stop the export

SciStack does not edit the user's source files to hide a string (L2, and
hard-coded paths in function bodies). Instead, after every section is built,
one scan looks for every needle in every bundle file. In hide mode any hit
REFUSES the export (no bundle is left behind) and lists each place and the
remedy, e.g. "PathInput Xsens is declared in src/pkg/inputs.py with a
root_folder: declare it in scistack_entities.toml instead". Reason for refusing
rather than warning: the user asked for a public-release bundle, and a warning
that scrolls past leaves their folder names published.

History is different, because the user cannot edit the past: its text columns
are replaced (below), not refused.

## Steps

### Option and owner

- `ExportOptions.hide_path_locations`; `EXPORT_FLAGS` row.
- `scifor`: `PathInput.without_location()` (+ MATLAB needs nothing: export is Python).
- `scidb/project/path_redaction.py`: `PathRedaction`, `RedactionReport`.
- `export_project` builds the redaction when the option is on and passes it in
  `ExportContext`.
- Logging: `[redaction]` INFO per section (what was hidden, counts), DEBUG per needle.

### Declarations and manifest (L1, L3, L7, L8)

- Split `_rewrite_path_inputs` into a text-level function
  (`entities text + per-arm transform -> text`) so import (roots, key map) and
  export (redaction) share the one reader/writer pair. Export applies it to the
  BYTES going into the bundle; the exporter's files on disk are never touched.
  Because the entities file is written by `CodeSection` (GUI layer), the code
  section gets a small `ctx.redaction` call for that one file, or scidb
  post-processes the file by its known path. Decide at build time; the rewrite
  itself stays in scidb.
- `GuiSection.export`: `path_inputs` entries through `redaction.path_input`.
- Manifest: omit `project.root`. To keep import working without it, the
  config's in-project absolute paths are made relative at EXPORT time using the
  existing `_write_config` transform (import already accepts a missing root).

### History and verbatim GUI tables (L4, L5, L6)

- The live database is never modified. `redacted_dump(duck, tables, redaction)`:
  `table_copy.dump` -> `table_copy.load` into a scratch in-memory DuckDB ->
  apply -> `table_copy.dump`. No new hooks in `table_copy`. (Fallback if a
  scratch load proves awkward: a `SELECT * REPLACE` override in `dump`.)
- Apply = two passes:
  - structural: `__pathinput__` rows of `_constant` get
    `redaction.path_input(spec)` (ids and content hashes come from the name, so
    they do not change);
  - textual: every VARCHAR/TEXT column of the history tables gets the needle
    replacement. Generic on purpose: no hand-kept list of "columns that may hold
    a path" to fall out of date.
- `_pipeline_path_input_history` (GUI verbatim mode): `root_folder` emptied by
  the GUI section, rows de-duplicated (root is part of its primary key).
- The history pass runs after the providers, so it knows every needle.

### Leftover scan and refusal (L2, L9, L10)

- Scan all text members by bytes and the redacted scratch tables by SQL.
- Hits in hide mode -> `BundleError` listing them; temp file removed.
- L9 (external code folders) and L10 (derived data) are NOT rewritten. External
  folders that contain a needle are caught by the scan. For data, the export
  says once: "saved data is exported as it is; SciStack does not look inside it".
- `export_project` returns an `ExportReport` (path, redaction report, warnings),
  mirroring `ImportReport`. Three callers to update: `headless.export_open_project`,
  `bundle_cli._cmd_export`, `api/bundles._export_project_bundle` (find them with
  LSP `findReferences` first). CLI prints it; the GUI export shows it.

### Import side

- `_rewrite_path_inputs`: when the manifest says locations were hidden and no
  `--path-root` was given for a name, flag "PathInput X: the exporter hid its
  folder; give yours with --path-root X=FOLDER, otherwise files are looked for
  in the project folder".
- `preview()` / `bundle-info`: `paths_hidden: true`, shown as "hidden by the
  exporter" instead of "(none)". The VS Code import prompt uses the same field.

### Tests

Integration first (scidb/tests, scistack-gui/tests):

- hide export: the root string appears in NO bundle member and in no value of
  any Parquet table; record ids, invocation ids equal those of a preserve export;
- preserve export unchanged (root present, manifest root present);
- absolute template split; EachOf arms; aliases and key_regex survive;
- failure reason containing a resolved file path is replaced;
- a source-declared PathInput with a root, and a hard-coded path in a function
  body, each refuse with the place named; nothing written;
- hide export -> import: report flags each hidden PathInput; `--path-root` writes
  it; `verify` still reproduces;
- the exporter's entities file and database are byte-identical before and after;
- `export_choices()` offers the flag with the `ExportOptions` default.

### Docs

`docs/claude/portability.md`: new section "Hiding PathInput locations", E-table
row, and the L1-L10 inventory. `docs/gui-manual-testing.md` entry for the
checkbox and the refusal popup.

## Decisions needed

1. Default: preserve (recommended, today's behaviour) or hide?
2. Leftovers in hide mode: refuse the export (recommended) or write it and warn?
3. External code folders and the home directory (L9): report only when they
   contain a hidden folder (recommended), or hide them as well?
4. Name: "hide" in the UI and flag (recommended, plain) vs the issue's "obfuscate".
