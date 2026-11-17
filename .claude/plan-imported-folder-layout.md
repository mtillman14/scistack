# Plan: one `.scistack/imported/` folder, split by concern

Status: BUILT 2026-11-16 (user: "if it's run history, call it run history"). Follows issue #20 (logs in bundles). Report key history.archived -> history.kept_at.

## Problem

An imported project's `.scistack/` holds only things that came from the sender's bundle,
but the names don't say so, and they're spread across three ideas:

| Today | What it is | Who reads it |
| --- | --- | --- |
| `.scistack/import.json` | record of the import (bundle name, key map, options) | `verify` (key map) |
| `.scistack/environment.json` | sender's Python/packages | `install_project_requirements` |
| `.scistack/wheelhouse/` | sender's library wheels | `install_project_requirements` |
| `.scistack/archive/<export time>/` | sender's run history (+ the dataset rules riding in it) when not loaded live | `verify` (newest `*/tables.json`) |
| `.scistack/exporter-logs/<export time>/` | sender's log tails (issue #20) | people |

- "archive" names a storage state (kept, not live), not the origin. Only history is stored there; nothing
  else earns the name.
- `STATE_DIR = ".scistack"` is documented as "where an import keeps…" but is named like general project state.
- `<export time>` subfolders: an import always makes a NEW project, so a project holds exactly one import.
  The extra level is never more than one folder deep.
- The constants live in two modules (`environment.STATE_DIR`, `bundle.ARCHIVE_DIR` / `EXPORTER_LOGS_DIR` /
  `IMPORT_RECORD`). That's one concept (the import's footprint) with two owners.

## Proposal

```
.scistack/
  imported/            everything that came from the sender's bundle, never edited by SciStack
    import.json        the import record
    environment.json   sender's environment  (install reads it)
    wheelhouse/        sender's wheels       (install reads it)
    run-history/       sender's run history when not loaded live (verify reads tables.json here)
    logs/              sender's log tails
```

- One owner: a small `scidb/project/imported.py` with `IMPORTED_DIR = ".scistack/imported"` and the
  sub-names; `bundle`, `environment`, `verify` and `inspect/cli` import from it.
- No `<export time>` level (one import per project). The export time is already in `import.json`.
- `verify` with no `--against` reads `imported/run-history/`; its error text and the CLI help name it.
- Beta: no migration. A project imported before this keeps its old folders; `verify` there needs
  `--against <old folder>`.

## Steps

- Owner module + constants; switch `bundle.py`, `environment.py`, `verify.py`, `inspect/cli.py`.
- Import writes the new layout; history README text names `imported/run-history/`.
- Tests: `test_bundle.py` (history archive, declined history, logs), verify tests, the extension's
  `bundleImportCore.test.ts` if it names the path.
- Docs: `docs/sharing/export-import*.md`, `docs/claude/portability.md`, decision log entry.
- Logging: import logs one line listing what it put under `imported/`.
