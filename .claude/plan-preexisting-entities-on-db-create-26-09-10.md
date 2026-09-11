# Plan: a pre-existing `scistack_entities.toml` must populate the GUI on database creation

Date: 2026-09-10

## Symptom

Create a new SciStack database in a project folder that already contains a
`src/scistack_entities.toml`. The Variables / Parameters / PathInputs it
declares do not appear in the GUI.

## Root cause: two layers disagree about *where the entities file is*

There are two independent implementations of "which file is this project's
entities file":

| Layer | Implementation | Conventional-path fallback? |
|---|---|---|
| `scidb.entities.entities_path` | reads `entities_file` from the project config; **falls back** to `src/scistack_entities.toml` if that file already exists | yes |
| `scistack_gui.config.load_config` (config.py:385-393) | reads `entities_file` from the section; `None` otherwise | **no** |

`registry.load_from_config` only reads the entities file when
`config.entities_file is not None` (registry.py:294), so it uses the GUI's
answer -- the one without the fallback.

That produces two concrete holes:

### Hole 1 -- `scistack.toml` exists but carries no `entities_file` key

`project_init_service.ensure_project_files` asks **scidb** whether the project
already has an entities file (project_init_service.py:127). The fallback says
yes, so it returns early -- "nothing to create" -- and never writes the
`entities_file` key. Then `load_config` reads that same `scistack.toml`, sees
no key, and returns `entities_file=None`. The registry never opens the file.
Its entities are live for scidb and MATLAB (`+scidb/entities.m` routes through
`entities_path`) and invisible in the GUI.

This hole is hit by **every** entry point.

### Hole 2 -- no `scistack.toml` at all, VS Code entry point

`ensure_project_files` (which would create `scistack.toml` and point it at the
existing entities file) runs from `bootstrap.open_or_create_project` only. That
covers the browser CLI (`__main__.py`) and `POST /api/bootstrap/create`.
`server.py`'s JSON-RPC `main()` -- the VS Code extension's entry point --
duplicates the whole startup sequence inline and **never calls it**. So
creating a database from VS Code in a bare folder leaves the project in
folder-scan mode, where `entities_file` is `None` by construction
(`_folder_scan_config`, config.py:974).

### Hole 3 -- the creation wizard re-points an entities file it didn't put there

`POST /api/bootstrap/create` ran its own eager
`config.set_entities_file(db_path, req.entities_file)` before delegating, and
the wizard's "Entities file" field always sends its value (default
`src/scistack_entities.toml`). So creating a database in a project whose
config already said `entities_file = "pipeline/my_entities.toml"` swapped the
key for a brand-new empty file, and every declaration in the real one
vanished from the GUI. Same symptom, different cause.

Everything downstream is fine once the registry has the entities: parameters
get canvas nodes (`build_parameter_nodes` merges the registry in), PathInputs
get seeded (`seed_undiscovered_path_inputs`), and variables reach the new
database because `create_db` -> `configure_database` runs *after* the registry
load and auto-registers `BaseVariable._all_subclasses`.

## Fix

### Stage 1 -- scidb owns the decision, once (CLAUDE.md NOTE 3)

`scidb/src/scidb/entities.py`: split the rule out of `entities_path` into

```python
def resolve_entities_path(root, section) -> Path | None
```

taking an already-located project root and its already-parsed scistack
section. `entities_path` becomes locate-then-delegate. New: an
`entities_file = ""` empty string is an **explicit opt-out** returning `None`
(see Stage 2's `clear_entities_file`).

### Stage 2 -- the GUI asks scidb instead of re-deriving

- `scistack_gui/config.py` `load_config`: `entities_file =
  resolve_entities_path(project_root, section)`, with an INFO log naming which
  branch answered. This closes Hole 1 for all entry points.
- `config.clear_entities_file` now writes `entities_file = ""` rather than
  dropping the key. Without this, the Paths popup's "clear" button would
  become a silent no-op for a file at the conventional path, since the
  fallback would immediately re-adopt it. The empty string preserves the
  button's exact meaning and makes the opt-out visible to scidb and MATLAB
  too, which the key-deletion form never was.
- `registry.load_from_config`: log at INFO when *no* entities file is
  configured, so the log answers "why did nothing load?" (NOTE 2).

### Stage 3 -- both entry points run the same project init

`server.py` `main()`: call `ensure_project_files(db_path, args.project)` before
the registry load, skipped in `--module` mode, exactly as
`bootstrap.open_or_create_project` does. Closes Hole 2.

### Stage 4 -- one create-only path for the entities file

`ensure_project_files` takes an optional `entities_file` ("where to put one
**if the project has none**"), threaded through
`bootstrap.open_or_create_project`. `api/bootstrap.create` drops its eager
`set_entities_file` call and passes the field through instead. Closes Hole 3,
and collapses two write paths into one policy:

- project already has an existing entities file -> keep it; a *different*
  requested path is reported as a warning, never applied;
- config names a file that doesn't exist yet -> create it **where the config
  says**, not where the caller asked;
- project has none -> create at the requested path (or the conventional one);
- `entities_file = ""` -> create nothing.

### Stage 5 -- tests

- `scidb/tests/test_entities_toml.py`: `resolve_entities_path` -- explicit key
  wins, conventional fallback only when the file exists, `""` opts out.
- `scistack-gui/tests/test_config.py`: `load_config` picks up the conventional
  file with no key; stays `None` when it does not exist; `""` opts out;
  `clear_entities_file` writes `""`.
- `scistack-gui/tests/test_project_init.py`:
  - end-to-end regression for the reported bug -- project dir with a
    `scistack.toml` that has no `entities_file` key plus a pre-existing
    `src/scistack_entities.toml` declaring a Parameter/Variable/PathInput ->
    `load_config` + `registry.load_from_config` -> the entities are in the
    registry and in `layout_service.get_parameters()`.
  - source-level guard that **both** `server.py` and `bootstrap.py` call
    `ensure_project_files`, so the VS Code path can't silently drift again.
  - the four create-only cases above.
- `scistack-gui/tests/test_bootstrap.py`: creating a database through the
  wizard endpoint keeps a project's existing `entities_file` key and its
  declarations, and discovers a file the config does not name.

## Test commands

```
python -m pytest scidb/tests/test_entities_toml.py -q
python -m pytest scistack-gui/tests/test_config.py scistack-gui/tests/test_project_init.py scistack-gui/tests/test_bootstrap.py -q
```

(one package per invocation -- `project_pytest_one_package_at_a_time`)
