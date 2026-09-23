# The entities file: declaring Variables, Parameters and PathInputs in TOML

**Status: implemented 2026-09-01, uncommitted. Stages 1–6 of
`.claude/plan-entities-toml-26-08-31.md`.** One thing has never been
executed: `+scidb/entities.m`. There is no MATLAB in the dev environment,
so its Python half is tested and its MATLAB half is correct-by-inspection.

This is the **write** half's format. `code-discovery-categories.md` covers
how all six kinds are *discovered*; `entity-editability-model.md` covers the
*policy* around writing (what may be edited, and why).

## What it is

One TOML file per project — `src/scistack_entities.toml` by default — that
declares Variables, Parameters and PathInputs. It is the **only file the
GUI writes entity declarations into**, and both Python and MATLAB read it.

```toml
# src/scistack_entities.toml
# Variables come first: a bare top-level key placed after a [section]
# header would be parsed as belonging to that section.
variables = ["StepLength", "EmgEnvelope"]

[parameters]
SAMPLING_RATE_HZ = 1000                    # one value
WINDOW_SECONDS   = [10, 20, 30]            # three values (fan-out)
SUBJECT_IDS      = ["01", "02"]            # stays string, never 1/2
CONFIG           = { fld1 = 1, fld2 = 2 }  # the dict IS the value

[path_inputs]
EMG_FILE = "{subject}/{session}_emg.csv"
RAW_FILE = { template = "{subject}/raw.csv", root_folder = "/data/raw" }
```

Read it from Python:

```python
from scidb import entities

entities.WINDOW_SECONDS      # -> Parameter(10, 20, 30)
entities.EMG_FILE            # -> PathInput('{subject}/{session}_emg.csv')
```

…and from MATLAB, through the same loader over the Python bridge:

```matlab
e = scidb.entities();
scidb.for_each(@process, inputs=struct('f', e.EMG_FILE), window=e.WINDOW_SECONDS);

scidb.entities();            % or: assign every name into the base workspace
```

## The four rules that make it unambiguous

There is no `type` key and there are no options tables. The **section fixes
the kind**, and the entry's value is the value. Four rules follow:

1. **An array is always the alternative list.** `[10, 20, 30]` is three
   Parameter values, not one list-valued Parameter. To declare a Parameter
   whose single value really is a list, nest it: `X = [[1, 2, 3]]`.
2. **An inline table means the *value* under `[parameters]`, and the
   *field set* under `[path_inputs]`.** A PathInput has no dict-shaped
   value, so a table there is unambiguously `{template, root_folder}`; any
   other key is a rejected entry. A Parameter has no fields, so a table
   there is the value — which is how `CONFIG = { fld1 = 1 }` works at all.
3. **A Variable entry has no value.** `variables` is a bare array of names.
4. **Nothing is re-parsed.** TOML types are taken as they come.

Rule 4 is the one with teeth: `SUBJECT_IDS = ["01", "02"]` comes back as
the strings `"01"`, `"02"`. There is no `eval`, no literal parser, nothing
in the path that can turn `"01"` into `1`
(`feedback_zero_padded_schema_keys`). Both formats this replaces — an
exec'd `.py` module and a statically-parsed `.m` script — round-tripped
values through a literal parser to get there.

## What the format deliberately cannot express

| Not expressible | Where it lives instead |
|---|---|
| A Parameter `description` | `scidb.Parameter(30, description=...)` in Python |
| A Variable docstring or non-default `schema_version` | a `class X(BaseVariable)` statement |
| Custom `to_db`/`from_db` | a Python class |
| PathInput `aliases` / `key_regex` / `regex` | a Python declaration |
| Computed values (`Parameter(*range(10, 60, 10))`) | a Python declaration |

None of these are lost — they are **read-only declarations**, discovered
and displayed exactly as before, and refused by the editor with their exact
source location. That is the same contract that has always applied to a
declaration outside the entities file.

The GUI never sends a description or docstring (its create forms only
collect a name), but `create_parameter`/`create_variable` still accept one
for their non-TOML targets. Passing one with a TOML target logs a WARNING
naming the value rather than dropping it silently.

## Coexistence: three declaration surfaces, one writable

Revised D1 of the plan, after the question "can the TOML live alongside the
`.m` and `.py` entities files?":

| Surface | Discovered | Written by the GUI |
|---|---|---|
| `entities_file` (TOML) | yes | **yes — the only one** |
| `variable_file` (`.py`) | yes | no |
| `[matlab] entities_file` (`.m` script) | yes | yes, pending a decision (see below) |
| Any other scanned `.py`/`.m` | yes | no |

Python coexistence is free: `registry._scan_module_parameters` runs over
every scanned module, so a `.py` file's declarations are discovered whether
or not it is the write target. `config.load_config` additionally folds
`variable_file` into `modules` when nothing else covers it — before, that
entry was guaranteed by `set_variable_file`; nothing writes the key now, so
nothing maintains it either.

**One writable target is the point.** Two would reopen "which file does a
new Parameter land in?" for every create, and double the write path that
confinement, staleness and rollback all rest on.

*Open:* `[matlab] entities_file` is still writable. Now that MATLAB can
read the TOML, revised D1 says demote it — but that removes a working edit
path from MATLAB-only projects, so it is a user decision, not a silent one.

## Where the code lives

**`scidb/src/scidb/entities.py` owns the grammar** — scidb defines
`BaseVariable`, `Parameter` and (re-exported) `PathInput`, so it is the
layer that knows what a declaration of one means (CLAUDE.md NOTE 3). It is
the exact counterpart of `scidb.source_edit` for the Python form.

Read half:
- `load(path) -> EntitiesFile` — parse, validate, construct.
- `entities_path()` / `load_for_project()` — resolve the file from the
  project config (mtime-cached) found AT `scifor.project_root()` (the pinned
  root, else the cwd) by `scifor.discovery.project_config_at` -- no upward
  walk (D-2026-09-23-1). The GUI pins its root into scifor, so the GUI and a
  plain script read the same config for the same folder.
- `resolve_entities_path(root, section)` — the resolution **rule**, split
  out so a caller that already located the config and parsed its section
  (`scistack_gui.config.load_config`) asks scidb instead of reimplementing
  it. Three cases, in order: an `entities_file = "..."` key wins; an
  `entities_file = ""` is an explicit opt-out (what the GUI's "clear
  entities file" writes — the only way to stop reading a file that sits at
  the *conventional* path, since deleting the key would just fall through);
  otherwise the conventional `src/scistack_entities.toml`, **only if it
  already exists**. `is_entities_opt_out(section)` separates the opt-out
  from "there is simply nothing here", which matters only to
  `project_init_service`, i.e. the caller deciding whether to *create* one.

  The GUI used to read the key itself and stop, so it missed the
  conventional fallback: a project holding `src/scistack_entities.toml`
  with no key had its entities read by scidb and MATLAB and never loaded
  into the GUI registry — nothing it declared appeared in the GUI at all
  (2026-09-10; `.claude/plan-preexisting-entities-on-db-create-26-09-10.md`).
- module `__getattr__` — `entities.WINDOW_SECONDS`.

Write half — `find_entry_span`, `render_*`, `upsert_entry`, `add_variable`.
Line-level splices, never whole-file regeneration, so comments and
neighbouring entries survive an edit byte for byte.

Around it:
- `scistack_gui.registry._load_entities_file` registers the results,
  **last** in `load_from_config` so a TOML declaration wins over a
  same-named one found in a module — the entities file is what the GUI
  writes, so if the two disagree, what the user just edited is what they
  should see.
- `scistack_gui.services.target_file_service` owns the write *policy* and
  dispatches on suffix (`is_toml_target`): `.toml` → this grammar,
  otherwise Python's. Legacy single-file mode (`--module pipeline.py`) has
  no config file to record an entities file in, and auto-creating a
  `scistack.toml` there would flip the project into config-driven discovery
  as a side effect of creating a Parameter.
- `scimatlab.bridge.load_entities` + `+scidb/entities.m` — the MATLAB path.

## Errors are per entry

A bad declaration is recorded with its **name and line** and skipped; every
good entry in the same file still loads. The rejections are: a non-string
in `variables`, a name that is not a valid identifier, an unknown key in a
`[path_inputs]` table, a PathInput table with no `template`, a `variables`
key nested inside a section, and a duplicate name across sections.

This is not a small quality-of-life detail. The `.py` entities file it
replaces was **executed**, so one bad line took every entity in the file
down at once — and silently, because module-load failures during discovery
are logged at DEBUG, not raised.

Rejected entries surface in the GUI's load-errors panel
(`registry._record_load_error`) and, for MATLAB, as warnings raised by
`scidb.entities()` — someone at the MATLAB prompt never sees that panel.

## Two traps the format removed

**Stale bytecode.** A `.py` entities file is imported, and
`SourceFileLoader` validates cached `.pyc` against the source's mtime
(whole seconds) and size. An entity edit routinely changes neither —
`Parameter(30)` → `Parameter(45)` is byte-for-byte the same length, landing
in the same second as the load before it — so Python re-executed the stale
bytecode and the GUI kept showing the old value, with nothing logged
anywhere. `target_file_service._invalidate_bytecode` exists for that, and
is now a no-op for non-`.py`. TOML is parsed, never imported: the whole
class of bug is gone by construction rather than by remembering to
invalidate a cache.

**`variables` in the wrong place.** A bare top-level TOML key written below
a `[section]` header binds to that table, so a `variables` list added after
`[parameters]` silently becomes `parameters.variables` and every name in it
disappears. The loader detects exactly this and rejects it with "move it
above the first section header"; `initial_text()` scaffolds the key in the
right place so neither the GUI nor a hand-edit has to get it right later.

## MATLAB specifics

Parameters and PathInputs cross the bridge as **plain data** and are rebuilt
as MATLAB objects — handing MATLAB the constructed Python objects would give
it Python proxies, not the MATLAB classes `for_each` expects. `root_folder`
is `None` (MATLAB `[]`) when unset, never `""`, so "no root" stays
distinguishable from "rooted at the empty string".

**Variables cannot work that way.** MATLAB has no runtime class creation and
`class(obj)` is what names the database table, so a Variable must be a real
`classdef` file. `matlab_registry.materialize_variable_stubs` writes one per
TOML-declared variable into `matlab_variable_dir`. The TOML entry is the
declaration of record; the stub is *generated output*, not a second place
the variable is declared. It is create-only — a stub whose declaration later
disappears is left alone, because deleting a generated-but-still-referenced
file is how a pipeline stops running mid-session
(`feedback_never_delete_mark_hidden`).

Generated MATLAB commands emit `scidb.entities();` after the `addpath`
block, plus the legacy script line when one is configured — both, when both
exist, because they declare different names. Re-emitted every time, which
is what makes a GUI edit visible to a kept-warm sidecar session: the Python
loader re-reads whenever the file's mtime changes. That was the original
argument for a script over a classdef (whose `Constant` properties MATLAB
caches for the session), and TOML keeps it.

## See also

- `config-file-formats.md` — the `entities_file` key and where it is written.
- `entity-editability-model.md` — what may be edited, and the write policy.
- `code-discovery-categories.md` — how all six kinds are discovered.
- `.claude/plan-entities-toml-26-08-31.md` — the plan, decisions D1–D5, and
  per-stage status.
