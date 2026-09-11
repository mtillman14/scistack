# Entity declaration surfaces

Which files can declare a Variable, Parameter or PathInput; which of them
the GUI may write; and how a declared name travels from disk into the
sidebar and onto the canvas.

Written 2026-09-01, after a failure whose three symptoms all came from gaps
in this model. See `.claude/plan-entity-surfaces-and-reload-cost.md`.

## The surfaces

There are four kinds, and only the first is writable.

| kind | file | who writes it |
| --- | --- | --- |
| **writable** | `src/scistack_entities.toml` (`entities_file`) | the GUI |
| **read-only** | `src/scistack_entities.py` | the user, by hand |
| **read-only** | `src/scistack_entities.m` (`[matlab] entities_file`) | the user, by hand |
| **generated** | `src/scistack_matlab_variables/*.m` | `scimatlab.stubs` |

`scistack_matlab_variables/` is **not** a declaration surface, despite the
name symmetry. MATLAB requires one public `classdef` per file named after
the type, so a Variable declared in the TOML cannot be handed over the
bridge as a value the way a Parameter or PathInput can — it needs a real
file on the MATLAB path before `RawEMG()` resolves. That directory holds
those materialized stubs. Deleting one is safe; it is rewritten on the next
run. Editing one is pointless; the TOML is the declaration.

(It was called `scistack_variables` until 2026-09-01. The old folder is
never deleted but is no longer added to the MATLAB path, so leftover stubs
are inert and cannot shadow the live ones. `scimatlab.stubs.legacy_stub_dir`
exists only to report one.)

## Why the `.py`/`.m` surfaces still exist

The TOML deliberately cannot express everything:

- a Variable with a custom `to_db`/`from_db`, or a non-default `schema_version`
- a PathInput needing `aliases`, `key_regex` or `regex`
- a Parameter whose value is computed rather than literal

Those stay in Python (or MATLAB), read-only. `update_declaration` refuses to
edit them and reports `{file, line}` so the UI can point at the source
instead of failing generically.

`ensure_language_stubs` creates the relevant stub file per language actually
in use, only when absent, so there is an obvious place to put such a
declaration. A fresh stub declares nothing and is inert.

## How a name reaches the sidebar

```
declaration on disk
   │
   ├─ .toml ──► scidb.entities.load() ──► registry._load_entities_file
   ├─ .py   ──► import ──► BaseVariable.__init_subclass__
   └─ .m    ──► matlab_registry ──► scimatlab.bridge.register_matlab_variable
   │                                        (creates a Python surrogate class)
   ▼
BaseVariable._all_subclasses        {name: class}      ← scidb owns this
registry._variable_sources          {name: source}     ← who registered it
   │
   ▼
pipeline_service.list_variables ──► sidebar ──► drag ──► put_layout
```

Two dicts, deliberately. `_all_subclasses` is scidb's and answers "what
types exist"; `_variable_sources` is the GUI registry's and answers "which
of those did *I* register, and from where". The second is what makes the
first prunable.

### The append-only trap

`__init_subclass__` fires when a class statement executes. There is no
matching hook for "this class went away", so `_all_subclasses` could only
ever grow. Every other registry a scan maintains (`_functions`,
`_parameters`, `_path_inputs`, …) is cleared and rebuilt; variables were
not. A name was therefore registered for the life of the *process*:

- a declaration deleted from disk stayed live;
- a previously-opened project's variables stayed live;
- `create_variable` refused names with no source file behind them
  ("A variable named 'RawEMG' already exists" with nothing to point at);
- the sidebar listed phantom types, which could be dragged onto the canvas
  and then failed inside MATLAB as `Unrecognized function or variable`.

`BaseVariable.unregister(name)` is the missing counterpart. It takes a name
rather than clearing wholesale, because the discovery layers must only
withdraw registrations they can prove they made — scidb's own types, a test
fixture's, or another importer's are not theirs to delete. That proof is
`_variable_sources`.

## Reload cost — read this before adding a refresh

A full `_refresh_registries()` re-imports every configured Python module and
re-classifies every MATLAB source. Measured on a real project (scidb.log,
2026-09-01):

| stage | cost |
| --- | --- |
| `config.load_config` | 2.5 s |
| `registry.load_from_config` (19 modules) | 1.6 s |
| `matlab_registry.load_from_config` (303 sources) | **14.9 s** |
| **total** | **~16.5 s** |

Creating one variable used to pay all of it. It cannot change any file but
the one just written, so it now doesn't:

| what changed | reload |
| --- | --- |
| an entity in the TOML | `registry.reload_entities_file()` — one TOML parse |
| a declaration in a `.m` | `matlab_registry.reload_source(path)` — one file |
| the loaded single-file module | `registry.refresh_module()` |
| *which* files are configured | `reload_registries_from_disk` (full — correct) |
| user hit 🔄 Refresh Code | full (deliberate escape hatch) |

`target_file_service._reload_after_write` is the dispatcher. **Route new
write paths through it.** If you find yourself adding a `refresh_all()`,
you are almost certainly re-reading 300 files to learn about one.

Narrow reloads prune by source before re-reading, so a declaration removed
by the edit actually disappears rather than lingering from the last scan.

## Init: what a project is guaranteed to have

`bootstrap.open_or_create_project` **and `server.py`'s JSON-RPC `main()`**
(the VS Code entry point, which duplicates the startup sequence inline and
must call the same step — it did not until 2026-09-10) → `project_init_service`:

1. `scistack.toml` at the resolved project root (created if absent).
2. `entities_file` key + `src/scistack_entities.toml` (created if absent).
3. `scistack_entities.py`/`.m` per language in use (created if absent).

Steps 1–2 run *before* the first `load_config` so they cost no extra reload;
step 3 runs after it and needs none, since a fresh stub declares nothing.

Packaged projects (`pyproject.toml`) are left alone and reported — the GUI
never hand-edits a pyproject (`config._reject_packaged_project`).

Before this existed, `add_path` could create a `scistack.toml` with no
`entities_file`, and `entities_path` only falls back to the conventional
location *if the file already exists*. The result was a project with no
writable surface at all — the log read `entities_file=None (writable)` — and
no stub directory either, since `variable_stub_dir` returns `None` when
there is no entities file to sit beside.

Step 2 is deliberately a no-op when the config says `entities_file = ""`
(the explicit opt-out `clear_entities_file` writes) — otherwise every
project open would silently put back the file the user just cleared.

**The mirror-image failure, 2026-09-10.** Step 2 asks *scidb* whether the
project already has an entities file, and scidb's answer includes the
conventional fallback. So for a project with a pre-existing
`src/scistack_entities.toml` and no `entities_file` key, init correctly
concluded "nothing to create" — and `config.load_config`, which did **not**
have the fallback, then reported `entities_file=None`, so the registry never
opened the file. Its Variables/Parameters/PathInputs were live for scidb and
MATLAB and invisible in the GUI. Both sides now go through
`scidb.entities.resolve_entities_path`; there is one rule.

## Who decides that a variable type is unusable

A type that resolves nowhere fails inside MATLAB as `Unrecognized function
or variable 'X'`, minutes after the run starts, naming neither the cause nor
the fix (2026-09-01: warned 15:33:03, failed 15:35:02).

The fix is **visibility, not refusal**, and it is worth being precise about
why — the obvious design here is wrong twice over.

**Placing an undeclared variable node is legal.** A manual variable node
whose label nothing declares yet is a designed state: it graduates to a
canonical `var__` id once a run gives it DB history
(`graph_builder.merge_manual_nodes`), and paste/duplicate/extract copy such
nodes wholesale. `put_layout` logs one and moves on. An earlier version
refused it and broke four flows.

**Python is not the authority on MATLAB's path.**
`matlab_command._unresolvable_var_types` sees classdefs the GUI registry
happened to parse, plus entities-file declarations. It cannot see a
`RawEMG.m` the user puts on the path from their own `startup.m`. So it
*reports* — into the log, into the generated script as a comment, and into
the run console through `result["warnings"]` before MATLAB starts — and
never blocks. `scimatlab/stubs.py` states the same rule for the same
reason, which is why `+scidb/entities.m` asks MATLAB itself via
`exist(name, 'class')` rather than deciding from Python.

The authoritative check therefore lives in MATLAB:
`+scidb/entities.m` warns `scidb:entities:noClassdef` for anything that
still does not resolve after stub materialization, before the `for_each`.
Escalating that warning to an `error` is the correct way to fail fast, and
is the open follow-up.

`scimatlab.stubs.invalid_name_reason` is a different guard, and it *is* a
refusal: a classdef filename must equal the class name, so a name that is
not a valid MATLAB identifier can never resolve no matter what is on the
path. It is rejected at the only place that turns a name into a filename.
