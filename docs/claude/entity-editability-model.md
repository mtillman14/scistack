# Entity editability: how the GUI and script authoring modes cohere

**Status: ALL STAGES (1–10) IMPLEMENTED, uncommitted, 2026-08-25.**
Plan of record: `.claude/plan-gui-entity-editing-26-08-24.md` (decisions
D1–D7, per-stage status). Rejected alternatives are preserved in
`.claude/plan-gui-source-cohesion-26-08-24.md`.

> **Superseded in part, 2026-09-01: the entities file is now TOML.**
> Everything below about *policy* — what may be edited, confinement,
> staleness, rollback, the additive rules, PathInput history — still holds
> exactly. What changed is the *format* the declarations are written in, and
> that there is now **one** writable file for both languages rather than one
> per language. Where this document says the entities file is
> `src/scistack_entities.py` or a MATLAB script, read
> `src/scistack_entities.toml`; see **`entities-toml-format.md`** and
> `.claude/plan-entities-toml-26-08-31.md`. The MATLAB-script and
> Python-module sections below now describe **read-only** surfaces that are
> still fully discovered.

**Two things still need the user, not the test suite:**

1. **A MATLAB run.** There is no MATLAB in the dev environment, so
   `+scidb/Parameter.m` and the `for_each.m` changes are correct by
   inspection only. The *file-level* MATLAB round-trip (parse → splice →
   re-parse → registry) IS covered, in
   `tests/test_entity_round_trip.py::TestMatlabEntitiesRoundTrip`; the
   runtime half is not.
2. **A browser pass over the panels.** Editing, and the read-only
   "declared in foo.py:42" path, are runtime UI behaviours no test here
   exercises.

If you are reading this and the code disagrees, the code wins — check the
plan's per-stage status before assuming this doc is current. (See
`gui-export-to-plain-python.md`, which sat at "not yet built" long after the
feature existed, for why this warning is here.)

## The question this answers

The framework supports two authoring surfaces for the same pipeline:
GUI-first (build in the canvas, export to a script-only pipeline) and
script-first (write plain `scidb` code, import into the GUI to manage). The
"source is truth, DB is run history" migration (commits `066cc53` →
`89f4f35`, see `code-discovery-categories.md`) made all six entity kinds
source-declared. A side effect: Sweeps, PathInputs and Constants became
**read-only in the GUI** — creatable and hideable, but not editable.

## The core principle

**"Source is the single source of truth" does not imply "the GUI is a
read-only viewer."** It implies the *file* is where the value lives. A GUI
that edits the file is still fully consistent with the principle — that is
what an IDE is.

Read-only was an artifact of having built only the *append* path
(`target_file_service.append_and_refresh`), not a consequence of the
decision. The fix is targeted rewriting of a declaration, not an overlay
layer.

Consequences of that framing:

- There is **no staging/overlay of entity values.** An edit writes to
  source immediately and the registry re-scans. No dirty state, no
  commit-on-run, no "run with uncommitted edits" question.
- The invariant the migration protected still holds: **every recorded run
  corresponds to source that existed on disk.**

## What is editable, and what is not

| Where a declaration lives | Editable in the GUI? |
|---|---|
| The designated entities file | **Yes** |
| Any other source file | **No — permanently, by design** |

GUI writes are confined to the entities file so the GUI never surprises a
user by editing their hand-written modules. This is a contract, not a gap:

- **Both languages: `config.entities_file`, default
  `src/scistack_entities.toml`** (since 2026-09-01). One
  language-neutral file, read by Python directly and by MATLAB through
  `scidb.entities()`.
- `config.variable_file` (`.py`) and `[matlab] entities_file` (`.m`) are
  **read-only legacy surfaces**: still discovered and displayed, never
  written. They are the same read-only case as any other module, and need
  no new concept to describe.

A read-only entity shows its exact declaration site
(`src/analysis/params.py:42`) rather than a generic "edit it in source"
hint, with a click-to-open link under the VS Code extension.

**Read-only is shown up front, not discovered on save (2026-09-23).** The
rule has one owner, `target_file_service.entity_editability(kind, name)` →
`{editable, reason: None|"read_only"|"unknown", file, line, message}`. Two
consumers read it: `update_declaration` (refuses the write with exactly that
file/line/message) and the sidebar panels, via the
`get_entity_editability` RPC (`GET /api/entities/{kind}/{name}/editability`)
and `useEntityEditability`. A `read_only` answer greys out every edit
control in `PathInputSettingsPanel` / `ParameterSettingsPanel` and shows
`ReadOnlyDeclarationBanner`. The lock decision itself is
`sourceLocation.isLockedForEditing` (pure, unit-tested): only `read_only`
locks; `null` (not answered yet / request failed) and `unknown` fail open,
because the write-time refusal is still the backstop. Before this, a
MATLAB-declared PathInput's Root Folder accepted typing and only refused it
on blur.

**There is deliberately no "Adopt into the entities file" action.** Moving a
declaration means deleting it from the user's file (violates the
confinement rule); copying it creates a duplicate name across two scanned
modules, which `registry._register_parameter`'s last-loaded-wins-with-a-warning
path resolves arbitrarily.

## Parameters: one class, one node type

**`scidb.Parameter` is the only configuration-value construct.** The value
count is the only thing that varies, and it may be zero:

```python
SAMPLING_RATE_HZ = scidb.Parameter(1000, description="Recording rate")
WINDOW_SECONDS   = scidb.Parameter(10, 20, 30)
THRESHOLDS       = scidb.Parameter(*range(10, 60, 10))   # plain varargs
CUTOFF_HZ        = scidb.Parameter()                     # not yet valued
```

**A Parameter with no values is a legal, declared state** — legal at rest
(source, entities file, registry, canvas), refused at execution. It is what
the "New parameter" form creates, since that form collects only a name, and
what removing the last value in `ParameterSettingsPanel` leaves behind. The
GUI used to scaffold a placeholder `0` instead; once written, that is
indistinguishable from a value the user chose, so it showed as a checked
value on the node, fed `for_each`, and got stamped into any records produced
before the user noticed.

Refusal lives at `for_each` expansion (`scifor.require_alternatives`, called
by all four expansion sites: `scifor`/`scidb` × Python/MATLAB), not at
construction. Two reasons. It is where the harm is: the Cartesian product
over a zero-length axis is empty, so an unvalued Parameter bound to a
function makes `for_each` iterate zero times, write no records, and return
as though it had succeeded. And it is the only place MATLAB can put it — a
superclass constructor call may not sit in a conditional branch, so
`+scidb/Parameter.m` has exactly one `obj@scifor.EachOf(args{:})` call and
`args` is empty for a value-less Parameter. `EachOf` therefore accepts zero
alternatives in both languages. The GUI refuses it earlier still, in
`execution_service.build_run_inputs`, so the message can name the declared
Parameter the user sees on the canvas rather than the signature parameter it
feeds.

`EditTab.tsx`'s category strip is five, not six: Submodules, Functions,
Variables, **Parameters**, Path Inputs.

**Why one class rather than two.** The predecessors were `scidb.constant()`
(one value) and `scidb.Sweep()` (many) — two constructs for one idea. Adding
a second value therefore changed the entity's *kind*, which made it vanish
from a "Constants" tab and reappear under "Sweeps" for what the user
experiences as "I added a value." An interim design merged only the
*presentation* while keeping both source constructs; that was a veneer, and
the split survived in the registry, the scanners, and a `source_kind` field
the node had to carry.

`Parameter` **is** a `scifor.EachOf`, and `EachOf` expansion has **no branch
for a single alternative** — so `Parameter(30)` produces byte-identical
`version_keys`/`call_id` to a bare `30`. That is the fact the whole design
rests on: adding a value is adding an argument, with no change of form, id,
node, tab, or history. There is nothing to convert, and no conversion
dialog.

One consequence worth stating: **there is no "constant vs sweep" anywhere in
the stack** — one registry (`registry._parameters`), one scanner
(`_scan_module_parameters`), one node builder (`build_parameter_nodes`), one
service (`parameter_service`), one route family (`/api/parameters`), one
node component (`ParameterNode.tsx`).

### Generated value sets

`ParameterSettingsPanel`'s **Generate** section writes a whole list at once
("Replace values"), as against **Add value**, which appends one. Generated
sets render as a *single* compact row — `0:2:20 — 11 values` — with one
checkbox, in both the sidebar list and the canvas node. Values added one at
a time keep their per-value rows exactly as before.

Three things make that work, and each was a deliberate choice:

- **The button is the signal.** "Replace values" sends a `group`
  (`{kind, spec}`) alongside the same flat value list every other edit
  sends; "Add value" does not. Nothing infers generated-ness from the values
  themselves, so a hand-typed `10, 20, 30` is never collapsed.
- **The grouping is GUI state, not source.** It lives in
  `_pipeline_parameter_value_groups` (one row per Parameter), never in the
  declaration. Source stays a flat list of values in all three languages, so
  `render_parameter` / `render_parameter_value` / `render_matlab_parameter`,
  `version_keys` and MATLAB parity are all untouched by the feature. Per
  CLAUDE.md NOTE 3: grouping is a display concern, so it lives in the GUI
  layer.
- **Source still decides which values exist.** `build_parameter_nodes`
  checks the recorded members against the declaration on every read; if any
  member has left source (a hand edit, or the panel's `×`), the group is
  stale and every value renders individually. A stale group can never mask a
  value that is really declared.

The compact label is rendered backend-side
(`graph_builder.render_value_group_label`) and shipped in the node's `data`,
because `ParameterSettingsPanel` receives the node's `values` as a prop — so
the canvas and the sidebar show the identical string by construction rather
than by two frontend implementations agreeing. The one checkbox hides or
unhides every member through the batched
`pipeline_store.hide_parameter_values` / `unhide_parameter_values`, and a set
reads as unchecked as soon as *any* member is hidden (no tri-state).

## Edit semantics, by kind

One axis governs everything: a declaration holds one value or several, and
**adding a value is always additive.**

| GUI concept | Single form | Multi form |
|---|---|---|
| **Parameter** | `scidb.Parameter(2)` | `scidb.Parameter(2, 5)` |
| **Path Input** | `scidb.PathInput(t1)` | `scidb.EachOf(scidb.PathInput(t1), scidb.PathInput(t2))` |
| Variable | *(a type — no value, not editable)* | — |

A Parameter needs no wrapping at all — it IS an `EachOf`, so more values is
just more arguments. PathInput has no such class, so it wraps explicitly in
`EachOf`. Both are additive; only PathInput changes its expression shape.

In the TOML entities file the same axis reads as scalar-vs-array, and the
wrapping disappears entirely — an array *is* the alternative list:

| GUI concept | Single form | Multi form |
|---|---|---|
| **Parameter** | `W = 2` | `W = [2, 5]` |
| **Path Input** | `P = "a.csv"` | `P = ["a.csv", "b.csv"]` |
| Variable | `variables = ["StepLength"]` | — |

The scalar→array transition is a change of *form* in the file, but not of
kind, id, node, tab or history — and it is the same single-span splice a
value edit performs.

### Rule 1 — adding a value is always additive

Never orphans history, never changes node identity. The registry scanner
already accepts "an `EachOf` whose every alternative is a `PathInput`" as a
PathInput, and a Parameter is a Parameter whether it holds one value or
several. Same node id, position, tab and controls throughout.

Run history survives because `EachOf` is resolved at the top of
`for_each()` into recursive calls with concrete values — version_keys,
branch_params and rid expansion only ever see the scalar `2`, identical
whether it was declared alone or as one of several
(`each-of-variant-expansion.md`). An already-computed combo stays green;
only genuinely new values run.

### Rule 2 — editing a value in place is non-destructive, for every kind

History follows the node across an edit. Prior runs stay visible and stay
green; only genuinely new values run.

- **Parameter**: history is keyed by the *concrete value* in `version_keys`,
  and the node displays every historical value beside the current source
  value. Editing `2` → `5` adds a row.
- **PathInput**: same outcome, by a different route — see below.

> **REVERSED 2026-09-25 (user decision).** The name IS now the identity:
> `name=` is required and `to_key()` is the name alone
> (`docs/claude/identity-layers-pathinput.md`). The objection below — two
> runs with one name and different templates collide into one provenance
> node — is now the INTENDED behaviour: one name is one PathInput wherever
> its files live, so moved data and other machines keep every id; a
> genuinely different input gets a different name. Two arguments sharing a
> name with different templates inside one call are refused
> (`parameter.check_path_input_names`). The paragraph is kept for the
> history of the decision.

**Do not "fix" PathInput identity by putting the name in `to_key()`.** This
is the trap; an earlier draft of this doc suggested it. `to_key()` feeds
`version_keys` *and*
`provenance.compute_pathinput_record_id(spec)`, which content-addresses the
PathInput's node in the bipartite provenance graph
(`_sha16(PATHINPUT_TYPE, f"spec:{spec}")`).
`foreach_config._serialize_inputs` states the requirement directly: without
a content-derived identity, "two different templates would collapse into
the same version-key group." Keying on the name would let two runs with the
same name and different templates collide into one provenance node. **A
template change is a different input; a different provenance node and no
cache hit is correct.**

The defect was never identity — it was **attribution**. The GUI used the
provenance key as its only way to decide which canvas node a historical run
belongs to (`graph_builder.resolve_path_input_name` content-matches
template+root_folder against the registry), which conflates "what computed
this" with "which node should display it."

The fix is therefore entirely GUI-side, with no scidb change: a GUI-owned
`_pipeline_path_input_history (name, template, root_folder)` table,
append-only, written from **exactly one place** —
`target_file_service`, immediately before a write-back overwrites a
template. `resolve_path_input_name` then tries: live registry content-match
→ history content-match → `__unresolved__` + WARN.

Consequences: GUI template edits stop detaching history; PathInput obeys the
same show-historical-values-beside-the-current-one rule as Parameters;
renames are covered from the other direction (the template is unchanged, so
strategy 1 still matches).

> **Update 2026-09-25: that last clause holds only for runs recorded without
> a name.** Since F38 (2026-09-24), runs record `declared_name` on the
> PathInput edge, and that name wins over the content match. So a rename,
> even one with the template unchanged, would draw every earlier run as a
> ghost node under the old name. The GUI rename control
> (`layout_service.rename_path_input`, `.claude/plan-pathinput-rename.md`)
> therefore also writes `_pipeline_path_input_renames` (old → new). The only
> reader of that table is `graph_builder.resolve_renamed_path_input`, which
> redirects a recorded name only when it is no longer declared. The rename
> also moves every piece of state keyed by the node id
> `pathInput__NAME[::scope]` (`pipeline_store.rebase_node` +
> `layout.rebase_node_positions`), because the name is part of that id.
> **A rename done by hand in the TOML still leaves those ghosts**, for the
> same reason a hand template edit detaches history. `scidb graph` (CLI)
> does not read the GUI rename record either.

**Scoped deliberately narrowly.** The table pays for the capability Stage 5
introduced — one-click template editing — and nothing else. In particular it
does **not** record on every registry scan. An earlier draft did, to cover a
template edited *directly in source*, but that cost a database write on
every project load for a case that (a) has always detached history, since
before Stage 5 there was no GUI template edit at all
(`code-discovery-categories.md` §4), (b) stays visible via the WARN and the
`__unresolved__` node, and (c) is done by the user most likely to understand
the consequence. Editing a template by hand still detaches its history, and
that is unchanged pre-existing behaviour, not a regression.

It is also **not scoped by `pipeline_id`**: what a recorded template *meant*
does not vary by scope.

If this ever needs to be more than a GUI convenience, the cleaner design is
to stamp the declared name onto the `PathInput` at scan time (the way
`Parameter` already carries `source_file`) and have scidb record it as
**non-key** invocation metadata — attribution recorded where the run is
recorded, no GUI-side index. That touches scidb's provenance write path, and
the name must never enter `to_key()` or two templates collapse into one
provenance node.

### Rule 3 — removing a value never unwraps

A Sweep pared back to one value stays a one-element Sweep; an
`EachOf(PathInput(...))` pared to one stays wrapped. Behaviour is identical
and auto-unwrapping would make source churn as the user edits.

### Variables are not editable

A Variable is a *type*, not a value — nothing to splice. Renaming one
orphans history far more severely than a PathInput edit ever did (records
are keyed by the type name, and there is no name↔value history to fall back
on). Variables stay create-only.

## MATLAB

> **Superseded 2026-09-01.** The writable entities file is the TOML one,
> read from MATLAB via `scidb.entities()` (`entities-toml-format.md`). The
> `.m` entities script described below is still parsed and discovered, and
> the reasoning that follows — why a *script* rather than a classdef, and
> why value getters went — is preserved because it is exactly why the TOML
> file is re-read on every access instead of cached per session.

### The entities file is a plain script

```matlab
% scistack_entities.m
test_path_input = scidb.PathInput('{subject}/{subject}_{session}.csv');
test_window     = scidb.Parameter(10, 20, 30);
test_threshold  = scidb.Parameter(2, description='Detection cutoff');
```

Pipeline code runs `scistack_entities;` and the names are in scope.

MATLAB has no module-level bindings, which is why the original
implementation used a **value getter** convention (a zero-arg function
returning the constructed object). But the GUI never needed an *importable*
binding — it needs a **statically parseable declaration**, and a script line
is exactly that.

**Value getters were removed entirely on 2026-08-25**, along with the
`[matlab] path_inputs`/`sweeps` config lists. An interim version of this
plan kept them as a read-only discovery path ("declared outside the entities
file"), but two discovery conventions for one concept contradicts
`feedback_beta_no_deprecation` (clean breaks, no shims) and buys nothing:
the read-only concept is carried by Python hand-written modules and, on the
MATLAB side, by a folder-scan-discovered entities script that isn't the
configured `entities_file`. **One declaration form per language.** A
function that happens to construct a Parameter is now just a function.

Two alternatives were rejected:

- **One getter file per entity as the writable target** — makes the
  "designated entities file" a directory, with no structural parity with
  Python.
- **A `classdef` with `properties (Constant)`** — MATLAB evaluates those
  once per session and caches, so every GUI write-back would need a `clear`
  to take effect. A script is re-read on every run, so there is no cache
  to invalidate. It also collided with the existing rule that any `classdef`
  file is never scanned for functions/getters.

`BaseVariable` classdefs still live one-per-file in `[matlab] variable_dir`
— MATLAB requires one public classdef per file named after the file, so
they cannot live in the entities script. Both settings exist and mean
different things.

That constraint outlived the format change, and is why a Variable is the
one kind the TOML file cannot fully own: MATLAB has no runtime class
creation, so a classdef stub is generated per TOML-declared variable. The
TOML entry is the declaration; the stub is generated output.

**Who writes the stub, and where.** The writing lives in
`scimatlab.stubs.write_variable_classdefs` (making a declaration
referenceable from MATLAB is a MATLAB-wrapper concern, CLAUDE.md NOTE 3);
`variable_stub_dir` picks `[matlab] variable_dir` when configured and
`<entities_file.parent>/scistack_variables` otherwise. Two callers, with the
same rule but different authorities for "does this type already have a
classdef?":

- `+scidb/entities.m`, at the top of every run — asks MATLAB
  (`exist(name, 'class')`), writes the ones that fail through
  `py.scimatlab.bridge.ensure_variable_classdefs`, `addpath`s the directory
  and `rehash`es. MATLAB's path is the only authority that sees a
  hand-written classdef sitting somewhere else, and writing a second one
  would shadow it.
- `scistack_gui.matlab_registry.materialize_variable_stubs`, at registry
  load — same rule against the classdefs the registry parsed, and it runs
  *after* `load_from_sources` so those are all known.

This used to be gated on `[matlab] variable_dir` being configured, and a
project without one got no classdef and no warning: the run died with
`Unrecognized function or variable 'RawEMG'` from inside a `for_each` call
(see `.claude/plan-matlab-variable-classdef-materialization.md`). Two
related diagnostics exist now — `scidb:entities:noClassdef` from MATLAB when
a declared variable still does not resolve, and a `% WARNING:` comment plus
a log line from the command generator when a script is about to call
`Type()` that neither a classdef nor a declaration accounts for.

Note the GUI reads `entities_file` **only** from the explicit config key,
while `scidb.entities.entities_path` also accepts the conventional
`src/scistack_entities.toml` when it exists. A project relying on the
convention without the key still runs (MATLAB self-heals at run time), but
its declarations do not appear in the GUI.

### `scidb.Parameter` in MATLAB

`+scidb/Parameter.m` — `classdef Parameter < scifor.EachOf`, with
`description`, `.values`, and `.value` (which errors on a multi-valued
Parameter rather than silently picking one). This closes
`code-discovery-categories.md` §3's old "MATLAB has no equivalent" note for
constants.

Because it IS an `EachOf`, `+scidb/for_each.m`'s existing Step 0 expansion
handles it with **no unwrapping step at all**. An interim design used a
`scidb.Constant` value holder plus an explicit unwrap loop at the top of
`for_each`; subclassing `EachOf` deleted that code instead of adding to it.

**Hard requirement:** the values recorded into `version_keys` must be
byte-identical to Python's, or the same pipeline diverges in history
depending on which language ran it. This holds by construction —
`Parameter(30)` expands to one call carrying a bare `30` in both languages —
rather than by two matching unwrap implementations.

The constructor peels a trailing `description=...` off its varargs before
calling the superclass: `scifor.EachOf` treats every argument as an
alternative, so the description would otherwise silently become a value.

Implementing the Python side surfaced a pre-existing bug: nothing unwrapped
a wrapped constant before hashing, so it reached `canonical_hash` as an
unknown type and raised `ValueError: Unserializable data type` — passing a
declared constant straight into `for_each` failed outright. `Constant` had
60 unit tests and none exercised the execution path.

`+scidb/` also gains a one-line `PathInput` subclass shim over the
`+scifor/` original, so both languages' entities files read identically.

### Execution and staleness

Tier selection is unchanged: MathWorks VS Code extension → terminal; else
`matlab` on PATH → the kept-warm `matlab_sidecar.MatlabSidecar` engine;
else copy-paste. The terminal stays preferred inside VS Code because it
gives real breakpoints and workspace inspection.

The sidecar is kept warm, so its workspace persists across commands. The
script form makes staleness after a write-back trivial to handle:
`api/matlab_command.py` emits `scistack_entities;` right after the
`addpath` block of every generated command — one place, all tiers,
idempotent. No cache invalidation, no write-back→session notification
channel.

## Explicitly out of scope

- **Wiring/DAG round-tripping.** `export = eject`, `import = adopt`. Script
  → GUI import stays create-once; GUI → script export stays a flattened
  one-way snapshot (`code-discovery-categories.md` §6).
- **Variable editing/renaming** (above).
- **Adopting foreign declarations** into the entities file (above).

## See also

- `entities-toml-format.md` — the format declarations are written in now,
  and the coexistence rules for the two read-only legacy surfaces.
- `code-discovery-categories.md` — how all six kinds are *discovered*
  (the read half; this doc is the write half).
- `each-of-variant-expansion.md` — why `Sweep` history and `Constant`
  history are interchangeable.
- `scistack-gui-pending-constants.md` — the staging table whose role
  narrows to "inline, unnamed constants" once Parameters are editable.
- `.claude/plan-constant-source-of-truth-26-08-22.md` — the per-value
  include/exclude mechanism the Parameter node inherits.
- `.claude/plan-matlab-pipeline-execution.md` — the MATLAB execution tier
  ladder.
