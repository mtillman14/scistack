# Intent and fact

*Written 2026-09-19, from the design discussion that followed
`.claude/plan-input-binding-seam.md`. The implementation plan is
`.claude/plan-intent-and-fact.md`. Companion to
`docs/claude/input-binding-round-trip.md`, which defines the one concept this
model was first derived from.*

## Why this document exists

Several concepts the GUI depends on are not named anywhere in the code. The
GUI re-derives them from the outside — it *predicts* what a lower layer
*computes* — and the two agree only as long as someone remembers to keep them
agreeing. The column-selection bugs of 2026-09-19 are one instance; the
`wiring_id`/`call_id` pair and the duplicated selector normalizer are others.

Underneath all of them is a distinction with no name:

> **A statement about a run that should happen is a different kind of thing
> from a record of a run that did happen — and the system stores both, mixes
> them at five different sites, and has never said which wins.**

This document names them, states the rules, and says where each rule is
consumed. It is the conceptual reference; the plan is the work.

---

## 1. The cut

**Fact** is provenance: `_record`, `_invocation`, `_invocation_input/_output`.
A fact is what a run did. Facts are never edited and never overridden — they
are the floor everything else resolves against, and the only thing a
reproduction can appeal to.

**Intent** is a statement about a run that has not happened yet: which
columns, which values are in play, which edges feed what, which schema levels
to iterate, which run options. Intent is authored by a person, is revisable,
and may contradict what history recorded — that contradiction is a user
changing their mind, not corruption.

**Display intent** — canvas positions, parameter value groupings, hidden
subpipeline ports, hypothesis prose — is a third thing and is deliberately
OUT of scope here. It changes nothing about what runs, so putting it through
a resolver buys nothing.

The test for whether a stored thing is execution intent: *would changing it
change what a run computes?* If yes, it is governed by the rules below.

---

## 2. The five rules

1. **Statement.** A unit of intent attaches to a SUBJECT (a call site, a
   parameter, an edge, a variable type), names an ASPECT (columns, run
   options, hidden, schema location, wiring, constants), and carries a SCOPE.
2. **Surface.** A surface is a place statements are written: a source file
   (`.py`, `.m`), or the intent store. Surfaces are PEERS — never merged, and
   neither is a view of the other.
3. **Origin.** A run's origin names which surfaces it reads, in order:
   * GUI run — intent store, then source;
   * script run — source ONLY (GUI statements are not read);
   * replay of a recorded invocation — history only.
4. **Resolution.** Nearest scope wins within a surface; then surfaces in
   origin order; then history as the floor. One function, one place.
5. **Fact is never intent.** Provenance is not a surface, is never edited,
   and is compared against rather than merged into.

Everything else in this document follows from these.

---

## 3. The nouns

| noun | what it is |
|---|---|
| `Statement` | one piece of intent: `(subject_kind, subject_ref, scope, aspect, value, origin, stated_at)` |
| `Surface` | where statements are written — `source` or `store` |
| `Origin` | which surfaces a run reads, in order |
| `Scope` | the pipeline/hypothesis a statement applies to, or `global` |
| `Aspect` | which normalizer owns the payload (see §5) |
| `RunPlan` | the resolved result: bindings, constants, run options, schema filter — exactly what `for_each` needs, and nothing more |
| `Decision` | per resolved field: the winning value, its surface and scope, and what it beat |

`RunPlan` is the output everything executes. `Decision` is the output
everything *explains* — the logs, the canvas chips, `scidb trace`, and the
round-trip guards are all reports on a `Decision`.

### The record shape

```
(subject_kind, subject_ref, scope, aspect, value_json, origin, stated_at)
```

* `subject_ref` is a STABLE id — a `wiring_id`, a declared name — never a
  placement-qualified canvas id. Placement (`::{pipeline_id}`) is a display
  attribute, not identity (`docs/claude/placement-qualified-ids.md`).
* `scope` is on EVERY row. Today three of the seven execution-intent tables
  scope and four do not, and no one can say why from the table alone.
* `origin` records which surface a statement came from, so precedence is data
  rather than convention.
* `stated_at` makes "the user's most recent statement wins" a query.

---

## 4. Where intent lives today

`scistack_gui/pipeline_store.py` defines 13 tables. Sorted by what they
really are:

| | tables | governed here? |
|---|---|---|
| **execution intent** | `_node_config` (schemaSelection, schemaLevel, whereFilters, runOptions, columnSelections), `_pipeline_edges`, `_pipeline_hidden_combos`, `_pipeline_hidden_constant_values`, `_pipeline_hidden_nodes`, `_pipeline_hidden_edges`, `_pipeline_pending_constants` | **yes** |
| **display intent** | layout positions, `_pipeline_parameter_value_groups`, `_pipeline_hidden_ports`, `_hypotheses` | no — stays as is |
| **attribution index** | `_pipeline_path_input_history` | no — it repairs fact, it does not state intent |
| **fact** | scidb's provenance tables | never |

Each of the seven invented its own key shape, its own scoping answer, and its
own story for what happens when the subject changes identity. One shape
retires all seven answers.

---

## 5. Aspects and their owners

An aspect names the module that owns its payload shape, its "absent" rule and
its comparison. Exactly one owner each, in the lowest layer that can host it:

| aspect | payload | owner |
|---|---|---|
| `columns` | `{"columns": [...], "iterate": bool}`, `None` = whole variable | scidb (merging `provenance_save.compute_input_selectors` + the GUI's `column_selection.normalize`) |
| `run_options` | `distribute` / `as_table` / `iterate` | `scidb.foreach_config` |
| `schema_location` | which levels iterate, which are pinned | the location-filter owner (`docs/claude/location-filter-semantics.md`) |
| `wiring` | which source feeds which parameter | `scidb`, alongside the input-binding shape (`docs/claude/input-binding-round-trip.md`) |
| `hidden` | which call sites / values / edges are excluded | scidb (it already stores `excluded` on records) |
| `constants` | pending and recorded constant values | `scidb.foreach_config` |
| `variant_selection` | a plot's named pins over variant space (`VariantSet`), keyed by name, about the plotted variable; no fact side | `scidb.intent` (shape), `scistackplot.spec.VariantSet` (payload) |

The GUI imports all of them. The precedent that this works is `call_id`:
scidb owns the recipe (`foreach_config.CallSite`), computes
it forward and reconstructs it backward (`provenance_query.config_call_id`),
and the GUI calls it instead of hashing its own.

---

## 6. Where it is consumed

| site | today | with this model |
|---|---|---|
| GUI run (`api/run.py`) | `derive_fn_targets` → `_attach_db_*` → `_attach_column_selections` → `filter_hidden_*` → `build_run_inputs` | gather statements → `resolve(origin=gui)` → execute the `RunPlan` |
| MATLAB run (`api/matlab_command.py:1415`) | its own copy of the binding logic | the same `RunPlan`, rendered to MATLAB |
| code export (`code_export_service.py`) | resolves, then prints | unchanged in shape — and it becomes the proof that `RunPlan` is complete |
| canvas rendering | chips computed on a path separate from the run | chips render the same `RunPlan`, so a chip that disagrees with the run is structurally impossible |
| `scidb` CLI / Inspector | `trace` shows fact | `trace` shows intent, fact and the `Decision` between them |
| Plot Studio | `VariantSet` pinning in its own vocabulary | a pin is a statement with `aspect="variant_selection"` |
| tests | poke tables, assert on logs | build statements, assert the `RunPlan` |

Five derivation paths collapse to one. Every "these two must agree" comment
in the codebase becomes one object both sides read.

---

## 7. Decisions taken (2026-09-19)

**A script run ignores GUI intent.** Origin `script` reads source only. The
alternative — a checkbox ticked last week silently changing what
`python pipeline.py` does — is indefensible.

*Consequence, which must be surfaced rather than tolerated:* the canvas can
show a statement the last run never used. The `Decision` records which
surfaces a run read, so the node can say **"stated here, not used by the last
run — it ran from source."** A silent divergence here is the same bug class
this whole model exists to kill.

**No write-back; surfaces are peers.** Ticking a checkbox does NOT rewrite
`pipeline.py`. Write-back only makes sense if the script is the single
authority and the GUI is an editor for it, which the decision above rules
out. The bridge already exists and stays the only one: **code export is
write-back**, at whole-pipeline granularity. The rejected alternative would
require a per-field source editor that parses and rewrites Python and MATLAB,
preserves comments, and handles a statement whose source line no longer
exists.

**Scope on every row; resolution walks `scope → global`; duplication copies.**
Three behaviours from two rules: state it at `global` to mean everywhere,
state it at the scope to shadow, and a duplicate owns its rows so editing it
cannot reach the original. `scope_service._clone_nodes` already copies node
config on duplicate (`scope_service.py:425`) — this makes the biggest table's
existing behaviour the rule.

*Rejected:* inheritance from the original (`derived_from` chains). One rule
instead of two, but an edit to the original silently changes a copy made
weeks ago, and it needs a cycle guard and a detach operation — more
carveouts, of the runtime-surprise kind.

---

## 8. Non-goals

* Display intent stays where it is.
* scidb does not gain a GUI table. It defines the record shape, the aspect
  normalizers and the resolver, and receives a plain payload; the STORE stays
  GUI-side. MATLAB and the CLI construct the same payload without touching a
  GUI table. If scidb ever had to import `pipeline_store`, the design is
  wrong.
* No general framework for "state a layer cannot carry". The aspects in §5
  are the list; a seventh gets added when a seventh appears.

## 9. One constraint on the design

Resolution is on the hot path of every run, every canvas render and every
export. It must be **batched** — resolve once per graph build, not once per
node — for the same reason the provenance helpers are
(`docs/claude/…` the `*_batch` rule: per-record provenance calls on a load
path are an N+1 trap). A resolver that is correct and per-node is not
shippable.

## 10. Ground truth

| what | where |
|---|---|
| intent tables today | `scistack-gui/scistack_gui/pipeline_store.py:92-300` |
| the five derivation paths | `services/execution_service.py`, `api/run.py`, `api/matlab_command.py`, `services/code_export_service.py`, `domain/graph_builder.py` |
| the working precedent (one recipe, both directions) | `scidb/src/scidb/foreach_config.py` + `provenance_query.config_call_id` |
| what a binding is | `docs/claude/input-binding-round-trip.md` |
| why ids move | `docs/claude/placement-qualified-ids.md` |
| duplication semantics | `services/scope_service.py::duplicate_pipeline`, `_clone_nodes` |

## 11. Status — 2026-09-19, branch `refactor/intent-and-fact`

Every stage of `.claude/plan-intent-and-fact.md` is built:

* **Store:** `_intent` holds all seven execution-intent tables' worth of state
  plus `variant_selection`. `pipeline_store` keeps its accessors and
  delegates (`scistack_gui/intent_store.py`, `GRADUATED_ASPECTS` is the
  progress bar). The old tables are left in place, rows untouched, after a
  marker-guarded one-time import each; dropping them is a separate step.
* **Origin:** `_run.origin`; `scidb.intent.run_origin` / `set_ambient_origin`;
  the GUI run thread, the compiled pipeline and GUI-generated MATLAB commands
  label `gui`; unlabelled is `script`.
* **Divergence:** `graph_builder.mark_unused_intent` → amber chip + NOT
  REFLECTED note; `scidb intent <fn>` / `scidb trace --intent` from the CLI.
* **One derivation:** the MATLAB route renders
  `execution_service.variable_inputs_view(targets)`; its own copy is gone.
* **Schema level default** (`execution_service.default_schema_level`, one
  owner for the run thread and the compiled pipeline): the node's own level,
  else where the function last ran (`provenance_query.recorded_schema_keys`),
  else the level its inputs imply — every key any bound Variable's records or
  PathInput template carries, in dataset order (`variable_schema_keys` +
  `finest_schema_keys`); the coarser input broadcasts — else every key, only
  when there is nothing to go on.

Still `global` scope on every stored row until execution is scope-aware.
