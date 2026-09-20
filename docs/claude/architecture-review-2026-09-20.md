# Architecture review — 2026-09-20

*Written at the end of the intent/fact overhaul (`refactor/intent-and-fact`,
21 commits, all green), while the guts were open. Companion plan:
`.claude/plan-architecture-2026-09-20.md`. Measured against the code as of
`16a58bd7`; the numbers are `wc -l` / `grep -c`, not estimates.*

## What this is

An inventory of the structural problems that made this week's work slower
and riskier than it should have been, each with the evidence that surfaced
it and the change that removes the CLASS of problem rather than the instance.
It is deliberately not a wishlist: everything here bit at least once this
month, most of it this week.

## The numbers

| | |
|---|---|
| `scidb/foreach.py` | 5,888 lines; `for_each` is a 975-line body with 22 parameters; `_save_results` takes 17 |
| `scidb/database.py` | 5,082 lines; `DatabaseManager` has 73 methods |
| GUI API | 108 JSON-RPC handlers (`server.py`) AND 106 FastAPI routes (`api/*.py`), hand-mirrored |
| `for_each` implementations | Python 2,574 + 5,888 lines; MATLAB 2,974 + 1,953 — mirrored by hand |
| Function-level (lazy) imports | 352 in scidb, 850 in the GUI |
| Comments saying "trap" / "silently" / carrying a date | ~300 in source |
| `docs/claude` | 109 files, 27k lines; 127 plan files in `.claude/` |
| `PlotStudio.tsx` | 3,916 lines |
| Tests | 5,120 test functions across 7 suites |

---

## 1. Identity is still two systems

**Evidence.** The entire intent/fact effort was one instance of "the GUI
predicts what scidb computes, and the two agree by convention." Column
selections are fixed. `wiring_id` is not: every `_intent` row is keyed by
`fn__{fn}__{wiring_id}`, and `wiring_id` exists only in
`scistack_gui/domain/graph_builder.py`, reconstructed from provenance with
normalisation patches (`strip_path_input_params`) added each time it
disagreed with the run path. `call_id` is the model that works — scidb owns
the recipe (`foreach_config.call_id_from_version_keys`), computes it forward
and backward (`provenance_query.config_call_id`), the GUI imports it.

**Change.** Move the wiring recipe into scidb (it is `call_id` minus
constants; `config_call_id` is most of it), export it, have the GUI import
it. Then add the test tier that would have caught every bug this week at
authoring time: **parity tests** — run something for real, reconstruct it
from provenance, assert forward == backward — for `invocation_id`,
`call_id`, `wiring_id`, and the per-param selector.

---

## 2. `for_each` assembles provenance two ways

**Evidence.** The `for_columns` bug (Stage 3) existed because a
full-iteration row and an aggregation row build their input edges by
DIFFERENT code — `__graph_var_bindings` on one path, the indexed
`__upstream` fallback on the other, with `_variable_bindings` preferring
whichever is present. Two derivations of one fact, inside one function.
Finding it meant reading ~15 disjoint regions of a 975-line body; every fix
this week was a block spliced into it. `_save_results` takes 17 parameters
because the phases share state through the argument list.

**Change.** Not a rewrite. Extract the three phases — prepare/bind,
execute, save — as functions over a typed `RunState`, and make "a result
row → its input edges (with selectors)" exactly ONE function that both modes
call. `check_selector_round_trip` (Stage 2) is the regression harness for
the refactor: it must stay silent throughout.

---

## 3. The GUI API is two hand-mirrored transports

**Evidence.** Every feature is two registrations (`_h_*` in `server.py`,
`@router.*` in `api/*.py`), copied argument parsing, and an entry in a
hand-maintained list of handlers that must not hold the DuckDB lock. The
`plot_variant_sets_save` RPC was nearly not registered this week.

**Change.** One declarative handler table — name, params model, service
function, lock policy — from which both transports are generated. A test
asserts every service entry point is reachable through both.

---

## 4. Lazy imports are the symptom of import cycles

**Evidence.** 1,200 function-level imports. `foreach ↔ provenance_save ↔
database ↔ provenance_query` in scidb; `execution_service ↔ pipeline_store
↔ intent_store ↔ graph_builder` in the GUI. It weakens LSP navigation,
hides dependency direction, and is why "where does X belong" recurred all
week (the `_duck` accessor, the selector normalizer, the schema-level
default).

**Change.** Draw the module DAG once; break each cycle by moving the shared
TYPE or pure helper down a layer; hoist imports to module top. A single
smoke test that imports every module keeps it fixed.

---

## 5. Rules live in comments instead of types

**Evidence.** The placement-id trap has been fixed four times
(`placement-qualified-ids.md`) because `fn__x__cid` and `fn__x__cid::main`
are both `str`. "Indexed binding names fold on READ only", "a
ColumnSelection is never in `rid_keys`", "`[]` vs `None` for schema keys"
— each is a paragraph somewhere and an assertion nowhere. ~300 dated
"trap" comments and 27k lines of `docs/claude` is the code carrying its
history as prose.

**Change.** For each recurring rule: a type (`BareNodeId` / `PlacedNodeId`),
an assertion at the seam, or a test. Then prune `docs/claude` to a curated
set of conceptual references plus short ADRs — `intent-and-fact.md` is the
shape to aim for — and archive the rest.

---

## 6. Scope-awareness is half done

**Evidence.** Every `_intent` row is stored at `global`; `get_hidden_node_ids
(None)` fail-opens across scopes; execution ignores `pipeline_id`; three
docstrings call it a "deferred follow-up". Half-done scoping generates
traps: the store has the column, the resolver walks it, and nothing feeds it.

**Change.** Decide. Either a run is scoped — `pipeline_id` travels with the
run request into derivation and the resolver, `_intent` rows are written at
the scope they were stated in, duplication copies by scope — or hypotheses
are just tabs and the column is dropped. Then finish it, in one session.

---

## 7. MATLAB is a second implementation, not a bridge *(separate effort)*

**Evidence.** Two `for_each`s mirrored by hand. This week `schema_keys`
semantics, `origin`, and the binding rendering each needed a MATLAB mirror
written blind. The MATLAB suite has not been fully green since `91547a1e`
(`project_matlab_test_suite_triage`).

**Change.** Extend the sidecar model: MATLAB builds the spec and calls the
user's function; Python plans combos, binds inputs, saves, records. Until
then, a parity harness that runs one spec through both and diffs the
records.

---

## 8. `DatabaseManager` is a god object *(separate effort)*

**Evidence.** 73 methods spanning storage, schema, variants, filters and
provenance; `foreach.py` reaches into `db._duck` throughout because there is
no other door — this week's additions did too.

**Change.** Split by concern behind the same facade (storage, schema,
provenance reads — `provenance_query` already is one — variant resolution).

---

## 9. Frontend *(separate effort)*

**Evidence.** `PlotStudio.tsx` is 3,916 lines. Node `data` is an untyped
bag extended ad hoc (`unusedIntent` this week). Two vite targets are built
by two commands, and "rebuilt one, forgot the other" is a memory entry. The
GUI is rarely visually checked; `gui-manual-testing-todo.md` grows every
session.

**Change.** Split PlotStudio by section; generate the TS types for node
data from the pydantic models; one build script for both targets; a
five-test Playwright smoke suite over the standalone bundle for the flows
that keep breaking (run a node, chips, panel persistence).

---

## 10. Migrations are bespoke *(small)*

**Evidence.** Six `ALTER … IF NOT EXISTS` blocks, marker rows in `_intent`,
and six superseded tables awaiting a manual drop.

**Change.** One ordered, idempotent migration list for the GUI tables.

---

## What is fine

Testing. 5,120 tests, real DuckDBs in fixtures, integration suites over a
real example dataset, and this week's `xfail(strict=True)` tests worked as
executable design documents. Log-line assertions look brittle but they ARE
the observability contract. The only missing tier is the parity tests in §1.

## Order

1 → 2 → 4 → 3 → 6 continue this week's thread and are each a few sessions.
7 and 9 are their own efforts. 8 and 10 fit wherever they are touched next.
