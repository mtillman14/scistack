# Plan: the topologies view in the GUI

Surfaces `Inspector.topologies` — Problem 9 in
`.claude/plan-run-state-and-duplicate-nodes.md` — in the GUI. The Python API
and the CLI (`scidb variants <name>`) are built and tested; this is the third
surface.

## The question it answers

*"Why does this variable have more variants than I expected?"*

The user asked it on 2026-09-22 after a run split `grSides` into two nodes, and
the existing tooling could not answer it. `Inspector.provenance` (the panel
that already exists) is **top-down**: pin a variant, trace where it came from.
You cannot pin a variant you do not know exists. This view is **bottom-up**:
what is in here, and which of it is still live.

## What it shows

Two levels, exactly as the CLI renders them:

```
GAITRiteLoaded — 1 topology, 2 variants

  loadGaitRiteOneFile(gaitRitePath: …, gaitRiteConfig: …) → GAITRiteLoaded
    [1] gaitRiteConfig={…}   run=distribute=false   code=4999059d
        560 records · first 2026-09-14 11:02 · last 2026-09-19 16:41
        load: SUPERSEDED (an older run-option set)
        420 locations   subject/session/speed — SS01/BL/SSV, … (+417)
    [2] gaitRiteConfig={…}   run=distribute=true    code=4999059d
        450 records · first 2026-09-22 13:49 · last 2026-09-22 13:50
        load: CURRENT
        450 locations   subject/session/speed/trial — SS01/BL/SSV/1, … (+449)
```

**The `load:` line is the reason this exists.** Everything above it is context
the user would want anyway; that line is the only place in the GUI where "this
variant is not what a run will read" is visible. Two variants that look
equally alive is the shape of the 2026-09-22 bug.

## Rules this follows

**Nothing is computed in TSX.** One backend call returning
`Inspector.topologies`' result. The `ProvenancePanel` docstring states the
rule and the reason — "a rule written in TSX has no test, and two
implementations of 'which run produced this' would be two answers" (CLAUDE.md
NOTE 3) — and this is the same rule for the same reason.

**The service is a shell.** `services/provenance_service.py` is the pattern to
copy: its module docstring is explicit that nothing there computes provenance,
and it explains why the GUI calls the shared API rather than shelling out to
`scidb --json` (a subprocess would open a second connection to a single-writer
DuckDB file — the write-lock contention the MATLAB run-ownership work removed).

**Every screen has a terminal equivalent.** The panel prints the
`scidb variants <name>` line that reproduces what it shows, as
`ProvenancePanel` does with `scidb trace`. That is what keeps the two surfaces
checkable against each other.

---

## Stages

### Stage 1 — the service and the RPC

`services/variants_service.py::variable_topologies(db, name)` — a shell over
`db.inspect.topologies(name)`, returning `dataclasses.asdict` of each
`VariantSummary` plus the topology key, so the payload is the same shape
`scidb variants --json` prints.

Handler `variable_topologies` in `api/provenance.py` (it belongs beside
`variable_provenance` — same question area, same service shape), registered in
the handler table so both transports reach it.

`NotFoundError` → HTTP 400, as `variable_provenance` already maps it: asking
about a name that is not a variable is a user typo, not a fault.

**Test:** `scistack-gui/tests/` — the RPC returns the same grouping
`Inspector.topologies` does for the same database, and an unknown name is a
400 rather than a 500.

### Stage 2 — the panel

`components/Variants/TopologiesPanel.tsx`, mounted from `App.tsx` beside
`ProvenancePanel`.

- variable picker: reuse whatever `ProvenancePanel` uses to choose a variable;
  do **not** write a second one;
- one section per topology, `fn(inputs) → output` as the heading;
- one row per variant: constants, run options, short code hash, record count,
  first/last saved, and the `load:` verdict;
- locations collapsed to a count plus a sample, expandable — a loader with 450
  of them must not bury the row;
- the `scidb variants <name>` line at the bottom.

**The verdict needs to read at a glance.** `CURRENT` neutral,
`SUPERSEDED` muted/struck, `PARTIALLY SUPERSEDED (n of m)` warned. Partial is
not a rounding of the other two — run options are judged per function
globally, so a variant can lose some locations and keep others, and that case
is the trial-4 orphan that started this whole investigation.

**Test:** a render test over a fixture payload with one topology and two
variants, one superseded — asserting the verdict text appears and the
superseded row is visibly distinguished. Frontend tests live under
`frontend/src/**/__tests__` (mind the `tsconfig.test.json` include-list trap,
`project_show_sample_overlay`).

### Stage 3 — reachable from where the question is asked

The question arises **at a node on the canvas**, not from a menu. Add
"Variants…" to a variable node's context menu, opening the panel on that
variable.

Keep the menu entry separate from "Provenance…": they are different
directions (bottom-up vs top-down) and collapsing them into one entry would
hide the one that answers "what is in here".

**Test:** covered by the manual checklist below rather than by an automated
one — it is a menu wiring, and the existing DAG context-menu tests are the
pattern if one is cheap.

### Stage 4 — docs

- `docs/claude/variant-provenance-introspection.md`: a §"The bottom-up view",
  stating that `provenance` and `topologies` are the two directions and that
  neither answers the other's question.
- `docs/gui-manual-testing-todo.md`: an item with the real check — open it on
  `GAITRiteLoaded` in the Stroke-R01-Aim-2 database and confirm the two
  run-option variants appear with the older one marked superseded.

---

## Not in scope

- **Acting on what it shows.** No "delete this variant", no "re-run under
  these options". This is a read-only view, and the project's ethos is hide,
  never delete (`feedback_never_delete_mark_hidden`). Hiding a variant is a
  separate decision with its own consequences for node state.
- **Replacing the flat table.** `scidb variants --flat` stays; some questions
  are better as one row per variant.
- **The plot-side variant picker.** `VariantDagPopup` selects variants to
  plot. This explains what exists. Merging them would make a selector that
  also has to be a report.

## Sequencing

Independent of `plan-node-identity.md`, but it will get cheaper if that lands
first: `_node_wiring` carries `(node_id, wiring_id, run_id, first_seen,
last_seen)` (D-2026-09-22-2), which is most of this view's chronology recorded
directly rather than derived from the save log. Nothing here needs to wait for
it, and nothing here blocks it.

One open item it inherits: `pipeline_variants` groups by `(output_type, fn,
inputs, constants, glue, run options)` since Problem 11. If a variable turns
out to have more variants on screen than that key explains, the key is wrong
again — that is the thing to check first.
