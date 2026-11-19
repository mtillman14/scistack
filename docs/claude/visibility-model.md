# Visibility: what is hidden, on which canvas

*Written 2026-11-19. Owner: `scistack_gui/domain/visibility.py` (`Visibility`).
Plan: `.claude/plan-visibility-model.md`. Cases: `visibility-cases.json`.*

"Hidden" means removed from view but kept for audit and undo (the project
never deletes). This page covers which hides apply to what, across the main
canvas, hypothesis canvases and submodules.

## The rule

**Everything you can see is an occurrence on exactly one canvas, and a hide
belongs to that occurrence, on that canvas.** A reader never chooses whose
hides apply. It asks about a node or an edge, and the occurrence's own canvas
decides, so one edge gets the same answer whichever canvas asks.

| Thing | Its canvas | Hidden when |
|---|---|---|
| Function, glue, input/output port node | its stored row (one occurrence per id) | its bare id is hidden on that canvas |
| Shared entity (variable, Parameter, PathInput, Merge) | `::canvas` suffix; a bare id takes the canvas of the other end of the edge | its bare id is hidden on that canvas |
| Placement (`use_…`) | its parent canvas | — |
| Stored edge | the canvas of its ends | its own id or connection id is hidden there, or an end is hidden there |
| Connection traced through ports | none: it is made of segments | a segment is not live; or the whole connection is hidden on either end's canvas (adoption retires a rewired node's old connections on its canvas) |
| Parameter value (checkbox) | the canvas of the Parameter occurrence that feeds the argument, traced through ports | that canvas hid the value |
| Hidden combo | global by design: a `call_id` is one computation everywhere | always |

## Across the hierarchy

- **Hypothesis canvases share nothing.** Duplicating one copies its hides at
  that moment (`scope_service._clone_nodes`); after that they are independent.
- **A submodule's inside is one canvas every placement shares**, so a hide
  inside it applies to every placement. To make one placement differ,
  Duplicate the submodule (user decision D2, 2026-11-19). There are no
  per-placement overrides.
- **The lines into and out of a placement's ports live on the parent canvas.**
  Each placement's parent controls its own.
- **Removing a port** (hiding its input/output node inside the submodule)
  removes it for every placement. The outer lines into it bind nothing and show
  as lines to a port that doesn't exist; nothing hides them silently (D3).

## Who asks what

| Question | Ask |
|---|---|
| Which stored edges are live, for any reader | `edge_view.effective_edges(db, scope).drawn` (judged per edge, independent of `scope`) |
| What one canvas draws or runs (its own nodes, history edges) | `vis.hidden_node_ids(canvas)`, `vis.hidden_edge_ids(canvas)`, `vis.node_hidden_on(canvas, id)` |
| May a stored row claim a wiring | `vis.hidden_rows()` (each row on its own canvas) |
| Unticked values for a run of one node | `execution_service.hidden_values_for_node(db, node_id)` |
| A run that names no node (name-scoped fallback) | `vis.hidden_node_ids_any_canvas()`, `vis.hidden_values_any_canvas()`: no canvas, so every canvas's |

`pipeline_store.get_hidden_node_ids` / `get_hidden_edge_ids` read one canvas's
raw statements and refuse `None`. Only the stores and `visibility.py` call raw
hide reads; `tests/test_visibility_owner_guard.py` enforces that, with one
listed display exception.

Writers state hides on the occurrence's canvas: `layout_service._hide_edge_in_its_scope`
and `put_edge`'s unhide both use `Visibility.canvases_of_edge`, so a redraw
always finds the hide its removal wrote.

## Why (Chesterton's fence)

Before 2026-11-19 about fifteen readers each picked whose hides applied:

- the asking canvas, with a `strip_placement` fallback (`effective_edges`): a
  hidden copy of `xsensPath_6MWT` inside a submodule cut main's
  `pathInput__xsensPath_6MWT::main -> use_…` line, so running the submodule
  directly failed with "no runnable MATLAB step" (scistack-debug.log 2026-11-19
  12:38:43);
- every canvas unioned (`get_hidden_node_ids(db, None)`: identity claims, port
  tracing), so a node removed in hypothesis B lost its claim in A;
- the function node's canvas, for Parameter values, so unticking a value on
  main did nothing for a step inside a submodule.

Patching each reader never converged; one owner with no asking canvas removes
the choice.

## When nothing can run

`execution_service._wiring_blocker` is the one place that says why a node's
wiring runs nothing (a port with nothing connected, checked first; a removed
line; no output line, noting a submodule output port that leads nowhere).
`no_target_reason` exposes it; the MATLAB pipeline script's `% SKIPPED`
comments and its `NoRunnableStepError` (an `ActionableError`, so never "likely
a SciStack bug") list each step's reason, and the Python run's "can't run"
message uses it too.
