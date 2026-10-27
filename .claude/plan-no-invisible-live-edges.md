# Plan: no invisible edge can bind (2026-10-27)

## Problem

Recurring bug class: an edge the user cannot see still feeds a run.
Latest case: function argument renamed `df_in` -> `data_path`; the stored edge
on `in__df_in` stayed in `edge_view.effective_edges().drawn`, so the run bound
`df_in=...` and failed with `unexpected keyword argument 'df_in'`. The canvas
never rendered it (no `df_in` port), so it could not be selected or deleted.
Earlier cases: hidden manual twins, retired `param__X` ports, renamed glue
ports (2026-10-21), hidden nodes' edges, cross-tab hides.

## Root cause

"Drawn" has two definitions:

1. Server: `edge_view.effective_edges` = stored, not hidden, not on a hidden
   node, plus special cases (`_on_retired_port`, `_on_missing_glue_port`).
2. Real: React Flow renders an edge only if BOTH end handles exist on the
   nodes it received (`data.input_ports` from `graph_builder.port_visibility`).
   Otherwise it drops the edge without telling anyone.

The run binds from (1); the user sees (2). Each special case covers only one
way they can differ. `tests/test_edge_parity.py` reads "canvas" as the
edges `/api/pipeline` returns, i.e. (1), so it can't catch this.

## Fix: one predicate, one owner

1. `edge_view` owns `is_drawable(edge, ports)`: both end nodes drawn AND the
   target handle is an `in__X` the node draws (same `port_visibility` /
   `registry.function_arguments` as the canvas; output side the same way).
   This replaces `_on_retired_port` and `_on_missing_glue_port`, and covers
   function nodes. If the signature can't be read, the edge is kept (as now).
2. `/api/pipeline` sends only drawable edges; the run, the disconnected check
   and the MATLAB command keep reading `effective_edges`, so all of them use
   the same rule.
3. Decided 2026-10-27: when the argument goes away, the edge is disconnected
   and the user redraws it. Ports are left alone.
   - On Refresh Code (`pipeline_service.refresh_module` ->
     `layout_service.disconnect_removed_arguments`), any stored edge on an
     `in__X` whose function no longer has `X` is HIDDEN through the same
     hide `delete_edge` uses. The row is not hard-deleted
     (never-delete ethos), and renaming back does NOT bring it back.
   - If the signature can't be read (syntax error, MATLAB not scanned), no
     edges are hidden.
   - No badge (user, 2026-10-27): the user changed the signature, so the
     disconnected port explains itself. A WARN and an audit event
     (`_canvas_change`) name each disconnected edge.
   - Not on the undo stack: Refresh Code is not an undoable handler.
     Redrawing the connection unhides it.
   - `edge_view.missing_port` (step 1, implemented) stays as the safety net for any path that
     skips the refresh, e.g. a file edited while the GUI was closed.

## Logging / tripwires

- WARN once per edge id when an edge is not drawable, naming the handle and
  the node's current ports.
- Graph build self-check: WARN if an edge is sent whose handle is not among the
  node's sent ports. Should never happen after the fix.
- Frontend: `report_canvas_refresh` reports how many received edges it could
  not resolve to a handle. A non-zero count is a WARN in scistack.log, so any
  future divergence shows up in the field.

## Tests

- Parity test "canvas" reader = edges whose handles exist in the received
  node's `input_ports`/outputs (what React Flow does), applied to EVERY
  scenario.
- New scenarios: renamed function argument; removed argument; renamed back
  (edge returns); stale edge next to a live edge on the new port; glue rename
  (existing); MATLAB function with unknown signature keeps edges.
- Invariant property test: for every scenario, edges binding == edges
  rendered.
