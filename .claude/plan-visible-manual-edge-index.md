# Plan: the manual-edge index only sees edges the canvas draws

## Bug (scidb.log 2026-10-01 11:29–11:32)
`calculateSymmetryOneVector` would not run. `v` was bound to
`['GAITRiteLoaded', 'GaitRiteLoaded_UA']`, which generate_matlab_command refuses as EachOf.
The user had DISCONNECTED `GAITRiteLoaded` (hid the history edge
`e__GAITRiteLoaded__calculateSymmetryOneVector__<tok>`) and drew `GaitRiteLoaded_UA`.

An older hand-drawn edge `var__GAITRiteLoaded -> fn__calculateSymmetryOneVector__<tok>` (in__v)
was still stored in the layout. build_edges never draws it, because it is superseded by its
history twin, hidden or not. But `manual_edge_handle_index` indexed EVERY stored manual edge, so
`manual_input_overrides` and the other 6 callers counted an edge nobody can see.
This is two owners for "is this manual edge visible?".

## Fix
1. `graph_builder.history_twin_edge_id(edge, manual_nodes)` is the one spelling of the
   DB-derived edge id that a manual edge duplicates (var / Parameter / PathInput sources,
   using the same id formats as build_edges).
2. `graph_builder.manual_edge_is_hidden(edge, hidden_edge_ids, manual_nodes)` is the one rule:
   hidden if its own id is hidden or its history twin is hidden.
3. `manual_edge_handle_index(manual_edges, *, hidden_edge_ids, manual_nodes=None)`:
   `hidden_edge_ids` is REQUIRED, so every caller decides, and the index skips hidden edges.
   It logs INFO the first time each edge is dropped (debug after that) with source -> target.handle.
4. build_edges' manual loop uses `manual_edge_is_hidden` for its own-id check plus twin check
   (its broader "visible twin" dedup stays, since a visible twin is the same source anyway).
5. All 7 callers pass hidden_edge_ids (and manual_nodes where they have it).

## Side effect (intended)
A history node whose edge was hidden and that has only a stale twin is now correctly
"disconnected"; before, the twin silently "covered" the handle.

## Tests
`scistack-gui/tests/test_hidden_manual_twin.py`:
- index drops a manual edge whose history twin is hidden; keeps it otherwise
- manual_input_overrides with a hidden history edge + stale twin + new edge -> bare new type
- hidden_wirings: a stale twin alone does not cover a hidden handle
- PathInput / Parameter twin ids match build_edges' ids
