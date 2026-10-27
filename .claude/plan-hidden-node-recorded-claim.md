# Plan: a hidden node must not out-claim a visible one

## Bug (scistack.log 2026-10-26 16:53:09)

`fn__pandas.read_csv__f4ff19e9f5544592` was hidden on 2026-10-22. A new
hand-placed `fn__pandas.read_csv__qsiu0z` was wired identically and run; the
run recorded a `_node_wiring` row for it. The next build found two RECORDED
claimants for wiring `19071db8003f5548` and:

- raised "Two nodes are wired the same" naming the hidden node;
- attributed runs to the hidden node (oldest), so the visible node stayed red.

## Root cause — two owners of "may a hidden node claim?"

- `api/pipeline.py` filtered hidden nodes out of the STATED claims
  (`claimable`, and the `step_claims` input).
- `domain/node_identity.resolve_identities` took RECORDED claimants straight
  from `associations`, unfiltered.

## Fix

1. `resolve_identities(..., hidden_node_ids=...)` is the ONE owner.
   - Stated claimants: hidden ones never claim (unchanged rule, moved here).
   - Recorded claimants: a visible claimant beats a hidden one. If every
     recorded claimant is hidden, the hidden node keeps the wiring — hiding
     never changes any node's identity (project_hidden_pathinput_identity).
   - Ambiguity is computed over the claimants that remain, so a hidden node
     is never named in the popup.
   - INFO log when a hidden claimant is passed over.
2. `api/pipeline.py`: drop the two `hidden_everywhere` pre-filters and pass
   `hidden_node_ids=hidden_everywhere` instead.
3. Tests: `tests/test_node_identity.py::TestHiddenClaimants` (pure domain) +
   an AST guard that `api/pipeline.py` passes `hidden_node_ids`.
4. Docs: `docs/claude/node-identity.md` §12.
