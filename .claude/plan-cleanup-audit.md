# Plan: cleanup audit (identify before fixing)

Output: `docs/claude/cleanup-audit.md` — one ranked findings register.

1. **Measurements** — DONE 2026-09-23 (size, churn×fix hotspots, function
   shape, import graph + private imports, lazy imports, MATLAB parity, silent
   excepts, N+1 signals, dead code, forwarding depth, markers).
2. **Concept-ownership table** — concept → owner → consumers → rivals.
   Seed from memory notes (rid spine, ids.py, entity_editability, variant space,
   project root, schema level default) + hotspots.
3. **Critical-path flow diagrams** — save, load/load_all, for_each (Py + MATLAB),
   GUI graph build (`_build_graph`, 951 lines), plot load. Annotate queries,
   locks, caches, reconstructions. User runs timing on aim2 to confirm perf.
4. **Re-check 2026-09-20 review** — mark closed/open items.
5. **Rank findings** — severity × blast radius; each with suggested fix,
   logging, and regression test (NOTE 2).

Scanner scripts committed as `tools/audit/` (user approved 2026-09-23).
