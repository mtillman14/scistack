# Design review (pointer)

`docs/design-review/` is the as-built architecture review of SciStack, started
2026-11-01: what each package **is**, measured (module maps, call flows, who
imports what), plus its weaknesses and refactor candidates.

- **Reading order:** bottom-up, `README.md` then 01 (foundations) → 02 scifor →
  03 sciduckdb → 04a/04b/04c scidb → 05 satellites → 06 plot & stats → 07 GUI →
  08 umbrella → **09-synthesis.md** (themes T1–T8 and the target layering).
- **Refactor IDs** in commit messages and in `.claude/plan-tidy-*.md` (R1, D3,
  B6, W5, H1, N1, G7, U2, …) name rows in those docs; the prefix says which
  doc (table in `.claude/plan-tidy-00-index.md`).
- A fixed finding gets a `> **Done (date, commit):** …` line under its row.
