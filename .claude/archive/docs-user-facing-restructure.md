# Plan: Refocus docs on user-facing packages (scifor, scidb, scihist)

## Goal
Make the published docs emphasize the **three user-facing layers** — `scifor`,
`scidb`, `scihist` — and the features at each layer. Internal packages
(`scilineage`, `sciduckdb`, `scipathgen`, `scicanonicalhash`, `scimatlab`,
`scidb-net`) should be **barely referenced** in user-facing pages, and only via a
"(see [Internals: X] for details)" pointer. Their purpose/concepts get a
dedicated **Internals** section.

## Core reframing principle
`scihist` re-exports the entire downstream API (`lineage_fcn`, `LineageFcn`,
`configure_database`, `set_schema`, `Fixed`, `Col`, ...). Therefore:

- **Lineage/provenance is presented as a FEATURE of `scihist`**, not a separate
  layer. User-facing examples import `from scihist import ... , lineage_fcn`
  instead of `from scilineage import lineage_fcn`.
- **Content hashing / versioning** is presented as a capability of `scidb`/`scihist`;
  `scicanonicalhash` named only as "(see Internals)".
- **DuckDB storage** is a `scidb` capability; `sciduckdb` named only in Internals.
- **MATLAB usage** stays user-facing (users write `scifor.*` / `scidb.*` in MATLAB);
  the `scimatlab` *package* is the internal bridge, named only in Internals/MATLAB Setup.

The three user-facing layers, front and center everywhere:
```
scihist   reproducible pipelines: for_each + automatic lineage + recompute-only-what's-stale
  │
scidb     typed, versioned storage + DB-backed for_each
  │
scifor    batch iteration over conditions on plain tables (no database)
```

## Nav changes (mkdocs.yml)
- Add a new top-level section **Internals** (before Project):
  - `internals/index.md` — what the internal packages are and why you rarely touch them
  - `internals/scilineage.md` — provenance engine behind scihist's lineage
  - `internals/sciduckdb.md` — the DuckDB persistence layer behind scidb
  - `internals/scipathgen.md` — metadata→path generation
  - `internals/scicanonicalhash.md` — deterministic content/function hashing
  - `internals/scimatlab.md` — the MATLAB bridge
  - `internals/scidb-net.md` — optional networking/serialization
- Rename API nav entries to drop internal framing:
  - "Lineage (Thunk System)" → "Lineage & Provenance"

## Per-file edits (user-facing pages)
Sweep these to (a) route imports through `scihist`/`scidb`/`scifor`, (b) demote
internal-package mentions to "(see [Internals: X])", (c) keep all behavior/ground
-truth accurate:

- `index.md` — 4-row layer table → 3 rows; quick example imports `lineage_fcn`
  from `scihist`.
- `getting-started/choosing-a-layer.md` — remove the standalone **scilineage**
  layer section; fold its capability into the **scihist** section ("automatic
  lineage & staleness"). Quick-chooser table → 3 user-facing rows + a one-line
  note that provenance is internally `scilineage` (see Internals).
- `getting-started/installation.md` — lead with installing by the 3 layers
  (`pip install scistack` / top layer pulls the rest). Keep the source/editable
  dependency-order install but frame internal packages as "dependencies pulled in
  for the layer." Drop the scilineage-only minimal-set row (or move to Internals note).
- `concepts/architecture.md` — lead with the 3 user-facing layers; move the
  dependency graph + "what each internal layer owns" + "Peripheral packages" into
  a clearly-marked "Under the hood (internal packages)" subsection that links to
  Internals.
- `concepts/lineage.md` — reframe as scihist's provenance feature; imports via
  `scihist`; internal-package mentions → "(implemented in scilineage — see Internals)".
- `concepts/caching.md`, `concepts/hashing.md`, `concepts/glossary.md`,
  `concepts/variables.md`, `concepts/node-states.md` — same demotion treatment.
- `api/index.md` — packages table leads with scidb/scihist/scifor; scilineage row
  folded into scihist ("lineage API, re-exported by scihist"); scimatlab kept as
  MATLAB bridge.
- `api/lineage.md` — user-facing imports via `scihist`; underlying package noted once.
- `guide/lineage.md`, `guide/caching.md`, `guide/walkthrough.md`,
  `guide/database.md`, `quickstart.md` — import sweep + demotion.
- `project/contributing.md`, `project/faq.md` — contributing legitimately names
  internal packages (dev setup); keep but ensure faq routes users to user-facing API.

## New Internals pages (concise, concept-focused)
Each page: one-paragraph purpose, where it sits, what concepts it owns, and a
"you normally reach this through <user-facing layer>" note. Source material from
existing READMEs + `concepts/architecture.md`'s "What each layer owns".

## Verification
- `mkdocs build --strict` must pass (no broken nav/links) — user runs it.
- Preserve every `<!-- Ground truth ... -->` block; update them only where the
  reframing changes the asserted import path (e.g. lineage_fcn now via scihist).
- Do NOT change documented behavior/signatures — this is a framing/navigation
  restructure only.

## Decisions (approved 2026-06-03)
1. Section name: **"Internals"**.
2. **Demote `scilineage` fully** — remove as a user-facing layer everywhere;
   lineage is a `scihist` feature; `scilineage` appears only in Internals.
