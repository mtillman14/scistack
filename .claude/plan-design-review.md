# Plan — as-built design review (docs/design-review/)

Started 2026-11-01. Goal: a standalone, diagram-led inventory of every Python
package so we can decide on a refactor of the layers of abstraction. Reviewed
bottom-up in dependency order; each package doc answers Job / Shape / Flows /
Contract (measured by grep) / Assessment (strengths, weaknesses with evidence,
refactor candidates). Mermaid diagrams, each with a caption.

## Batches

1. DONE — README (stack map, sizes, import matrix), 01 foundations
   (scistacklog, scicanonicalhash, scipathgen, scilineage), 02 scifor,
   03 sciduckdb.
2. DONE — 04a structure, 04b write path, 04c read path (scidb).
3. DONE — 05 scihist, scidb-net, scimatlab.
4. 06 csv-stats, scistackplot, scistackplotdb, scistackstats, scistackstatsdb.
5. 07 scistack-gui: Python backend (server.py RPC + api/ FastAPI + services +
   domain), extension, frontend. Probably 07a/07b/07c.
6. DONE — 08 umbrella, 09 synthesis (themes T1–T8, target layering, phased plan, decisions).

## Rules

- Numbers measured, never estimated; cite file:line where a claim is checkable.
- Check in with the user after each batch.
- docs/design-review/ is excluded from the mkdocs build (mkdocs.yml exclude_docs).
