# Plan: Stage 3 — Artifact provenance stamping (D4), figures AND stats reports

Implements **D4** of `docs/claude/endpoints-viz-and-stats-design.md`: embed a
provenance blob in every endpoint artifact so a file found in a paper draft
years later traces back to its exact DB record. Per user direction, this
covers **both** endpoint kinds: `plot_` figures (PNG/SVG/PDF) **and** `stat_`
PDF reports (the csv-stats `report_path` artifact) get the same blob.

## The blob

One JSON object, format-independent:

```json
{
  "scidb_stamp": 1,
  "record_id": "41ca2eb1d8f4...",        // the artifact's OWN record (primary key)
  "function": "plot_timeseries",
  "inputs": {"signal": ["9e4bdc3d8cb5...", "..."]},   // consumed record_ids per param
  "schema": {"subject": "S01", "trial": "2"},
  "database": "gait_study.duckdb",
  "timestamp": "2026-07-06T14:02:11"
}
```

- `record_id` is the primary key (D4): via the bipartite graph it reaches the
  invocation, function hash, and branch_params — embedding derived hashes
  would be redundant indirection. The rest is human-readable redundancy that
  survives DB loss/detachment.
- For `plot_`, `record_id` is the path record; for `stat_`, it is the stat
  JSON record whose `report_path` names the stamped PDF.
- **Draft artifacts get the FULL blob minus `record_id`** (decided
  2026-07-06): `draft: true` replaces `record_id`, and `function`, `inputs`
  (consumed record_ids per param), `schema`, `database`, `timestamp` are all
  present — identical to what the finalized record would stamp. A draft
  figure on disk is thus fully traceable to the exact input records that
  produced it, even though no output record exists. Only `plot_` drafts
  produce a file; `stat_` drafts resolve PathOutput to None, so there is
  nothing to stamp.

## Where stamping happens (NOTE 3: the scidb save path, not the renderer)

Stamping the **file post-hoc** (not via `savefig(metadata=)`) is the one
mechanism that works for Python-rendered and (stage 4) MATLAB-rendered
artifacts alike, and it can include the `record_id`, which only exists after
the save.

- **One shared blob builder, called from two places** (both in the save
  phase, so drafts and records stamp identically):
  - **Record mode:** inside `_save_results` per row — the one place that
    simultaneously knows the saved `record_id`, the row's schema combo, and
    the consumed rids (`__rid_*` columns in full iteration; the
    `combo_to_rids` group lookup in aggregation).
  - **Draft mode:** the save is suppressed, but `_for_each_save_resolved`
    still runs with the result table and state — a draft-stamping pass there
    builds the SAME blob per row (rids from the same `__rid_*`/
    `combo_to_rids` sources, schema from the row's schema columns) with
    `draft: true` in place of `record_id`. This replaces the earlier idea of
    stamping in `_make_plot_wrapper`, which only knows the path and fn name.
  - Artifact path per row: `plot_` → the output value (the path string);
    `stat_` → parse `report_path` from the result JSON (absent → nothing to
    stamp).
  - `_save_results` / `_for_each_save_resolved` gain an
    `endpoint_kind: str | None` param (`"plot"`/`"stat"`/None) threaded from
    Step 1.55/1.56.
- Stamping failures **warn and continue** (a figure that renders but can't be
  stamped must not fail the pipeline); every stamp/failure is logged (NOTE 2).

## New module: `scidb/src/scidb/artifact_stamp.py`

`stamp_artifact(path, blob) -> bool`, `read_artifact_stamp(path) -> dict | None`
(reader exported from `__init__.py` — users and the future `scidb report`/
`scidb trace` CLI both need it). All dependency-free (stdlib only):

- **PNG** — insert a `tEXt` chunk (keyword `scidb:provenance`) after `IHDR`;
  CRC via `zlib.crc32`. JSON with `ensure_ascii=True` keeps it latin-1-safe.
  ~40 lines. Reader: walk chunks.
- **SVG** — insert `<metadata id="scidb-provenance">…</metadata>` right after
  the opening `<svg …>` tag (XML-escape the JSON). Reader: substring scan.
- **PDF** (figures saved as .pdf AND csv-stats reports) — **incremental
  update**: append a new Info object carrying
  `/scidb_provenance <hex-encoded JSON>` (hex string avoids all escaping), a
  new xref subsection, and a trailer with `/Prev` pointing at the old
  `startxref`. Original bytes stay untouched, which is what makes this safe.
  Works for classic-xref producers — reportlab (csv-stats) and matplotlib
  both qualify. If trailer parsing fails (xref-stream PDFs from other tools):
  **sidecar fallback**. Reader: backwards byte-scan for the marker — works
  regardless of xref flavor, no parsing.
- **Anything else** (`.jpg`, unknown, or a failed embed) — sidecar
  `<artifact>.provenance.json` next to the file, plus a log line saying why.

## Files

| File | Change |
|---|---|
| `scidb/src/scidb/artifact_stamp.py` | new: stamp/read + per-format helpers + sidecar fallback |
| `scidb/src/scidb/__init__.py` | export `read_artifact_stamp` (and `stamp_artifact`) |
| `scidb/src/scidb/foreach.py` | `endpoint_kind` threaded to `_for_each_save_resolved`/`_save_results`; shared blob builder; record-mode stamp in `_save_results`, draft-mode stamp in `_for_each_save_resolved` |
| `scidb/tests/test_artifact_stamp.py` | new (below) |
| `scidb/tests/test_plotting.py` / `test_stat_leaves.py` | one assertion each: recorded artifact carries its record_id |
| `docs/claude/plotting-leaf-nodes.md` | stamping section |
| `docs/claude/endpoints-viz-and-stats-design.md` | D4 → implemented |

## Tests (`test_artifact_stamp.py`)

1. **Unit round-trips**, no DB: stamp+read PNG, SVG, PDF files produced by
   matplotlib (`savefig` to each format); file still loads afterward
   (`plt.imread` for PNG; PDF: `%PDF` header intact, original bytes are a
   prefix of the stamped bytes, reader finds the blob).
2. **plot_ record mode:** run a `finalized=True` plot pipeline → PNG stamp's
   `record_id` equals `PlotFigure.load(...).record_id`; `inputs` rids match
   the consumed `RawSignal` records; `schema` matches the combo.
3. **stat_ record mode:** `stat_` with a `.pdf` PathOutput (fn writes a
   minimal PDF via matplotlib — no csv-stats dependency) → PDF stamp's
   `record_id` equals the stat record; `report_path` in the record and the
   stamped file agree. A csv-stats-guarded variant stamps a real reportlab
   report (skips on broken deps).
4. **plot_ draft:** rendered PNG carries the FULL blob — `draft: true`, no
   `record_id`, but `function`, `inputs` (matching the consumed RawSignal
   rids), `schema`, `database`, `timestamp` all present and equal to what a
   finalized run would stamp; DB untouched.
5. **Aggregation split:** a `stat_` over two variant groups with a
   per-group `{low_hz}`-templated PathOutput → each PDF stamped with ITS
   group's record_id and input rids (D1 composition).
6. **Fallbacks:** unsupported extension → sidecar written + logged; stamping
   a nonexistent/garbage file → warns, pipeline completes; a PDF whose
   trailer can't be parsed → sidecar.
7. **Re-stamp on re-render:** change the plot fn, re-run finalized → new
   record_id in the stamp (old record preserved in DB per keep-everything).

Run: `pytest scidb/tests/test_artifact_stamp.py scidb/tests/test_plotting.py scidb/tests/test_stat_leaves.py -q`, then full sweep (user runs).

## Risks / watch items

1. **PDF incremental update is the riskiest piece.** Mitigations: original
   bytes untouched, reportlab/matplotlib both write classic xref, parse
   failure → sidecar, and the reader never depends on the update being
   well-formed (byte-scan). If a real viewer chokes on stamped PDFs, flip
   PDF to sidecar-only (one-line change in the format dispatch).
2. **`stat_` fns that pass `filename` to csv-stats get their PDF written by
   csv-stats, then stamped by scidb afterward** — ordering holds because
   stamping runs in the save phase, strictly after the scifor loop.
3. Multiple outputs per endpoint row: v1 stamps the artifact of each output
   row it can resolve a path for; rows without a resolvable path are logged
   and skipped.
4. `skip_computed` skips leave existing stamps in place — correct, since the
   skipped record IS the stamped record.
5. MATLAB-rendered figures (stage 4) inherit stamping for free — it lives in
   the Python save path their records already flow through.
