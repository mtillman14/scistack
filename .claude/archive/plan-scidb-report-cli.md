# Plan: Stage 5 — `scidb report` (endpoint report surface)

The payoff stage of `docs/claude/endpoints-viz-and-stats-design.md`: collect
every **finalized endpoint record** (plot_ figures, stat_ results + their PDF
reports) from a database into a single self-contained HTML page + a stats
table, with per-artifact provenance. Extends the observability CLI
(`scidb/src/scidb/inspect/`, `.claude/plan-db-observability-cli.md`) —
read-only, Inspector-side.

## User surface

```
scidb report db experiment.duckdb                      # everything endpoint-produced
scidb report db experiment.duckdb --fn plot_gait       # one endpoint fn
scidb report db experiment.duckdb --var StepLenTTest   # one output type
scidb report db experiment.duckdb -o reports/paper1    # output dir (default ./scidb-report-<dbname>-<date>)
scidb report db experiment.duckdb --json               # machine-readable manifest only
```

Python facade (same pattern as everything else on the Inspector):

```python
report = db.inspect.report(fn=None, variable=None)   # -> ReportData (dataclass)
db.inspect.write_report("reports/paper1", fn=None, variable=None)  # -> Path to index.html
```

## What counts as an endpoint record (the discovery rule)

No new bookkeeping was added for "this is an endpoint" — derive it from the
graph, which is already sufficient:

- A record whose **producing invocation's `function_name` starts with
  `plot_`** → figure entry; its stored VARCHAR value is the artifact path.
- ...starts with **`stat_`** → stats entry; its stored VARCHAR is the result
  JSON; `report_path` inside it (if present) is the report artifact.
- Latest-per-variant only by default (same collapse `records()` uses);
  `--all-versions` includes superseded ones. Excluded records skipped.
- Drafts by design never appear (no records) — the report is the
  *finalized* surface. Say so in the header.

Implementation: one SQL pass over `_invocation` ⨝ `_invocation_output` ⨝
`_record` (+ `_record_save` for timestamps, `_schema` LEFT JOIN for combos —
root-level grand-aggregation outputs have NULL schema_id; same trap as the
stage-2 skip-gate fix) filtered by `function_name LIKE 'plot\_%'/'stat\_%'`,
then batch provenance via the existing `*_batch` helpers (memory:
per-record provenance calls are the N+1 trap on hot paths).

## `ReportData` (the manifest; `--json` emits it directly)

```
ReportData:
  db_name, generated_at, filters
  figures:  [FigureEntry: record_id, fn, schema{}, branch_params{},
             artifact_path, artifact_exists, stamp_ok, timestamp]
  stats:    [StatEntry: record_id, fn, schema{}, branch_params{},
             result{parsed json}, report_path?, report_exists, timestamp]
  warnings: [str]   # missing files, stamp mismatches, unparseable JSON
```

`artifact_exists`: paths stored are workstation-local; a moved DB yields
missing files → warn per entry, render the entry anyway with its metadata
("figure file not found at <path>"). `stamp_ok`: read back via
`read_artifact_stamp` and compare `record_id` — a mismatch means the file
was overwritten by a different run since (stale artifact!) and gets a loud
per-entry warning; sidecar-stamped and unstamped-but-existing files are
noted, not warned.

## HTML output (`write_report`)

One **self-contained** `index.html` (inline CSS, no JS dependencies, no
external requests — it must open from a USB stick in ten years):

- Header: db name, generation time, filters, counts, the draft note.
- **Figures section**, grouped by producing fn, then ordered by schema combo:
  each entry embeds the image (PNG/SVG base64-inlined via `--embed`, default
  ON ≤ a size cap, else relative `<img src>` to copied files; PDFs get a
  link, not an embed) with a caption: schema combo + branch_params (the
  variant identity — two `low_hz` variants render side by side, labeled) +
  record_id + fn.
- **Stats section**, one HTML table per (fn, result-key-set) family: rows =
  schema combo + branch_params + the flattened top-level scalar keys of the
  result JSON (nested dicts like `summary_statistics` collapse to a
  details/expandable `<pre>` block). Cross-family keys are NOT unified —
  per-test-type tables, consistent with D5's no-universal-schema decision.
- **Artifact copying**: `write_report` copies existing artifacts into
  `<outdir>/artifacts/` (name-collision-proofed with a record_id-prefix)
  so the report folder is portable; entries link to the copies. `--no-copy`
  links to original absolute paths instead.
- Also writes `manifest.json` (the ReportData) and `stats.csv` (the flat
  stats tables concatenated with a `test_family` column — the
  paste-into-a-paper artifact) alongside `index.html`.

Rendering is string-template Python in a new `inspect/report.py` — no
Jinja/markdown deps (CLI stays stdlib+pandas, matching the existing
`render.py` philosophy).

## Files

| File | Change |
|---|---|
| `scidb/src/scidb/inspect/report.py` | new: endpoint-record discovery SQL, ReportData dataclasses, HTML/CSV/manifest writers |
| `scidb/src/scidb/inspect/api.py` | `Inspector.report()` + `Inspector.write_report()` delegating to report.py |
| `scidb/src/scidb/inspect/cli.py` | `report` subcommand (parser + cmd fn; `--fn`, `--var`, `-o`, `--all-versions`, `--no-copy`, `--embed/--no-embed`, `--json`) |
| `scidb/tests/test_report.py` | new (below) |
| `docs/claude/observability-api-design.md` | report command section |
| `docs/claude/endpoints-viz-and-stats-design.md` | stage 5 → done |
| `docs/claude/plotting-leaf-nodes.md` | pointer to `scidb report` |

## Tests (`scidb/tests/test_report.py`)

Seed once per class: a real pipeline — two variants of a processed signal →
finalized `plot_` (PNG per subject×variant via `{low_hz}` placeholder) +
finalized `stat_` (per-variant JSON records, one with a PDF report) + one
plain processing output (must NOT appear) + one draft endpoint run (must NOT
appear).

1. **Discovery**: `report()` finds exactly the endpoint records — figures
   list matches the plot records (per variant), stats list the stat records;
   the processing output and drafts are absent; excluded records absent.
2. **Filters**: `fn="plot_x"` and `variable=StatOut` narrow correctly;
   `--all-versions` includes a superseded record after a re-run.
3. **Stamp verification**: normal case `stamp_ok=True`; overwrite one PNG
   with a figure from a different record → `stamp_ok=False` + warning.
4. **Missing artifact**: delete one PNG → `artifact_exists=False` + warning;
   `write_report` still succeeds and the entry renders with its metadata.
5. **write_report output**: `index.html` exists, self-contained (no
   `http(s)://` references), contains each record_id, the variant labels,
   and embedded/linked images; `artifacts/` holds copies; `manifest.json`
   round-trips to the ReportData; `stats.csv` parses with pandas and has
   one row per stat record.
6. **Root-level records**: a grand-aggregation stat (NULL schema_id) appears
   (the LEFT JOIN regression).
7. **CLI**: `scidb report db <path> --json` (subprocess or direct cmd-fn
   call, matching existing CLI test style) emits valid JSON;
   `-o` writes the folder; bad `--fn` → clean error, not a traceback.
8. **Batch provenance**: discovery over ~50 records issues O(1) provenance
   queries (assert via the existing timed/query-count hook if present,
   else skip this guard).

Run: `pytest scidb/tests/test_report.py -q`, then the full sweep (user runs).

## Out of scope (v1)

- PDF/LaTeX export, figure thumbnails/resizing, HTML theming options.
- Cross-database reports (one DB per report).
- Interactive filtering in the page (static HTML by design).
- MATLAB surface: none needed — reports are generated from the DB by the
  CLI; MATLAB users run the same `scidb report db ...` command.
- Report-of-drafts mode (drafts have no records; if wanted later, it would
  scan artifact stamps on disk instead of the DB — different tool).

## Risks / watch items

1. **Large embedded figures** → size cap per image (default ~2 MB) with
   automatic fallback to file links + a warning naming the oversized ones.
2. **Stored stat JSON that fails to parse** (hand-saved records) → entry
   renders the raw string in a `<pre>`, warning added, never a crash.
3. **Windows/POSIX path mixing** in stored artifact paths → treat stored
   paths as opaque strings; existence check via `Path(p)`; copies avoid the
   issue for portable reports.
4. `--var` on a type produced by both an endpoint and (historically) a
   processing fn: filter is on the PRODUCING fn prefix, so only
   endpoint-produced records of that type appear — document in help text.
