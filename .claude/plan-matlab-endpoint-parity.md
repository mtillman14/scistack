# Plan: Stage 4 — MATLAB endpoint parity (D7)

Brings the endpoint machinery (stages 1–3.5) to the MATLAB path: `plot_` /
`stat_` leaves, `finalized` draft/record, artifact stamping, PathOutput
(which does not exist in MATLAB at all yet) with variant placeholders +
collision guard, `AcrossVariants`, and the `share_limits` port.

**Governing principle (D7 + PathInput precedent):** correctness lives in
Python; MATLAB touches only the local environment (rendering figures,
running the loop). Figure handles never cross the bridge — only path
strings. Where MATLAB needs a Python decision, Python pre-computes it in
`for_each_prepare` and MATLAB *applies* it (the `PathInput.apply_discovery`
pattern) — this also sidesteps the bridge's key-sanitization problem
(MATLAB struct fields cannot contain dots like `bandpass.low_hz`).

## What MATLAB inherits for FREE (verify, don't build)

- **Variant auto-split (D1)**: `for_each_prepare` runs `_for_each_prepare`,
  so `__vsig_*` expansion, `combo_to_rids`, and the group-bound skip gate
  (via the hook's `_agg_binding_ref`, already filled inside prepare) all
  happen before combos cross the bridge.
- **Record-mode stamping + draft stamping (D4)**: both live in
  `_for_each_save_resolved` / `_save_results` — the bridge's `for_each_save`
  already calls the former; it just doesn't pass `endpoint_kind` yet.
- **Placeholder injection + collision guard (stage 3.5)**: run inside
  prepare — active as soon as reconstructed inputs contain real
  `scifor.PathOutput` objects (phase A).

## Phase A — PathOutput on the MATLAB path (prerequisite for everything)

There is no `+scifor/PathOutput.m` and no `'path_output'` bridge kind today.

1. **`+scifor/PathOutput.m`** — template holder mirroring `PathInput.m`'s
   builder style: `scifor.PathOutput("plots/{subject}_{low_hz}.png")`,
   exposed as `scidb.PathOutput` alias if the existing packages do that for
   other wrappers (match `Fixed`/`Merge` precedent).
2. **Describe kind**: `+scidb/for_each.m::describe_input_for_python` emits
   `py.dict('kind','path_output','template',<char>)`;
   `bridge._reconstruct_input_for_keys` gains the `'path_output'` clause →
   `scifor.PathOutput(template)`. With that, Python prepare's endpoint
   detection, placeholder scan, injection, and collision guard all run
   unchanged.
3. **Pre-resolved paths cross the bridge** (NOT the template): prepare
   resolves each PathOutput per expanded combo Python-side and returns
   `resolved_path_outputs: {param: [path_per_combo]}` aligned with
   `full_combos` (add to the prepare return dict + cache). `{ColName}` is
   left UNresolved in the strings (literal-replace composes); MATLAB's
   for_columns loop strrep's `{ColName}` per column. This avoids porting
   resolution, and dotted/injected placeholder keys never cross as struct
   fields.
4. **`+scifor/for_each.m`**: when `resolved_path_outputs` is present (passed
   through from `+scidb/for_each.m`), substitute the per-combo path as the
   input value each iteration (plus the `{ColName}` strrep inside
   for_columns). Pure-MATLAB scifor usage (no bridge) gets minimal native
   resolution: strrep of `{key}` from the combo struct — schema keys only,
   documented as the scifor-layer subset.

## Phase B — endpoint policy shared with the bridge

Step 1.55/1.56 endpoint logic lives in `scidb.for_each`'s body, which the
MATLAB path bypasses (it calls `_for_each_prepare` directly).

1. **Refactor** the policy block into `_endpoint_policy(fn_name, inputs,
   finalized)` in `scidb/foreach.py` returning `(endpoint_kind, path_param,
   save_suppressed)` — used by `scidb.for_each` (which additionally wraps
   the Python fn) and by `bridge.for_each_prepare` (which does NOT wrap;
   MATLAB wraps its own fn). One source of truth for: prefix detection,
   plot-requires-PathOutput error, stat `as_table` default, draft
   save-suppression, non-endpoint `finalized` warning, draft `[draft]`
   notice.
2. **Bridge threading**: `for_each_prepare` gains `finalized: bool = False`;
   caches `endpoint_kind` + `save_suppressed`; returns `path_param` and
   per-combo `stat_draft` info to MATLAB. `for_each_save` passes
   `endpoint_kind=cached` to `_for_each_save_resolved` and forces
   `save=False` when draft — policy stays Python-side; MATLAB only forwards
   its `finalized` opt.
3. **MATLAB opt**: `+scidb/for_each.m` accepts `finalized` (default false)
   and passes it to prepare. `fn_name` already exists via `func2str(fn)`
   (line ~56). **Constraint documented + validated**: endpoint detection
   requires a NAMED function handle — `func2str(@(x)...)` starts with `@`,
   never matches the prefix; if `finalized` is passed with an anonymous
   handle, warn.

## Phase C — MATLAB `plot_` wrapper (in `+scidb/for_each.m`)

When `endpoint_kind == "plot"`, wrap the user fn before handing it to the
MATLAB scifor loop:

- Returned **graphics handle** (figure/axes/tiledlayout — detect via
  `isgraphics` / `isa(r,'matlab.ui.Figure')`; use `ancestor(r,'figure')`
  for axes) → export to the combo's resolved path, `close(fig)` (memory
  bound across combos, mirroring `plt.close`), return the path **char**.
- Export by extension: `exportgraphics` for `.png/.pdf/.jpg`;
  `print(fig,'-dsvg',path)` for `.svg` (exportgraphics can't write SVG).
- Returned char/string → passthrough (fn saved it itself). Anything else →
  error mirroring Python's message.
- Draft mode: identical rendering (the flag only changes the Python-side
  save/stamp behavior — same asymmetry as Python: plot drafts still render).
- Stamping needs nothing: the path string lands in the result table, and
  `_save_results` / `_stamp_draft_endpoint_artifacts` stamp the file
  Python-side (PNG from `exportgraphics` is a normal PNG; `tEXt` insertion
  format-level, renderer-independent).

## Phase D — MATLAB `stat_` wrapper + Python normalization helper

MATLAB runs the fn, but JSON canonicalization must be byte-identical to the
Python path (skip_computed and reproducibility depend on it). MATLAB's
`jsonencode` differs from Python's `json.dumps` (key order, float format,
NaN→null), so **normalization stays in Python**:

1. **Refactor** `_make_stat_wrapper`'s body into
   `normalize_stat_payload(result_dict_or_json_str, report_path, finalized)
   -> str` (strip `date`, `_jsonify_stat`, `report_path` embed, canonical
   dumps) — the Python wrapper and the bridge both call it.
2. **Bridge entry** `normalize_stat_result(json_str, report_path, finalized)
   -> str` for MATLAB.
3. **MATLAB wrapper** (when `endpoint_kind == "stat"`): fn must return a
   struct (or containers.Map, or a JSON char) → `jsonencode` → bridge
   `normalize_stat_result` with the combo's resolved report path (or empty)
   → returns the canonical payload string as the combo result. Draft:
   resolved path replaced by empty (`[]` → Python None semantics: csv-stats
   equivalent fns in MATLAB receive empty and should skip their report) and
   the pretty JSON `disp`'d with the `[stat draft]` banner.
4. `as_table` defaulting for stat_ comes from `_endpoint_policy` (phase B),
   so MATLAB stat fns receive the long-format table with schema columns —
   same contract as Python.

## Phase E — `AcrossVariants` MATLAB builder

Mirror `Variant.m` exactly (it's the template: constructor validation +
describe kind + bridge reconstruction, no `build_scifor_input_from_desc`
case since it's consumed at load/prepare time):

- `+scidb/AcrossVariants.m`: holds the inner spec; rejects Merge/EachOf/
  ColumnSelection with the same messages as Python.
- `describe_input_for_python` emits `'across_variants'` kind;
  `_reconstruct_input_for_keys` rebuilds `scidb.AcrossVariants(inner)`.
- Everything else (pooling, bp-column attach, full-iteration no-op warning)
  happens in Python prepare — already built in stage 1.

## Phase F — `share_limits` MATLAB port (scifor layer)

Port `scifor/foreach.py::_compute_shared_limits` to `+scifor/for_each.m`
(the for_columns port is the precedent):

- Accept `share_limits` opt (struct: input name → cellstr of group keys);
  group the input's table by those keys; numeric min/max across all data
  columns (flattening array cells); per-combo inject `{input}_limits` as a
  `[min max]` double.
- **Injection gate differs from Python** (MATLAB can't inspect kwarg names):
  inject only when the user's fn signature has capacity — `nargin(fn)`
  count > the input count, or varargin (`nargin < 0`). Document: MATLAB
  plot fns wanting limits declare the trailing `{input}_limits` argument.
- `+scidb/for_each.m` forwards the opt; grouping keys are schema keys
  (present in the loaded tables), so no bridge involvement.

## Files

| File | Change |
|---|---|
| `+scifor/PathOutput.m` | new: template holder + minimal native `{key}` resolve |
| `+scifor/for_each.m` | consume `resolved_path_outputs` (+ `{ColName}` strrep); `share_limits` port |
| `+scidb/for_each.m` | `finalized` opt; endpoint detection via `func2str`; plot_/stat_ wrappers; `path_output` + `across_variants` describe kinds; forward share_limits |
| `+scidb/AcrossVariants.m` | new |
| `scidb/src/scidb/foreach.py` | `_endpoint_policy` refactor (no behavior change on the Python path); `normalize_stat_payload` extraction |
| `scimatlab/src/scimatlab/bridge.py` | `finalized` param; endpoint policy via `_endpoint_policy`; `resolved_path_outputs` in prepare return + cache; `endpoint_kind`/draft in `for_each_save`; `normalize_stat_result` entry; `path_output`/`across_variants` reconstruction |
| `scimatlab/tests/matlab/scidb/TestForEachPlotEndpoint.m` | new (below) |
| `scimatlab/tests/matlab/scidb/TestForEachStatEndpoint.m` | new |
| `scimatlab/tests/matlab/scidb/TestAcrossVariants.m` | new |
| `scimatlab/tests/matlab/scifor/TestShareLimits.m` | new |
| `scimatlab/tests/test_bridge_endpoints.py` | new: Python-side bridge tests (no MATLAB needed) |
| docs | plotting-leaf-nodes.md MATLAB section; design doc D7 → implemented; archive/matlab-for-each-current-state.md update |

## Tests

**Python-side (I can write + user runs with pytest; no MATLAB required)**
`test_bridge_endpoints.py`: `_endpoint_policy` parity (same decisions as
scidb.for_each for plot/stat/none × finalized); `normalize_stat_result`
byte-equality with the Python wrapper's output for the same dict;
`for_each_prepare(finalized=False)` on a stat_ fn_name suppresses save in
`for_each_save` (no records) while returning results; `path_output`
reconstruction round-trip; `resolved_path_outputs` alignment with
`full_combos` incl. variant placeholders (two groups → two paths) and
guard propagation (colliding template → ValueError from prepare);
Python-path regression: `scidb.for_each` behavior unchanged after the
`_endpoint_policy` refactor (existing endpoint test files re-run green).

**MATLAB-side (user runs in MATLAB; written to mirror the Python tests)**
- `TestForEachPlotEndpoint`: figure-handle export (file exists, path record
  with `finalized=true`), draft renders-without-recording, stamp read back
  via `py.scidb.read_artifact_stamp`, per-variant `{low_hz}` files, char
  passthrough, anonymous-handle warning.
- `TestForEachStatEndpoint`: struct → stored canonical JSON (`date`
  stripped, `report_path` embedded); draft prints + resolves path to empty
  + writes nothing; cross-language identity — a MATLAB stat_ run then a
  Python run of the equivalent fn with skip_computed skips (byte-identical
  payload; the real prize of Python-side normalization).
- `TestAcrossVariants`: pooled single call with bp columns through the
  bridge; constructor rejections.
- `TestShareLimits`: per-group identical limits, groups differ (port of the
  Python assertions).

## Risks / watch items

1. **`func2str` anonymous handles** — detection impossible; warn when
   `finalized` given with one (phase B). MATLAB local functions in scripts
   also name fine via func2str.
2. **`resolved_path_outputs` alignment** — full_combos order must be stable
   between prepare's return and MATLAB's loop (it is today: MATLAB iterates
   the returned list in order); assert alignment length in prepare.
3. **skip_computed byte-identity for stat_** — hinges entirely on
   normalization being Python-side; the cross-language skip test is the
   canary.
4. **`exportgraphics` colorspace/dpi defaults** differ from `fig.savefig` —
   cosmetic only; stamping and records don't care.
5. **share_limits injection heuristic** (`nargin`) is weaker than Python's
   signature inspection — documented contract instead of magic; if it
   proves confusing, a follow-up could pass limits inside an options struct.
6. **MATLAB test debt**: existing MATLAB tests must stay green
   (TestForEachSkipComputed especially — prepare's cache shape changes).
7. I cannot run MATLAB here: MATLAB tests are written blind and verified by
   the user (per standing workflow); Python bridge tests carry the load
   that pytest can reach.

## Suggested implementation order

A (PathOutput + bridge kind + pre-resolution) → B (policy refactor +
threading; Python regression suite must stay green here) → C (plot wrapper)
→ D (stat wrapper + normalize helper) → E (AcrossVariants builder) → F
(share_limits port) — each phase independently testable; A+B+C alone
already deliver MATLAB figure endpoints end-to-end.
