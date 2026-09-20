# Plan — cross-layer integration tests over the gait symmetry example

Drafted 2026-09-19. Status: **stages 1–2 built, unrun (no Python in the dev
environment — the user runs pytest).**

## Why

Every suite today is per package, with a small fixture built inside the test.
Nothing runs a **real pipeline, from files on disk, through every layer, to a
GUI response**. The `examples/aim2` gait symmetry dataset is the first fixture
that spans all five schema levels, four record levels, scalar and 1-D data,
and it is checked into the repo — so it can be the spine of one suite that
would have caught the class of bug the per-package tests cannot see:
a `for_each` change that breaks the GUI's plot service, a storage change that
drops zero padding, a plotting change that disagrees with what was stored.

## Stage 1 — the dataset can reach every layer (done)

Gaps closed before writing any test:

* **1-D data.** `waveforms/` — one file per cycle, 51 samples x three joints,
  loaded as a dict of lists so each joint is ONE array per record
  (`CycleWaveform`). This is what the plot layer's line/band paths, the cell
  statistic and transport downsampling need. `knee_excursion` reduces a curve
  to a scalar in the pipeline.
* **A categorical subject-level column.** `group` (control / treatment) in the
  demographics file — a factor variable for grouping plots by something that
  is not a schema key.
* Waveforms live in a subfolder so their names cannot also match the per-cycle
  symmetry template.

## Stage 2 — the suite (done, unrun)

`tests/integration/`, its own pytest root (never run in the same invocation as
a package suite — `conftest` collision, see memory).

| file | layer(s) | what it pins |
|---|---|---|
| `conftest.py` | — | Builds the database ONCE per session by running the example pipeline over a subset (2 subjects x 2 sessions); `SCISTACK_INTEGRATION_FULL=1` runs all of it |
| `test_pipeline_levels.py` | scifor + scidb | record counts per level; a coarse value reaches every finer combination; `distribute` files rows by the returned `cycle` column; `for_columns` reassembles; `Fixed` stays within a subject; two `Parameter` values = two variants; a second run adds nothing |
| `test_storage_roundtrip.py` | sciduckdb + scidb | zero-padded keys stay strings; coarse records leave the finer keys NULL; a 1-D record round-trips as a sequence |
| `test_plot_layer.py` | scistackplot(+db) | a default spec opens; bar/box resolve; **`plot_data` parity against the drawn marks on real data**; the joints spread one column per field; a 1-D measure refuses the CSV and draws a band |
| `test_gui_service.py` | scistack-gui | `describe` / `capabilities_for` / `resolve_figures` / `save_plot_data` over the same database |
| `test_provenance.py` | scidb inspect (+ scilineage/scihist) | a cycle record traces to a producing invocation naming its function |

## Running

```
pytest tests/integration -q                        # subset: ~120 cycle records
SCISTACK_INTEGRATION_FULL=1 pytest tests/integration -q   # all 720
```

## Open / deliberately not covered

* **MATLAB** — `scimatlab` has its own suite and there is no MATLAB here.
* **GUI frontend** — `docs/gui-manual-testing-todo.md` remains the by-eye list.
* **Glue nodes, `PathOutput`, `plot_`/`stat_` endpoints, code-version
  variants, `where=` filters and exclusions** — not in the example pipeline
  yet; each is a small addition to `pipeline.py` when wanted.
* The fixture cost is one pipeline run per session. If that proves slow, the
  next step is a cached `.duckdb` built once per day rather than per session.
