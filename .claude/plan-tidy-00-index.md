# Plan — codebase tidy-up (index)

*Drafted 2026-11-01 from the as-built design review in `docs/design-review/`
(README + docs 01–09). **Read `docs/design-review/09-synthesis.md` first.** It
holds the themes (T1–T8), the target layering, and the refactor IDs used
below (R1, W1, B3, G7, …). Each ID's evidence is in the doc it came from:

| ID prefix | Doc |
|---|---|
| R | 02-scifor |
| D | 03-sciduckdb |
| B | 04a-scidb-structure |
| W | 04b-scidb-write-path |
| Q | 04c-scidb-read-path |
| H, N, M | 05-scidb-satellites |
| P, PD, S | 06-plot-and-stats |
| G | 07-gui |
| U | 08-scistack-umbrella |

## The plans, in execution order

| # | Plan file | Theme | Size | Depends on |
|---|---|---|---|---|
| 0 | `plan-tidy-p0-provenance-atomicity.md` | T8 correctness | small | — |
| 1 | `plan-tidy-p1-dead-weight.md` | T7 dead code, stale docs | mechanical | decisions D1, D4, D6, D7 |
| 2 | `plan-tidy-p2-boundaries.md` | T3 + T6 enforce layers, declare APIs | mechanical, wide | P1 done (fewer packages to fence) |
| 3 | `plan-tidy-p3-homes.md` | T1 project runtime, T2 schema owner | medium | P2 import tests in place; decisions D2, D3 |
| 4 | `plan-tidy-p4-decompose.md` | T4 giant functions, T5 "latest" | large, many sessions | P3 |
| 5 | `plan-tidy-p5-cross-language.md` | T5 copies across MATLAB / TS / codegen | large | P4 phase boundaries |

Each phase is **independently shippable**: every stage inside a plan ends with
the user's test run green and (when the user asks) a commit. Do not start a
phase until the previous one's checklist below is ticked.

## Global rules for every plan (from CLAUDE.md + memory)

1. **Never run Python, pytest or `python3 -m py_compile` yourself**, not even
   for syntax checks. Give the user the exact command and wait for their
   report. Syntax-check edited Python with `npx pyright <file>`
   (`reference_pyright_syntax_check`) or hand them
   `python3 -m py_compile <file>`.
2. **pytest one package per invocation.** `conftest` module names clash across
   packages. For example `python -m pytest scidb/tests -x -q`, then separately
   `python -m pytest scistack-gui/tests -x -q`.
3. **Logging + regression test for every change** (CLAUDE.md note 2). Each
   stage names the log line and the test it adds.
4. **One concept, one owner** (note 4). When a stage moves an owner, grep for
   every reader in the same stage.
5. **Beta: no deprecation shims, no compatibility aliases.** Rename or remove
   everywhere in one change (`feedback_beta_no_deprecation`). This overrides
   "mark hidden" for **code**. The never-delete rule is about user **data**.
   Code removal is fine where a decision below says so.
6. **Commit only when the user asks**, on the checked-out branch (default
   `main`), and push. Leave `.DS_Store` out (`feedback_commit_push_main`).
   Suggest a commit at the end of each stage.
7. **Frontend changes need both bundles rebuilt** before they take effect
   (`project_frontend_bundle_rebuild`):
   ```
   cd scistack-gui/frontend
   npm run build
   VITE_BUILD_TARGET=webview npm run build
   ```
   Node/npm work in the sandbox, so you may run these and `npm test` yourself.
8. **GUI-visible changes** get an entry in `docs/gui-manual-testing-todo.md`
   (`feedback_gui_manual_testing_doc`).
9. **Removing a package** also means checking:
   - `dev-install.sh`
   - `.github/workflows/ci.yml` (build loop) and `publish.yml`
   - `.github/scripts/test-shards.sh` (the ONE owner of shard groups)
   - `scistack-gui/tests/test_version_single_owner.py`
   - `scidb/pyproject.toml` `override-versions-of`
   - `ruff.toml`
   - `tools/audit/run.sh`
   - `sys.path.insert(... "<pkg>" ...)` lines in every `tests/conftest.py`
   - `docs/` and `mkdocs.yml` nav
   - `CLAUDE.md`'s package list
10. **Before editing a file that the user may have edited, re-read it.** Before
    appending, check its end (`feedback_verify_eof_before_append_edit`).
11. **Update the design-review docs when a finding is fixed.** Add a one-line
    `> **Done (date, commit):** …` under the refactor row in the relevant doc,
    and tick the status table below.

## Design bar (applies to every stage, added 2026-11-01)

The goal is a well-factored project. When a stage leaves a choice open, pick
the shape a maintainer of **fastapi, black, requests, click, pydantic or
sqlmodel** would merge. In concrete terms:

1. **Small public surface, clear private interior.** Each module exposes a
   few named entry points (`requests.Session`, `click.command`). Everything
   else is `_private`. A cross-package caller never reaches into another
   package's `_name`. If it needs one, promote it to a documented public
   function first (e.g. `SciDuck.transaction()` replaces `_begin`/`_commit`).
2. **Context managers for paired actions.** Open/close, begin/commit and
   acquire/release go through `with` (sqlmodel `Session`, `requests.Session`),
   never as hand-matched calls in `try/except/finally`.
3. **Functions do one thing and read top to bottom.** A function that needs
   section banners ("PHASE 1…4") is several functions. Aim for under ~60
   lines. Pass values in explicitly; don't let a function read globals
   halfway through (`db or get_database()` is resolved once, at the edge).
4. **Explicit data types at boundaries.** Use frozen dataclasses (or pydantic
   models at wire boundaries) instead of loose dicts and positional tuples,
   like fastapi/pydantic request models. Dict keys starting with `__` are a
   sign a type is missing.
5. **Errors are typed and raised, never logged and swallowed.** There is one
   exception hierarchy per package, rooted in a base class (like
   `requests.RequestException`). Catch the narrowest type, and only where you
   can act on it. `except Exception: Log.error(...)` is acceptable only at a
   top-level boundary (RPC handler, CLI entry), and it must re-raise or turn
   the error into a reported failure.
6. **Imports at module top.** No function-local imports, except the D8
   allow-list (cycles or heavy optional dependencies, each with a comment).
7. **Names say what, not how or when.** No `_v2`, `_new`, `legacy_`, or
   migration-era comments. A comment explains *why*. When the code changes,
   its comment changes in the same edit.
8. **Type hints on every public signature.** They must pass `npx pyright`
   with no new errors. Use `X | None`, not `Optional`.
9. **Tests target behaviour through the public API**, like click's
   `CliRunner` and fastapi's `TestClient`. Monkeypatch only at a seam the
   module itself names. One test file per behaviour, with a docstring saying
   what regression it guards.
10. **Leave it cleaner.** When a stage touches a function, fix that
    function's local mess (dead branch, stale comment, duplicate helper).
    Don't widen the diff beyond that function.

When the bar and a stage's literal steps disagree, follow the bar, and note
the deviation in the plan's Progress section.

## Decisions (all defaults confirmed by the user, 2026-11-01)

| # | Decision | Default | Used by | Confirmed? |
|---|---|---|---|---|
| D1 | Delete scidb-net, scihist (after moving its tests into scidb), `scipathgen` + `PathGenerator.m` | **delete** | P1 | ☑ 2026-11-01 |
| D2 | Project runtime split: `scifor.project` (model-free) + `scidb.project` (model-bound); no new distribution | **yes** | P3 | ☑ 2026-11-01 |
| D3 | scidb owns all project-DB DDL; other packages declare tables through a scidb helper; sciduckdb creates none | **yes** | P3 | ☑ 2026-11-01 |
| D4 | Merge scilineage's function hash into scicanonicalhash (`scicanonicalhash.functions`), delete scilineage | **yes** | P1 | ☑ 2026-11-01 |
| D5 | Codegen keeps emitting literal seaborn; reduce cost via a recorded reduction plan (P2) | **keep** | P5 | ☑ 2026-11-01 |
| D6 | csv-stats legacy API (`ttest.py`, `anova.py`, `utils/`) | **out of scope here** (separate repo; the user decides there) | — | ☑ 2026-11-01 |
| D7 | Library taps (`scistack.user_config`) | **delete** (no consumer; re-add when the add-library dialog needs it) | P1 | ☑ 2026-11-01 |
| D8 | Lazy imports after P2 | **allowed only from an allow-list that may only shrink** | P2 | ☑ 2026-11-01 |
| D9 | `for_each` save on a per-output save failure: roll back the whole call's save (all-or-nothing), or keep today's partial save of the other outputs | **all-or-nothing** | P0 stage 2 | ☑ 2026-11-01 |

If a decision is changed, edit the affected plan's stage before starting it.

## Status

| Phase | Stage | Status | Commit |
|---|---|---|---|
| P0 | 1 stop swallowing `record_run` failures | ☑ 2026-11-01 | (this commit) |
| P0 | 2 one transaction for data + graph | ☑ 2026-11-01 | (this commit) |
| P1 | 1–8 (see plan) | ☐ | |
| P2 | 1–7 | ☐ | |
| P3 | 1–7 | ☐ | |
| P4 | 1–9 | ☐ | |
| P5 | 1–6 | ☐ | |

## Handing over between sessions

When a session ends mid-plan, update the status table and the plan's own
"Progress" section with:
- the last stage finished;
- what is half-done;
- the exact test command the user last ran and its result.

The next session starts by reading this index, the plan in progress, and
`docs/design-review/09-synthesis.md`.
