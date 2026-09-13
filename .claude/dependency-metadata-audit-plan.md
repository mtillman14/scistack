# Dependency Metadata Audit & Repair Plan

Status: **partially landed** — see below
Date: 2026-09-13
Branch: `main` (at `3ca87f43`, tag `v0.1.27` on `dceb1ef3`)

### Landed 2026-09-13 (uncommitted)

**Stage 1 complete.** `requires-python` is now `">=3.10"` in all 14 packages (12
changed from `">=3.9"`). `Programming Language :: Python :: 3.13` added to the 9
packages that listed 3.10–3.12 without it; `scistackplotdb` also gained the 3.10
classifier its `requires-python` already implied. Verified: no `3.9` reference
remains in any `pyproject.toml` or workflow, and every `ruff.target-version` /
`mypy.python_version` already read `py310` / `3.10`.

Four packages still carry no classifier block at all — `scidb-net`, `scimatlab`,
`scistack-gui`, `scistack` — so there was nothing to extend. Writing full
classifier blocks for them is separate work.

**The tomli gap is closed.** `tomli>=2.0 ; python_version < '3.11'` added to the
runtime `dependencies` of `scidb`, `scifor`, `scistack` and `scistack-gui` — the
four packages whose modules import it at module scope with no graceful
degradation. Removed from `scistack`'s `dev` extra, where it had been mis-filed
and so never reached users. `scistackplot` also imports `tomli`, in
`PlotSpec.from_toml`, but that one is function-local and raises a clear
"needs Python 3.11+ or the 'tomli' package" message — and has no callers
anywhere in the repo, so it stays undeclared by design.

**`scistack-gui` is now in CI** (Stage 6, item 6): added to the `test` job's
`PACKAGES` list and to the `build` job's loop so its metadata gets `twine
check`ed like every other published package. The install step gained `fastapi`,
`uvicorn[standard]` and `httpx` (TestClient's transport — `conftest.py` builds
one for most of the suite), plus an explicit `tomli` for the 3.10 leg so that
job no longer relies on coverage dragging it in. Not verified against a runner —
this suite has never executed in CI, so the first run may surface real failures;
the two concurrency tests are the likeliest flakes.

`scistack` is in the `test` job too. Its uv tests are guarded by
`skipif(shutil.which("uv") is None)` and the rest mock `subprocess.run`, so a
runner with no uv binary skips those and runs the remainder.

**Two dead dependencies removed** from `scistack-gui`: `jupyter_client` and
`ipykernel`. Nothing in the package or its tests imports either — they are
leftovers from the original GUI design (`.claude/scistack-gui-plan.md` planned a
`kernel.py` and a `CodeDrawer.tsx` Jupyter drawer that were never built). The
stale reference in `db.py`'s module docstring was corrected too. This matters
more now that the GUI is a dependency of `scistack`: they would have landed in
every `pip install scistack`.

Plus a minimal slice of Stages 2 and 6, enough to publish `scistack-gui` and make
`pip install scistack` install it:

- `.github/workflows/publish.yml` — `scistack-gui` added to the build loop;
  exclusion comment replaced with the committed-bundle caveat.
- `scistack/pyproject.toml` — `"scistack-gui"` added to `dependencies`,
  deliberately unversioned.
- `scistack-gui/pyproject.toml` — added `pydantic`, `scifor`, `scimatlab`,
  `scistackplot[mpl]>=0.1.23`, `scistackplotdb>=0.1.23`, `matplotlib>=3.8`, and
  the `tomli` marker; added `license`; added an sdist target (without it,
  hatchling sweeps ~11MB of TS sources and sourcemaps into the sdist).

**Still outstanding before a tag:** the PyPI pending trusted publisher for
`scistack-gui` (manual, must precede the tag). **Still outstanding overall:**
Stage 1 (`requires-python`), Stage 3 (floors), Stage 4 (the regression test that
would have caught all of this), Stage 5, and the rest of Stage 6 — the uv hook
removal, CI coverage for the GUI, README/classifiers, and the node build step.

## Why

The declared packaging metadata across the 14 packages has drifted from what the
code actually imports and from what CI actually tests. Three independent classes
of drift, in descending severity:

1. **Undeclared runtime imports** — the code imports distributions the owning
   `pyproject.toml` never names.
2. **`requires-python = ">=3.9"` is false** in 11 packages — they use PEP 604
   unions at runtime.
3. **Version floors are fossils** — `>=0.1.0` internal pins under a strict
   lockstep release, and third-party floors from 2021.

None of this is visible day to day because `dev-install.sh` installs every
package with `--no-deps` into one editable environment. Nothing in the repo ever
resolves a dependency graph, so the declarations are unexercised prose. The
first person to `pip install scistackplotdb` from PyPI finds out instead.

---

## Evidence

### Class 1 — undeclared runtime imports

Split by whether anything else in the graph happens to supply them.

**Genuinely missing** (no path in the dependency graph provides these):

| Package | Missing | Import site |
|---|---|---|
| scistack-gui | `scistackplot`, `scistackplotdb` | `services/plot_service.py` (~15 sites) |
| scistack-gui | `scimatlab` | `matlab_registry.py:640`, `config.py:612`, `api/pipeline.py:106` |
| scistack-gui | `scistack` | `startup.py:134` — **resolved by Stage 6: call site removed, no declaration needed** |
| scistack-gui | `matplotlib` | `plot_service.py:869` (unguarded, PNG export path) |
| scidb, scifor, scistack, scistack-gui | `tomli` on Python < 3.11 | see below |

**Transitively satisfied but undeclared** (works today; breaks silently the day
an intermediate package drops the dep):

| Package | Undeclared | Import site | Supplied by |
|---|---|---|---|
| scihist | `scifor` | `__init__.py:22` (top-level) | scistack-db |
| scistackplotdb | `scistacklog` | `hierarchy/variants/endpoint/load/source.py` (top-level) | scistackplot, scistack-db |
| scistack-gui | `scifor` | `config.py:17`, `registry.py:27` (top-level) | scistack-db |
| scistack-gui | `pydantic` | 9 × `api/*.py` (top-level) | fastapi |
| scimatlab | `scifor` | `bridge.py:180`, `stubs.py:77` | scistack-db |
| scimatlab | `numpy`, `pandas` | `bridge.py:21-22` (function-local in `_describe_value`) | scistack-db |
| scidb-net | `pandas`, `numpy` | `client.py:12`, `serialization.py:24-25` (top-level) | scistack-db |

**Correctly optional, no change needed to behaviour** — but worth exposing as
extras so users can opt in deliberately:

- `scidb` → `pyarrow` (`database.py:1381`, guarded by `except ImportError: pass`)
- `scifor` → `pandas` / `numpy` (all function-local or `TYPE_CHECKING`)
- `scistackplot` → `matplotlib` / `plotly` (already correct: `render/__init__.py`
  defers both behind function-local imports, and the `mpl` / `interactive`
  extras exist)

#### The tomli sub-case is live, not hypothetical

Six modules do a hard `tomllib` → `tomli` fallback with no graceful degradation:

```
scidb/src/scidb/discover.py:52        scifor/src/scifor/discovery.py:37
scidb/src/scidb/entities.py:90        scistack/src/scistack/user_config.py:40
scistack-gui/scistack_gui/config.py:33  scistack/src/scistack/uv_wrapper.py:48
```

`tomllib` is 3.11+. On Python 3.10 — which is the **CI floor** and the floor the
user docs advertise (`docs/getting-started/installation.md:20`) — `import scifor`
raises `ModuleNotFoundError` unless `tomli` is installed. No package declares it
at runtime (`scistack` has it in `dev` extras only).

CI is green on 3.10 purely by accident: `pytest-cov` → `coverage` →
`tomli; python_full_version<="3.11.0a6"`. A test-only dependency is propping up a
runtime import. A user's non-test 3.10 install has no such luck.

Only `scidb/src/scidb/inspect/cli.py:51` degrades properly (logs and skips).

### Class 2 — `requires-python` is wrong in 11 packages

They declare `>=3.9` but use PEP 604 unions at runtime, in files that lack
`from __future__ import annotations`. Annotations on `def` signatures evaluate at
definition time, so these raise `TypeError` on import under 3.9:

```
scifor/src/scifor/pathinput.py:55     _project_root_override: Path | None = None
scifor/src/scifor/foreach.py:89       as_table: list[str] | bool | None = None
scidb/src/scidb/database.py:163       branch_params_filter: dict | None
scidb/src/scidb/variant.py:126-127    fn: str | None = None
scihist/src/scihist/database.py:14    schema_keys: list[str] | None = None
scidb-net/src/scidbnet/client.py:194  lineage_hash: str | None = None
```
(plus ~10 more files)

Everything else in the repo already says 3.10: `ruff.target-version = "py310"`,
`mypy.python_version = "3.10"`, the trove classifiers, the CI matrix
(`3.10 / 3.12 / 3.13`), the installation docs, and `scistackplot` /
`scistackplotdb`'s own `requires-python = ">=3.10"`. Only the `requires-python`
field in these 11 packages disagrees.

Secondary classifier drift: `scistackplotdb` lists only 3.11/3.12 despite
`requires-python = ">=3.10"`; only `scidb` claims 3.13 although CI tests it
everywhere.

### Class 3 — version floors

**Internal pins.** `publish.yml` is a strict lockstep release: one `v*` tag
builds and publishes all 12 shipped packages at that version, with a guard step
that refuses any non-clean artifact. Every internal pin is nevertheless
`>=0.1.0`. The concrete failure:

```
scistackplotdb declares  scistack-db>=0.1.0
scistackplotdb imports   scidb.provenance_query.variant_identity_batch   (added 2026-09-06, first tag v0.1.23)
                         scidb.provenance_query.code_versions_batch      (added 2026-09-08, first tag v0.1.23)
                         scidb.variant.CODE_PIN_PREFIX, LATEST_VERSION   (added 2026-09-08, first tag v0.1.23)
```

`pip install scistackplotdb==0.1.27` alongside a pinned `scistack-db==0.1.10`
resolves happily and then fails with `ImportError` at first import. The true
floor is `>=0.1.23`. Same for `scistackplot>=0.1.0` — that package did not exist
before v0.1.23, so the floor is not merely loose, it is unsatisfiable-by-intent.

**Third-party floors.** Declared vs. what is actually installed and tested:

| Declared | Installed / tested | Gap |
|---|---|---|
| `pandas>=1.3` (2021) | 3.0.3 | two majors |
| `numpy>=1.20` (2021) | 2.4.6 | one major |
| `duckdb>=0.9` | 1.4.4 | pre-1.0 floor |
| `matplotlib>=3.6` | 3.11.0 | five minors |

Commit `dceb1ef3` ("Gate pandas-3 precondition in composed-key test") shows
pandas 3 is an active compatibility surface. Nothing declares an upper bound and
nothing tests the declared lower bound, so both ends of every range are fiction.

---

## Decisions

### Settled (user, 2026-09-13)

- **`pip install scistack` is the one-stop-shop command.** `scistack-gui` gets
  published to PyPI and becomes a hard dependency of `scistack` — not an extra.
  Direction is `scistack → scistack-gui`, never the reverse. See Stage 6.
- **No uv.** No `uv` extra, and the GUI's uv lockfile hook comes out (Stage 6).
  Full removal of uv from `scistack` itself is a separate, larger piece of work —
  see the note in Stage 6.
- **GUI plot packages are required, not optional.** Follows from the one-stop-shop
  decision: Plot Studio is a first-class panel. `_require_scistackplot()`
  (`plot_service.py:49`) stays as defence-in-depth for partially-installed dev
  environments, with its message corrected. Must be `scistackplot[mpl]` — the
  save/export path calls `render_matplotlib`.

### Still open

1. **Third-party floor policy.** Options: (a) leave the 2021 floors alone;
   (b) raise to something defensible-but-generous — `pandas>=2.0`, `numpy>=1.24`,
   `duckdb>=1.0`, `matplotlib>=3.8`; (c) raise to what CI actually exercises
   (latest). **Recommendation: (b)** — honest without pretending to support
   versions nobody has run.

2. **Internal-floor maintenance.** Stage 5 is optional automation. Without it,
   floors are hand-maintained and will drift again.

3. **Full uv removal from `scistack`** — separate plan, or leave as is?

---

## Stages

Each stage is independently committable and independently verifiable.

### Stage 1 — Correct `requires-python` and classifiers

Files: all 14 `pyproject.toml`.

- `requires-python = ">=3.9"` → `">=3.10"` in: path-gen, scicanonicalhash,
  scidb-net, scidb, sciduckdb, scifor, scihist, scilineage, scimatlab,
  scistack-gui, scistack, scistacklog.
- Add `"Programming Language :: Python :: 3.13"` wherever 3.10–3.12 are listed.
- `scistackplotdb`: add the missing 3.10 classifier alongside 3.11/3.12.

No code changes. This narrows what we promise; it cannot break an install that
works today.

### Stage 2 — Declare the missing runtime dependencies

**scistack-gui** (the bulk of the damage) — add to `dependencies`:
```
"scifor",            # config.py:17, registry.py:27 — top-level
"scimatlab",         # matlab_registry, config, api/pipeline
"pydantic>=2.0",     # 9 × api/*.py — top-level
"matplotlib>=3.8",   # plot_service.py:869 — PNG export
"scistackplot[mpl]", # Plot Studio  (pending decision 1)
"scistackplotdb",    # Plot Studio  (pending decision 1)
"tomli>=2.0 ; python_version < '3.11'",
```
Also give it the `readme`, `license`, and `classifiers` every other package has,
and an sdist target to match.

**`scistack` must NOT appear in `scistack-gui`'s `dependencies`** (corrected from
an earlier draft of this plan, which wrongly listed it as required). The
dependency runs the other way — see Stage 6. The GUI's only use of `scistack` is
`startup.py:132-146`, which wraps `from scistack.uv_wrapper import ...` in
`try/except ImportError` and logs "scistack package not importable — skipping
lockfile staleness check". Stage 6 removes that call site outright, so no
declaration of any kind is needed.

**scistackplotdb**: add `"scistacklog>=0.1.23"`.
**scihist**: add `"scifor>=0.1.23"`.
**scimatlab**: add `"scifor>=0.1.23"`, `"pandas>=2.0"`, `"numpy>=1.24"`.
**scidb-net**: add `"pandas>=2.0"`, `"numpy>=1.24"`. (Slated for removal and
excluded from `publish.yml`; fixing it is two lines, so do it rather than leave a
known-wrong file behind.)

**tomli**, added to `dependencies` of scidb, scifor, scistack, scistack-gui:
```
"tomli>=2.0 ; python_version < '3.11'",
```
and removed from `scistack`'s `dev` extra, where it is currently mis-filed.

**New extras** for things that are correctly optional but currently invisible:
- `scidb`: `arrow = ["pyarrow>=14"]`
- `scifor`: `pandas = ["pandas>=2.0", "numpy>=1.24"]`

### Stage 3 — Fix the version floors

Internal pins, everywhere: `>=0.1.0` → `>=0.1.23`. That is the release in which
the last cross-package API break landed (`scistackplot` first shipped;
`variant_identity_batch` / `code_versions_batch` / `CODE_PIN_PREFIX` /
`LATEST_VERSION` appeared in `scidb`). Going forward the policy is: **bump the
internal floor in the same commit that adds a cross-package API**, never at
release time.

Third-party floors per decision 2.

`scistack-gui`'s bare `"scistack-db"` and `"scihist"` (no floor at all) get the
same `>=0.1.23`.

### Stage 4 — Regression test (CLAUDE.md NOTE 2)

New: `tests/packaging/test_dependency_declarations.py` + its own `conftest.py`.
Run as a **separate pytest invocation** — it must not share a collection root
with any package's `tests/` (see the one-package-at-a-time rule).

The test walks every `pyproject.toml` and asserts:

1. **Import/declare agreement.** AST-parse every module under each package's
   source root, collect module-level `Import`/`ImportFrom` names (skipping
   `TYPE_CHECKING` blocks and anything nested inside a function or `try`), map
   top-level module → distribution via a table in the test, and assert each is
   declared in `dependencies` or reachable through a declared *internal* package.
   Purely transitive third-party reliance fails: metadata states intent.
2. **`requires-python` consistency** — every package agrees, and agrees with
   `ruff.target-version`, `mypy.python_version`, the lowest trove classifier, and
   the CI matrix floor parsed out of `.github/workflows/ci.yml`.
3. **Internal floors are uniform** — every `sci*` pin across all 14 files names
   the same floor, so a future bump cannot land in only some files.
4. **tomli guard** — any module importing `tomli` in a `tomllib` fallback belongs
   to a package declaring the `python_version < '3.11'` marker.

Assertion 1 is the one that would have caught all of Class 1. Assertion 3 is the
one that would have caught Class 3.

CI wiring: a new step in `ci.yml`'s `test` job running this directory on its own,
plus a comment explaining why it is a separate invocation.

**Diagnostics** (CLAUDE.md NOTE 2, second half): the failure messages must print
the resolved import → distribution → declaring-package chain, not just
`assert False`. When this fails in two years the message is the whole story.

Also improve `plot_service.py:49`'s `_require_scistackplot()` message to name the
extra (`pip install 'scistack-gui[plot]'` or `scistackplot[mpl] scistackplotdb`)
rather than the bare distributions, since bare `scistackplot` lacks the
matplotlib renderer the GUI needs.

### Stage 5 — (optional) keep it from drifting

Two loose ends found while auditing, both adjacent rather than central:

- **`dev-install.sh --no-deps` hides third-party requirements entirely.**
  `ci.yml` compensates with a hand-maintained `pip install pandas numpy duckdb`
  line and a comment saying "keep in sync with the `dependencies` blocks". That
  sync is exactly what Stage 4's test can verify. Optionally add a
  `dev-install.sh --with-deps` mode that installs third-party requirements from
  the declarations while still using `--no-deps` for siblings.
- **`scistack-gui`'s 47 test files and `scistack`'s 5 never run in CI.** The CI
  `PACKAGES` list omits both. The GUI is where the Class 1 defects concentrate.
  Adding them needs `jupyter_client`, `ipykernel`, `plotly` in the CI install.

Neither blocks Stages 1–4.

---


### Stage 6 — Publish `scistack-gui`, and make it a dependency of `scistack`

**Decision (user, 2026-09-13): `pip install scistack` is the one-stop-shop
command and must install everything, GUI included.** So `scistack-gui` gains a
PyPI presence and `scistack` takes a hard dependency on it — not an extra.

```toml
# scistack/pyproject.toml
dependencies = [
    "scistack-db>=0.1.23",
    "scimatlab>=0.1.23",
    "scistack-gui>=0.1.23",   # new
]
```

#### Why this direction is the safe one

The import graph already points this way. `scistack-gui` imports `scistack` in
exactly one place — `startup.py:134`, the uv lockfile staleness check — and it is
`try/except ImportError` with graceful degradation. Every other GUI→sibling edge
(`scidb`, `scifor`, `scihist`, `scimatlab`, `scistackplot`, `scistackplotdb`)
points *downward*. Remove that one call site (below) and `scistack →
scistack-gui` is a clean one-way edge with no cycle anywhere in the graph.

Had it gone the other way — `scistack` declared as a GUI dependency *and*
`scistack-gui` declared as a `scistack` dependency — pip would face
`scistack → scistack-gui → scistack`. Resolvable under lockstep, but it makes the
layer diagram meaningless and the build order undefined.

#### uv (user decision: not using uv at all)

Removing the GUI's uv hook is what keeps the edge one-way, and it is small:

- `scistack_gui/startup.py` — drop the lockfile-staleness function and its
  `from scistack.uv_wrapper import ...`; drop its call sites in `bootstrap.py`
  and `server.py:1708`.
- `scistack-gui/tests/test_startup.py` — drop the corresponding coverage.

The other GUI files that mention uv (`config.py:510`, `api/project.py:333`,
`bootstrap.py:7`) are comments only — update the wording, no logic.

**Out of scope, needs its own plan:** uv is still load-bearing inside `scistack`
itself — `uv_wrapper.py` (398 lines), `project.py:35,211-218` (runs `uv sync` on
project creation), `__init__.py:21-37` (re-exports it), and
`tests/test_uv_wrapper.py` (643 lines). Ripping that out is a ~1000-line change
across the project-creation path and should not ride along on a packaging fix.
Flagged for a separate decision.

#### Publishing blockers, in order

1. **Stages 1–3 are hard prerequisites.** Publishing the GUI with today's
   declarations ships a package that cannot work from a clean install: no
   `scistackplot`/`scistackplotdb` (Plot Studio dead), no `matplotlib` (PNG
   export dead), no `tomli` on 3.10 (`config.py:33` raises at import). The
   extension already has a `dependency_missing` diagnostic for precisely this
   ("`scistack_gui` is installed, but something it imports at startup is not" —
   `extension/README.md:27`). Do not publish before they land.

2. **The frontend bundle.** `scistack_gui/static/` **is** committed — 3 tracked
   files (`index.html` + hashed `assets/index-*.css|js`) — so a wheel built today
   would ship a working standalone UI. The `publish.yml` comment claiming the
   GUI "needs a JS frontend build before it can ship a correct wheel" is
   therefore only half true: the risk is not a *missing* bundle but a **stale**
   one, since `dev-install.sh` never runs vite and a committed `.tsx` fix is dead
   until the bundle is rebuilt. Fix in the release pipeline:

   ```yaml
   - uses: actions/setup-node@v4
   - run: cd scistack-gui/frontend && npm ci && npm run build   # → ../scistack_gui/static
   ```

   run *before* `python -m build`. Then either stop committing `static/` (build
   it in CI only) or add a CI check that a fresh build is byte-identical to what
   is committed — the second is better, because it keeps editable dev installs
   working while making staleness a test failure instead of a silent ship.

   Only the `standalone` vite target matters here. The `webview` target
   (`VITE_BUILD_TARGET=webview` → `extension/dist/webview/`) belongs to the
   `.vsix`, which ships through `vsce package`, not PyPI.

3. **Package metadata.** `scistack-gui/pyproject.toml` has no `readme`,
   `license`, `classifiers`, or sdist target. Add them (Stage 2 already does).

4. **PyPI project + trusted publisher.** The name must be claimed and
   `publish.yml` registered as a trusted publisher for it, exactly as for the
   other 12. **Manual, must happen before the first tag that includes it** —
   otherwise the release publishes 12 packages, fails on the 13th, and leaves a
   `scistack` on PyPI whose `scistack-gui` dependency does not resolve. Consider
   publishing the GUI once under a pre-release tag to validate the path.

5. **Remove the exclusion** in `publish.yml`'s build loop and update the comment
   above it (which will then be wrong on both counts).

6. **CI must actually test it first.** `scistack-gui`'s 47 test files have never
   run in CI. Shipping a package to PyPI that CI does not exercise is how the
   Stage 1 defects got here. Add `scistack-gui` (and `scistack`) to `ci.yml`'s
   `PACKAGES` list; the install step needs `jupyter_client`, `ipykernel`, and
   `plotly` on top of what it already installs.

#### Why publishing is right regardless of the `scistack` question

The VS Code extension spawns `python -m scistack_gui.server` against the user's
selected interpreter (`extension/src/pythonProcess.ts:5,88`), and fails with a
diagnostic when "this interpreter has no scistack_gui" (`pythonProcess.ts:103`).
`extension/README.md:11` already tells users the interpreter "must have
`scistack-gui` installed". Today the only way to satisfy that is cloning the
monorepo and running `dev-install.sh`. The extension is unshippable to anyone
else until this package is on PyPI.

#### Note on install weight

`pip install scistack` will now pull `fastapi`, `uvicorn[standard]`,
`jupyter_client`, `ipykernel`, `matplotlib`, `scistackplot`, and
`scistackplotdb`. That is the accepted cost of the one-stop-shop decision.
`README.md:137` and `docs/getting-started/installation.md` describe the old,
narrower behaviour and need updating to match.
## Out of scope

- Upper bounds / a lockfile. Worth discussing separately; this plan only makes
  the declared floors true.
- The stale `.venv` state (`canonicalhash-0.1.0`, `scidb-0.1.0`,
  `sci_matlab-0.1.0`, `scirun-0.1.0` dist-info left from the old package names,
  plus a `canonicalhash/` directory still on `sys.path` that can shadow
  `scicanonicalhash`). Local environment hygiene, not repo metadata — but worth a
  clean rebuild after Stage 2, and it explains why the old names still appear.
- `plotly` is absent from the local `.venv` even though `scistackplot`'s render
  tests call `render_plotly()` unguarded. CI installs it; local runs would fail.
- Documentation touch-ups: `docs/getting-started/installation.md:44-60` lists a
  manual editable-install sequence that omits `scistacklog`, `scistackplot`,
  `scistackplotdb`, and `scistack`; the root `README.md:137` describes what
  `pip install scistack` pulls in without mentioning the plotting packages.

## Verification

No Python here, so every stage hands back commands rather than running them:

```
pytest tests/packaging                  # Stage 4, on its own
pytest scistackplotdb/tests             # unchanged behaviour
pytest scistackplot/tests
pytest scidb/tests
python -m build --outdir /tmp/dist_check scistackplotdb && python -m twine check /tmp/dist_check/*
```

The real proof for Stages 2–3 is a resolver test that `dev-install.sh` cannot
give us — in a throwaway venv:

```
pip install --dry-run scistackplotdb --no-index --find-links /tmp/dist_check
```
