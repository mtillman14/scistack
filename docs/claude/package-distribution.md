# Package Distribution: Versioning, Dependencies & the Dev-Install Contract

How the 14 packages in this monorepo are versioned, released, wired together in
development, and what that machinery hides. Rewritten 2026-09-13 — the previous
version of this file documented package names and a tagging scheme that no
longer exist (`canonicalhash`, `thunk`, `pipelinedb`, `scirun`, `scidb-matlab`,
and per-package `<package>-v<version>` tags). If you find those names in a stale
`.venv`, that is where they came from.

## Directory → distribution → import name

| Directory | PyPI distribution | Import name | Published? |
|---|---|---|---|
| `scistacklog/` | `scistacklog` | `scistacklog` | yes |
| `scicanonicalhash/` | `scicanonicalhash` | `scicanonicalhash` | yes |
| `path-gen/` | `scipathgen` | `scipathgen` | yes |
| `sciduckdb/` | `sciduckdb` | `sciduckdb` | yes |
| `scifor/` | `scifor` | `scifor` | yes |
| `scilineage/` | `scilineage` | `scilineage` | yes |
| `scistackplot/` | `scistackplot` | `scistackplot` | yes |
| `scidb/` | **`scistack-db`** | **`scidb`** | yes |
| `scimatlab/` | `scimatlab` | `scimatlab` | yes |
| `scihist/` | `scihist` | `scihist` | yes |
| `scistackplotdb/` | `scistackplotdb` | `scistackplotdb` | yes |
| `scistack/` | `scistack` | `scistack` | yes |
| `scistack-gui/` | `scistack-gui` | `scistack_gui` | **not yet** — see "Publishing the GUI" below |
| `scidb-net/` | `scidb-net` | `scidbnet` | **no** — outdated, slated for removal |

Two names to keep straight: the directory `scidb/` builds the distribution
`scistack-db`, which installs the import package `scidb`. So `pip install
scistack-db` but `import scidb`, and in a `dependencies` list you write
`scistack-db`. Everything else is import-name == distribution-name, except
`path-gen/` → `scipathgen` and `scistack-gui/` → `scistack_gui`.

## Layering

```
Layer 0   scistacklog, scicanonicalhash, scipathgen, sciduckdb
Layer 0.5 scifor        (→ scistacklog)
          scistackplot  (→ scistacklog)          in-memory plotting, no DB
Layer 1   scilineage    (→ scicanonicalhash)
Layer 2   scistack-db   (→ scipathgen, scicanonicalhash, sciduckdb, scifor, scistacklog)
Layer 3   scistackplotdb (→ scistackplot, scistack-db)
          scimatlab, scihist, scidb-net, scistack  (→ scistack-db)
Layer 4   scistack-gui  (→ everything)
```

The plotting split matters: `scistackplot` is deliberately database-free — it
takes a long-format DataFrame and a `PlotSpec` and knows nothing about scidb.
`scistackplotdb` is the adapter that loads scidb variables into that long table.
Anything that needs a database to make a plot belongs in `scistackplotdb`;
anything about plot semantics belongs in `scistackplot`. (This is CLAUDE.md
NOTE 3 applied to the plotting stack, and it is why `scistack-gui`'s
`plot_service.py` is explicitly only an adapter.)

## Versioning: lockstep, derived from git tags

Every package uses `hatchling` + `hatch-vcs`, all with the same stanza:

```toml
[tool.hatch.version]
source = "vcs"
raw-options = { search_parent_directories = true, local_scheme = "no-local-version" }
```

`search_parent_directories = true` is the load-bearing part: each package walks
up to the **repo root** `.git`, so all 14 read the *same* tag. There is no
version string anywhere in the repo to edit, and no package can have a version
of its own. One tag, one version, everything.

`.github/workflows/publish.yml` fires on `v*`, runs the full CI suite as a gate,
builds all 12 shipped packages into one `dist/`, and publishes via PyPI trusted
publishing (OIDC, no secrets — each project must list the workflow as a trusted
publisher). A guard step greps the built filenames for `.dev`, `aN`/`bN`/`rcN`,
or `+local` segments and refuses to publish if any appear, because hatch-vcs
emits those whenever HEAD is not exactly on the tag or the tree is dirty.

Consequence worth internalising: **a release is all-or-nothing and version
numbers carry no per-package meaning.** `scistackplot 0.1.27` and
`sciduckdb 0.1.27` say only "same commit", not "changed together".

## The dev-install contract, and what it hides

`dev-install.sh` installs everything editable in layer order, every line with
`--no-deps`:

```bash
pip install -e $SCRIPT_DIR/scistacklog --no-deps
...
```

`--no-deps` is there for one good reason: without it, pip would happily fetch a
sibling from PyPI and shadow your editable checkout. With it, the local tree
always wins.

**The cost — and this is the thing to remember — is that `dev-install.sh` never
resolves a dependency graph.** No third-party requirement is installed by it, no
internal floor is ever checked, and no `dependencies` list in any
`pyproject.toml` is ever read. The declarations are inert text in development.

`ci.yml` papers over the third-party half with a hand-maintained line:

```bash
pip install pandas numpy duckdb
pip install pytest pytest-cov matplotlib seaborn plotly tomli-w
```

with a comment instructing you to keep it in sync with the `dependencies`
blocks. Nothing enforces that sync.

So the environment in which all of this is developed and tested — one flat venv
with every package editable and every third-party dep pre-installed — is the one
environment in which broken metadata is undetectable. A defect only surfaces for
someone doing a real `pip install` of one package.

### Accidental-satisfaction traps this creates

- **Transitive third-party deps.** `scidbnet/client.py` imports `pandas` at
  module level and `scidb-net` never declares it — it arrives via `scistack-db`.
  Correct today, silently broken the day `scistack-db` drops pandas.
- **A test-only dependency propping up a runtime import.** Six modules do a hard
  `tomllib` → `tomli` fallback (`scifor/discovery.py:37`, `scidb/discover.py:52`,
  `scidb/entities.py:90`, `scistack/user_config.py:40`,
  `scistack/uv_wrapper.py:48`, `scistack_gui/config.py:33`). `tomllib` is 3.11+,
  CI's floor is 3.10, and no package declares `tomli` at runtime. CI passes
  because `pytest-cov` → `coverage` → `tomli; python_full_version<="3.11.0a6"`.
  A user's non-test 3.10 install gets `ModuleNotFoundError` from `import scifor`.
  Only `scidb/inspect/cli.py:51` degrades gracefully.
- **Internal floors nobody resolves.** Every internal pin reads `>=0.1.0` even
  though `scistackplot` did not exist before `v0.1.23`, and `scistackplotdb`
  calls `scidb` APIs (`variant_identity_batch`, `code_versions_batch`,
  `CODE_PIN_PREFIX`, `LATEST_VERSION`) that also first shipped in `v0.1.23`.
  Under lockstep, pip picking "newest of both" hides it; any pin on one side
  exposes it.

**Rule of thumb:** a package's `dependencies` must name everything it imports at
module level, including things a sibling happens to supply. Metadata states
intent; transitive availability is a coincidence, not a contract.

## Python version floor

The real floor is **3.10**, for a reason unrelated to policy: many modules use
PEP 604 unions (`str | None`) in `def` signatures without
`from __future__ import annotations`, and those annotations evaluate at
definition time. `scifor/pathinput.py:55`, `scidb/database.py:163`,
`scihist/database.py:14` and roughly a dozen others raise on 3.9 at import.

Everything except the `requires-python` field already agrees on 3.10:
`ruff.target-version = "py310"`, `mypy.python_version = "3.10"`, the trove
classifiers, the CI matrix (`3.10 / 3.12 / 3.13`),
`docs/getting-started/installation.md:20`, and `scistackplot` /
`scistackplotdb`'s own `requires-python = ">=3.10"`. Only the other 11 packages'
`requires-python = ">=3.9"` disagrees, and it is simply wrong.

## Optional dependencies: which are real

`scistackplot` is the model to copy. `render/__init__.py` exposes
`render_matplotlib` and `render_plotly` as thin wrappers that import
`.mpl` / `.plotly_` **inside the function body**, so `import scistackplot` costs
nothing and the `mpl` / `interactive` extras are honest:

```python
def render_matplotlib(resolved):
    from .mpl import render        # matplotlib only touched here
    return render(resolved)
```

Note that `mpl` includes `seaborn` even though the renderer itself does not use
it — `codegen.py:95` *emits* `import seaborn as sns` into generated `plot_`
endpoint source, so seaborn must be present wherever that generated code later
runs. That is a property of the generated artifact, not of the renderer.

`scidb` → `pyarrow` (`database.py:1381`) is genuinely optional and correctly
guarded with `except ImportError: pass`, falling through to a slower path.
`scifor` → `pandas`/`numpy` are all function-local or under `TYPE_CHECKING`.

The counter-example is `scistack_gui/services/plot_service.py`, which guards the
plotting imports with `_require_scistackplot()` and a "pip install scistackplot
scistackplotdb" hint — presenting them as optional while declaring no extra that
would install them, and while `plot_service.py:869` imports `matplotlib`
unguarded anyway. Reading the guard, note that bare `scistackplot` is not
enough: the save/export path calls `render_matplotlib`, so the GUI needs
`scistackplot[mpl]`.

## Publishing the GUI

`scistack-gui` and `scidb-net` are the two packages `publish.yml` excludes.
`scidb-net` is excluded because it is outdated and slated for removal. The GUI's
exclusion carries this comment:

> scistack-gui is intentionally excluded — it needs a JS frontend build before it
> can ship a correct wheel.

That is only half true, and the distinction matters. The built standalone bundle
**is committed**: `scistack_gui/static/index.html` plus
`static/assets/index-*.css|js` are tracked in git (only `frontend/dist/` is
ignored, and that is a different output path). A wheel built today would contain
a working UI. The real risk is not a *missing* bundle but a **stale** one —
`dev-install.sh` never runs vite, so a committed `.tsx` change is inert until
someone rebuilds, and publishing would ship whatever was last committed.

Two vite targets, only one of which is a packaging concern
(`frontend/vite.config.ts`):

| `VITE_BUILD_TARGET` | Output | Consumed by |
|---|---|---|
| `standalone` (default) | `../scistack_gui/static/` | FastAPI, and therefore the wheel |
| `webview` | `../extension/dist/webview/` | the VS Code `.vsix` via `vsce package` |

Only `standalone` reaches PyPI. The webview target is a single un-split bundle
because VS Code webviews can only load nonce-bearing scripts from the extension
directory — it ships through `vsce`, never through a wheel.

**Why the GUI needs to be on PyPI at all:** the VS Code extension spawns
`python -m scistack_gui.server` against the user's selected interpreter
(`extension/src/pythonProcess.ts:5`) and reports a diagnostic when "this
interpreter has no scistack_gui" (`pythonProcess.ts:103`).
`extension/README.md:11` already tells users their interpreter "must have
`scistack-gui` installed" — but today the only way to satisfy that is to clone
the monorepo and run `dev-install.sh`. Until the package is published, the
extension cannot be shipped to anyone outside this repo.

The planned end state (see `.claude/dependency-metadata-audit-plan.md` Stage 6)
is that `scistack-gui` is published and `scistack` depends on it, making
`pip install scistack` the single command that installs everything. That
direction is load-bearing: the GUI must **not** depend on `scistack`, or the
graph gains a `scistack → scistack-gui → scistack` cycle. The one place the GUI
reaches upward today — `startup.py:134`, importing `scistack.uv_wrapper` for a uv
lockfile staleness check — is removed as part of that stage.

## What CI actually covers

`ci.yml` runs pytest per package over a fixed list:

```
scistacklog scicanonicalhash path-gen scifor sciduckdb
scilineage scidb scihist scistackplot scistackplotdb
(+ scimatlab only when a `matlab` binary exists)
```

Not in that list, and therefore never run: **`scistack-gui` (47 test files)** and
`scistack` (5). The `build` job does build `scidb-net` and `scistack` and runs
`twine check`, but never imports them.

Each package gets its own `pytest` invocation. That is required, not stylistic:
several packages have a top-level `conftest.py` and `from conftest import ...`
in their tests, so spanning two `tests/` directories in one invocation collides
on collection.

`typecheck` runs mypy only on the four packages that declare `[tool.mypy]`
(`scicanonicalhash`, `path-gen`, `sciduckdb`, `scilineage`) and is
`continue-on-error: true`.

## Where to look

- `dev-install.sh` — layer order, and the `--no-deps` decision
- `.github/workflows/publish.yml` — lockstep release, the non-clean-version guard
- `.github/workflows/ci.yml` — the hand-maintained third-party install list
- `docs/claude/layer-friction-analysis.md` — which layer owns which concern
- `.claude/dependency-metadata-audit-plan.md` — the outstanding repair plan for
  the drift described above
