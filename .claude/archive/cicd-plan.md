# CI/CD + Release Plan for the SciStack monorepo

## Goal
Public-ready CI (test/lint/build gate on every PR) and a low-friction, single-tag
release flow to PyPI for all library packages.

## Decisions
- **Lockstep versioning.** All packages share one version. One tag `vX.Y.Z`
  publishes every package at that version. Right choice while the packages form
  one dependency tree and move together. Can split to independent later.
- **Version derived from git tags via `hatch-vcs`.** No more hardcoded
  `version = "0.1.0"` in 11 `pyproject.toml` files. The tag *is* the version;
  untagged builds get dev versions like `0.2.0.dev3+g<sha>`.
- **CI-only publishing via OIDC trusted publishing.** Local `twine upload`
  retired. `pypi_upload_mult.sh` deleted.

## Packages & dependency order
Directory → distribution name:
```
scicanonicalhash → scicanonicalhash   (layer 0, no internal deps)
path-gen         → scipathgen         (layer 0)
scifor           → scifor             (layer 0)
sciduckdb        → sciduckdb          (layer 0)
scilineage       → scilineage         (layer 1: scicanonicalhash)
scidb            → scidb              (layer 1: scipathgen, scicanonicalhash, sciduckdb, scifor)
scimatlab        → scimatlab          (layer 2: scidb)
scihist          → scihist            (layer 2: scidb, scilineage)
scidb-net        → scidb-net          (layer 2: scidb)
scistack         → scistack           (layer 2: scidb)
```
**Auto-published set = these 10 pure-Python libraries.**

**`scistack-gui` is excluded from auto-publish** — it has a `frontend/` +
`extension/` (JS build, `package-lock.json`) and needs a JupyterLab-extension
build pipeline before it can ship a correct wheel. It still gets converted to
hatch-vcs for version uniformity, but is not built/uploaded by the release job.
Revisit when the frontend build is wired up.

## Files changed
1. **11 × `pyproject.toml`** — switch to hatch-vcs dynamic versioning:
   - `requires = ["hatchling"]` → `["hatchling", "hatch-vcs"]`
   - add `[tool.hatch.version]` with `source = "vcs"`
   - replace `version = "0.1.0"` with `dynamic = ["version"]`
2. **`ruff.toml`** (new, repo root) — repo-wide lint/format defaults matching the
   per-package style already in scicanonicalhash/scilineage/sciduckdb/path-gen.
3. **`.github/workflows/ci.yml`** (new) — `lint`, `test` (py matrix), `build`
   jobs. Exposes `workflow_call` so the release workflow reuses it as a gate.
4. **`.github/workflows/publish.yml`** (rewritten) — on `v*` tag: run CI, then
   build all 10 libraries into one `dist/` and publish via trusted publishing.
5. **`pypi_upload_mult.sh`** — deleted.

## CI policy (honest defaults)
- `ruff check` + `ruff format --check`: **blocking**. (Run `ruff format` once to fix.)
- `pytest` matrix on **3.10 / 3.12 / 3.13** (tooling targets py310; see note below): **blocking**.
- `mypy` on the 4 packages that declare `[tool.mypy] strict`: **non-blocking
  (continue-on-error) to start**, since the repo has never been type-gated.
  Flip to blocking once clean.
- `twine check` on every built dist: **blocking**.

## One-time setup the user must do (cannot be done from code)

**Bootstrap ordering matters.** No git tags exist yet. With no tag, hatch-vcs
resolves to a version *below* `0.1.0` (e.g. `0.0.post1.dev3+g…`), which does NOT
satisfy the internal `>=0.1.0` pins — so the editable install step in CI will
fail until the first `v0.1.0` tag is on GitHub. Do this in order:

1. **Configure PyPI trusted publishers first.** For each of the 10 projects, add
   a trusted publisher: repo = this repo, workflow = `publish.yml`, environment =
   `pypi`. (PyPI → project → Settings → Publishing.) For projects that don't
   exist on PyPI yet, use the **"pending publisher"** form (same page) so the
   first publish creates them.
2. **Create a GitHub environment named `pypi`** (Settings → Environments),
   optionally with required reviewers as a manual release-approval gate.
3. **Merge these CI/CD changes to `main`.** (See note below on the intro PR.)
4. **Push the bootstrap tag — this is also your first release:**
   `git tag v0.1.0 && git push origin v0.1.0`. This seeds the version for all
   future CI runs *and* triggers `publish.yml` to ship `0.1.0` to PyPI.
5. **Branch protection on `main`** requiring the `lint` / `test` / `build` checks.

**Note on the introductory PR:** the PR that adds these workflows runs CI before
any tag exists, so its `test`/`build` jobs may fail on version resolution. Two
clean options: (a) push `v0.1.0` onto the branch tip before merging so CI sees a
tag, or (b) merge via admin override, then immediately push `v0.1.0`. After the
tag exists, every subsequent CI run resolves versions normally.

## Out of scope (flagged, not done)
- **261 tracked build artifacts** (`dist/`, `__pycache__/`, `*.egg-info/`,
  `site/`). Should be `git rm --cached`'d and `.gitignore`'d before going public.
  Offer to do this separately.
- **`requires-python = ">=3.9"` vs `target-version = "py310"` mismatch.** Tooling
  and classifiers assume 3.10+. Recommend bumping `requires-python` to `>=3.10`.
  Left as-is for now; CI matrix starts at 3.10.

## Validation (user runs locally — no Python in assistant env)
After edits, before tagging:
```
pip install build twine hatch-vcs
# version now resolves from git (will be a dev version until you tag):
python -m build --outdir /tmp/distcheck scicanonicalhash
python -m twine check /tmp/distcheck/*
# lint:
pip install ruff && ruff check . && ruff format --check .
```
