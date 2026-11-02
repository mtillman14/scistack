# Plan P1 — remove dead weight, fix stale docs

*Index: `.claude/plan-tidy-00-index.md` (global rules apply; rule 9 is the
package-removal checklist). Theme T7 in
`docs/design-review/09-synthesis.md`. Requires decisions D1, D4 and D7
confirmed.*

Every stage is mechanical. Each one ends with:
- a repo-wide grep showing zero remaining references, excluding
  `docs/design-review/` and `.claude/` (historical);
- the affected packages' tests, run by the user one package at a time.

Generic reference sweep, run per removed name:
```
grep -rn "<name>" --exclude-dir={node_modules,build,site,.git,__pycache__,design-review} . | grep -v '^./.claude/'
```

## Stage 1 — delete scidb-net (N1, doc 05)

Evidence: it cannot be imported. `server.py:22` imports
`scilineage.lineage.LineageRecord`, and `client.py:148` / `__init__.py:48`
import removed scilineage names. CI already skips its tests
(`test-shards.sh:35`) and publishing (`publish.yml:62`).

1. `git rm -r scidb-net/`.
2. Remove it from:
   - `dev-install.sh:40`;
   - the build loop in `.github/workflows/ci.yml` (~124);
   - the exclusion entries in `.github/scripts/test-shards.sh` (~35);
   - the comment in `publish.yml` (~62);
   - `test_version_single_owner.py`, if listed;
   - docs: `docs/internals/scidb-net.md`, `docs/internals/index.md`,
     `docs/concepts/architecture.md`, `docs/getting-started/installation.md`,
     `docs/project/contributing.md`, `mkdocs.yml` nav, and the `CLAUDE.md`
     package list ("scidb-net (optional)").
3. Sweep `scidb-net` and `scidbnet`.
4. User runs `bash .github/scripts/test-shards.sh check`, if that subcommand
   exists (see `project_ci_test_sharding`).

## Stage 2 — move scihist's tests into scidb, then delete scihist (H1, doc 05)

scihist (121 lines) is a deprecated shim: `scihist.for_each` is
`scidb.for_each` with `skip_computed=True`. Nothing in non-test source imports
it. Its 18 test files (7,961 lines) test scidb, so they move.

1. **Fixtures.** `scihist/tests/conftest.py` has:
   - `db`, built with `scihist.configure_database`, which is
     `scidb.configure_database`;
   - autouse `clear_global_state`;
   - helper `make_simple_mock_db`, imported by `test_foreach.py` via
     `from conftest import make_simple_mock_db`.

   `scidb/tests/conftest.py` already has `db` and an autouse
   `clear_global_db`.
   - Compare the two `DEFAULT_TEST_SCHEMA_KEYS`. scihist uses
     `["subject", "trial"]`. If scidb's differs, give the moved tests their
     own fixture `hist_db` and rewrite `db` → `hist_db` in the moved files.
   - Copy `make_simple_mock_db` into `scidb/tests/conftest.py`.
   - Copy `scihist/tests/data/` to `scidb/tests/data/hist/` (check for name
     clashes first). Fix the paths in the tests that use it (`state_trials`).
2. **Move the files** as `scidb/tests/test_hist_<oldname>.py`. No file names
   clash except `conftest.py`. The prefix keeps their origin visible.
3. **Rewrite imports in the moved files:**
   - `from scihist import for_each` → add at module top
     `from functools import partial` / `from scidb import for_each as _for_each`
     / `for_each = partial(_for_each, skip_computed=True)`. This keeps each
     test's semantics. A test helper is not a shim.
   - Every other `scihist.X` → `scidb.X` or `scifor.X`. See
     `scihist/src/scihist/__init__.py` for the mapping. `check_*_state` comes
     from `scidb.state`.
   - Drop `test_shim.py`, which tests the shim itself.
4. `git rm -r scihist/` and the MATLAB folder
   `scimatlab/src/scimatlab/matlab/+scihist/`. Check
   `scimatlab/tests/matlab/` for `+scihist` tests and remove them.
5. Fix scidb docstrings that mention scihist. `foreach.py` has 3 (~397, 416,
   545): reword them to describe the parameter, not the history. Also fix the
   GUI comments in `server.py` (~218, ~898) and `db.py` (~455), and
   `scidb/src/scidb/state.py` / `scidb/tests` mentions.
6. Run the package-removal checklist (index rule 9) and sweep `scihist`.
7. User runs `python -m pytest scidb/tests -x -q`. This is the big one: about
   18 more files.

## Stage 3 — delete PathGenerator ×2 (doc 01; the user's note there)

1. `git rm -r path-gen/`, `scidb/src/scidb/paths.py`,
   `scimatlab/src/scimatlab/matlab/+scidb/PathGenerator.m` and
   `scimatlab/tests/matlab/scidb/TestPathGenerator.m`.
2. Remove `"scipathgen"` from `scidb/pyproject.toml` (dependency and
   `override-versions-of`).
3. Remove `sys.path.insert(0, str(_root / "path-gen" / "src"))` from every
   test file that has it. That is about 21 files: `scidb/tests/conftest.py`,
   `scidb/tests/test_exclusions.py`, `scimatlab/tests/test_bridge*.py`,
   `scistack/tests/conftest.py`, `scihist/...` (gone after stage 2),
   `scidb-net/...` (gone after stage 1). Find them with
   `grep -rln '"path-gen"' --include='*.py' .`.
4. Remove the `path-gen` entry from `scimatlab/tests/matlab/setup_paths.m`.
5. Fix the line in `scimatlab/setup.md`, `dev-install.sh` (~13 and the layer
   comment ~32), the `ci.yml` and `publish.yml` loops, `test-shards.sh`
   ("small" group), `ruff.toml`, `tools/audit/run.sh`, and
   `test_version_single_owner.py`.
6. Docs:
   - delete `docs/internals/scipathgen.md`;
   - fix `docs/internals/index.md`, `docs/concepts/architecture.md`,
     `docs/concepts/glossary.md`, `docs/project/contributing.md` and
     `docs/getting-started/installation.md`;
   - fix `docs/claude/package-distribution.md` and
     `docs/claude/docs-ground-truth-test-map.md`;
   - check `mkdocs.yml`.
7. Sweep `PathGenerator`, `scipathgen` and `path-gen`.
   `examples/_archive/aim2/main.py` is archived and already broken. Leave it.
8. User runs `python -m pytest scidb/tests -x -q` and
   `python -m pytest scimatlab/tests -x -q`.

## Stage 4 — merge scilineage into scicanonicalhash (D4, doc 01)

scilineage is now only `compute_function_hash`,
`compute_function_hash_with_sources` and a re-export of `canonical_hash`
(`scilineage/src/scilineage/hashing.py`, 253 lines). Its README describes a
removed system.

1. Create `scicanonicalhash/src/scicanonicalhash/functions.py` with the
   contents of `scilineage/hashing.py`, minus the `canonical_hash`
   re-export. Export `compute_function_hash` and
   `compute_function_hash_with_sources` from `scicanonicalhash/__init__.py`.
2. Move `scilineage/tests/test_hashing.py` to
   `scicanonicalhash/tests/test_function_hash.py`. Merge any fixtures from
   `scilineage/tests/conftest.py`.
3. Rewrite the importers. The source sites are:
   - `scidb/src/scidb/foreach_config.py:9` (`from scilineage.hashing import …`);
   - `foreach.py` ~1561;
   - `glue.py` ~230;
   - anything else found by `grep -rn "scilineage" --include='*.py' scidb scistack-gui scimatlab scifor`.

   Also check `scidb/src/scidb/{rename,lineage_save,database,environment,provenance}.py`
   and `scistack-gui/scistack_gui/library_functions.py`. Some of these are
   only comments. Fix the wording.
4. **Non-import references that matter:**
   - `scifor/src/scifor/failures.py` `FRAMEWORK_PACKAGES`: remove
     `"scilineage"`. The frame classifier must still classify the new module
     as framework; `scicanonicalhash` is presumably already listed, so
     verify.
   - `scistacklog` `LAYERS` contains `"scilineage"`. If the moved module logs
     under `logging.getLogger(__name__)`, its logger becomes
     `scicanonicalhash.functions`, which is not a layer. Either add
     `"scicanonicalhash"` to `LAYERS` or log under an existing layer. Update
     `scistacklog/tests/test_log.py` and the README layer list. That README
     currently lists 8 layers while `LAYERS` has 12; fix the list at the same
     time and add a test that the README lists exactly `LAYERS`.
   - `sciduckdb/tests/test_fetch_locking.py` mentions scilineage; check.
5. Make `scidb/pyproject.toml` depend on `scicanonicalhash` only, dropping
   `scilineage`. Run the package-removal checklist for `scilineage`
   (`dev-install.sh`, CI loops, shards, the version test, `ruff.toml`,
   `tools/audit/run.sh`, `docs/internals/scilineage.md` + index,
   `CLAUDE.md`'s list, `scimatlab/README.md`).
6. `git rm -r scilineage/`. Sweep `scilineage`.
7. **Identity check.** Function hashes must not change, because they are
   stored in `_invocation.function_hash`. The code moves byte-for-byte, but
   confirm that `_is_user_defined` / `_STDLIB_DIR` / site-packages detection
   still behaves the same from the new location. The module's own file path
   does not affect the result: it hashes the *target* function. Add
   `test_function_hash_unchanged_by_move`, which hashes a fixed fixture
   function and asserts a hard-coded expected hex that the user captures
   **before** the move with:
   `python -c "from scilineage import compute_function_hash; import <fixture module> as m; print(compute_function_hash(m.f))"`.
8. User runs `python -m pytest scicanonicalhash/tests -x -q`, then
   `python -m pytest scidb/tests -x -q`.

## Stage 5 — shims and dormant APIs (B6 doc 04a, D3 doc 03)

1. Delete `scidb/src/scidb/hashing.py` (a re-export of scicanonicalhash).
   `grep -rn "scidb.hashing\|from .hashing" .` and point importers at
   `scicanonicalhash`.
2. **`SciDuck.save` / `load` / `list_versions` / `delete`:** no package
   outside sciduckdb calls them (`docs/design-review/03-sciduckdb.md`
   "Contract"). Verify again with
   `grep -rnE "\.(save|load|list_versions|delete)\(" --include='*.py' scidb/src scistack-gui/scistack_gui scistackplotdb/src scimatlab/src | grep -i duck`.
   - Then delete those four methods and any private helpers only they use
     (`_restore_types`? check usage first: `scidb` calls
     `_duck._restore_types` once, so keep it if so).
   - Delete or trim their tests in `sciduckdb/tests`.
   - **Keep** the variable-group methods. scidb uses `add_to_group`,
     `remove_from_group`, `list_groups` and `get_group`.
3. Rewrite `sciduckdb/README.md`. Its job is now codec + locked connection +
   `_schema` ids (3 short sections). The "three save modes" and "version
   management" text goes.
4. User runs `python -m pytest sciduckdb/tests -x -q`, then
   `python -m pytest scidb/tests -x -q`.

## Stage 6 — umbrella leftovers (U2, U3; doc 08; decision D7)

1. `git rm src/scistack/__init__.py`. It is a stray second `scistack` package
   at the repo root (`__version__ = "0.1.0"`, from commit `edff478e`). Remove
   `src/` if it ends up empty.
2. D7: delete `scistack/src/scistack/user_config.py` and
   `scistack/tests/test_user_config.py`, and drop the re-exports from
   `scistack/src/scistack/__init__.py`. That module docstring becomes "the
   `scistack` CLI". Check whether `tomli` is still needed in
   `scistack/pyproject.toml` (it was there for `user_config`).
3. User runs `python -m pytest scistack/tests -x -q`.

## Stage 7 — stale comments and docstrings (W5 docstring half, doc 04b/04c)

Doc-only edits in code. Behaviour is unchanged.
- `foreach_config.ForEachConfig` docstring: "creates a new version_keys
  group".
- `state.check_node_state` docstring: "`version_keys.__fn` matches fn".
- `state._get_latest_record_at_location` docstring: `version_keys` / `__fn`.
- `scidb/README.md`:
  - the exclusion paragraph ("hashed and stored in `version_keys`
    (`__schema_overrides_hash`)"): check against `database-model.md`;
    exclusions are now in `__scidb_schema_overrides`;
  - the Logging section: two sinks at INFO → journal `scistack.log` plus
    `scistack-debug.log` at DEBUG. Copy the scistacklog README's description.

Find more with
`grep -rn "version_keys\|_record_metadata\|_for_each_expected\|LineageFcn\|thunk" --include='*.py' scidb/src scifor/src scistack-gui/scistack_gui scimatlab/src`.
Fix the **comments and docstrings** only. Identifiers named `version_keys` are
renamed in P4 (W5 rename), not here.

## Stage 8 — stale `docs/claude` documents

1. Move to `docs/claude/archive/`, adding a first line
   `> Archived 2026-11-xx: describes the pre-2026-06 model. Current: database-model.md and docs/design-review/04b, 04c.`:
   - `docs/claude/scidb-for-each-internals.md`;
   - `docs/claude/scidb-identity-and-data-flow.md`;
   - `docs/claude/scistack-gui-backend-internals.md` (pointing to
     `docs/design-review/07-gui.md`).

   Grep for links to them and fix those links.
2. Add `docs/claude/design-review.md`, a 10-line pointer: what
   `docs/design-review/` is, its reading order, and that refactor IDs in
   commit messages refer to it. Future sessions look in `docs/claude` first
   (CLAUDE.md).
3. Mark the done rows in each design-review doc
   (`> **Done (date, commit):** …`), and tick P1 in the index.

## Progress
- (none yet)
