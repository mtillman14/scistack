# Plan: authored libraries, forks of forks, results across renames, tunables

Status (2026-10-23): ALL FOUR STAGES IMPLEMENTED; all tests pass (uncommitted). Design
reviewed through three rounds; A8/A9 accepted. Implementation notes: invocation_id
now folds in the function name (R2 needs two rows); test actor `_latest` fixed
(`_record_metadata` no longer exists). This plan is organised
around ONE OWNER PER CONCEPT (CLAUDE.md note 4): every stage below names the
owners it creates or changes and the consumers it rewires, and no step may
compute something an owner already answers.

Tests (the contract; they do not change with this restructuring):
- `scistack-gui/tests/library_fork_story.py`: the four-person story, the
  per-person session runner (`Chain`), the call log (`$EMG_CALL_LOG`).
- `scistack-gui/tests/test_library_fork_chain.py`: B1-B10, C1-C3, A8, A9,
  both front ends.
- `scistack-gui/tests/test_library_tunables.py`: D1-D2, E1-E8, GUI.
- `scistack-gui/tests/library_fork_actor.py`: one person's session.

Run: `cd scistack-gui && pytest tests/test_library_fork_chain.py tests/test_library_tunables.py -v`

---

## 1. Goal

Share -> install + fork -> edit -> share my fork -> someone else installs
(and forks) it -> it runs, from the GUI and from the terminal.

User requirements that shape the design:
- A library looks the same on every machine however many times it was
  shared. The only visible difference: unpackaged project code
  (`src/<prj>/`) vs packaged code (a library, mine or someone else's).
- Automate it: no hand-switching a project to its own library.
- Renaming must not re-run unchanged steps.
- Project code always trumps library code where they could conflict
  (Parameters, PathInputs). Variables keep today's error on conflicting
  definitions.
- Tunable values of a placed library pipeline are visible and editable from
  the project, this round.

## 2. Decisions (all accepted)

- **R1** A library you make (⇪ Share) or copy (✎ Make my own copy, under a
  new name) is a library you AUTHOR: it lives in `<project>/libs/<name>/`
  with the one library layout, the project uses it (one copy), and its
  pipelines are editable there. Installed libraries stay 🔒.
- **R2** When a rename leaves a function's code hash unchanged, its results
  are re-recorded under the new name without computing; otherwise the
  report says why not and the step needs a run.
- **D1** Project values trump library defaults (Parameters, PathInputs).
- **E** Tunables: Parameter nodes are project-level (auto-declared on
  placing); other step values stay per placement, listed in a project-wide
  view; "Make this a Parameter" links a placement to a project Parameter.
- **A8** `[library] derived_from` = full ancestry, newest first. Never who.
- **A9** `from <lib> import fn` works (generated `__init__.py` re-exports).
- Taken while writing the tests, not objected to: module-path collisions
  refuse naming both files; publishing is the ⇪ Share action on a pipeline
  of a library you author; pipeline ids survive share and copy.

## 3. Owners

The backbone of the implementation. "New" = to be written; everything else
exists and is reused.

| # | Concept | Owner (layer) | Consumers |
|---|---|---|---|
| O1 | Where a project's authored libraries live (`<root>/libs/<name>/`) and making their `src/` importable | `scifor.discovery` (owns project layout: `own_package_dir`, `PathInsert`). New `authored_library_dirs(root)` | O3, O20, share/copy/publish, the session bootstrap |
| O2 | Which packages are this project's libraries | `scidb.names.library_packages` (`packages` in scistack.toml) | everything |
| O3 | Whether a library is AUTHORED here | `scidb.names.authored_libraries()` (new): DERIVED from O2 + O1. No stored flag, no second list | O11, O12, O13, list rows, publish |
| O4 | A function's recorded name | `scidb.names.function_name` | registry, recording, canvas labels |
| O5 | A library's files: layout, pyproject, entities file (incl. A8 `derived_from`), generated `__init__.py` (A9) | `scidb.library` (gains an update mode: `write_library(..., existing="create" or "update")`) | the authoring service only |
| O6 | Reading a library: documents, defaults, schema keys, `derived_from`, dependencies | `scidb.library.read_library` (gains `dependencies`, `derived_from`) | copy, placement, sync |
| O7 | The rename plan: old label -> new label, module and file targets, MATLAB targets, collisions | `scistack_gui/services/library_authoring.plan_library` (new). Computed ONCE per action and passed as an object; never recomputed downstream | O5 input, O10, R2, report |
| O8 | Python import rewriting | `scistack_gui.code_rewrite.rewrite_imports` | O7 only |
| O9 | MATLAB call/handle qualification (bare -> `new.fn` and `old.fn` -> `new.fn`) | new `scistack_gui.matlab_rewrite`, moved out of `library_share._MatlabPlan` | O7 only |
| O10 | Switching the project to a library (packages, pipeline ownership, canvas relabel, R2 call) | `library_authoring.switch_project(db, plan)` (new) | share, publish, copy |
| O11 | This project's ids for a library's pipelines | `library_service.local_pipeline_ids`: existing `_library_pipelines` rows first, derived ids only when none | sync, list rows, O10 |
| O12 | Content of an authored library's pipeline | the project canvas (working copy). `pipelines/*.json` is OUTPUT, written only by publish. Sync never writes over a seeded authored pipeline | sync, publish, "unpublished changes" (via `scidb.library.definition_hash`) |
| O13 | Library pipeline read-only rule | `library_lock` (consumes O3) | the stores |
| O14 | Result identity and the skip rule | scidb (`invocation_id` computation, `invocation_exists`) | R2 builds new rows through these functions, never a copied formula |
| O15 | Code-hash equality | `scilineage.hashing.compute_function_hash`; MATLAB `scimatlab.source` | R2 |
| O16 | Re-recording results under a new name (R2) | new `scidb.rename_functions(db, mapping)` (history layer) | O10 (GUI and terminal) |
| O17 | Parameter / PathInput VALUES | the project's entities file + registry; a library's file holds DEFAULTS only | runs, tunables, placement check |
| O18 | What a library pipeline needs | `library_service._requirements` | placement check, auto-declare, `used_by_libraries`, tunables |
| O19 | Placement check and auto-declare (incl. D1 `project_values`) | `library_service.placement_check` / `declare_requirements` (called by placing; no second path) | place API, tunables |
| O20 | New library name validity (identifier, not importable, folder free) | `library_authoring.check_new_name` (new; absorbs today's checks in `share_as_library`) | share, copy, CLI |
| O21 | Step-value overrides | the placement's binding (`_pipeline_uses.binding`). Promote stores a REFERENCE to the Parameter, never its value | tunables API, project view, runs |
| O22 | Step-value key `fn.param` | scidb `PipelineBinding` (same format as `branch_params`) | tunables, promote, report `step_values` |
| O23 | Tunables of a placement (read model) | `library_service.tunables(db, use_id)` (new): composes O17, O18, O21, O22, `suggest_key_map`; stores nothing | GET per placement, GET project view (iterates placements, same function) |
| O24 | The authoring report | `library_authoring.AuthoringReport` (new) REPLACES `ShareReport` and `CopyReport` | GUI dialogs, CLI `--json`, logs |
| O25 | The `pip install -e` hint | one helper in `library_authoring` | O24 |
| O26 | Frontend: one API call per action, one tunable-row component | `ShareLibraryButton`, `LibrariesSection` (copy), new `TunablesPanel` + `TunableRow` reused by the project view | the canvas and sidebar |
| O27 | User docs for sharing libraries | `docs/sharing/libraries.ui.md`; concept note `docs/claude/linked-vs-independent-copies.md` | — |

### Rules that keep it one-owner
1. Share, publish and copy are thin entry points. Each only GATHERS SOURCES
   (a project submodule's closure; an authored library's canvas; an
   installed library's package) and then calls the same three owners in
   order: `check_new_name` (O20) -> `plan_library` (O7) -> `write_library`
   (O5) -> `switch_project` (O10). No entry point writes files, renames or
   switches anything itself.
2. Every output that mentions a new name (files, document labels, canvas
   labels, history rows, MATLAB calls, report) reads it from the one plan
   object (O7).
3. "Authored" is never stored (O3). "Which ids" is answered by O11 only.
4. The canvas owns an authored pipeline's content; files are output (O12).
5. Values are owned by the project (O17) or the binding (O21); defaults by
   the library. Nothing copies a value from one to the other except the
   explicit, logged auto-declare of a MISSING name (O19).
6. GUI and CLI call the same services; the CLI adds only argument parsing.

### Removed (second owners today)
- `copied_libraries` naming: `scidb.names.copied_libraries`,
  `COPIED_LIBRARIES_KEY`, the copied branch of `library_of`,
  `config.mark_library_copied`. Beta: no migration.
- `library_copy`'s own file writer and its Variable re-declaration into the
  project's entities file (the new library's entities file declares them).
- `library_share._MatlabPlan._qualify` (moves to O9), `ShareReport`,
  `CopyReport` (O24), `share_defaults`' `dest` (O1 decides the folder).

## 4. Flows, as calls to owners

- **⇪ Share a project submodule** (`POST /api/libraries/share {pipeline_id,
  name}`; `scistack library create --from-submodule X --name N`): sources =
  the submodule's closure (`canvas_snapshot`) + the project code it uses
  -> O20 -> O7 -> O5 create at O1's folder -> O10 (add to `packages`,
  pipelines owned by N with the same ids, canvas labels `fn` -> `N.fn`,
  O16 carries results).
- **⇪ Share a pipeline of a library you author** (publish): sources = the
  canvas (O12) + project code newly used on it -> O7 (collisions against
  what the library already holds) -> O5 update -> O10 (relabel newly
  carried functions, O16 for them).
- **✎ Make my own copy** (`POST /api/libraries/copy {name, new_name}`;
  `scistack library copy L --as N`): sources = the installed package (O6)
  -> O20 -> O7 (`L.x` -> `N.x`, `+L` -> `+N`, `derived_from` = [L, *L's]) ->
  O5 create -> O10 (remove L from `packages`, add N, re-own L's pipelines to
  N with the same ids via O11, relabel, O16).
- **Place a library pipeline** (`POST /api/pipelines/{parent}/uses`): O19
  declares what is missing (D1: existing project names untouched).
- **Tunables** (`GET /api/pipeline-uses/{use_id}/tunables`,
  `GET /api/tunables`, `PUT .../tunables`, `POST .../tunables/promote`):
  read through O23, write through O21 (and O17 for promote's new Parameter).

## 5. Stages

Each stage: owners created/changed -> consumers rewired -> removals ->
tests expected to pass -> logging. The user runs the tests after each stage.

### Stage 1: authored libraries (R1, A8, A9)
- Owners: O1 `authored_library_dirs` + sys.path (in the one place project
  code becomes importable); O3; O5 update mode + A8/A9; O6
  `dependencies`/`derived_from`; O7; O9; O10 without R2; O11; O12 in
  `sync_libraries`; O13 consumes O3; O20; O24; O25.
- Consumers: `library_share` and `library_copy` become entry points over
  O7/O5/O10 (or merge into `library_authoring`); `api/libraries.py`
  (`new_name`, no `dest`, `authored` in list rows); `library_cli`
  (`--as`, no `--into`); canvas relabel through `pipeline_store.rebase_node`
  / `relabel_manual_nodes` (verify first: reuse, don't add a third).
- Removals: section 3 "Removed".
- Tests: `TestAuthoredLibraries`, `TestSameShapeEverywhere`,
  `TestBobsLibrary`, `TestCarolInstallsBobsLibrary`, MATLAB, collisions,
  one-session, upgrade, wheel. `TestResultsCarryOver` still fails (Stage 2).
- Logging: `[library_authoring]` one INFO per function (old -> new, source
  file, carried or kept and why), per module target, per refusal with both
  sources; `[names]` authored set when it changes; `[library_service]` sync
  skip reason for authored pipelines; O11 which id source was used.

### Stage 2: results across renames (R2)
- Owners: O16 `scidb.rename_functions`, built on O14 and O15.
- Consumers: O10 calls it with the plan's mapping; O24 gains `carried`,
  `not_carried`.
- Tests: `TestResultsCarryOver`.
- Logging: `[rename_functions]` per function: hash before/after, records
  re-recorded, or the reason not carried; per downstream function in order.

### Stage 3: Parameters, PathInputs, tunables backend (D1, D2, E1-E8)
- Owners: O19 extended (`project_values`, auto-declare on placing); O21
  binding accepts a Parameter reference (scidb `PipelineBinding`, O22
  key format); O23; `used_by_libraries` from O18; E8 `step_values` in O24
  from O7's view of un-wired inputs.
- Consumers: place API calls O19; new tunables routes; parameters list rows.
- Tests: `test_library_tunables.py`.
- Logging: `[library_service]` placement: declared / kept-project / needs
  root; tunables writes (use, key, old -> new); promote (key -> Parameter).

### Stage 4: frontend and docs
- O26: Share dialog without a folder, showing old -> new and
  carried/not carried; copy asks for a name; `TunablesPanel` on the placed
  node and the project view, both from `TunableRow`; Parameter rows show
  `used_by_libraries`. Pure helpers + node tests; bundle rebuild.
- O27: user docs (Fork a library, Tunables, project-wins rule); the
  docs/claude note's known gaps updated.

## 6. Still to verify while implementing
- The existing node relabel machinery (`rebase_node` /
  `relabel_manual_nodes`) fits O10.
- The single place where project code becomes importable for a session (O1
  `PathInsert` is a context manager).
- MATLAB code hash after requalification (`scimatlab.source`): decides
  whether MATLAB steps are carried or listed as not carried.
- Schema-key vocabulary across hops (a pipeline adapted with `key_map`):
  untested; the tests use `subject` everywhere.

## 7. History (condensed)
- Original gap: Make my own copy put code in `src/<pkg>/<lib>/` with
  `copied_libraries` keeping `<lib>.fn`; `library_share` then treated those
  functions as another library's (left out, listed as a dependency).
- Round 1: layout goal -> R1. Gaps found in Parameters / PathInputs /
  boundary nodes.
- Round 2: renaming re-runs because `__fn` is in record ids, call ids and
  `branch_params`; rewriting history rejected; alias table rejected (two
  names); R2 accepted. Project trumps library. Tunables this round.
- Round 3: Variables keep the error; tunables split (Parameters project
  level, step values per placement + project view + promote).
- A8, A9 accepted. One-owner audit folded into this plan.
