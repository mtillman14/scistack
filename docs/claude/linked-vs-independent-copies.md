# Linked vs independent copies of a pipeline

Written 2026-10-23 to back the GUI Sharing docs (`docs/sharing/*.ui.md`).
Answers one question: when a pipeline is reused, does an edit at the source
reach the reuse? "Linked" is only about the **canvas**. Results are
content-addressed (function + inputs + code + settings), so the skip check is
global regardless of how a pipeline got onto a canvas.

## The three sizes

```
Project            .scistack bundle (scidb/bundle.py; GUI ⇩ export project)
└── Hypothesis     a pipeline tagged in _hypotheses (a tab)
    └── Submodule  a pipeline placed via _pipeline_uses (child_pipeline_id)
```

A hypothesis and a submodule are both pipelines; the hypothesis tag only
decides tab vs Submodules list. Sharing one moves the same document shape.

## Rules, with owners

| Action | Result | Linked? | Owner |
|---|---|---|---|
| Place a submodule twice (drag from Submodules) | two `_pipeline_uses` rows, same `child_pipeline_id` | yes, one pipeline | `add_pipeline_use` |
| ⎘ Duplicate (submodule node) | its OWN nodes copied with fresh ids; nested placements keep the same `child_pipeline_id` (fresh use_id, binding copied) | own steps no; nested submodules still shared | `scope_service.duplicate_pipeline` |
| ⎘ on a hypothesis tab | `duplicate_pipeline` + `tag_as_hypothesis` | same as above | `duplicate_hypothesis` |
| ⇩ / ⇧ import `.json` | closure document; `pipeline_id` preserved | no: snapshot | `portability_service` |
| Import, id known locally, identical content (recursive `canvas_snapshot.signature`) | local pipeline REUSED (unhidden) | n/a | `_resolve_pipeline` |
| Import, id known locally, content differs | FORK: fresh id, "(imported)" name suffix; local untouched | no | `_resolve_pipeline` |
| Import, globals (Parameter/PathInput/Sweep) by name | local REUSED; missing PathInput/Sweep materialised into source | n/a | `portability_service` |
| Import `.scistack` | always a NEW project, code copied into `src/<pkg>/` | no | `scidb.bundle.import_project` |
| Library pipeline (🔒, installed) | seeded with ids from existing `_library_pipelines` rows, else DERIVED (`local_pipeline_ids`, O11); re-synced when the document's `definition_hash` changes; project placements + bindings untouched | yes, follows installed version | `library_service.sync_libraries` |
| Library pipeline (✏, AUTHORED: `libs/<name>/`, `scidb.names.authored_libraries`) | the canvas is the working copy; NEVER re-synced from its files (O12); editable (`library_lock` skips it) | no: the files are output, written by ⇪ publish | `library_authoring.share` (publish) |
| Edit inside a library pipeline | refused (`LibraryPipelineReadOnly`) during a user edit only | — | `library_lock` |
| ⇪ Share a submodule | library written to `libs/<name>/`; project lists it; its pipelines OWNED by it with the SAME ids; canvas relabelled `fn` -> `name.fn`; results carried (`scidb.rename_functions`) where the code hash is unchanged | the project now uses its own library | `library_authoring.share` |
| ✎ Make my own copy | NEW name: library written to `libs/<new>/` (same layout, `derived_from`), old unlisted, ALL placements re-owned (same ids), labels `old.fn` -> `new.fn`, results carried | unlinked, all-or-nothing per library | `library_authoring.copy` |
| × remove library | unlisted from `packages`; pipelines stay as they are; stays installed | unlinked | `config.remove_package` |
| Library drops a pipeline in a new version | reported as orphaned, left in place | no longer updates | `sync_libraries` |

## What a `.json` file does NOT carry

No code, no history, no data, no hidden edges (`include_hides=False`:
hides belong to the sender's run history). Hidden PORTS do travel. Missing
functions/variables come back as `unresolved_labels`. So a `.json` share is
only useful when the recipient already has the code (same project lineage or
same installed library).

## Fixed by the fork chain (2026-10-23, `.claude/plan-library-fork-chain.md`)

- A library's wiring is editable in the project that AUTHORS it (B5/B6): the
  canvas is the working copy, ⇪ publishes (`write_library(existing="update")`:
  adds code, never overwrites a changed file, merges declarations).
- The source project switches to its own library automatically (B1), and a
  rename carries results whose code hash is unchanged (R2,
  `scidb.rename_functions`); `invocation_id` folds in the recorded function
  name since then, so the same code under two names is two activities.
- `copied_libraries` is gone: a copy is a library under a new name (B2).

## Known gaps (2026-10-23)

- **Re-importing an updated `.json`** forks instead of updating, by design
  (the local copy and the new file differ). No "update from file" action.
- **Unpublished changes** of an authored pipeline are not flagged in the UI
  (the owner row's `definition_hash` vs a fresh capture would say so).
- **MATLAB steps** are never carried across a rename when their calls were
  requalified (the `.m` text, hence its hash, changed): reported `not_carried`.
- **Schema-key vocabulary across hops** (a pipeline adapted with `key_map`,
  then copied) is untested.
- A carried record's id is content-addressed but not the id a fresh run would
  write (version keys are not stored, so the save-time recipe cannot be
  replayed); the skip gate and node state never compare record ids, only
  invocations.

## Related

- `portability.md` ("Reusing code", import steps I9/I14)
- `edge-model.md`, `node-identity.md` (placement-qualified ids make
  independent graduation per scope possible)
- User docs: `docs/sharing/overview.ui.md` "Linked or independent?"
