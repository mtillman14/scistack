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
| Library pipeline (🔒) | seeded with DERIVED ids; re-synced when the document's `definition_hash` changes; project placements + bindings untouched | yes, follows installed version | `library_service.sync_libraries` |
| Edit inside a library pipeline | refused (`LibraryPipelineReadOnly`) during a user edit only | — | `library_lock` |
| ✎ Make my own copy | code copied to `src/<pkg>/<lib>/` (MATLAB `matlab/+<lib>/`), ALL placements switch, pipelines released (same ids), names stay `lib.fn` via `copied_libraries` | unlinked, all-or-nothing per library | `library_copy.make_own_copy` |
| × remove library | unlisted from `packages`; pipelines stay as they are; stays installed | unlinked | `config.remove_package` |
| Library drops a pipeline in a new version | reported as orphaned, left in place | no longer updates | `sync_libraries` |

## What a `.json` file does NOT carry

No code, no history, no data, no hidden edges (`include_hides=False`:
hides belong to the sender's run history). Hidden PORTS do travel. Missing
functions/variables come back as `unresolved_labels`. So a `.json` share is
only useful when the recipient already has the code (same project lineage or
same installed library).

## Known gaps (2026-10-23)

- **A library's wiring can't be edited anywhere**, author's project included
  (the lock applies wherever it is listed). Workaround documented to users:
  re-share to a new folder and copy `src/<lib>/pipelines/*.json` over.
  `create_library` refuses a non-empty folder, so there is no re-share-in-place.
- **Re-importing an updated `.json`** forks instead of updating, by design
  (the local copy and the new file differ). No "update from file" action.
- **Switching the source project to its own library** is manual (place the
  library pipeline, hide the original); functions get `lib.fn` names, so they
  run again under new identities (inferred, not tested).
- Re-adding a library after Make my own copy is untested (`copied_libraries`
  may conflict); the user docs deliberately don't describe it.

## Related

- `portability.md` ("Reusing code", import steps I9/I14)
- `edge-model.md`, `node-identity.md` (placement-qualified ids make
  independent graduation per scope possible)
- User docs: `docs/sharing/overview.ui.md` "Linked or independent?"
