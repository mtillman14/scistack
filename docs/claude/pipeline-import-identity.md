# Pipeline Import/Export Identity Model

Status: **implemented** (2026-08-14, user-reported bug fix). See
`.claude/plan-pipeline-import-identity.md` for the as-drafted record. Code:
`scistack_gui/services/portability_service.py` (`_resolve_pipeline`,
module docstring's "Identity-based reuse"), `scistack_gui/stores/pipeline_store.py`
(`create_pipeline`'s optional `pipeline_id`, `get_pipeline`,
`list_all_pipelines`). Tests: `scistack-gui/tests/test_portability.py`
(`TestSubmoduleContentDedup`, `TestHiddenPipelineReimport`,
`TestExportImportSameDatabase`).

## The bug

A pipeline named "test (imported)" was hidden (user-facing "delete" — see
`feedback_never_delete_mark_hidden`: hide never actually deletes data),
then the same export file was re-imported. Import raised the raw
`create_pipeline` error: `a pipeline named 'test (imported)' already
exists`, instead of restoring the hidden pipeline.

**Root cause:** import's name-collision pre-check (`_unique_name`) was
seeded from `list_pipelines()`, which excludes hidden pipelines, while
the actual creation guard (`create_pipeline`) checks names against *all*
pipelines, hidden included (deliberately — see
`test_duplicate_name_rejected_even_when_hidden`). So a name pre-cleared
by the visible-only check could still collide at creation time when the
taken name belonged to a hidden pipeline.

Separately: the **root** pipeline of any import was *never* matched
against existing local content at all — only non-root submodules got
name-based content dedup. So even a byte-identical re-import of something
you already had (hidden or not) always created a redundant "(imported)"
duplicate instead of restoring/reusing it.

## The broader question it raised

Should sub-pipeline names be globally unique, given pipelines are meant
to be shared across users via import/export? **No** — two different
users' independently-created, same-named pipelines with different
content are expected to coexist. Names are just a mutable display label,
de-duplicated by suffix on collision; they are never a blocking
uniqueness constraint for reuse decisions.

## The model: `pipeline_id` as portable identity

`_pipelines.pipeline_id` (`pipe_{uuid4().hex[:12]}`, minted once at
creation) was already exactly the right stable identity — it just wasn't
being preserved across import (the doc used to say "entirely fresh ids,
never reuses/collides with local ids"). No new schema/column was needed;
import now simply **preserves the id from the document whenever
possible**, instead of always minting a fresh one.

Applied uniformly to root **and** every submodule in the closure
(`_resolve_pipeline`, post-order — children resolved before parents so a
parent's own content signature can include its already-resolved
children):

1. **Local pipeline exists with this exact `pipeline_id`, content matches**
   (via the existing recursive `_content_signature` comparison) →
   **reused in place**, unhidden if it was hidden. No new pipeline row.
2. **Local pipeline exists with this exact `pipeline_id`, content
   differs** (locally edited since export, or independently diverged) →
   **forked**: a fresh id is minted, name suffixed via `_unique_name`
   against *all* local names (hidden included, closing the original gap).
   The existing local pipeline is left untouched.
3. **No local pipeline holds this id at all** → created fresh,
   **preserving the imported id**; name only suffixed if it collides with
   some *other* pipeline's name (different identity, e.g. two users'
   independent same-named pipelines).

A consequence worth remembering: importing an **unchanged** document back
into the very database it came from is now a no-op reuse, not a
duplicate — see `TestExportImportSameDatabase.test_unchanged_reimport_is_a_noop_reuse`.
Previously same-db reimport always produced a fresh, renamed copy of
itself.

## Known related gap (not fixed here, out of scope)

`scope_service.duplicate_pipeline` (the manual "duplicate" UI action)
still does not auto-suffix on a name collision — it lets
`create_pipeline`'s `ValueError` propagate straight to the user if they
type an already-taken name. That's a separate, manual-name-entry flow
untouched by this fix; worth revisiting if the same auto-suffix ergonomics
are wanted there.
