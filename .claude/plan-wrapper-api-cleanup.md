# Plan: clean up the wrapper syntax and public API (2026-11-12)

Status: all four picked stages BUILT 2026-11-12, uncommitted; pytest + MATLAB suites not yet run (user runs them).

The request covers two different things: the **docs** (how the syntax is shown)
and the **code** (the signatures themselves). Each stage below is one commit, and
the user runs the suites between commits (memory: user runs tests). It's beta, so
there are no deprecation shims (memory `feedback_beta_no_deprecation`).

## What's untidy today (verified in source)

| # | Where | Problem |
|---|---|---|
| 1 | MATLAB `scidb.Variant` | CORRECTED after reading the bridge: `fn=`/`code_version=`/`run_options=` already reached Python by accident (`wire.py` rebuilds `Variant(inner, **pairs)`), untested. The real bug was MATLAB flattening nested Variants, so an outer `fn=` re-namespaced the inner pins. Also `+scidb/Merge.m` called the non-existent `py.scidb.fixed.Fixed` (MATLAB `Merge(..., Fixed(...)).to_csv` broken). |
| 2 | Python `PathInput(path_template, root_folder=None, regex=False, aliases=None, key_regex=None, *, name)` | The one required option, `name`, comes last. `root_folder` and `regex` can be passed positionally (`PathInput(t, "/data", True, name=...)`), but MATLAB and TOML accept them by name only. CORRECTED: zero Python call sites pass it positionally (checked with a paren-balancing scan); every generator already writes keywords. |
| 3 | `ColumnSelection(data, columns=None, ...)` | The docstring says the default is `[]` and that `None` is "accepted for backward compatibility". That's a compat alias in beta. MATLAB has no `excl_columns` at all (Python has had it since the column-failure work). MATLAB takes `iterate` positionally, Python by keyword. |
| 4 | `scidb` exports | `scidb.for_each(locations=...)` is documented, but `LocationFilter` isn't re-exported from `scidb`, so you have to import it from `scifor`. |
| 5 | Docs (`docs/api/scifor.md`, `scidb.md`) | After the last two edits, each wrapper shows up three times: a row in the purpose table, a line in the syntax block, and a row in the keyword table. The same facts are said three times, which breaks the one-owner rule. |

## Deliberately not proposed (grug: say no)

- **Renaming wrappers or reordering `for_each`'s positional arguments.** Too much churn for too little gain.
- **Trimming `scifor.__all__`** (`narrow_frame`, `OptionSpec`, `UNSET`, ...). scidb imports these across the package boundary, and the docs already list only the user-facing names.
- **Hiding `GlueSpec`'s `language` / `source_file` / `source_text`.** The bridge sets them, and the docs already say users don't need them.
- **Dropping the bare `ColName` (no parentheses) form.** It has 3 users and costs nothing.
- **Accepting a struct for MATLAB `locations=`** (it takes JSON text today). Hardly anyone writes it by hand; the GUI generates it.

## Stages

### docs_one_entry_per_wrapper (docs only)
Replace the purpose table, the syntax block and the keyword tables with **one
entry per wrapper**:
- a heading,
- the signature,
- a short keyword table (only when it has keywords),
- a Python/MATLAB example pair.

The purpose table shrinks to a name → one-line index linking to each entry. In
`scidb.md`, the shared wrappers link to the scifor entries instead of being
repeated.

### matlab_variant_parity
- `+scidb/Variant.m` peels `fn`, `code_version` and `run_options` off the
  name-value pairs and stores them as properties.
- `+scidb/for_each.m` ships them in the `variant` spec.
- The Python bridge passes them to `scidb.Variant(...)`. Python stays the one
  owner of what they mean.
- Logging: the bridge logs the rebuilt Variant at DEBUG.
- Test: a MATLAB test pinning `fn=` / `code_version=`, plus a Python bridge test
  checking that a spec with `fn` rebuilds `Variant(X, fn=..., ...)` and not a
  branch param named `fn`.

### pathinput_keyword_only
- New signature:
  `PathInput(path_template, *, name, root_folder=None, aliases=None, key_regex=None, regex=False)`.
  Same order as MATLAB and TOML; `name` goes first among the options because
  it is required.
- Fix every positional call site (scripted, then grepped to confirm none remain).
- Codegen (GUI script export, MATLAB generator) already writes keywords, so this
  is verified rather than changed.
- Test: a positional `root_folder` raises `TypeError`.

### column_selection_tidy
- The default `columns` becomes `None` with one meaning ("every data column"),
  documented as such. The words "backward compatibility" go.
- MATLAB gains `excl_columns`, as a name-value pair to match Python.
- `iterate` is accepted as `iterate=true` as well as positionally. The
  positional form stays, because `for_columns` builds it.
- Test: MATLAB `excl_columns` parity with the Python test.

### scidb_reexport_locationfilter
- Add `LocationFilter` to `scidb`'s imports and `__all__`.
- Docs then import it from `scidb` in the scidb examples.

## Outcome (2026-11-12)

- **matlab_variant_parity**
  - `+scidb/Variant.m` no longer merges nested Variants; Python's `Variant` owns
    the merge and `fn=` per level. It rejects `EachOf`.
  - `wire.py` logs the rebuilt pins at DEBUG.
  - `Merge.m` now calls `py.scifor.Fixed`.
  - Tests: `scimatlab/tests/test_bridge_variant_options.py` (new);
    `TestVariant.m` gains nested-stays-nested, conflict-at-run, `fn=` pin and
    rejects-EachOf tests (the old construction-time conflict test was replaced).
- **pathinput_keyword_only**
  - Signature: `PathInput(path_template, *, name, root_folder=None, aliases=None, key_regex=None, regex=False)`.
  - Test added to `scifor/tests/test_pathinput_name.py`.
- **column_selection_tidy**, reduced to wording.
  - `None` is the real default, and the bridge sends it, so it is not a compat alias.
  - SAID NO to MATLAB `excl_columns` (a new feature through the loop, the wire and
    `for_columns`; nothing in the GUI uses it) and to a second spelling of `iterate`.
- **scidb_reexport_locationfilter**: done, no test (an import-only test is
  against memory `feedback_no_import_guard_tests`).
- **docs_one_entry_per_wrapper**
  - `docs/api/scifor.md` "Input wrappers": index table, then one section per wrapper.
  - `docs/api/scidb.md` "Input wrappers": scidb-only wrappers; `GlueSpec`
    moved into "Glue".
  - `docs/api/for-each.md` is now a pointer page; its `#column-selection`
    anchor is kept as a "Moved" stub.
