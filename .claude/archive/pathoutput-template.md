# Plan: scifor.PathOutput (per-combo + per-column output filename template)

Supersedes `.claude/colname-path-template.md`. The `ColName(template)` form is
**reverted**; templated output paths get their own honest wrapper.

## Why
`ColName(path)` overloaded a *column-name marker* to emit a *filesystem path*.
`PathInput` is the wrong host too: in scidb it drives filesystem **discovery** and
**per-combo loading** of *existing input files* (Step 3, `PerComboLoader`, regex
match-or-FileNotFoundError). An *output* filename is the opposite — a file to be
written that doesn't exist yet, reaching the function as a plain argument.

## Concept
`scifor.PathOutput(template)` — a pure output-path template. Substitutes:
- **per-combo metadata** keys: `{subject}`, `{session}`, ... (any key in the combo)
- **per-column** token `{ColName}` (the current for_columns column)

Literal `str.replace` (like PathInput), so other braces pass through. Returns a
`Path` when given a `Path`, else a `str`. No discovery, no regex, no `.load`.

```python
"data_column": scifor.ColName,                  # back to: just the column name
"filename":    scifor.PathOutput(root / "{subject}_{ColName}_anova2way.pdf"),
```

## Resolution timing (scifor only — scidb delegates the loop to scifor.for_each)
- PathOutput is a **constant** (`_is_data_input` False) -> lands in `constant_inputs`.
- **Non-iterate** combo: resolve with `metadata` (no column) just before `_call_fn`.
- **for_columns** combo: resolve per column inside `_run_column_iteration` with
  `metadata` + the current column. (`metadata` is newly threaded into that fn.)
- Guard: a PathOutput whose template contains `{ColName}` but with **no** iterate
  input is a hard error, mirroring the deferred-ColName guard.

scidb needs **no loop changes**: `_convert_inputs` already passes non-loadable,
non-PathInput values through as constants, and `state.full_combos` carries the
schema-key metadata. scidb only re-exports `PathOutput`. (PathOutput has no
`.load`, is not in `_is_loadable`'s tuple, and is not a `PathInput`, so discovery
and per-combo loading never touch it.)

## Changes

### scifor
- **new** `scifor/src/scifor/pathoutput.py`: `PathOutput(template: str|Path)` with
  `resolve(metadata=None, column=None)`, `has_column_token`, `__name__`/`__repr__`.
  Token constant `COLUMN_TOKEN = "{ColName}"`.
- `scifor/src/scifor/__init__.py`: export `PathOutput`.
- `scifor/src/scifor/foreach.py`:
  - import `PathOutput`.
  - after Step 6.5: if any constant PathOutput `has_column_token` and no
    `iterate_params` -> raise (needs-iterate error).
  - non-iterate call site: `_call_fn(fn, _resolve_path_outputs(filtered_inputs,
    metadata, None), n_outputs)`.
  - `_run_column_iteration(..., metadata)`: per column, resolve PathOutput params
    with `metadata` + `col`.
  - helper `_resolve_path_outputs(kwargs, metadata, column)`.

### scidb
- `scidb/src/scidb/__init__.py`: re-export `PathOutput` (from `scifor`).
- `scidb/src/scidb/foreach_config.py` `_get_direct_constants`: exclude `PathOutput`
  alongside `ColName`. Both are per-combo resolution markers, not scalar
  constants, and a raw `PathOutput` would break the un-`default=str` `json.dumps`
  of `config_keys` (foreach.py:501). The output path is write bookkeeping, not
  computation identity, so excluding it from version keys is also semantically
  right (changing only the output filename won't fork the version group).

### Revert ColName(template)
- `scifor/src/scifor/colname.py` -> original 2-form version (drop template/resolve/
  has_template/__repr__).
- `scifor/src/scifor/foreach.py` `_run_column_iteration`: deferred ColName back to
  `call_kwargs[name] = col`.
- `scidb/src/scidb/colname.py` -> original 2-form version.
- `scidb/src/scidb/foreach.py` `_convert_inputs`: deferred branch back to
  `SciforColName()`.

## Tests
- scifor `test_foreach_standalone.py`: remove the 4 ColName-template tests; add
  PathOutput tests — `{ColName}` per column (Path + str type preserved), combo
  metadata substitution, metadata+column combined, no-token pass-through,
  `{ColName}`-without-iterate raises.
- scidb `test_for_columns.py`: remove the 2 ColName-template tests; add a
  PathOutput end-to-end test (metadata + `{ColName}` resolved through
  scidb.for_each, reaching the fn as a plain Path).
