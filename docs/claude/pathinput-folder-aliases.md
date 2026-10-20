# PathInput spelling aliases

## What it is

One schema-key value can be spelled several ways on disk. A common case is a
session spelled in full in a folder name and abbreviated in a file name:

```
Baseline/Xsens/S01_BL.xlsx
FollowUp/Xsens/S01_FU.xlsx
```

Use the **same placeholder in both places** and declare which spellings mean
the same value:

```python
scifor.PathInput(
    "{session}/Xsens/{subject}_{session}.xlsx",
    aliases={"session": {"BL": ["Baseline"], "FU": ["FollowUp"]}},
    name="xsens",
)
```

The dict key (`"BL"`) is the **canonical** value. It is what gets stored as
the `session` schema key. The listed spellings, plus the canonical itself,
all match on disk. Aliases only affect matching: they never decide how a
path is written, and they never touch identity (`to_key()` is the name
alone), so editing them is always safe.

## Matching rules (owner: `scifor/src/scifor/pathinput.py`)

- **`load()`** tries the literal path first. If that misses, the fallback
  scan (`_fallback_scan`) checks **each occurrence** of an aliased
  placeholder separately against the alias map. `Baseline/.../S01_BL.xlsx`
  matches `session="BL"`, and also `session="Baseline"`: metadata may carry
  any spelling.
- **`discover()`** canonicalizes each capture **before** its "same key, same
  value" consistency check. Two spellings of one canonical don't conflict.
  Two different canonicals in one path (`Baseline/.../S03_FU.xlsx`) do, and
  that file is not a match.
- **Numeric beats alias.** If a value is numeric-like and eligible for the
  numeric fallback, that placeholder is matched numerically
  (`_fallback_segment_regex` checks numeric first).
- **The capture map holds the last occurrence** of a repeated key, so
  `load_with_captures(..., report="all")` can under-report: in the example
  above it reports nothing, because the filename's `BL` equals the given
  value. No caller relies on alias resolutions being reported.
- Several matching files (`Baseline/` and `1. Baseline/` both present)
  raise "matched N files".

## Aliases are not zero-padding ambiguity (2026-10-20 fix)

`scidb.foreach._load_pathinput_checked`, and its MATLAB mirror
`+scidb/+internal/load_pathinput_checked.m`, raise `SchemaKeyTypeError` when
an undeclared schema key needed a spelling bridge. They used to treat **every**
bridge that way. So an aliased `{session}` folder (`BL` resolved to
`Baseline`) raised a misleading "zero-padded filenames" error.
`load_with_captures(report="numeric")` now reports numeric bridges only, and
both policy checks ask for that. scifor owns the classification.

## Declaring it

| Surface | Form |
|---|---|
| Python | `PathInput(..., aliases={"session": {"BL": ["Baseline"]}})` |
| MATLAB struct | `aliases=struct('session', struct('BL', "Baseline"))`: canonicals must be MATLAB identifiers |
| MATLAB JSON text | `aliases='{"session": {"01": ["Baseline"]}}'`: any canonical; this is what generated code emits |
| `scistack_entities.toml` | `X = { template = "...", aliases = { session = { BL = ["Baseline"], "01" = "Pre" } } }` |

TOML details (owner `scidb.entities`, `_parse_path_input_aliases` and
`_render_aliases`):
- Aliases live per arm. In an alternates array, each table carries its own.
- A lone spelling may be a bare string; it is normalized to a list.
- A canonical that isn't a bare TOML key (`"1. Baseline"`) is written quoted.
  `render_value`'s identifier-only key rule is deliberately not used here.
- An alias key that isn't a placeholder, or a spelling claimed by two
  canonicals, rejects **that entry only**, with PathInput's own message.

## Where aliases must ride along (one owner each)

Every writer that re-renders a declaration from a dict drops aliases unless
they're in the dict:

| Seam | Owner |
|---|---|
| Registry object → display dict (`aliases` per arm, `{}` when none) | `graph_builder.path_input_display` |
| Sidebar edit (`update_path_input`: TOML, `.py`, `.m` renderers) | `path_input_service` → `render_path_input_value` / `source_edit.render_path_input` / `matlab_parser.render_matlab_path_input` |
| Deep copy | `layout_service.deep_copy_path_input` (now copies alternates too) |
| Portability import | `portability_service` → `create_path_input(..., aliases)` |
| Bundle import rewrite (key map renames alias keys like placeholders) | `scidb.bundle._rewrite_path_inputs` / `_rename_alias_keys` |
| MATLAB run command | `matlab_command_service._declared_path_input` → `api.matlab_command._format_path_input_arm` |
| MATLAB aliases argument text | `matlab_parser.render_matlab_aliases` (JSON char, never a struct) |
| MATLAB `scidb.entities()` | `scimatlab.bridge.load_entities` sends `aliases_json` → `+scidb/entities.m` |

Library sharing (`library_share`) ships template strings only. Like root
folders, aliases describe one dataset's spellings.

## GUI

PathInput sidebar, **Spelling Aliases** section: pick a placeholder, type
the stored value and the other spellings (comma-separated), then Add. Each
row has a ×. Pure edits are in `frontend/src/components/Sidebar/pathInputAliases.ts`;
validation is scifor's, and a refused write shows its message. Every
`update_path_input` call sends `aliases`, because the update rewrites the
whole declaration. The canvas node shows a `≈N aliases` badge.

## Diagnostics

- `scidb.log`: `[path_input_service] update_path_input: ... aliases=...`, and
  `generate_matlab_command: ... emits N template(s) [name: tpl aliases=[keys]]`.
- scifor DEBUG: `pathinput_fallback: matched ... (captures, numeric keys,
  alias keys, report)` and `pathinput_alias_unresolved` for an on-disk
  spelling with no entry.
- `[entities] Declared path input X with N template(s), alias keys [...]`.

## Tests

- `scifor/tests/test_pathinput_aliases.py`: `TestOneKeyTwoSpellingsInOnePath`, `TestReportNumericOnly`
- `scidb/tests/test_schema_key_types.py::TestForEachAliasBridge`
- `scidb/tests/test_entities_toml.py::TestPathInputAliases`
- `scidb/tests/test_bundle.py::test_path_input_aliases_follow_their_placeholder_through_a_key_map`
- `scimatlab/tests/test_bridge_project_root.py::TestLoadEntitiesAliases`
- MATLAB: `TestPathInput.m` (`test_alias_json_*`, `test_alias_one_key_two_spellings_in_one_path`), `TestSchemaKeyTypes.m::test_alias_bridge_needs_no_declaration`, `TestEntitiesAliases.m`
- GUI: `test_matlab.py::TestPathInputAliasesInMatlabCommand`, `test_entity_update_endpoints.py::TestPathInputAliasesEndpoint`, `test_pipeline_scopes.py::test_deep_copy_keeps_aliases_and_alternates`
- Frontend: `pathInputAliases.test.ts`
