# PathInput aliases: test, TOML grammar, native GUI editing (2026-10-20)

User ask: a template like `Baseline/Xsens/{subject}_BL.xlsx` spells the session
two ways. `scifor.PathInput(aliases=...)` already supports this
(`{session}/Xsens/{subject}_{session}.xlsx` + `aliases={"session": {"BL": ["Baseline"]}}`),
but (a) no test covers one key appearing twice with different spellings,
(b) `scistack_entities.toml` refuses `aliases`, (c) the GUI cannot show or edit them.

## Shape (one owner each)

| Concept | Owner |
|---|---|
| Matching semantics | `scifor.pathinput.PathInput` (unchanged) |
| TOML `aliases` grammar, parse + render | `scidb.entities` (`_parse_path_input_aliases`, `render_path_input_value(..., aliases=)`) |
| Registry object -> display dict (now with `aliases` per arm) | `graph_builder.path_input_display` |
| Display dict -> MATLAB command | `matlab_command_service._declared_path_input` -> `_format_path_input_arm` |
| aliases -> MATLAB argument text | `matlab_parser.render_matlab_aliases` (JSON char) |
| MATLAB accepts struct OR JSON text | `+scifor/PathInput.m` `aliases_to_py` |

TOML form (per arm, table form only):
```toml
xsens = { template = "{session}/Xsens/{subject}_{session}.xlsx", aliases = { session = { BL = ["Baseline"] } } }
```
A bare string spelling is accepted and normalized to a list. Canonical keys
that are not bare TOML keys (`"01"`, `"1. Baseline"`) are written quoted.

Aliases never touch identity (`to_key()` is the name), so editing them is safe.

## Stages
1. scifor regression test: same key twice, different spellings (load + discover).
2. scidb.entities parse/render + tests; source_edit + matlab_parser renderers.
3. GUI backend: display dict, update/create/deep-copy plumbing, MATLAB command emission, bundle import rewrite (kmap renames alias keys). Logging at each seam.
4. MATLAB `PathInput.m`: `aliases` accepts JSON text (generated code needs non-identifier canonicals).
5. Frontend: "Aliases" section in PathInputSettingsPanel; rebuild both vite targets.
6. Docs: `docs/claude/pathinput-folder-aliases.md`, GUI manual testing entry.
