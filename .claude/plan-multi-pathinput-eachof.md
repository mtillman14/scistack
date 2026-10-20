# Plan: two PathInputs on one port = EachOf (2026-10-20)

**Status:** implemented 2026-10-20 (approved), uncommitted, tests pass. Step 6 resolved by making scidb take a PathInput's declared name from its key (`provenance_save._pathinput_name`); `parameter_names` omits EachOf PathInput args.

## Problem

scidb.log 2026-10-20 16:25–16:30: `cbrace_cosmed_path` and
`cbrace_cosmed_path_controls` were both drawn onto `load_cbrace.filePath`. The
GUI ran ONE of them, the last drawn:

- `[edge_resolver] parameter 'filePath' rebound by edge manual__661gi7: pathinput 'cbrace_cosmed_path_controls' replaces pathinput 'cbrace_cosmed_path'`
- `[graph_builder] ... 2 PathInputs drawn onto 'filePath' — a history node binds ONE; using the last drawn`

This is the documented rule ("a drawn PathInput replaces the recorded one",
docs/claude/edge-model.md, `manual_path_input_overrides`). The user's
decision: several PathInputs on one port are an **EachOf**, the same as several
variables.

## Design

One shape and one owner:
- **Binding:** `{"kind": "pathinput", "ref": [name, ...]}` is always a list,
  like variables. The one reader is `edge_resolver.path_input_names(binding)`.
- **Identity:** each arm records its OWN PathInput name, so a run writes one
  wiring per arm. As with a variable EachOf, the node claims every arm's
  wiring (`_claim_stated_wirings`, varied one param at a time), so the second
  arm can't fork a node (docs/claude/node-identity.md).

## Steps (each with tests)

1. `edge_resolver.resolve_function_edges`: accumulate PathInput edges on one
   handle into the ref list. Variables already do this. Not a rebind warning.
2. `graph_builder.manual_path_input_overrides` returns `{param: [names]}`.
   It replaces the recorded PathInput when drawn, and gives an EachOf when
   several are drawn. Drop the "binds ONE" warning, and log the arms at INFO.
3. `_claim_stated_wirings`: claim one wiring per PathInput arm.
4. Python run (`execution_service._build_inputs`): several names become
   `EachOf(*arms)`. Each declaration's own alternates are flattened into it.
5. MATLAB (`matlab_command_service._collect_edge_path_inputs` /
   `resolve_path_input_params`): one dict per param, with `arms: [...]` of
   `_declared_path_input` dicts. `_format_path_input` emits
   `scifor.EachOf(...)` over every arm of every declaration.
6. Remaining readers of a PathInput binding's ref: variant_resolver (313,
   598, 651), execution_service (1098, 1888, 1964), custom_plot_service
   (136), matlab_command_service (207 `parameter_names`). Each goes through
   `path_input_names`. `parameter_names` maps a param to one declared name,
   so an EachOf param records each arm's own name. Verify scidb's
   `parameter_names` bridge accepts per-arm names. If it doesn't, scope that
   step first.
7. Schema level: an EachOf of PathInputs already contributes the union
   (`scidb.schema_level`). Confirm it does across different names.
8. Docs: edge-model.md "kind-specific" list, alternate-pathinput-templates.md
   (two names vs one name with alternates), GUI manual testing §.

## Not changing

- Identity stays name-only. Two names are two PathInputs (identity-layers doc).
- A single drawn PathInput still replaces the recorded one.
