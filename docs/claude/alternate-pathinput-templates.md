# Alternate PathInput templates

A PathInput can name several file templates under one name. These are its
**alternates**. It is declared as a list in `scistack_entities.toml`:

```toml
[path_inputs]
cbrace_cosmed_path = [
  "data/CBrace II Cosmed Data Summaries Ottobock 6MWT.xlsx",
  "data/CBrace II Cosmed Data Summaries Ottobock 6MWT CONTROLS ONLY.xlsx",
]
```

The same thing in Python source is
`scidb.EachOf(scidb.PathInput(a, name=N), scidb.PathInput(b, name=N))`.

## What it means

The declaration is an `EachOf` axis. The function runs once per template, and
the results are concatenated. Every arm carries the **same name**, and a
PathInput's identity is its name alone: `PathInput.to_key()` hashes
`name` only (see `identity-layers-pathinput.md`). So both arms produce the
same version keys. They don't collide only because the files hold different
schema locations, such as different subjects. If two templates write the same
location, the newer save wins (same-invocation-latest).

## Owners

| Concept | Owner |
|---|---|
| TOML list → `EachOf(PathInput, …)` | `scidb.entities` (`_build_path_input_arms`) |
| Registry object → display dict `{template, root_folder, alternate_templates}` | `scistack_gui.domain.graph_builder.path_input_display` |
| Display dict → the MATLAB command's PathInput dict | `matlab_command_service._declared_path_input` |
| Which PathInputs a MATLAB command binds (history + edges) | `matlab_command_service.resolve_path_input_params` (single-node AND pipeline routes) |
| Dict → MATLAB expression (`scifor.EachOf(...)` when alternates) | `api.matlab_command._format_path_input` |
| Expanding the axis at run time | `+scidb/for_each.m` Step 0 (MATLAB), `scidb.foreach` (Python) |
| Schema level of an EachOf of PathInputs (union of placeholders) | `scidb.schema_level` |

## Python vs MATLAB route

- **Python run:** `execution_service` binds the live registry object
  directly, which is already the `EachOf`. Every alternative runs.
- **MATLAB run:** the GUI writes a MATLAB script, so the registry object has
  to be turned into text. It goes registry → `path_input_display` →
  `_declared_path_input` → `_format_path_input`. Each arm becomes a
  `scifor.PathInput(template, name=N)`, and the arms are wrapped in
  `scifor.EachOf(...)`.

### The 2026-10-20 bug

`_collect_edge_path_inputs` copied only `template` and `root_folder` from the
display dict, and `_format_path_input` could only emit one `PathInput`. A MATLAB
run therefore discovered the primary template alone. In `scistack.log`:
`pathinput_discover: 1 combo(s)` and `inputs: {filePath: <scifor.PathInput>}`.
The fix routes every MATLAB PathInput dict through `_declared_path_input`.

### History never decides the templates

DB history records the one template each past run used, as `to_spec()` JSON
in `input_types`. An older row may hold only the name, from `to_key()`, with
no template at all. `resolve_path_input_params` therefore replaces any
history entry whose name is still declared with the declaration. History only
says which parameter is bound to which PathInput name. Canvas edges then
overlay on top, as before.

## Diagnostics

- `generate_matlab_command: <fn>: PathInput '<name>' bound to '<param>' emits
  N template(s) [...]` is logged once per bound PathInput per command. If N is
  1 when you expected more, the declaration didn't reach the registry. Check
  `get_path_inputs → ... alternate_templates`.
- In MATLAB, `EachOf expansion detected - 1 axis(es)` should appear, followed
  by one `pathinput_discover` per arm.
- `EachOfColumnMismatch` means the arms produced different schema-key columns,
  for example because one template has a `{subject}` placeholder and the other
  doesn't.

## Tests

`scistack-gui/tests/test_matlab.py`: `TestAlternatePathInputTemplates`,
`TestCollectEdgePathInputs.test_alternate_templates_are_kept`.

## Two PathInput nodes on one port (2026-10-20)

A second way to run several files is to draw two or more **differently
named** PathInput nodes onto one function input. That is an `EachOf` too, but
each arm is its own PathInput:

| | One name with alternates | Several nodes on one port |
|---|---|---|
| Declared | one TOML entry holding a list | one TOML entry per PathInput |
| Identity | one name; the arms share version keys | one name per arm |
| Recorded wirings | one | one per arm, all claimed by the node |
| `parameter_names` | the name | left out for that argument; scidb takes each arm's name from its key (`provenance_save._pathinput_name`) |

Owners:
- **Binding:** `edge_resolver.resolve_function_edges` accumulates the names.
  One name stays a bare string, because wiring ids hash that value; several
  become a list. Read it with `path_input_names`.
- **Drawn overrides on a history node:**
  `graph_builder.manual_path_input_overrides` returns the list.
  `_claim_stated_wirings` states one wiring per arm.
- **Dispatch:** `execution_service.record_dispatch_wirings` claims
  `path_input_arm_bindings(...)`, which is each arm's bare wiring.
- **Python run:** `build_run_inputs` builds `EachOf(*arms)`, flattening each
  declaration's own alternates into it.
- **MATLAB run:** `_collect_edge_path_inputs` → `_declared_path_inputs`
  appends the later declarations as alternates carrying their own `name`.
  `_format_path_input` gives each arm its own name.

Each node keeps only one current wiring, so the other arms' call sites count
as its history. The node's state still reflects every arm, and its output
colour follows the drawn edges (`outputs_with_manual_edges`).

Tests: `test_binding_identity.py::TestPathInputArms` and
`test_multiple_path_inputs_on_one_handle_are_an_each_of`;
`test_hidden_path_input_identity.py` (`test_two_drawn_on_one_handle_are_an_each_of`,
`test_an_each_of_of_two_drawn_path_inputs_keeps_one_node`,
`TestPythonRunBindsEveryArm`); `test_matlab.py::TestAlternatePathInputTemplates`;
`scidb/tests/test_pathinput_each_of_names.py`.
