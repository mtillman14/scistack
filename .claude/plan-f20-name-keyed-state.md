# Plan: name-keyed Parameter state reads the argument it feeds (F20)

Status: BUILT 2026-09-24 (uncommitted, all tests pass 2026-09-24). Tests: scistack-gui/tests/test_name_keyed_state_f20.py.

## Finding
GUI state about a Parameter is keyed by its DECLARED name (the node the user
clicks): pending values, hidden values. A run target's `constants` are keyed
by the function ARGUMENT. They differ whenever a Parameter declared
`gaitrite_config` feeds `gaitRiteConfig` (B1). Readers that compare the two
directly silently match nothing:

| reader | effect when names differ |
|---|---|
| `execution_service.apply_pending_overrides` | staged value never applied (Run, pipeline compile, code export, combo hiding) |
| `variant_resolver.merge_pending_constants` | pending value never merged into history targets |
| `variant_resolver.resolve_target_call_id` (pending names) | pending combo hides against the wrong call id |
| `variant_resolver.filter_hidden_constant_value_targets` | translates via Parameter bindings, but history targets have NONE -> falls back to the argument |
| `_infer_wired_constants`, `build_parameter_nodes` | correct (already translate / keyed by node) |

## Fix (one owner of the translation)
1. History-derived targets get Parameter bindings from the name their run
   recorded (`scidb.parameter.parameter_node_name`), like edge-derived targets
   already have. Inert for execution (`build_run_inputs` skips a Parameter
   binding whose argument already has a recorded value) and it makes
   history re-runs state the name to `for_each`.
2. `edge_resolver.declared_by_argument(target)` -> `{argument: declared}` for
   every constant (binding, else the argument itself), and
   `edge_resolver.by_argument(named, target)` -> a declared-keyed dict
   re-keyed to this target's arguments. THE translation; the four readers
   call it.
3. Logging: a DEBUG line whenever a name was translated (argument != declared).
4. Tests: a Parameter declared under another name — pending override applies,
   pending merge synthesizes, hidden value filters a history target, pending
   combo call id is re-resolved.
