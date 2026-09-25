# PathInput identity = its declared name (2026-09-25)

Supersedes the spec-in-identity step of
`plan-pathinput-invocation-identity-and-plot-open-speed.md` (the plot half of
that plan stands).

## Goal

A PathInput is identified by its NAME, everywhere, in scripts and in the GUI.
Template and root_folder are only for resolution and display. Moving data or
changing machines changes nothing. Two differently named PathInputs never
share an invocation.

## 1. `name=` is required (clean break, beta)

- `scifor.PathInput(template, *, name, root_folder=None, ...)`: `name` is a
  required keyword; `ValueError` if missing or empty. It replaces the
  existing settable `.name` attribute.
- `to_key()` becomes `{"__type": "PathInput", "name": ...}`. This is the one
  identity term: `call_id` (CallSite inputs), `__inputs` version key (so
  record ids are path-free), invocation binding
  (`compute_pathinput_record_id(to_key())`), and skip comparison. The full
  spec (template, root, regex, aliases, key_regex) moves to a new
  `to_spec()` and is stored on the `__pathinput__` record for display and
  queries (`invocation_path_inputs`), but never hashed.
- `wiring_id`'s PathInput term already is the declared name.
  `call_site_path_input_names`' spec fallback goes away (every record has a
  name).
- Declarers pass it: the entities TOML loader (key = name), the discovery
  scanner (binding name), GUI `create_path_input`/rename, portability
  import, and the MATLAB `scifor.PathInput(tmpl, name=...)` classdef plus the
  `for_each.m` 'pathinput' rebuild (carry `desc{'name'}`).
  `stamp_path_input_name` and the `.name`-by-declarer path are removed.
- ~380 test construction sites get `name=` (scripted, then an orphan check).

## 2. Uniqueness

- **Declaration surfaces (error):** TOML already rejects duplicate keys. A
  name declared in two discovered source files goes from WARN ("shadows") to
  a load error. GUI create/rename already refuses a taken name.
- **Within one run (error):** one `for_each` call where two inputs carry
  the same PathInput name with different specs raises. Alternatives inside
  one `EachOf` are exempt: they are one PathInput spanning several locations
  by design.
- **Across runs (no error):** the same name with a different spec than the
  database last recorded is the "data moved / other machine" case. It logs
  INFO `PathInput X: template changed <old> -> <new> (identity unchanged)`.
- **Not at object construction:** a process-wide "name already
  instantiated" check would fire on every re-run of a script, notebook cell
  or MATLAB section, on every EachOf alternative, and on the move case.

## 3. Canvas: refuse a second node for the same PathInput in one scope

- Backend `layout_service.put_layout`: for `pathInputNode`, build the
  scope's view and refuse `{"ok": False, "error": "PathInput 'X' is already
  on this canvas"}` if a `pathInputNode` with that label is visible. Log INFO
  with the existing node id.
- Frontend drop handler: same check against the current node list before the
  optimistic add (no flash). On a backend `ok: false`, remove the optimistic
  node and show the error.
- Scope = one canvas (hypothesis tab / sub-pipeline). The same PathInput in
  two different tabs stays allowed.

## 4. The failing test

`config_from_inputs` (never-run fallback) skipped PathInputs, so its call_id
never matched the recorded config. It then predicted extra invocations with
no PathInput binding ("missing 2"). It now carries `path_inputs` = `{param:
to_key()}` like `function_variant_configs`.

## Tests
- scidb: two names -> two invocations; same name, moved root -> same
  invocation and record ids, skip skips; renamed -> different; EachOf
  alternatives share one identity; duplicate name within one call raises;
  mixed prediction green.
- scifor: `name` required; `to_key` is name-only.
- GUI: duplicate drop refused in the same scope, allowed in another scope;
  discovery duplicate across files is an error.
- MATLAB: `scifor.PathInput` requires `name`; round-trip keeps it.
