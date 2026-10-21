# Plan: PathInput `key_regex` in the entities TOML, GUI and MATLAB command

## Context (2026-10-21)
`scifor.PathInput(key_regex={key: pattern})` already exists (Python + `+scifor/PathInput.m`,
doc `docs/claude/pathinput-key-regex.md`). A TOML-declared PathInput cannot use it:
`scidb.entities._PATH_INPUT_KEYS` rejects `key_regex` ("declare richer forms in Python"), and
`matlab_command._format_path_input` never emits it. The RERC project is TOML-declared, so
`subject = 'R2_\d{3}'` and the ambiguous `{speed}{trial}` split are both unreachable.

Complements the 2026-10-21 discovery fix (keys bound by an earlier folder now match literally):
key_regex covers a key's FIRST appearance, where an underscore or an adjacent placeholder is ambiguous.

## Stages
1. **scidb.entities (owner of the TOML grammar)**: accept `key_regex = { subject = 'R2_\d{3}' }`
   on a PathInput table and on each alternate arm. Validate: key is a placeholder (PathInput's
   own check), pattern compiles, pattern has no capturing groups (they would break the named-group
   compile in `_segment_to_regex`). Reject with a message naming the PathInput and key. Doc the
   TOML literal-string tip (`'...'` so `\d` is not an escape). INFO log of the loaded patterns.
2. **TOML writer** (the same path the aliases editor uses): round-trip key_regex, written as
   literal strings.
3. **MATLAB command**: `_format_path_input` emits `key_regex=struct(...)` next to `aliases=`;
   `to_spec`/display dict carry it. The template log line shows `key_regex=[keys]`.
4. **GUI sidebar**: per-placeholder "pattern" field beside aliases in the PathInput panel, with
   live validation (compile + no capture groups) via the scidb validator, never a second copy.
5. **Observability**: discovery INFO names the effective pattern per key; the zero-result WARN
   already prints the compiled pattern.
6. **Tests**: scidb entities parse/reject cases; MATLAB command emission; scifor discovery with
   `subject='R2_\d{3}'` on `{subject}_{session}.xlsx` and `speed='[A-Za-z]+', trial='\d+'`;
   GUI manual-testing doc entry.

## Decisions (2026-10-21)
- Per-PathInput patterns (not a project-wide `[schema_keys]` section).
- Regex first, then alias canonicalization. An aliased key's declared spellings ALWAYS
  match, even outside the pattern (`(?:pattern|spellings)`, `PathInput._key_pattern`).
- No live frontend validation: the save is refused with scifor's message (one owner),
  matching how aliases already work.
- Also found while wiring: `scimatlab.bridge` + `+scidb/entities.m` rebuild each arm, so
  key_regex needed its own hop there (`key_regex_json`).

## Status
All stages implemented. Both vite bundles rebuilt; frontend tests pass. Python tests written, not yet run.
