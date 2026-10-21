# PathInput: disambiguating adjacent placeholders with `key_regex`

## Purpose

`PathInput` templates sometimes have two `{key}` placeholders with no
literal delimiter between them, e.g. `"{subject}_EMG_{speed}{trial}.mat"`
matching `SS01_EMG_SSV10.mat`. `discover()`'s segment-to-regex compiler
(`_segment_to_regex`, `scifor/src/scifor/pathinput.py`) gives every
placeholder the same greedy catch-all group `(?P<key>[^/\\]+)`. With no
literal anchoring the boundary between two such groups, Python's regex
backtracking resolves the ambiguity the same way every time: the earlier
placeholder gets everything except the last character needed to satisfy the
later placeholder's `+` quantifier — regardless of what that character
actually is. For `SSV10`, that means `speed="SSV1"`, `trial="0"`: visibly
wrong, and not a "letters vs. digits" read at all, just "give the last
group the minimum 1 character it needs."

As of 2026-08-05, `key_regex` lets a caller declare the true boundary
explicitly instead.

## API

```python
PathInput(
    "{subject}_EMG_{speed}{trial}.mat",
    key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"},
)
```

- `key_regex: dict[str, str] | None` — maps a placeholder key to a raw
  regex fragment (no capturing group) substituted for the default
  `[^/\\]+` when that key appears in a `discover()` segment.
- Validated at construction like `aliases`: a `key_regex` key that isn't
  an actual template placeholder raises `ValueError` ("not a placeholder").
- Fully general — not a letters/digits-specific heuristic baked into the
  engine. The caller supplies whatever pattern actually distinguishes the
  two fields (letters vs. digits, fixed width, etc.). If the on-disk value
  doesn't match the declared pattern (e.g. a stray `"1a"` where `trial` is
  declared `\d+`), that file simply fails to match — a safe failure mode,
  not a silent mis-split.
- Scope: only `_segment_to_regex` (used by `discover()`/`apply_discovery()`)
  consults `key_regex`. The zero-pad/alias fallback scan
  (`_fallback_segment_regex`, see `docs/claude/pathinput-zero-padded-matching.md`)
  always receives a fully-known combo already, so it never faces this
  ambiguity and doesn't need `key_regex`.
- `to_key()` includes `key_regex` in the serialized version-key payload
  only when non-empty, so pre-existing saved keys stay byte-identical.

## MATLAB

`+scifor/PathInput.m` gained a `key_regex` constructor option (a flat
struct: `key_regex.(key) = pattern`), marshaled to a Python dict via the
new `key_regex_to_py` static helper (same shape as `aliases_to_py` but
without the nested canonical/spelling structure). No Python-side behavior
changes are needed beyond the constructor kwarg — discovery/matching is
fully owned by Python.

## 2026-10-21: one rule, TOML + GUI + MATLAB command, aliases, bound keys

- **One pattern rule**: `scifor.key_regex_pattern_error(pattern)` — a
  non-empty string that compiles with NO capturing group (it would shift
  every named group after it; `(?:...)` is fine). Asked by the constructor,
  `scidb.entities` (TOML) and `path_input_service.key_regex_error` (GUI save,
  checked before the write so the sidebar shows the real reason).
- **Entities TOML**: `key_regex = { subject = 'R2_\d{3}' }` on a PathInput
  table or alternate arm. Written back by `scidb.entities._render_key_regex`
  as literal (single-quoted) strings, falling back to an escaped basic string
  for a pattern holding `'`.
- **Carried everywhere `aliases` are**: `graph_builder.path_input_display`
  (node data, MATLAB command, portability), `_declared_path_input`,
  `_format_path_input_arm` (run command: `key_regex=struct('k', 'p')` via
  `matlab_parser.render_matlab_key_regex`, the one MATLAB spelling), the
  three declaration renderers, and the `scimatlab.bridge` entities payload
  (`key_regex_json`, jsondecoded in `+scidb/entities.m`). The command
  re-renders the declaration, so a missing hop silently drops the pattern.
- **With aliases** (user decision): an unbound aliased key matches
  `(?:pattern|spelling|...)`, i.e. declared alias spellings always match even
  when the pattern would not; the capture is then canonicalized. Regex
  first, alias second. Owner: `PathInput._key_pattern`; logged at
  construction.
- **Bound keys match literally**: once an earlier segment captured a key,
  later segments match that value's spelling(s) instead of re-capturing it
  (`_segment_to_regex(segment, bound)`). key_regex only shapes a key's FIRST
  capture. A zero-combo walk WARNs with the deepest failing segment, its
  compiled pattern and sample rejected entries (`_log_zero_result`).
- **GUI**: sidebar "Key Patterns" section (`pathInputKeyRegex.ts`, pure
  edits; no frontend validation). Every `update_path_input` write sends
  `key_regex`, since the update rewrites the whole declaration.

## Rejected alternative

Automatic letters-vs-digits inference (no `key_regex` argument; the engine
guesses the split on its own) was rejected: it silently breaks when two
adjacent fields share a character class (e.g. `{session}{trial}` both
digit-only, or `{muscle}{side}` both letter-only) or when a field is
itself alphanumeric (a repeat-trial suffix like `"1a"`, a speed code like
`"V1"`). The engine has no way to know from the template alone which side
is which, or whether the letters/digits split even applies — so the
pattern must be declared explicitly per key.

## Key files

| File | Role |
|------|------|
| `scifor/src/scifor/pathinput.py` | `key_regex` validation (`_validate_key_regex`), consumption in `_segment_to_regex`, `to_key()` |
| `scimatlab/src/scimatlab/matlab/+scifor/PathInput.m` | `key_regex` constructor option, `key_regex_to_py` |
| `scifor/tests/test_pathinput_key_regex.py` | Python coverage (validation, discovery split, to_key, unaffected-by-delimiters, no-match-on-violation) |
| `scimatlab/tests/matlab/scifor/TestPathInput.m` | `test_key_regex_*` MATLAB cases |

## See also

- `docs/claude/pathinput-zero-padded-matching.md` — the numeric fallback
  scan this note's "out of scope" section refers to.
- `docs/claude/input-markers-colname-pathinput-pathoutput.md` — PathInput's
  role among the input markers.
