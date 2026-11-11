# PathInput folder-name aliases (match-only)

## Goal

Let a schema key have multiple on-disk spellings that all resolve to one
canonical value, e.g.:

```python
PathInput(
    "{subject}/{session}/data.mat",
    aliases={"session": {"BL": ["Baseline", "1. Baseline"]}},
)
```

`{session}` in the data table is `"BL"`. On disk, a folder named `Baseline`
*or* `1. Baseline` *or* `BL` all satisfy it. Match-only: no PathOutput /
path-construction use case, so we never have to pick which spelling to
*write*.

Lives entirely in `scifor/src/scifor/pathinput.py` — no scidb policy layer.
scidb already re-exports `PathInput` bare (`scidb/__init__.py:26: from scifor
import ... PathInput ...`), so nothing changes there; it gets the feature for
free, same as it already does for `regex=` and the numeric-padding fallback.

## API

`PathInput.__init__` gains `aliases: dict[str, dict[str, list[str]]] | None = None`.

- Outer key: a placeholder key from the template.
- Inner key: the canonical value (what shows up in the data table / metadata).
- Inner value: list of on-disk spellings that mean that canonical value.
  The canonical string itself is always implicitly valid on disk — no need
  to list it in its own alias list.

Construction-time validation (fail fast, not at first `load()`):
- Every outer key must be in `placeholder_keys()` — else `ValueError`.
- No spelling (including a canonical acting as its own implicit spelling)
  may appear under two different canonicals for the same key — ambiguous,
  `ValueError`.

## Mechanism

Two directions, both needed for the feature to actually work end to end
(for_each calls `load()`; discovery calls `discover()`):

### `load()` / `load_with_captures()` — canonical → file on disk

Today's flow: substitute `str(value)` literally, `stat`; on miss, if any
value is numeric-like, scan the filesystem with a per-segment regex built by
`_fallback_segment_regex` (digit-run capture + int-equality check).

Plan: generalize that fallback machinery to a second equivalence kind
alongside "numeric":

- `_fallback_segment_regex` takes an additional `alias_keys: dict[key ->
  {spelling: canonical}]` (flattened reverse-lookup, canonical included).
  For a placeholder in `alias_keys`, emit an alternation regex of the
  escaped spellings instead of `(\d+)`, and record a `('alias', reverse_map)`
  check instead of `('numeric', int_value)`.
- The scan (`_numeric_fallback_scan`, likely renamed `_fallback_scan`) walks
  once per `load_with_captures` call, applying whichever checks apply per
  key — so a template with both a padded numeric key and an aliased key
  resolves in one walk, consistent with how numeric-only cases work today.
- Trigger condition broadens from "any numeric-like metadata value" to
  "any numeric-like value OR any key with an alias table whose value is one
  of its declared canonicals."
- Multiple on-disk matches for one call (e.g. both `Baseline/` and
  `1. Baseline/` exist) → `RuntimeError`, same "matched N files" wording
  the numeric path already uses (MATLAB's message-sniffing translation to
  `scifor:PathInput:MultipleMatches` keeps working unmodified).
- Resolved alias spellings ride in the existing `resolutions` dict exactly
  like bridged numeric spellings do today (e.g. `{"session": "Baseline"}`).
- No pad-width-style caching for aliases in v1 — the literal-first check
  already gives the fast path once a canonical-named folder exists; scope
  stays tight.

### `discover()` — file on disk → canonical

`_walk` currently captures raw regex-matched segment text straight into
`bindings`. Plan: canonicalize each captured value against that key's alias
table *before* it's used for the same-key consistency check (two segments
binding the same key with different raw spellings of one canonical must not
be flagged as conflicting) and before it lands in the returned combo dict.

- Match found (raw equals canonical or a listed spelling) → store canonical.
- No match for a key that *has* an alias table → **fail open**: keep the
  raw string unchanged (consistent with scifor's existing tolerant
  behavior — it has never errored on an unrecognized filename shape) but
  emit a `Log.debug` line naming the key and the unmatched raw value, so an
  unrecognized folder-name variant is observable instead of silently
  producing a stray, uncanonicalized value in the data table. This is the
  main diagnostic surface for typo'd folder names (NOTE 2).

## Logging

New `Log.debug` calls, same style/layer tag as the existing
`pathinput_numeric_fallback` lines:
- alias fallback scan triggered / matched, in `load_with_captures`.
- canonicalization applied in `discover()`.
- unresolved spelling under an aliased key in `discover()` (see above).

## Tests — `scifor/tests/test_pathinput_aliases.py`

Mirrors `test_pathinput_padded.py` conventions:

1. `load()`: canonical-named folder exists on disk → resolves directly, no
   fallback triggered.
2. `load()`: canonical missing, one alias spelling present → resolves,
   `load_with_captures` reports the bridged spelling.
3. `load()`: two alias spellings of the same canonical both present →
   `RuntimeError` ("matched N files").
4. `load()`: value has no alias entry for that key → unaffected, literal
   behavior only (regression guard).
5. `discover()`: on-disk spelling variant → combo comes back with the
   canonical value.
6. `discover()`: unrecognized on-disk spelling under an aliased key → raw
   value passed through unchanged + debug log emitted.
7. Combined: one template with a numeric-padded key and an aliased key
   resolved together in a single `load()` / `discover()` call.
8. Construction-time: overlapping spelling across two canonicals → `ValueError`.

## MATLAB — `+scifor/PathInput.m`

Needs actual changes (unlike the numeric-padding feature, which needed
none, because that logic lives entirely inside `load()`/`discover()` which
MATLAB already delegates to Python unmodified):

- Constructor's `arguments` block is a fixed name list (`root_folder`,
  `regex`) — add `options.aliases` and marshal the nested MATLAB
  struct/cell (`aliases.session.BL = ["Baseline", "1. Baseline"]` or
  similar MATLAB-side shape, needs a concrete decision) into a nested
  `py.dict`/`py.list` before constructing `py.scifor.pathinput.PathInput`.
- No other methods need changes — `load`, `load_with_captures`, `discover`,
  `apply_discovery` all already delegate to the Python object, which is
  where the new behavior lives.

## Open questions before implementing

1. MATLAB-side shape for nested alias input — struct-of-struct-of-string-array,
   or a different convention? Need something ergonomic in MATLAB syntax.
2. OK with generalizing the numeric-fallback internals (rename
   `_numeric_fallback_scan` → `_fallback_scan`, broaden
   `_fallback_segment_regex`) rather than writing a parallel, separate alias
   scan? Keeps one walk instead of two, but touches working code.
3. OK with fail-open + debug-log for unrecognized on-disk spellings in
   `discover()`, rather than raising?
