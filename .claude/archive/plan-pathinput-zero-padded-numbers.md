# Plan (v2): Zero-padded numeric filenames in scifor.PathInput — no new syntax

## Problem

Filenames like `6MWT-001.mat` use zero-padded numbers. Discovery is safe today —
`_segment_to_regex` captures `"001"` as a string and nothing downstream strips
it. The break is on the **resolution** side:

- `PathInput.load()` renders every metadata value with `str(value)`
  (`scifor/src/scifor/pathinput.py:107`), so numeric `1` → `6MWT-1.mat` →
  file not found.
- MATLAB `PathInput.load` pre-renders numeric scalars with `num2str(val)`
  (`+scifor/PathInput.m:93`) — same loss.

So exact matching only works if `"001"` is threaded as a string through every
layer; any numeric source (`trial=1:12`, numeric table column, MATLAB double)
breaks it. Current escape hatch: `regex=True` with hand-written quantifiers —
clunky, last-segment-only, invisible to discovery.

## Rejected: format-spec syntax `{trial:03d}` (plan v1)

Works, but requires users to know Python format-spec syntax — a poor fit for a
MATLAB-first audience. Kept as a possible future *explicit override* only.

## Rejected: infer `:03d` from an example filename

The user's suggestion, refined: if we must read the directory to learn the
padding width, we can skip the "learn a format, then re-render" indirection and
directly select the file whose number equals the value. Inference has extra
failure modes the direct approach doesn't: mixed widths in one directory, which
"example" file to trust, and cached-width staleness.

## Chosen design: numeric-equivalence fallback in `load()`

`{trial}` stays the only syntax. Resolution order per combo:

1. **Literal resolution first** (exactly today's behavior). If the resolved
   path exists → return it. Zero change for every current user.
2. **Numeric fallback** only when the literal path does NOT exist and at least
   one substituted value is numeric-like (int, integral float, or digit
   string — covers MATLAB's `num2str` output). Re-match the template against
   the filesystem, segment by segment:
   - each `{key}` bound to a numeric-like value matches `(\d+)` and the
     capture must satisfy `int(capture) == int(value)` — so `1` finds
     `001`, `01`, or `1`;
   - each `{key}` bound to a non-numeric value stays a literal;
   - reuses the existing `_segment_to_regex`/`_walk` machinery, so padded
     **directory** names (`sub-001/…`) work too, not just the last segment.
3. **Exactly one match** → return it (debug-log that the fallback fired and
   what matched). **Multiple** numeric-equal matches (`6MWT-1.mat` AND
   `6MWT-001.mat` both on disk) → `RuntimeError` listing them, mirroring the
   regex-mode multi-match error. **Zero** matches → return the literal path
   unchanged (preserves current semantics: `load()` never raised on missing
   files in non-regex mode; the user's function surfaces the failure).

### Why this is clean

- **No syntax at all** — the common case just works from both languages.
- **No stored state / no inference** — behavior derives from value + disk at
  the moment of resolution; nothing to cache or invalidate.
- **Backward compatible** — fallback fires only where today's behavior is a
  guaranteed downstream failure (literal path missing).
- **MATLAB free ride** — `PathInput.m` delegates `load` to the Python object;
  its `num2str(1)` → `"1"` is numeric-like, so the fallback covers it.
- **No auto-conversion of values** — discovered `"001"` stays a string
  (feedback rule: never auto-int zero-padded keys); user-passed `1` stays a
  number. Only the *filename lookup* is numeric-tolerant.

### Known caveat (flag, don't solve now)

Schema-key strings can differ by source for the same logical file: a
discovery-driven run stores `trial="001"`, an explicit `trial=1:12` run stores
`"1"`. That mixed-representation issue exists today and is adjacent to the
recorded `latest-record selection` future issue; this plan makes both
representations *resolve*, not unify. Document in the docs/claude note;
revisit canonicalization if it bites.

### Performance (as implemented)

Zero cost when literal paths exist (one `stat`). Two caches bound the
fallback cost for large folders / many combos:

1. **Pad-width cache** — a successful scan records each key's captured digit
   width; subsequent combos re-render with `zfill` and hit with a single
   `stat`, no directory scan. (This is the user's "infer the padding"
   idea, used as a cache layer rather than the primary mechanism.)
2. **Directory-listing memo** — per-instance, keyed by path, invalidated by
   directory mtime.

Net: a 1000-combo run over 1000-file folders does ~one listing + one regex
sweep total, then 2 stats per combo. Mixed pad widths degrade gracefully to
per-combo scans that still resolve correctly.

## Logging (NOTE 2)

- `Log.debug`-equivalent hook (module logger): literal path missed →
  fallback attempt, per-segment pattern, match found / ambiguity / no match.

## Tests

- `scifor/tests/test_pathinput_padded.py` (new):
  - literal hit short-circuits (no fallback, existing behavior).
  - `load(trial=1)` and `load(trial="1")` find `6MWT-001.mat`.
  - integral float (`1.0`, the MATLAB double case) finds it.
  - padded directory segment: `sub-001/trial_1.mat` from `subject=1`.
  - ambiguity (`6MWT-1.mat` + `6MWT-001.mat`) raises with both names.
  - zero matches returns the literal path (no raise).
  - non-numeric values never trigger fallback; regex=True mode untouched.
- `scimatlab/tests/matlab/scifor/TestPathInput.m`: MATLAB double resolves the
  padded file end-to-end.

## Files touched

| File | Change |
|------|--------|
| `scifor/src/scifor/pathinput.py` | numeric fallback in `load()`; shared segment matcher |
| `scifor/tests/test_pathinput_padded.py` | new test module |
| `scimatlab/tests/matlab/scifor/TestPathInput.m` | padded-resolution cases |
