# A Parameter value is recorded as declared

*Written 2026-10-01. Read this before touching how constant values reach
provenance, `constants_identity_key`, or the Parameter node's value rows.*

## The bug

`formulaNum = 6` is declared in `scistack_entities.toml`. MATLAB has no
integers, so the generated `scidb.Parameter(6)` reached `for_each` as `6.0`.
History stored `'6.0'` for 450 records. The declaration stringifies to `'6'`.
`graph_builder.build_parameter_nodes` merges history and declared values by
exact string, so the canvas showed two "6" rows. One said "450 rec history"
and was not a declared value, so a MATLAB Run said "no value yet" until the
user added a second 6.

The display was the visible symptom. The real fault was identity:
`provenance.constants_identity_key` is `repr`-based, so the same declared 6
run from MATLAB (`6.0`) and from Python (`6`) were two different variants.
There were two owners of "is this the same value".

## The rule

**The declaration owns the value.** When a run binds a declared Parameter
(`parameter_names` maps the argument to its declared name) and the passed
value equals a declared value but is spelled differently, the declared value
is what gets recorded.

Owner: `scidb.entities.declared_spelling(inputs, parameter_names, declared=None)`.
- `declared` defaults to `entities.load_for_project().parameters`, the only
  place a MATLAB project declares Parameters.
- An identical spelling (same `repr`) is left alone, so the call is idempotent.
- `bool` never matches a number (`True == 1` in Python, but those are different values).
- An unexpanded `EachOf` is left for expansion.
- A comparison that raises (an array against a list) is no match.
- Each substitution logs `[parameter] <name> (argument 'x'): recording the
  declared value 6 (int) for the passed 6.0 (float)`.

## Where it is applied

- **The top of `foreach._for_each_prepare`**, before glue normalisation, iterable
  resolution and `_build_call_identity`. Both run paths go through here: Python
  `for_each` after EachOf expansion, and MATLAB through `external_loop.prepare`.
- **The MATLAB bridge** (`scimatlab.bridge.for_each_prepare`), right after
  `resolve_for_columns`. The bridge builds its skip hook from the inputs
  *before* calling prepare, so without this the hook would predict identities
  from `6.0` and never match a recorded `6`. The bridge computes the merged
  `parameter_names` once and passes the same dict to prepare.

## What it does not do

Existing records keep the spelling they were saved with. Beta rule: no
migrations. A project that ran with `6.0` shows that as a separate history value
until those records are deleted. The next run records `6`.

Tests: `scidb/tests/test_declared_spelling.py`.
