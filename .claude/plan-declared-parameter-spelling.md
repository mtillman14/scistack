# Plan: record a Parameter value as declared, not as the transport spelled it

## Bug (scidb.log 2026-10-01 12:52; tmp.py confirmed)
`formulaNum = 6` is declared in scistack_entities.toml. MATLAB passed it as `6.0`,
so history recorded `'6.0'` (450 records). `build_parameter_nodes` matches history
and declaration by string, so the canvas drew two "6" rows. `constants_identity_key`
is repr-based, so a Python run of the same declared 6 would also be a different variant.

## Decision (user, option 1)
The declaration owns the value. scidb records the declared value when the passed
value equals it but is spelled differently.

## Change
1. `scidb.entities.declared_spelling(inputs, parameter_names, declared=None)` is the one
   owner. `declared` defaults to `entities.load_for_project().parameters`. It skips EachOf,
   never matches bool to a number, leaves identical spellings alone, and is idempotent.
   It logs INFO for each substitution.
2. It is called at the top of `foreach._for_each_prepare`, before glue, iterables and
   `_build_call_identity`, so it covers both run paths.
3. `scidb.external_loop` exports it. The MATLAB bridge applies it right after
   resolve_for_columns, before the skip hook, using the same merged parameter_names
   it passes to prepare.

## Not done (by decision: no migrations)
The existing 450 `'6.0'` records stay as a separate history value. The next run records `6`.

## Tests
`scidb/tests/test_declared_spelling.py` has pure rule tests and an end-to-end run.
In it, a MATLAB-style `6.0` and a Python `Parameter(6)` give one value `"6"` and one call site.
