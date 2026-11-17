# How a PathInput loader's node colour is decided

A **PathInput loader** is a function whose only inputs are PathInputs and
constants (e.g. `loadXSENSOneFile(xsensFilePath, colNamesStruct)`). It has no
database variable feeding it, so the usual node-state question ("which input
records exist, and have they all been processed?") has nothing to enumerate.
This doc explains the second question the canvas asks instead, and the
spelling rule that question depends on.

Related: [schema-key-types.md](schema-key-types.md) (numeric/string
declarations), [pathinput-zero-padded-matching.md](pathinput-zero-padded-matching.md)
(how `trial=1` finds `6mwt-001.xlsx` during a run).

## Two questions, one colour

`check_node_state` (`scidb/provenance/state.py`) gives a loader its colour
from two answers:

1. **The invocation answer.** For a loader, what's expected is exactly what it
   has already produced (`realized_inputless_invocations`). This answer alone
   is green as soon as anything ran, even if half the files were never loaded.
2. **The discovery gate** (`_discovery_gate`). This step walks the filesystem
   with the PathInput the loader last ran with (`pathinput_configs` rebuilds it
   from the stored spec). Every file on disk whose location the loader never
   saved counts as **missing**. The gate can only add red; it never turns a
   red node green.

The counts are merged. A healthy loader over 11 files reports
`up_to_date=11, missing=0`. When the two answers disagree about *which* 11
locations exist, every file is counted twice: `up_to_date=11, missing=11`,
and the node is red even though the run was fine.

The location picker (`provenance/locations.py` `_discovery_expected`) asks the
same discovery question through the same function,
`check_pathinput_node_state`, so the picker and the canvas badge always agree.

## The spelling rule: compare in the saved spelling

`PathInput.discover()` returns locations **as the disk spells them**:
`6mwt-001.xlsx` gives `trial="001"`. A run never saves that spelling for a
key declared `numeric`. `foreach_prepare` converts discovered locations to
the canonical number first (`"001"` → `1` → stored `"1"`), so `"001"`, `1`
and `1.0` are one location.

So the discovery check must convert the same way before it compares, or a
zero-padded file never matches its own saved location.

**One owner:** `scidb.store.schema_values.canonical_discovered_combos(combos,
key_types)`. Both the run (`run/foreach_prepare.py`) and the state check
(`check_pathinput_node_state`) call it. Don't add a second conversion
anywhere. Anything new that compares discovered locations with saved ones
must call this function.

- `numeric` keys: scifor's `canonicalize_combos`, then `schema_str`.
  Locations that become identical (`-1` and `-001` both on disk) are
  de-duplicated.
- `string` keys: the disk spelling is the identity and stays as it is. A
  string-declared `trial` saves `"001"`, and discovery compares `"001"`.
- Undeclared keys: left alone. A run that needed a numeric match for an
  undeclared key already raised and asked for a declaration.

Grid values passed to `check_pathinput_node_state(..., trial=[...])` are
converted the same way for numeric keys, so `trial=["001"]` and `trial=[1]`
ask about the same location.

## History: the loadXSENSOneFile red node (2026-11-17)

Files `PLT003_E01/data/6MWT/IMU/6mwt-001.xlsx … -011.xlsx`, with `trial`
declared numeric. The run saved `trial = "1" … "11"`, and the log showed
`canonicalized numeric schema key(s) ['trial'] in 11 discovered combo(s)`.
The state check used raw `pi.discover()`, expected `"001" … "011"`, and
logged:

```
pathinput node loadXSENSOneFile: red (discovered=11, should=11, up_to_date=0, missing=11)
state(loadXSENSOneFile ...): red (up_to_date=11, stale=0, missing=11)
```

A sibling loader under the same root (`loadDelsysEMGOneFile`) was green,
probably because its file names weren't zero-padded.

## Diagnosing a red loader

Grep `scistack-debug.log` for the function name and read these lines in order:

- `pathinput node <fn>: <colour> (discovered=, should=, up_to_date=, missing=)`.
  `up_to_date=0` straight after a successful run means a **spelling or
  identity mismatch**, not missing work.
- `pathinput node <fn>: discovered spellings canonicalized ... e.g. {...} -> {...}`
  (DEBUG): the conversion that was applied, with one example.
- `node <fn>: red — N expected invocation(s) not present, e.g. at [...]`:
  the location keys it is looking for. Compare them with the run's
  `for_each(<fn>) — N iterations: ...` line.
- `node <fn>: PathInput discovery matched nothing on disk ...` (WARN): the
  walk itself failed (unreachable root, Windows path on POSIX). In that case
  the gate is skipped, not trusted.

Regression tests: `scidb/tests/test_schema_key_types.py::TestDiscoveryStateSpellsLikeTheRun`.
