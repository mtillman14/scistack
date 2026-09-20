# Stage 2c — the selection is a value; the combo carries a handle

*Rewritten 2026-09-20 after the user pointed out that the first draft kept
`__rid_*` / `__vsig_*` as prefixed keys on the combo and the result row —
the very shape (a dict whose key prefix encodes what the value IS) that
`VariantAxes`, `RunBindings` and `CallSite` were built to remove. This
version removes it. Decision taken: MATLAB gets the selection pre-computed
(option (a) of the first draft).*

---

## 1. Two owners share one dict

A combination has two halves with different owners:

| half | what | owner | how it travels today |
|---|---|---|---|
| **location** | schema keys + declared iterables | scifor — drives its row filter, its `distribute` resolution, its result row | plain keys on the combo dict |
| **selection** | which record of each input this combination reads (one rid for an iterated / pinned / lineage-only input, a set for an aggregated one) | scidb — drives the frame filter AND the graph edges | `__rid_{param}` / `__vsig_{param}` keys on the SAME dict, told apart from the location by prefix |

And because the selection is stuffed onto the combo, it has to be stuffed
onto the **frame** too (`__record_id` renamed to `__rid_{param}`, a
`__vsig_{param}` column added) and into scifor's **global schema**
(`set_schema(current + rid_keys_for_schema)`, restored after the run) so
that scifor's schema filter selects rows by it. Eight places in two
languages then strip the prefixes back off. `RunBindings.rids_for_combo`
parses them back out with `is_rid_column` / `param_of`.

## 2. The typed version

**`Selection`** (`scidb.bindings`) — what one combination reads:

```python
@dataclass(frozen=True)
class Selection:
    rids: Mapping[str, tuple[str, ...]]   # param -> record ids (1 or many)
    groups: Mapping[str, str] = {}         # param -> variant signature, split aggregated inputs only
```

Built ONCE per combination at expansion time — where today the loops write
`full_combo[rid_column(p)] = rid`, they append a `Selection` instead.
`RunBindings.selections: list[Selection]` holds them in combo order.

**`COMBO_KEY = "__combo"`** — the combo dict scifor sees is the location
plus this ONE key: the index into `selections`. One reserved key with one
meaning, spelled in one place (`bindings.COMBO_KEY`) — like `__record_id`
on a frame. Not a family of prefixed keys encoding a union. scifor passes
it through untouched (`row = dict(metadata)`), `distribute` and
`for_columns` fan-out inherit it (`{**metadata, ...}`), and the save reads
`state.bindings.selection_of(row)`.

**The frame keeps `__record_id`.** No rename, no `__vsig_` column. Every
per-param structure keyed by `__rid_{param}` today (`rid_per_combo`,
`colsel_rid_per_combo`, `rid_populated_idx`) is keyed by the PARAM.

**scifor grows one hook**, `_select_rows(param, frame, combo) -> frame`,
called after its schema filter and before extraction. scidb's:

```python
def _select_rows(param, frame, combo):
    sel = state.bindings.selection_of(combo)
    rids = sel.rids.get(param) if sel else None
    if rids is not None and RECORD_ID_COLUMN in frame.columns:
        frame = frame[frame[RECORD_ID_COLUMN].isin(rids)]
    return frame.drop(columns=[RECORD_ID_COLUMN], errors="ignore")
```

**One rule, two readers.** The frame filter and the edge assembly read the
SAME `Selection`. Today they are two implementations (scifor's schema match
on `__rid_x`; scidb's `for_combo` parse of `__rid_x`) that agree only
because both happen to read one column.

## 3. What goes away

scidb: the Step 12 rename and the `__vsig_` frame column; `set_schema(extended)` + the Step 18 restore; `rid_keys_for_schema`; the `extended_metadata_iterables` padding; every `is_rid_column` / `param_of` / `vsig_column` read of a combo or row (`rids_for_combo`, `_should_skip`, `_normalize_variable_inputs`, `_apply_introspect`, `_variant_keys`, `_group_bp`, the expansion loops); `_guard_pathoutput_collisions`'s prefix test.
scifor: the four prefix strips and the "internal `__` schema key" concept; the schema is the dataset schema again.
bridge: `_sanitize_rid_key` and `rid_rename_map` shrink to two fixed names (`__combo`, `__record_id`).
MATLAB: the four `contains(..., "__rid_")` strips.

What stays: `rid_column` / `param_of` for the ONE remaining column
(`__record_id` is `RECORD_ID_COLUMN`; the introspect view still spells
`_record_id_{param}` for the user), `vsig_column` for `RecordPool` /
`signature_conflicts_with` internals, `is_internal_column` for the bridge.

## 4. MATLAB: pre-computed selection (decision (a))

The bridge already does "Python decides, MATLAB applies" (PathInput
resolution, PathOutput paths, combos). One more table: `row_selection` —
per combo index, per param, the rid list — is `state.bindings.selections`
serialised. `for_each.m` selects rows with
`ismember(frame.x__record_id, rids)` and drops the column, where today it
relies on the extended schema. `__combo` and `__record_id` cross as
`x__combo` / `x__record_id` (MATLAB cannot lead a field with `_`) — two
fixed renames, not a map. Python and MATLAB apply the same rule from the
same source.

## 5. Order of work (each green before the next)

1. **scifor: `_select_rows`.** Hook + `param_name` threaded to
   `_prepare_input`. No behaviour when None. Tests in `scifor/tests`.
2. **scidb: `Selection` + handle; frames keep `__record_id`; no schema
   extension.** `bindings.py`: `COMBO_KEY`, `Selection`,
   `RunBindings.selections` / `selection_of` / `edges_for`; `for_combo` and
   `rids_for_combo` reimplemented over the selection (same names, so the
   save, the draft stamp and `test_bindings` keep calling them).
   `foreach.py`: Step 12, `rid_per_combo` keyed by param, both expansion
   loops build `Selection`s, Step 15/18 stop touching the schema, the skip
   hook / normaliser / introspect / PathOutput helpers read the selection,
   `_select_rows` passed to scifor. Bridge: two fixed renames + the
   selection table in the cached payload. **Guard: the whole scidb suite;
   `test_identity_parity.py` must show byte-identical edges** — the rule
   that selects the rows is now the rule that writes them.
3. **scifor: remove the prefix strips.** Dead after (2); removing them
   proves it.
4. **MATLAB `for_each.m`**: apply `row_selection`; remove the strips.
   Guard: `scimatlab/tests` (Python) + the MATLAB suite.
5. **Docs**: `scidb-for-each-internals.md` Steps 12/15/18 and §aggregation,
   `variant-space.md` §6, `input-binding-round-trip.md`, `bindings.py`
   module docstring (the spellings list shrinks to one column).

## 6. Not changed

`invocation_id`, `record_id`, `call_id` — no identity recipe is touched;
the parity suite must show zero diffs. `RecordPool` / `VariantGroup` — the
aggregation pools are unchanged; a `Selection` is what one combo takes out
of them. `introspect=True`'s user-facing columns
(`_record_id_{param}`, `_branch_params_{param}`) — same output, resolved
through the selection instead of parsed off the row.

## 7. Risks

* **A frame with no `__record_id`** (a DataFrame passed directly, a
  PathInput, a Merge — Merge constituents have it stripped at load): the
  hook finds no entry for that param and passes the frame through.
* **`as_table`**: the hook drops `__record_id` before extraction, so the
  table the function sees loses exactly the column the schema strip used
  to remove. `test_as_table*` / `TestAsTable.m` guard it.
* **Coarse inputs**: the hook selects by rid, not by location, so a
  subject-level record beneath a per-session combo is selected by being in
  the selection — the `_rid_probe_key` blanking still decides WHICH rid
  goes into the selection at expansion. `test_coarse_input_provenance.py`.
* **The skip hook fires before scifor**, on scidb's own combos, so it reads
  the selection directly — no row involved.
* **Bridge payload size**: `#combos × #inputs` short lists, the same order
  as the combos themselves.
