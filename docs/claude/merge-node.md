# Merge: the join wrapper, and the Merge node on the canvas

*Written 2026-11-17, updated 2026-11-18 when the plan was built
(`.claude/plan-merge-node.md`). "Today" sections were the state before the
build; the "Decided" sections are now implemented. If the code disagrees,
**the code wins**: fix this doc. Python tests and the MATLAB suite were not
run by Claude; see the plan's status table.*

## What a Merge is

A `Merge` combines two or more inputs into **one** table, which is passed to a
function as a single argument:

```python
for_each(analyze, inputs={"combined": Merge(GaitData, ForceData, name="gait_force")}, ...)
```

The function never sees the Merge object. It receives a pandas DataFrame
(Python) or a MATLAB table, exactly as if `combined` were an ordinary
table-valued variable. A Merge is **not saved**: the joined table exists only
in memory for that call (just like a glue node's output, see
`free-code-glue-nodes.md`).

## Who owns what

| Concept | Owner | Consumers |
|---|---|---|
| The container (`tables`, `to_key`, `__name__`) | `scifor/src/scifor/merge.py` `Merge` | `scidb.Merge` (thin subclass adding a DB-aware `.to_csv()`, `scidb/src/scidb/model/merge.py`) |
| THE join rule | `scifor.merge.merge_frames` + `drop_constant_keys` | scifor per-combo loop (`scifor/foreach.py`), scidb per-combo loader (`scidb/run/foreach_common._resolve_per_combo_merge`), both flat-table exports (`scifor/csv_export.py`, `scidb/csv_export.py`) |
| The rule as data (spec) | `docs/claude/merge-cases.json` | `scifor/tests/test_merge_cases.py`, `scimatlab/tests/matlab/scifor/TestMergeCases.m` |
| MATLAB twin of the rule | `+scifor/merge_tables.m`, `+scifor/drop_constant_keys.m` | `+scifor/Merge.m`, `+scidb/Merge.m` |
| Version-key spelling | `scifor.merge.merge_key` (what `Merge.to_key()` returns) | `__inputs`, the `__merge__` record, the GUI's canvas prediction (`edge_resolver.merge_binding_key`) |
| Parsing a recorded key | `scidb.provenance.graph.parse_merge_spec` | scidb readers, GUI history, MATLAB command formatter |
| Join methods | `scifor.merge.MERGE_HOWS` | MATLAB `scifor.merge_hows()`, the GUI panel (node data `hows`) |

Never add a second join implementation. Every path above calls `merge_frames`
(or the MATLAB twin, held to the same case file).

## Today: how a Merge is loaded per combo (scidb)

`foreach_load._load_input` sees a `Merge`:

- If every constituent supports bulk load, each constituent is loaded whole and
  wrapped back into a `scifor.Merge(*loaded_tables)` (`foreach_load.py` ~l.574).
  scifor's loop then filters each constituent per combo and joins them.
- If any constituent lacks bulk load, it becomes a `PerComboLoaderMerge`, and
  `for_each` Step 16 calls `_resolve_per_combo_merge` once per combo. That
  function loads each constituent, unwraps `Fixed` and `ColumnSelection`, turns
  the result into a DataFrame, drops constant key columns and calls
  `merge_frames`.

`Fixed(...)` and `ColumnSelection` (`MyVar["col"]`) may wrap a constituent.
Nesting a Merge inside a Merge, or `Fixed(Merge(...))`, is an error.

## Today: THE join rule (strict; decided 2026-11-02, P6)

1. A per-combo constituent drops schema-key columns that hold one value (they
   only repeat the combo). Key columns that vary (e.g. `trial` within a
   `session` combo) stay and become join keys.
2. Constituents join **only on the schema-key columns they share**. If they
   share a non-key column, that is an error: Merge never joins on data values.
3. **No row is dropped silently.** A key value on one side only raises
   `Merge would drop rows`, and so does a many-to-many match. Rows follow the
   side whose keys repeat (otherwise the left side). Columns come out as
   left's, then right's minus the keys.
4. With no shared key, the join is column-wise: a 1-row constituent is
   broadcast, and any other height mismatch is an error. Flat-table exports
   pass `positional=False`, so they get an error instead.
5. A constituent with no rows is an error.

With 3+ constituents the join runs left to right: `((A ⋈ B) ⋈ C)`. **Order
matters** for row order, and (after the change below) for left/right.

## Decided 2026-11-17: `how=` and `name=`

```python
Merge(A, B, C, name="gait_force", how="left")
```

- **`how=`** is one of `"exact"` (the default: today's strict rule, unchanged),
  `"inner"`, `"outer"`, `"left"` or `"right"`. There is **one `how` per
  Merge**, applied at every left-to-right step. The tuple of allowed values has
  one owner, `scifor.merge.MERGE_HOWS`. The GUI dropdown and MATLAB validation
  read it; they do not copy it.
  - `inner`/`left`/`right` may drop rows, and `outer`/`left`/`right` may pad
    missing values with NaN. Asking for that explicitly is what makes it not
    silent, and every dropped or padded row is still logged at **WARN** with
    counts and the first few key values (`[merge]` tag).
  - These stay errors under every `how`: a shared non-key column (rule 2), a
    many-to-many match, and an empty constituent (rule 5). The rule still
    joins only on schema keys: there is no `on=` option.
- **`name=`** is **required**. A Merge with no name raises an error, which is
  the beta clean break; there is no auto-name to keep in sync between Python,
  MATLAB and the GUI. The name is the Merge node's identity on the canvas, so
  one named Merge can feed several functions, and code and canvas match by
  name (the same way PathInputs do, `identity-layers-pathinput.md`).
- **Identity:** `to_key()` spells the name, the `how` and the constituents.
  Changing `how` therefore makes a new variant of every consuming function, and
  so does changing a constituent. Records produced by an unnamed Merge before
  this change get a new identity and re-run once (beta rule: no migration).

## Decided 2026-11-17: the Merge node in the GUI

- A new node kind, `mergeNode`, created from the palette like a PathInput. The
  settings panel shows `name` and a `how` dropdown. The node name is required
  and unique in the project.
- **Ports.** There is one output and N+1 inputs: one per connected constituent
  plus exactly one empty trailing port. A fresh node shows two inputs. Each
  port has a **slot id** that is never reused: `in__m0`, `in__m1`, … The
  constituent order is the order of the slot ids. If you disconnect a middle
  port, the remaining ports close up visually and no stored edge is renamed.
  The trailing empty port is always `in__m{max+1}`.
- **What may feed a port:** variable nodes only (optionally with a column
  selection), one per port. A Merge node, PathInput, Parameter or glue node on
  a Merge port is refused by `put_edge` with a message. The Merge output may
  feed any function argument, and that argument must then have no other
  source.
- **Endpoint ref** for connection ids is `mg:{name}` (`edge-model.md` table).
- **Run.** `edge_resolver` turns a function argument fed by a Merge node into
  `scidb.Merge(*constituents_in_slot_order, name=, how=)`. The MATLAB command
  formatter emits the same thing in MATLAB syntax. From there the run is
  identical to a code run: one owner, `for_each`.
- **History → canvas.** A code-side `for_each(..., Merge(..., name="x"))` run
  draws the `mg:x` node with its constituent edges from the recorded
  `__inputs` spec (parser owned by `scidb.provenance.graph`, next to
  `parse_path_input_spec`). Adoption then turns those edges into stored rows,
  like any other history edge.

## Lineage: which constituent records a Merge-fed output names

**Confirmed 2026-11-17** by `scidb/tests/test_merge_provenance.py`: until
then a Merge-fed output recorded **no** edge to any constituent record. The
bulk loader strips `__record_id` from each constituent (it would clash in the
join), and `_build_run_bindings` gave the Merge param no binding.

**Fixed by `scidb/run/merge_lineage.py`:**
- The loader returns a `LoadedMerge`, which is a `scifor.Merge` and joins
  exactly as before. It also carries a `MergeLineage` recorded *before* the
  strip.
- `MergeLineage.rids_at(combo)` picks a constituent's rows the way scifor's
  per-combo filter does: a populated key must equal the combo's value, a
  `Fixed` pin overrides the combo, and an unpopulated (coarser) key broadcasts.
- The Merge param is a `LINEAGE_ONLY` binding (the ColumnSelection
  precedent). Both Selection builders in `foreach_prepare.py` (full iteration
  and aggregation) add `merge_rids_at(...)`. Its records become edges under
  the Merge's param, never an iteration axis.
- Branch params are **not** inherited from constituents, the same as before
  the fix.
- Not covered: a `PerComboLoaderMerge` (a constituent with no bulk load) still
  records no edges. It logs `[merge] provenance: ... loaded per combo`.

Effect: Merge-fed records get a new invocation identity and re-run once
(beta rule).

## Identity on the recorded side: the `__merge__` record (built 2026-11-18)

A Merge-fed argument records, besides one lineage edge per constituent
record, ONE edge to a virtual record of type `MERGE_TYPE` (`__merge__`,
`scidb/provenance/graph.py`) whose `_constant` value is the Merge's
`to_key()`. Without it the recorded call site named the argument by
whichever constituent type its edges listed last, so forward
(`ForEachConfig.to_call_id`) and recorded (`pipeline_variants[].call_id`)
call ids disagreed.

| Reader | What it does with `__merge__` |
|---|---|
| `save._merge_bindings` | part of `invocation_id` (save stamp and `record_run`) |
| `invocation_edges_batch` | `edges["merges"] = {param: key}`; constituents stay in `var_inputs` |
| `pipeline_variants`, `function_variant_configs`, the call-id batch | `input_types[param] = key` (the forward spelling) |
| `stored_invocation_signature` | `merges = {param: rid}`, never mixed into the variable rid sets |
| skip gate (`foreach._should_skip`) | recompute when the Merge record differs (`how`, name, constituents) |
| expected-invocation predictor | binds the Merge record + every constituent record at the location; coverage by `how` (below) |
| `aggregate_pipeline_variants` | the key stays in `input_params`; never a variable type |

**Predictor coverage rule** (node turns green): a location is expected when
all constituents have records there (`exact`, `inner`), the first
(`left`), the last (`right`), or any (`outer`). With 3+ constituents and
`left`/`right` this approximates the fold.

## Bridge (MATLAB)

`+scifor/merge_args.m` parses trailing `name=`/`how=` for both MATLAB classes
(one owner). `how` is validated against `scifor.merge_hows()`, which reads
Python's `MERGE_HOWS` (fallback copy guarded by `TestMergeHows`). `name`/`how`
cross the bridge in `describe_input_for_python` (spec `name`/`how`),
`entry.for_each_describe_loaded_input`, the scifor loop wire (`merge_name`,
`how`), and `prepare_merge` passes `How` to `merge_tables.m`.

## Known limits (2026-11-18)

- A Merge loaded per combo (`PerComboLoaderMerge`: a constituent with no bulk
  load) records no constituent lineage edges (its `__merge__` edge is
  recorded).
- Constituents' branch params are not inherited by the output.
- GUI: only variables (no column selection on a slot) feed a Merge; the
  canvas cannot reorder slots (disconnect and re-wire instead).
- A name recorded with two definitions is drawn once, from the first, with a
  `conflict` flag.
