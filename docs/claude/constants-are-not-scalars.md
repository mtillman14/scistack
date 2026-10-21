# Constants are not necessarily scalars

*Updated 2026-10-21: §2a, the GUI-layer sites and the source-scan guard.*

*Written 2026-09-14, after `generate_matlab_command` crashed with
`TypeError: unhashable type: 'dict'` on a project whose
`scistack_entities.toml` declares `delsys_config` and `gaitrite_config` as
inline tables. Concerns `scidb` (identity), `scistack-gui` (MATLAB command
generation) and, for the second half, `scistackplotdb`.*

## 1. The rule

A **constant** is a for_each input bound to a literal value rather than to a
variable type, a `PathInput`, or a `Parameter`. Nothing anywhere restricts that
value to a scalar:

- `docs/claude/entities-toml-format.md` rule 2 — *an inline table under
  `[parameters]` **is** the value*. So `delsys_config = {fs = 2000, channels =
  ["RHAM", "RTA"]}` puts a `dict` (holding a `list`) into the constants map.
- `_format_matlab_value` in `scistack-gui/scistack_gui/api/matlab_command.py`
  deliberately renders `dict` → `struct(...)` and `list` → string/numeric/cell
  array, mirroring `+scidb/+internal/from_python.m`, precisely so that such a
  value reaches the user's MATLAB function as a struct rather than as a char
  array holding a Python repr.

So dict- and list-valued constants are a **supported, ordinary** case, and every
consumer must be written for them.

## 2. Why this keeps breaking: the variant-grouping key

Almost every consumer of variants needs the same notion of *"did these two calls
use the same constants?"*, and every one of them expresses it by building a
**key** and then putting it in a `set` or using it as a `dict` key:

- group DB variant rows into one `for_each` call per wiring
- de-duplicate variants before execution
- decide whether two records came from the same producing variant
- collapse invocations into config variants

The obvious spelling is the wrong one:

```python
key = tuple(sorted(constants.items()))   # WRONG
```

It is hashable only while every value happens to be a scalar. The first real
project that declares a config-table parameter turns it into
`TypeError: unhashable type: 'dict'` — raised not at the point of the mistake
but from a `setdefault` or `set.add` several frames away. Note that *sorting*
succeeds (the tuples compare on their string keys, which are unique), so the
failure is purely at hash time and is invisible to any test whose constants are
all numbers.

The correct spelling, owned by scidb because identity is scidb's job:

```python
from scidb.provenance import constants_identity_key
key = constants_identity_key(constants)
```

It renders each value through `constant_value_repr` (`repr`), which is
deterministic for a given content and order — the same recipe
`provenance_save.record_run` already uses for its invocation cache key — sorts
by parameter name, and returns `()` for anything that is not a dict so a
possibly-missing field can be passed straight through.

**Every** grouping key over constants now routes through it:

| Site | What it groups |
|---|---|
| `provenance_query.function_variant_configs` | invocations → config variants |
| `provenance_query.pipeline_variants` | invocations → pipeline step variants |
| `provenance_query._producing_variant_key` | one record → its producing variant |
| `provenance_query.variant_keys_batch` | the batched form of the above |
| `locations.pathinput_configs` | invocations → PathInput discovery configs |
| `database.filter_variants_for_execution` | dedup before execution |
| `scistack_gui.api.matlab_command._group_variants` | variant rows → `for_each` calls |

The batched and per-record variant keys are compared against each other
(`current_records_by_schema_batch` mixes them), so they must stay **byte-
identical**. Sharing one helper is what guarantees that; two hand-rolled copies
is what put the warning in `variant_keys_batch`'s docstring in the first place.

### 2a. The GUI-layer consumers (2026-10-21)

The 2026-09-14 fix moved scidb's sites and `matlab_command._group_variants`,
but the GUI kept six hand-built keys of its own. A real project hit one of
them: running a **history** node that had a **drawn output edge** went through
`_apply_manual_output_types`, which built
`tuple(sorted(constants.items()))` and raised
`TypeError: unhashable type: 'dict'` on the project's `delsys_config` table.

The crash also showed a second defect. Target derivation runs *before*
`_run_in_thread`'s per-target `try`, so the exception left the thread with
nothing but a stderr traceback. No `run_done` was pushed and the run spun
until the user cancelled it, so the crash looked like a hang.
`api/run._report_crashed_run` now catches anything that escapes the thread.
It logs the traceback to `scidb.log` and, if the run is still registered in
`_active_runs` (every path that already sent `run_done` popped it first),
pushes a failed `run_done`. Pipeline runs already caught everything.

These sites now route through `constants_identity_key`:

| Site | What it groups |
|---|---|
| `services/execution_service._apply_manual_output_types` | history targets → one per drawn output (**the crash**) |
| `services/execution_service` (pipeline step loop, `seen_target_keys`) | dedup (constants, output) per step |
| `services/code_export_service` (`seen_keys`) | dedup steps in exported code |
| `services/matlab_command_service` (`seen_target_keys`) | dedup steps in MATLAB pipeline scripts |
| `domain/variant_resolver.deduplicate_variants` | dedup before a node run |
| `domain/variant_resolver.merge_pending_constants` (3 keys) | "does this pending combo already exist?" |
| `scidb/inspect/graph.py` (`constant_keys`) | distinct variants per step for `scidb` CLI |

`merge_pending_constants` and `inspect/graph.py` used `str(v)`, which is
hashable but was a second recipe. One visible difference: `str` treated
`1` and `"1"` as equal and `repr` does not. A pending value is coerced with
`ast.literal_eval` before comparison, so the two only disagree when the
recorded value is the *string* `"1"` and the user stages `1`. That is now a
new combo (int 1) instead of being silently swallowed. This is correct: the
function would receive a different type.

**The guard.** `scistack-gui/tests/test_constants_identity_gui.py::test_no_hand_built_constants_key`
AST-scans every tracked shipped `.py` (all `*/src/` plus
`scistack-gui/scistack_gui/`, excluding tests and the owner) for a
`tuple(...)`/`frozenset(...)` call whose argument iterates `.items()` of
something named `constants`. Grep can't do this reliably because the
docstrings quote the bad spelling. The one-off regression tests in §2 could
only protect the sites they named; the guard protects every future site.

### What is *not* the same problem

`str(val)` is fine wherever the result is only ever a display string or a
counter bucket (`graph_builder.py`, `get_aggregated_variants`'s `const_counts`,
`inspect/graph.py`'s `_value_str`). Membership tests like
`if val not in existing_list` are fine too — that is `==`, not `hash`. The trap
is specifically **hashing**.

### The regression tests

- `scidb/tests/test_provenance_identity.py` — the key is hashable for dict and
  list values, is order-insensitive, distinguishes differing dicts, and stays
  byte-identical to the `sorted((k, repr(v)))` recipe it replaced.
- `scistack-gui/tests/test_matlab.py::TestGroupVariantsNonScalarConstants` — a
  dict constant groups without raising, multi-output rows still collapse to one
  `for_each` call, and differing dict constants stay separate variants.
- `scistack-gui/tests/test_constants_identity_gui.py` — dict constants through
  `_apply_manual_output_types`, `deduplicate_variants` and
  `merge_pending_constants`, plus the source-scan guard (§2a).
- `scistack-gui/tests/test_api.py::TestRunEndpoint::test_crash_before_the_target_loop_reports_a_failed_run`
  — a crash in target derivation ends as one failed `run_done`.

## 3. The other half: `distribute` and identical content

The crash above surfaced while trying to *fix* a different problem, and the two
are worth remembering together because the second one is silent.

`distribute=True` splits one call's return value across the schema key one level
below the deepest iterated key (`scifor/foreach.py`, step 3): the function is
called once per (subject, session, speed) and its result is cut into
trial=1..N. With `distribute=False`, the same iteration instead calls the
function once per trial with the *same* input and saves the **whole** result at
each trial.

A record id is `hash(type | schema_version | content_hash | metadata)`
(`scicanonicalhash.hashing.generate_record_id`), and metadata includes the
schema keys. So the four trials are four genuinely distinct records that happen
to share one `content_hash`. Nothing is corrupt; nothing errors.

### Why it is invisible in a figure

N records with identical payloads render as N traces drawn exactly on top of
each other. The figure looks like **one line in whichever colour was drawn
last** — a legend with four trials and one visible colour — and filtering the
colour factor to any single value shows the same curve again. It reads as a
plotting bug, which is the worst possible place to start debugging from.

### How to see it

One query, read-only:

```
scidb --db <path> sql "SELECT r.content_hash, count(*) AS n,
  string_agg(DISTINCT s.trial, ',') AS trials
  FROM _record r JOIN _schema s ON s.schema_id = r.schema_id
  WHERE r.type = '<Variable>' AND r.excluded IS DISTINCT FROM TRUE
  GROUP BY 1 HAVING count(*) > 1 ORDER BY n DESC"
```

Each row is one group of locations holding identical data. Re-running the step
with `distribute=true` should empty the result.

**There is no automatic check (removed 2026-10-20, user decision).**
`load_variable` used to run this query on every load (`identical_content_groups`
+ `_log_identical_content`) and WARN "N group(s) of records hold IDENTICAL
data … should have run with distribute=True". It fired far more often on
legitimate data than on the mistake: any scalar variable with few distinct
values (1–10 ratings: 9 groups over 299 of 300 records; box-score points) has
byte-identical records by nature. The query, the WARN and both tests were
deleted; don't reinstate a load-time check. Use the SQL above by hand when a
figure shows the overplotting signature.

## 4. If you are adding a consumer

1. Grouping or de-duplicating by constants? Use
   `scidb.provenance.constants_identity_key`. Do not write
   `tuple(sorted(constants.items()))`. The guard test
   (§2a) fails if you do.
2. Rendering a constant into MATLAB or Python source? Use
   `_format_matlab_value` / the existing formatter — do not `str()` it.
3. Writing a test for either? Put a `dict` in the constants. A test whose
   constants are all `int`s cannot fail on any of this.
