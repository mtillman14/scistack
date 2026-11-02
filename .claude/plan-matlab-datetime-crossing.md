# Plan: datetime scalars never cross the MATLAB bridge bare

## Finding (2026-11-02, user's MATLAB R2024a Update 5)

In MATLAB R2024a, every Python `datetime` handed to MATLAB is converted to a
MATLAB `datetime`. That includes values returned from `py.` calls and values
read out of a py.list or py.dict:

- `scidb.internal.datetime_to_python(d,'scalar')` -> class `datetime` (MATLAB);
  sent back to Python as `tzinfo=datetime.timezone(+2h)`. The IANA zone is lost.
- `py.scimatlab.bridge.datetimes_from_matlab(..., 'scalar')` -> MATLAB `datetime`.
- `datetime_to_python(NaT,'scalar')` -> "NULL pointer returned from Python",
  which aborts the whole MATLAB test run.

Effects:

- `TestDatetimeRoundTrip/test_zoned_scalar_keeps_its_zone` fails (Europe/Berlin -> +02:00).
- `test_nat_scalar` kills the run.
- Offset zones look fine only because a fixed offset survives the conversion.

Columns (Series) and `'list'` results stay as py objects and are not affected
while they remain inside Python. The design assumption broken here is that a
py datetime stays a py object in MATLAB (e629f7ba). P4/P5 did not cause this.

## Rule (one owner)

`scimatlab.bridge.convert` owns how a datetime scalar crosses:

- It crosses ONLY inside a box, `DatetimeCrossing(value)`, a frozen dataclass.
  MATLAB does not convert the box.
- MATLAB never reads `.value`. It hands the box back to Python.

## Steps

1. **convert.py**
   - Add `DatetimeCrossing`.
   - Add `box_datetimes(obj)`. It boxes datetime/NaT/np.datetime64 scalars,
     recursing into list/tuple/dict and object Series.
   - Add `unbox_datetimes(obj)`, its inverse.
   - `datetimes_from_matlab(kind='scalar')` returns a box.
   - `datetimes_to_matlab` accepts a box.
   - DEBUG log of the boxed/unboxed counts.
2. **Python -> MATLAB.** Box every bridge return MATLAB reads data from:
   - `get_batch_data_item`;
   - `load_and_extract` / `wrap_batch_bridge` per-item data (and any `py_var.data`
     read: BaseVariable.m:986 switches to a bridge accessor that boxes);
   - the for_each / load paths that hand records to `from_python`.

   The inventory is the first task: grep every `py.scimatlab.bridge.*` and
   `py_var.<attr>` read in the .m files.
3. **from_python.m**
   - A `py.scimatlab.bridge.convert.DatetimeCrossing` branch ->
     `datetime_from_python(box)`.
   - The dict/list recursion asks Python for boxed elements.
4. **MATLAB -> Python.** Every Python entry that receives MATLAB-built data
   unboxes once:
   - `save_batch_bridge`;
   - BaseVariable.m's direct `py_db.save_variable` (route through a bridge
     `save_variable_bridge` that unboxes);
   - for_each output saves.

   to_python struct/cell nesting then carries boxes safely.
5. **Tests**
   - pytest `scimatlab/tests/test_bridge_datetimes.py`:
     - box/unbox round trip (named zone, offset, NaT, naive, nested dict/list);
     - `datetimes_from_matlab('scalar')` returns a box;
     - every bridge return path boxes.
   - A static guard: no .m file reads a datetime through `py_var.data` directly.
   - MATLAB: `TestDatetimeRoundTrip` as it is now. Add a struct-field datetime
     and a cell of datetimes.
6. Docs: `docs/claude/datetime-roundtrip.md` "MATLAB" section (the R2024a
   conversion and the box rule). Add a memory entry.

## Open question

- `timedelta` -> MATLAB `duration` is probably converted the same way (R2024a).
  Nothing sends timedelta today. Box it too in the same helper, or leave it?

## Progress

- **Built 2026-11-02 (untested; user decision: box timedelta too).**
  - `bridge/crossing.py`: the boxes, `box_datetimes` / `unbox_datetimes`, and
    `@matlab_entry`.
  - `@matlab_entry` decorates the data-carrying functions in entry.py (9 +
    to_csv_bridge), every `pipeline_*`, `datetimes_from/to_matlab`, and the new
    `durations_to_matlab` and `boxed_tolist`.
  - New entries: `save_variable_bridge`, `variable_filter`,
    `schema_key_filter`, `variable_data`.
  - MATLAB:
    - `from_python.m` box branches, plus a WARN for a MATLAB datetime it
      receives already converted;
    - `boxed_tolist` in the element paths;
    - `duration_from_python.m`;
    - BaseVariable save/filters/wrap_py_var and SchemaKey moved onto the
      bridge entries.
  - Tests: `test_bridge_crossing.py` (new); `test_bridge_datetimes.py` scalar
    and list results are boxes now; three new `TestDatetimeRoundTrip` cases.
  - Doc: datetime-roundtrip.md, "R2024a converts bare datetimes".
- Not done: no struct-field or cell datetime MATLAB test. Python's save of a
  dict holding a Timestamp is not established, so such a test would test
  that first.
