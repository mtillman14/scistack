# debug_example2_advanced.py

1. database.py DatabaseManager.\_ensure_meta_tables(): Why doesn't \_lineage table have output_number?
   A: The lineage record answers "what produced this specific variable?" - you already have the variable, so you don't need to know its output number. However, you raise a valid point: If you wanted to answer "was this the 1st or 2nd output of split_signal()?", that information is currently lost in \_lineage.
2. database.py DatabaseManager.\_ensure_meta_tables(): In \_lineage table, could/should the difference between inputs and constants be quantified as whether an argument is an OutputThunk (i.e. an input) or not (i.e. a constant)? In thunk.py PipelineThunk.compute_cache_key(), the docs say "(record_id for saved variables, content hash for others)". How is that different than OutputThunk or not?
3. database.py DatabaseManager.\_ensure_meta_tables(): In \_computation_cache table, how is cache_key computed?
4. debug_example2_advanced.py: Let's say I want to compare some value that I computed for subject=1 across different days, i.e. day=1 vs. day=2. Using this framework, how could I get a dict where the keys are the day values, and the values are the corresponding day's values? I think that's the format that would best lend itself to analysis to answer that question.
5. hashing.py: generate_vash() what happens when the data being saved is ~5 GB large? For example a massive list or tuple, or a huge numpy array or pd.DataFrame. Will there be a huge slowdown due to serialization?
6. preview.py: Remove the attempts to identify the "value" column. Columns are only really named "value" in example code.
7. In debug_example2_advanced.py: On line 223, `RawSignal.save()`, which calls BaseVariable.save(), why is `isinstance(data, OutputThunk)` `False`? Or really, why is the computation not cached?
8. In database.py DatabaseManager.load(): Is json_extract slow? e.g. `"json_extract(metadata, '$.subject') = ? AND json_extract(metadata, '$.session') = ? AND json_extract(metadata, '$.channel') = ?"`
9. In debug_example2_advanced.py line 310 butter_filter(), I don't think I'm seeing the cache hit because the result of db.get_cached_by_key() on line 101 of thunk.py results in cached = None. Line 312 has the same problem, but lines 311 and 313 properly hit the cache.

```text
--- Processing Pipeline (Second Run - Cache Test) ---
b2.was_cached: False
a2.was_cached: False
filtered2.was_cached: True
analytic2.was_cached: False
envelope2.was_cached: True

Results match original:
  Filter b: True
  Filter a: True
  Filtered: True
  Envelope: True
```

10. In the lineage inspection of the `envelope` variable, which has in its full lineage the function `butter_filter()`, why do I see 0 constants in the

11. Why is this happening in lines 434 - 441 of debug_example2_advanced.py, where the type is unknown?

```text
--- Derived Variables Query ---
Variables derived from raw signal:
  - Unknown: a1d326c2317a...
  - Unknown: 63b1eb9fc21d...
  - Unknown: de2fe42c8d48...
  - Unknown: 390d4839bb53...
```

12. On line 417 of debug_example2_advanced.py, DatabaseManager.get_full_lineage() appears to be broken. Here's the stack trace:

```text
--- Lineage Inspection ---
Traceback (most recent call last):
  File "/opt/homebrew/Cellar/python@3.13/3.13.2/Frameworks/Python.framework/Versions/3.13/lib/python3.13/runpy.py", line 198, in _run_module_as_main
    return _run_code(code, main_globals, None,
                     "__main__", mod_spec)
  File "/opt/homebrew/Cellar/python@3.13/3.13.2/Frameworks/Python.framework/Versions/3.13/lib/python3.13/runpy.py", line 88, in _run_code
    exec(code, run_globals)
    ~~~~^^^^^^^^^^^^^^^^^^^
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/adapter/../../debugpy/launcher/../../debugpy/__main__.py", line 71, in <module>
    cli.main()
    ~~~~~~~~^^
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/adapter/../../debugpy/launcher/../../debugpy/../debugpy/server/cli.py", line 508, in main
    run()
    ~~~^^
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/adapter/../../debugpy/launcher/../../debugpy/../debugpy/server/cli.py", line 358, in run_file
    runpy.run_path(target, run_name="__main__")
    ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/_vendored/pydevd/_pydevd_bundle/pydevd_runpy.py", line 310, in run_path
    return _run_module_code(code, init_globals, run_name, pkg_name=pkg_name, script_name=fname)
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/_vendored/pydevd/_pydevd_bundle/pydevd_runpy.py", line 127, in _run_module_code
    _run_code(code, mod_globals, init_globals, mod_name, mod_spec, pkg_name, script_name)
    ~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/mitchelltillman/.vscode/extensions/ms-python.debugpy-2025.18.0-darwin-arm64/bundled/libs/debugpy/_vendored/pydevd/_pydevd_bundle/pydevd_runpy.py", line 118, in _run_code
    exec(code, run_globals)
    ~~~~^^^^^^^^^^^^^^^^^^^
  File "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/general-sqlite-database/examples/debug_example2_advanced.py", line 417, in <module>
    full_lineage = db_manager.get_full_lineage(type(envelope.data), subject=1, session="morning", channel="EMG")
  File "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/general-sqlite-database/.venv/lib/python3.13/site-packages/scidb/database.py", line 656, in get_full_lineage
    var = self.load(variable_class, metadata)
  File "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/general-sqlite-database/.venv/lib/python3.13/site-packages/scidb/database.py", line 400, in load
    table_name = self._ensure_registered(variable_class, auto_register=False)
  File "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/general-sqlite-database/.venv/lib/python3.13/site-packages/scidb/database.py", line 272, in _ensure_registered
    raise NotRegisteredError(
    ...<2 lines>...
    )
scidb.model.exceptions.NotRegisteredError: Variable type 'ndarray' is not registered. No data has been saved for this type yet.
```
