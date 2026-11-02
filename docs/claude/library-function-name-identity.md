# Library Function Name Identity (`pandas.read_csv` vs `read_csv`)

## Overview

A **library function** is a function the user did not write —
`pandas.read_csv`, `numpy.mean`, a stdlib call — pinned to the canvas as an
ordinary function node. Unlike a discovered function, it has no file in the
user's project and never lives in `registry._functions`; it is imported on
demand at every use site (`scistack_gui/project/library_functions.py`, whose module
docstring explains why caching it in the registry was a bug).

That import-on-demand design leaves one thing to get right, and it is the
subject of this doc: **what the function is called.**

Two layers name a function by different mechanisms:

| Layer | Names a function by | For `pd.read_csv` gives |
|---|---|---|
| **GUI** | the canonical *reference* string | `pandas.read_csv` |
| **scifor / scidb** | `getattr(fn, "__name__")` | `read_csv` |

These must agree. If they don't, the same function exists twice under two
names on either side of the run boundary, and nothing reconciles them.

---

## 1. Where each name comes from

### The GUI side — the canonical reference

The user types `pd.read_csv`. `library_functions.canonical_reference()`
expands the conventional import alias (`pd` → `pandas`, `np` → `numpy`) at
the API boundary, so the alias never reaches persistence, node labels, or
generated code. From that point on the GUI knows this function as
`pandas.read_csv`:

- `_pipeline_builtin_functions` persists that string,
- the canvas node is `fn__pandas.read_csv__<call_id>` with label
  `pandas.read_csv`,
- `RunRequest.function_name` carries that string,
- `registry.get_function("pandas.read_csv")` resolves it by import.

### The scidb side — `fn.__name__`

Every recording site in the backend derives the function name from the
callable itself:

- `scifor/src/scifor/foreach.py:555` — the `for_each(...)` run banner
- `scidb/src/scidb/run/foreach.py:532` — batch save / provenance
- `scidb/src/scidb/provenance/state.py:396` — `check_node_state`

`pandas.read_csv.__name__` is `"read_csv"`. The qualified reference exists
only in the GUI's head; the callable itself has never heard of it.

---

## 2. The bug this caused

Handing the *raw* pandas callable to `for_each` produced a run recorded
under a name the canvas never uses:

```
[run_thread] Thread started ... function=pandas.read_csv
[scifor] for_each(read_csv) — 4 iterations: subject=3 values [...]
[scidb] [provenance] recorded run_id=... for 1364 record(s) of fn=read_csv
[pipeline] graph built successfully (scope=main): 5 total nodes (2 functionNode, ...)
```

Three distinct symptoms, one cause:

1. **A duplicate node.** The graph rebuild fetches functions from scidb and
   builds a node labeled `read_csv`. `graph_builder.merge_manual_nodes`
   then tries to graduate the manual node onto its DB counterpart by
   matching `(node_type, label)` — and `"pandas.read_csv" != "read_csv"`,
   so it graduates nothing and *adds* the manual node instead. The canvas
   ends up with two function nodes for one function.

2. **History invisible to the run path.** `derive_fn_targets(db,
   "pandas.read_csv")` queries by function name, so it never sees the run
   that was recorded as `read_csv`. The node only stays runnable because
   its manual edges independently describe the wiring.

3. **Permanently red.** `check_node_state` looks the node up by its label's
   name, finds no invocations under `pandas.read_csv`, and reports red —
   immediately after a successful run of 1364 records.

---

## 3. The fix — hand out a name-qualified callable

`library_functions.with_qualified_name(fn, canonical)` wraps the imported
callable so that `__name__` **is** the canonical reference. Both
`resolve()` and `validate()` return that wrapper, so there is exactly one
callable identity per reference and no way to reach `for_each` with the
bare name.

Setting `__name__` on `pandas.read_csv` directly is not an option — it is a
shared global that the user's own scripts import.

**What the wrapper does not change.** It is built with `functools.wraps`,
which sets `__wrapped__`:

| Consumer | Mechanism | Reads through? |
|---|---|---|
| Settings-panel parameter handles | `inspect.signature` | yes — follows `__wrapped__` natively |
| Sidebar item-info docstring | `inspect.getdoc` | yes — `__doc__` copied by `wraps` |
| Lineage function hash | `scilineage/hashing.py:_hash_source` | yes — `_unwrap()` peels `wraps` layers explicitly |
| "Go to source" | `inspect.getsourcefile` + `getsourcelines` | **no — must unwrap by hand** |

So the function hash and the signature are identical to the unwrapped
function. **Only the name differs**, which is the entire point.

### The `getsourcefile` trap

`inspect` is not consistent about wrappers, and the inconsistency is silent:

- `inspect.getsourcelines(fn)` calls `unwrap()` internally → pandas' line.
- `inspect.getsourcefile(fn)` reads `fn.__code__.co_filename` → the
  **wrapper's** file.

`pipeline_service.get_function_source` paired those two calls, so on any
wrapped callable it returned `library_functions.py` with a line number from
pandas — a plausible-looking result pointing at an arbitrary line of the
wrong file. It now calls `inspect.unwrap(fn)` before both.

Any future `getsourcefile`/`getfile` call site on a registry-resolved
callable needs the same treatment. Note this hazard is not specific to
library references — it applies to any decorated user function too; the
wrapper just made an existing latent bug reachable.

A reference whose name already matches (`len` → `len.__name__ == "len"`) is
returned unwrapped — no indirection where none is needed.

`_QUALIFIED` keeps one wrapper object per canonical reference. This is
*not* a resolution cache — `resolve()` still imports on every call, per the
module's no-caching rule — it only prevents identity churn, since
`resolve()` runs on every registry miss and every node-parameter read.

### Precedent

`scidb/src/scidb/run/foreach.py:76-89` wraps a `scidb.Merge` spec for exactly
the same reason: to give scifor the `__name__` it should record. Whenever a
layer invents a name for a callable that the callable does not carry
itself, that name has to be attached to the callable before it crosses into
scifor/scidb.

---

## 4. Consequences and gotchas

- **Pre-fix databases keep the old name.** Records written before this fix
  are recorded under `read_csv` and still render as their own DB-derived
  node. Per project ethos the fix is to hide it, not delete it (or reset a
  scratch example DB).

- **Function names may now contain dots.** `_invocation.function_name` can
  hold `pandas.read_csv`. Anything that assumes a function name is a bare
  Python identifier — code export writing `fn_name(...)` verbatim, node-id
  parsing that splits on `.` — must handle the qualified form. Node ids
  already do (`fn__pandas.read_csv__xn6cc4` splits on `__`).

- **This is a GUI-layer fix, unusually.** Per CLAUDE.md, logic belongs in
  the owning scistack layer — but here the GUI is the layer that invents
  the qualified name. scifor and scidb are right to trust `fn.__name__`;
  the caller is responsible for handing them a callable that knows its own
  name.

---

## 5. Tests

`scistack-gui/tests/test_library_functions.py`:

- `TestQualifiedName` — name is the canonical reference (including through
  an alias and a nested submodule), signature/docstring still read through
  to the real function, the wrapper calls through, identity is stable
  across `resolve()` calls, and `validate()` hands out the same callable.
- `TestQualifiedName.test_source_line_belongs_to_the_reported_file` —
  asserts the file/line pair is internally consistent, which is how the
  `getsourcefile` trap above surfaces. A test that only checks the file
  path, or only the line number, passes while "go to source" is broken.
- `TestRegistryLookup.test_lookup_hands_the_run_path_a_qualified_name` —
  the choke point `api/run` uses.

Identity assertions elsewhere in the suite go through `__wrapped__` (see
the `unwrap` helper in that file and
`test_builtin_functions.py::test_alias_resolves_at_lookup_too`).
