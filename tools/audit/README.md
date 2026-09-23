# tools/audit

Reproducible measurements behind `docs/claude/cleanup-audit.md` §1. Re-run after
each cleanup pass and compare against the numbers recorded there.

    tools/audit/run.sh [out_dir]    # default tools/audit/out (gitignored); ~3 s

Needs git, node, coreutils (no Python). Outputs:

| file | contents |
|---|---|
| `sizes.txt` | per-package source/test lines by language; 40 largest source files |
| `hotspots.txt` | per file: commits, fix-like commits, current lines (sorted by fixes) |
| `functions.txt` | longest / widest functions; counts over 100/200/400 lines, >10 params |
| `imports.txt` | cross-package import edges, private cross-package imports, lazy imports |
| `excepts.txt` | broad `except` blocks whose body swallows (`pass`, `return []`, ...) |
| `dup_names.txt` | top-level names defined in 2+ files — rival owner OR forwarder; check by hand |
| `dead.txt` | functions with no reference outside tests (and none at all) |
| `markers.txt` | trap/silently/workaround counts, date stamps, TODOs, `get_database()` calls |

Caveats: `pyscan.js` is indent-based, not a parser, so nested-closure spans are
approximate. `dead.txt` counts identifier tokens across py/m/ts/toml/json, so
names reached only by decorators (FastAPI routes) or string dispatch are false
positives. "Fix-like" commits are matched on message keywords and under-count
bundled fixes.
