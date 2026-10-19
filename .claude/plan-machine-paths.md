# Plan: machine-specific absolute paths (2026-10-19)

## Problem
`scistack.toml` is committed and opened on several machines (Windows `Y:\`,
Linux `/home/mtillman/mnt/rto`, macOS). An absolute path is right on one
machine only. On the others it is a discovery load error on every registry
refresh: scidb.log 2026-10-19 shows
`y:\home\mtillman\mnt\rto\LabMembers\MTillman\GitRepos\data-analytics-code-libraries`
("File does not exist") 22 times. That is a Linux path joined onto the Windows
project drive.

## Options
**A. Named roots (recommended).** scistack.toml writes a path once, portably:
`"{rto}/LabMembers/MTillman/GitRepos/data-analytics-code-libraries"`. Each
machine maps the root name in a machine-local file outside the repo:

```toml
# ~/.scistack/machine.toml   (Windows: %USERPROFILE%\.scistack\machine.toml)
machine_id = "6f1c…"          # generated on first use; used for logging only
[roots]
rto = "Y:/"                   # on the Linux box: "/home/mtillman/mnt/rto"
```

- One value in the repo, so no per-machine copies drift (NOTE 4).
- An unmapped root is a single clear notice ("root 'rto' is not mapped on this
  machine; add it to ~/.scistack/machine.toml"), not a load error per refresh.
- The GUI Paths popup offers "make portable" when a typed path starts with a
  mapped root.

**B. Machine-ID sections.** scistack.toml gains `[machines.<id>]` tables, each
listing that machine's absolute paths. This is closer to the original idea, but
every machine's paths live in the shared file: N copies of each path, a new
machine edits the repo, and the IDs mean nothing to a human reader.

## Stages (option A)
1. `scifor.discovery` owns it: `machine_config()` (read, create with a
   `machine_id` on first use, `~` via `Path.home()`), and `expand_roots(raw)`
   called inside `resolve_config_path` and `normalize_config_separators`, which
   are already the single choke point for config paths. INFO log on startup:
   machine id, file path, roots mapped. Tests: expansion, an unmapped root, no
   file, creation, Windows/POSIX separators.
2. An unmapped or missing root reports ONE structured notice per refresh
   (reusing the `windows_config_paths` startup-notice path) instead of a load
   error. Tests.
3. GUI Paths popup: show the expanded value next to the `{root}` form, and offer
   "make portable" for an absolute path under a mapped root. Writers keep
   `{root}`; they never write the expanded path back. Add to
   docs/gui-manual-testing-todo.md.
4. Docs: docs/claude/config-file-formats.md (the `{root}` grammar and the
   machine file).

## Open questions for the user
- A or B?
- Root syntax: `{rto}` vs `$rto` vs `rto:`.
- Machine file location: `~/.scistack/machine.toml` OK?

## Not covered by this plan
The other load errors in the same list are MATLAB *scripts* (no function
declaration): main_entrypoint_aim2.m, main_overground.m, and others. A separate
fix: classify scripts as "not a function" at DEBUG instead of as load errors.

## Status (2026-10-19)
User chose: A (named roots), `{rto}` syntax, `~/.scistack/machine.toml`.
- Stage 1 done: `scifor/machine.py`, tests `scifor/tests/test_machine.py`.
- Stage 2 done: `startup.check_machine_config_paths` (+ `check_config_paths`
  aggregate called by bootstrap AND server.main); config loops skip
  unresolvable entries at DEBUG. `windows_config_paths` narrowed to relative values.
  Also covers POSIX-absolute on Windows (the actual 2026-10-19 symptom).
- Stage 3 partial: `add_path` writes `{root}/rest` automatically (no prompt).
  Paths popup shows the full path (managed_path_details), `{root}` form greyed below.
- Stage 4 done: docs/claude/config-file-formats.md "Machine roots".
All tests pass (2026-10-19).
