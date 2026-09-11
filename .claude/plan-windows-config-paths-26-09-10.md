# Plan: config paths written on Windows must resolve on macOS

Date: 2026-09-10

## Symptom

`./scistack_matlab_variables` is populated at the project root; it should be
`./src/scistack_matlab_variables`.

## Root cause

`scidb.log`, 2026-09-09 onward:

```
[config] entities_file set to: /Users/mitchelltillman/.../aging-well-abilitylab/src\scistack_entities.toml
[entities] Materialized 2 MATLAB classdef(s) in /Users/.../aging-well-abilitylab/scistack_matlab_variables
[matlab_parser] Cannot read MATLAB file /Users/.../aging-well-abilitylab/y:\LabMembers\MTillman\GitRepos\aging-well-abilitylab
```

The project's `scistack.toml` was written by a **Windows** session and is
being read on **macOS**. `set_entities_file` records
`str(entities_file.relative_to(project_root))`, which on Windows is
`src\scistack_entities.toml`; `_toml_str` escapes the backslash, so it
round-trips as a literal backslash.

On POSIX `\` is a legal filename character, so nothing errors:
`project_root / "src\\scistack_entities.toml"` is a **file in the project
root** whose name happens to contain a backslash. The GUI created it, wrote
declarations into it, and read them back. The MATLAB classdef stub directory
is `<entities_file.parent>/scistack_matlab_variables` — and that parent is
now the project root. Hence the symptom.

The absolute entries have the same origin and are worse: `modules` /
`matlab.sources` hold `y:\LabMembers\...`, which `Path.is_absolute()` calls
False on POSIX, so they get joined onto the project root and every error
message reads `/Users/.../aging-well-abilitylab/y:\LabMembers\...`.

Not a regression from the entities-file work earlier today — the
root-level materialization is in the log from 2026-09-09. That change made
the GUI actually *use* `entities_file`, which is why it became visible.

## Fix

### Read — one resolver, in scifor

`scifor/discovery.py` (the layer that already owns `find_project_config` /
`read_scistack_section`, so scidb and the GUI cannot disagree):

- `normalize_config_separators(raw)` — `\` → `/`, POSIX only.
- `resolve_config_path(root, raw)` — the above, then join against *root*;
  logs the reinterpretation, and **warns when the literal backslashed path
  exists on disk**, naming both it and the corrected path. That file holds
  real declarations; the user must not watch them silently disappear.
- `is_windows_absolute(raw)` — `Y:\...` / `\\server\share`. Returned
  unchanged rather than joined onto the root.

Callers:

- `scidb.entities.resolve_entities_path` — the declared `entities_file`.
- `scistack_gui.config` — `_config_path` for every raw value
  (`modules` entries, `glue_dir`, `variable_file`, `matlab.variable_dir`,
  `matlab.entities_file`, `_resolve_raw_entry` for the Paths popup's
  comparisons) and `_config_pattern` for glob entries, which are expanded as
  text rather than as Paths.

### Write — record relative keys POSIX-style

`config._portable_relpath` (`Path.as_posix()`) for the `entities_file` and
`glue_dir` values written into scistack.toml. Windows reads `/` natively, so
POSIX form is correct on both platforms and a Windows session stops
producing Mac-broken configs.

### Report — a startup notice, not a silent repair

`startup.check_windows_config_paths(config)` records a **non-blocking**
`windows_config_paths` StartupError listing every backslashed value in the
config, calling out the Windows-absolute ones as unresolvable here. Wired
into both entry points beside `check_lockfile_staleness`.

## Manual cleanup this does not do (deliberately)

- `<root>/src\scistack_entities.toml` (a real file, with the declarations
  added since 2026-09-09) is left in place and reported. Never delete.
- `<root>/scistack_matlab_variables` becomes inert once the stub directory
  moves back beside the entities file; delete it by hand.
- The `y:\...` module/source entries cannot be repaired from here — re-add
  the mac-side paths in 📁 Paths.

## Test commands

```
python -m pytest scifor/tests/test_discovery.py -q
python -m pytest scidb/tests/test_entities_toml.py -q
python -m pytest scistack-gui/tests/test_config.py -q
```
