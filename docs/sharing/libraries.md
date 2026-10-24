# Libraries

<!-- Code variant of sharing/libraries.ui.md. Ground truth:
     scistack-gui/scistack_gui/library_cli.py (`scistack library
     list|add|remove|create|copy|build`, --project/--json before the
     subcommand, --db), scidb/library.py (layout, read_library,
     write_library/create_library, [library] derived_from),
     scifor.discovery authored_library_dirs (libs/<name>/src/<name>/) and
     ensure_authored_libraries_importable, scidb.names.library_of (recorded
     names lib.fn), scidb/rename.py rename_functions + foreach_config.
     function_hash_for, scidb/pipeline.py Pipeline.bind (params "fn.param" step
     values, _default_input_names), services/library_authoring.py (copy:
     derived_from newest first, ancestors dropped from dependencies). -->

A **library** is a set of SciStack functions packaged as an ordinary Python package, so that any number of projects can install it and use it. Use one when code (your lab's EMG preprocessing, say) belongs in several projects and you'll keep improving it: everyone who uses it gets your new version by installing it, the same way they'd update `numpy`.

Compared with copying a `.py` file between projects:

| | Copied file | Library |
| --- | --- | --- |
| Gets your later changes | no, each project has its own copy | yes, when they install the new version |
| Its functions are recorded as | the project's own (`bandpass`) | the library's (`emg_tools.bandpass`), so they never clash with a project's own `bandpass` |
| Carries MATLAB code, Variables, Parameter defaults | only what you copy by hand | yes, in a fixed layout |

There are two kinds of library in a project:

- **Yours**: it lives in the project's `libs/<name>/` folder. You edit its code there.
- **Installed**: someone else's, installed with `pip`. Change it by making your own copy (below).

## What's in a library

```
emg_tools/                        the library folder (a Git repository, or libs/emg_tools/ in a project)
├── pyproject.toml                name, version, dependencies
└── src/emg_tools/
    ├── __init__.py               `from emg_tools import bandpass` works
    ├── filters.py                Python functions        → recorded as emg_tools.bandpass
    ├── scistack_entities.toml    its Variables, Parameter / path input defaults, [library] table
    ├── pipelines/*.json          pipelines for the GUI canvas (optional)
    └── matlab/+emg_tools/*.m     MATLAB functions         → called as emg_tools.bandpass
```

Only `pyproject.toml` and the package are required. The `[library]` table in its [entities file](../getting-started/project-files.md#in-a-library-library) names the schema keys its pipelines are written for and, for a copy, which libraries it came from (`derived_from`). The `pipelines/` folder holds canvases that the GUI places; from code you use the library's functions, as below.

## Use a library

1. Install it into the project's `.venv` (see [Package and distribute it](#package-and-distribute-it) for where it might come from):

    ```bash
    python -m pip install emg_tools
    ```

2. List it in the project, so SciStack scans it and records its functions under its name:

    ```bash
    scistack library add emg_tools
    ```

    This adds it to `packages` in [`scistack.toml`](../getting-started/project-files.md#packages). `scistack library list` shows every listed library, its schema keys, pipelines and any problems; `scistack library remove emg_tools` unlists it.

3. Use its functions like your own:

    /// tab | Python

    ```python
    import emg_tools
    from scidb import for_each

    for_each(emg_tools.bandpass,
             inputs={"signal": RawEMG, "low_hz": 20},
             outputs=[FilteredEMG])
    ```

    ///

    /// tab | MATLAB

    The library's MATLAB folder is added to MATLAB's path for you.

    ```matlab
    scidb.for_each(@emg_tools.bandpass, ...
        struct('signal', RawEMG(), 'low_hz', 20), {FilteredEMG()});
    ```

    ///

The results are recorded as made by `emg_tools.bandpass`, with the hash of its code, so installing a version whose `bandpass` changed marks those results out of date, as any code change does.

Its Variables are not renamed: a library's `RawEMG` and your project's `RawEMG` are the same Variable when they're declared the same way. If they're declared differently, SciStack says so and names both places.

### Tunable values

A library brings defaults; your project decides.

- **Parameters** (like `CUTOFF`) are project-wide. **Your project's value always wins**: if your entities file declares `CUTOFF`, the library's default is ignored.
- **Path inputs** never carry a root folder from the library. Set yours in your entities file before running.
- **Step values** are a function's own default arguments that nothing feeds, like `gain=1.0` in `emg_tools.filter_emg`. When you compose a [pipeline](../api/scidb.md#pipelines) that uses another one, set a step value for that use only with `bind`, by the function's recorded name and the argument:

    ```python
    main.use(preprocessing.bind(params={"emg_tools.filter_emg.gain": 2.0}))
    ```

    The bound copy is a different computation from the original (its results are kept separately), and the original pipeline is not changed. The key is checked when you call `bind`.

## Make your own library

### From your code

A library of your own lives in the project's `libs/` folder, in the layout above:

```
my_study/
├── scistack.toml
├── libs/
│   └── emg_tools/
│       ├── pyproject.toml
│       └── src/emg_tools/
│           ├── __init__.py
│           └── filters.py
└── src/my_study/...
```

1. Move the general functions (see [Reusable code and project-specific code](../getting-started/writing-functions.md#reusable-code-and-project-specific-code)) into `libs/emg_tools/src/emg_tools/`, and import them from `__init__.py`.
2. Write `libs/emg_tools/pyproject.toml` with a `name`, a `version` (start at `0.1.0`) and the packages the code imports as `dependencies`. [packaging.python.org](https://packaging.python.org/en/latest/tutorials/packaging-projects/) has a template.
3. `scistack library add emg_tools`.
4. `python -m pip install -e libs/emg_tools`, so your scripts can `import emg_tools`. (The GUI and `scidb.scan_project` find `libs/<name>/` by themselves; a script started with `python` doesn't.)

SciStack treats any `libs/<name>/` folder holding `src/<name>/` as a library this project authors: its code is yours to edit, and nothing replaces it from an installed copy.

To package a **pipeline built on the GUI canvas** as a library instead, with its code, Variables and defaults collected for you, use `scistack library create --from-submodule <submodule> --name emg_tools` (see the GUI version of this page).

### Keep your results when a function moves into the library

Moving `bandpass` from your project into `emg_tools` changes the name it's recorded under, from `bandpass` to `emg_tools.bandpass`. Without anything else, every step that used it would run again. When the code didn't change, carry the results over instead:

```python
from scidb import rename_functions
from scidb.foreach_config import function_hash_for
import emg_tools

report = rename_functions(
    db,
    {"bandpass": "emg_tools.bandpass"},                                   # old name -> new name
    {"emg_tools.bandpass": function_hash_for(emg_tools.bandpass)},        # new name -> its code's hash now
)
print(report.carried)       # {"emg_tools.bandpass": 120}: records re-recorded, nothing ran
print(report.not_carried)   # [{"function": ..., "reason": "its code changed: ..."}], for example
```

Results are carried only when the function's code hash under its new name is the one its results were recorded with. Otherwise the step needs a run, and the report says why. The old name's results stay in the history, but no longer count as current. `scistack library create` and `scistack library copy` do this for you.

### Releasing a new version

Edit the code in `libs/emg_tools/src/emg_tools/`, raise `version` in its `pyproject.toml`, then share it again (next section). Changed functions mark the steps that use them out of date in every project that installs the new version.

## Package and distribute it

| To share with… | Do this | They install with |
| --- | --- | --- |
| Yourself, in other projects | nothing more | `python -m pip install -e "<project>/libs/emg_tools"` |
| Your lab, through Git | put the library folder in its own Git repository | `python -m pip install "git+https://github.com/your-lab/emg_tools"` |
| Anyone, as one file | `scistack library build libs/emg_tools` (the `.whl` appears in its `dist/`; `--out` puts it elsewhere) | `python -m pip install emg_tools-0.1.0-py3-none-any.whl` |
| Everyone | publish it to PyPI, like any Python package | `python -m pip install emg_tools` |
| Someone you're sending a whole project | `scistack export --wheelhouse` ([Export & Import](export-import.md)) | nothing extra |

Always install into the **project's** `.venv`.

## Make my own copy

To change an installed library for your project (or to share your own version of it), copy it under a new name:

```bash
scistack library copy emg_tools --as my_emg
```

Run it in the project folder (`--db` names the database if there's more than one). It:

- writes the library to `libs/my_emg/`, with its functions, MATLAB code (`emg_tools.bandpass` calls become `my_emg.bandpass`), Variables and defaults;
- lists `my_emg` in `scistack.toml` instead of `emg_tools` (which stays installed);
- carries every result made by an unchanged `emg_tools.*` function over to `my_emg.*`, and prints, per function, how many were carried or why not;
- switches every GUI canvas that placed an `emg_tools` pipeline to the copy.

Then change your scripts to `import my_emg`. Your next run finds the carried results and skips them.

The copy records where it came from and depends on none of it.

### Copies of copies

A copy can itself be copied. Each copy adds its parent to the front of `derived_from`, newest first:

| Library | Made by | `derived_from` | Depends on |
| --- | --- | --- | --- |
| `emg_tools` | your lab | (none) | its own packages |
| `my_emg` | `scistack library copy emg_tools --as my_emg` | `["emg_tools"]` | its own packages, not `emg_tools` |
| `site_emg` | `scistack library copy my_emg --as site_emg` | `["my_emg", "emg_tools"]` | its own packages, not `my_emg` or `emg_tools` |

Whoever installs `site_emg` needs neither `my_emg` nor `emg_tools`. The new name can't be a folder already in `libs/`, or the name of any module the project can already import, the library you're copying included.

## Next steps

- [Export & Import Projects](export-import.md): `--wheelhouse` sends the libraries a project uses along with it.
- [Pipelines](../api/scidb.md#pipelines): composing pipelines with `use` and `bind`.
