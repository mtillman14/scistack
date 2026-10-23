# Libraries

<!-- Ground truth: scistack-gui Sidebar/ShareLibraryButton.tsx (⇪ Share /
     publish; no folder asked), services/library_authoring.py (share, publish,
     copy: check_new_name -> plan_library -> write_library -> switch_project;
     refusals, collisions), scidb/library.py write_library (layout:
     pyproject.toml, src/<lib>/, generated __init__.py re-exports,
     pipelines/*.json, entities file with [library] derived_from,
     matlab/+<lib>/), scifor.discovery authored_library_dirs (libs/<name>/),
     scidb/rename.py rename_functions (results carried across a rename),
     library_cli.py (`scistack library create|copy|build`),
     Sidebar/LibrariesSection.tsx (+ / ✎ copy under a new name / × / 🔒 and ✏
     rows), services/library_service.py (seed, re-sync only for installed
     libraries, placing declares what is missing, tunables),
     Tunables/TunablesPanel.tsx + ProjectTunables.tsx, library_lock.py
     (installed libraries read-only), scidb/bundle.py wheelhouse, design
     .claude/plan-library-fork-chain.md and docs/claude/portability.md
     "Reusing code". -->

A **library** is a submodule packaged as an ordinary Python package, so that any number of projects can install it and use it. Use one when a group of steps (your lab's EMG preprocessing, say) belongs in several projects and you'll keep improving it: everyone who uses it gets your new version by installing it, the same way they'd update `numpy`.

Compared with sending a [submodule file](pipelines.md):

| | Submodule file (`.json`) | Library |
| --- | --- | --- |
| Carries the code | no | yes (Python and MATLAB) |
| Recipient can edit it | yes, it's their copy | after **✎ Make my own copy** |
| Gets your later changes | no, a file is a snapshot | yes, when they install the new version |

There are two kinds of library in a project, and the **Libraries** list shows which is which:

- **Yours** (✏): you made it here, with **⇪ Share** or **✎ Make my own copy**. It lives in your project's `libs/` folder, you edit its pipelines on the canvas, and **⇪** publishes your changes into it.
- **Installed** (🔒): someone else's, installed with `pip`. Read-only; make your own copy to change it.

A library looks the same on every computer, however many times it was copied: `pyproject.toml`, and `src/<name>/` with its code, its pipelines and its declarations.

## Share a submodule as a library

1. In the sidebar's **Edit** tab, find the submodule under **Submodules** and click **⇪** on its row.
2. Enter the **library name**: the name people will install and that its functions are called by (`emg_tools` → `emg_tools.bandpass`). Letters, digits and underscores, starting with a letter.
3. Click **Share**.

The library is written to `<your project>/libs/emg_tools/`, and your project switches to it straight away: the submodule is now the library's pipeline (the same one, wherever you placed it), its steps are called `emg_tools.…`, and it stays editable here. Nothing to install for the GUI; the report shows the `pip install -e` command for your own scripts.

**Your results stay current.** Renaming a step would normally mean running it again under its new name. When the step's code didn't change, SciStack copies its results over to the new name instead, without running anything: the canvas stays green. The report lists, for every renamed step, how many results were carried, or why not (its code changed, or it never ran), in which case it needs a run.

/// info | 📷 Screenshot
**Show:** the Submodules list in the Edit tab with the **⇪** button on one row highlighted, and the **Share as library** popover open with the name filled in and the `libs/` folder shown.
///

/// info | 📷 Screenshot
**Show:** the **Share as library** report: `filter_emg → emg_tools.filter_emg — results carried`, dependencies, step values, and the install command.
///

### What goes in

- The submodule's steps and wiring, including any submodules inside it.
- The code those steps run: each Python function's file, plus the project files it imports; each MATLAB `.m` file, plus the project helpers it calls.
- The Variables it uses.
- Its Parameters and path inputs, as **defaults**. A project that uses the library keeps its own values (see [Tunable values](#tunable-values)).
- The packages its code imports, as the library's dependencies.

What a library never holds: which subjects or sessions you ran it on, your run history, your data, or who made it. It does record which libraries it was copied from (`derived_from` in its `scistack_entities.toml`).

The report also lists the **step values** you left unwired, like a function's `gain=1.0` default that no Parameter feeds. People who use the library can still set them per placement, but if a value is meant to be tuned, consider making it a Parameter before sharing.

Some submodules can't be shared yet, and the message says why: one whose function code isn't found, one with another library's pipeline placed inside it, or one with a [glue](../gui/process.md) step in it.

### The library folder

```
libs/emg_tools/
├── pyproject.toml              name, version (starts at 0.1.0), dependencies
└── src/emg_tools/
    ├── __init__.py             `from emg_tools import bandpass` works
    ├── filters.py              your Python code, imports rewritten to emg_tools
    ├── scistack_entities.toml  its Variables, Parameter / path input defaults, derived_from
    ├── pipelines/
    │   └── preprocessing.json  the submodule itself
    └── matlab/+emg_tools/      your MATLAB code, if any
```

It's a standard Python package. Before sending it out, open `pyproject.toml` and check the dependency list, and add a `description` if you like.

## Change your library and publish it

Your own library's pipelines are edited like any submodule: open one from **Libraries** (✏), add or rewire steps, run them. The canvas is your working copy; the library's files change only when you publish.

To publish, click **⇪** on the pipeline's row under **Libraries**. It writes the canvas into the library, together with any of your project's code it now uses, renamed into the library (`rectify` → `emg_tools.rectify`, with its results carried over). Code already in the library is never overwritten: edit it in `libs/emg_tools/src/emg_tools/` directly.

If two things would land on the same name, publishing stops and names both, and nothing is written: for example your project's own `filter_emg` and the library's `emg_tools.filter_emg`, or your project's `steps.py` and the library's `steps.py`. Rename one and publish again.

## Package and distribute it

Pick whichever fits who you're sharing with.

| To share with… | Do this | They install with |
| --- | --- | --- |
| Yourself, in other projects | nothing more | `python -m pip install -e "<project>/libs/emg_tools"` (the command in the report) |
| Your lab, through Git | put the library folder in its own Git repository (GitHub, GitLab…) | `python -m pip install "git+https://github.com/your-lab/emg_tools"` |
| Anyone, as one file | build a wheel: in a terminal, `scistack library build "<library folder>"`. The `.whl` file appears in the folder's `dist/`. | `python -m pip install emg_tools-0.1.0-py3-none-any.whl` |
| Everyone | publish it to PyPI, like any Python package ([packaging.python.org](https://packaging.python.org/en/latest/tutorials/packaging-projects/)) | `python -m pip install emg_tools` |
| Someone you're sending a whole project | tick **Wheelhouse** when you [export the project](export-import.md): a wheel of every library the project uses goes inside the `.scistack` file and is installed on import | nothing extra |

Always install into the **project's** `.venv`: open a terminal in VS Code with the project open (the `.venv` is active there) and run the command.

From a terminal, the same actions are `scistack library create --from-submodule <name> --name emg_tools` and `scistack library copy emg_tools --as my_emg`.

## Use a library in a project

1. Install it into the project's `.venv` (above).
2. In the sidebar's **Edit** tab, under **Libraries**, click **+**, type the library's name and press **Enter**.
3. Its pipelines appear under it with a 🔒. Drag one onto the canvas to place it, and connect its inputs like any submodule. Click it in the list to look inside.

Placing it declares what your project is missing by itself: the Parameters it needs, with the library's defaults, and its path inputs, with the library's file templates and no folder yet (set where your files are before running).

If a pipeline shows faded ("Not loaded yet"), click **Refresh**.

/// info | 📷 Screenshot
**Show:** the **Libraries** section with one library (📦 emg_tools) and its 🔒 ⧉ preprocessing pipeline, and the same pipeline placed on the canvas and connected.
///

Its functions are named after the library (`emg_tools.bandpass`), so they never clash with your own `bandpass`. Its Variables are not: a library's `RawEMG` and your project's `RawEMG` are the same Variable when they're declared the same way. If they're declared differently, SciStack says so and names both places.

MATLAB code in a library is added to MATLAB's path for you; call it as `emg_tools.bandpass`.

### Tunable values

Click **⚙** on a placed library pipeline to see what you can tune:

- **Parameters** (like `CUTOFF`) are project-wide. **Your project's value always wins**: if your project already declares `CUTOFF`, the library uses your value, and its default is only shown beside it. Edit them in the sidebar.
- **Path inputs** say which file template they read and whether they still **need a root folder**. A library never carries one; set yours in the sidebar. A run before it's set says which path input needs one.
- **Step values** are a step's own settings that no Parameter feeds (`emg_tools.filter_emg.gain`). They belong to this placement: two placements of the same library keep their own. Type a new value to change it; the next run uses it. **P** (*Make this a Parameter*) creates a project Parameter holding the current value and points this placement at it.

Under **Libraries**, **Tunables of placed libraries** lists every placement in the project, nested ones included, with the same rows.

/// info | 📷 Screenshot
**Show:** a placed library pipeline with its **⚙** panel open: `CUTOFF 20 library default`, a path input marked *needs a root folder*, and a step value with its **P** button.
///

### Read-only, and why

An installed library's pipeline is **read-only** in your project. Anything you change inside it would be overwritten by the next version, so SciStack refuses the edit and says how to make it editable (make your own copy).

What you can still change, because it belongs to your project:

- where the pipeline is placed and what feeds it (its connections and input bindings),
- its tunable values (above),
- how its nodes are positioned,
- which subjects or sessions are excluded.

## When the library changes

Every time the project opens or you click **Refresh**, SciStack checks each installed library's pipelines against the installed version:

- **Unchanged:** nothing happens.
- **Changed** (you installed a newer version): the library's pipelines are replaced with the new ones. Your placements, connections and tunable values stay.
- **A pipeline the new version no longer has:** it stays on your canvas as it was, and SciStack reports it. Nothing is deleted.

Your own libraries are never replaced from their files: your canvas is the working copy.

A new version whose **code** changed marks the steps that use it as out of date, as with any code change: run them to bring the results up to date.

To get a newer version, install it again (`python -m pip install --upgrade emg_tools`, or the new wheel), then **Refresh**. With an `-e` install, edits to the library's code take effect at the next **Refresh**, without reinstalling.

### Releasing a new version

When your library is ready for others, raise `version` in its `pyproject.toml`, then push to Git, build a new wheel, or publish again.

## Make my own copy, or stop following

Two ways to stop an installed library's updates from reaching your project, under **Libraries**:

| Button | What happens | Use it when |
| --- | --- | --- |
| **✎ Make my own copy** | Asks for a **new name**. The library is copied to `libs/<new name>/` as a library of yours, every place you used the original switches to the copy, its steps are renamed `<new name>.…` (results carried over where the code is unchanged), and its pipelines become editable. The original leaves your list (it stays installed). | You want to change it for this project, or share your own version of it. |
| **×** Stop using | The library leaves your list. Its pipelines stay on your canvases as they were, and no longer update. | You no longer need it. |

Both are **unlinking**: from then on, new versions of the original don't reach this project. Make my own copy is all or nothing: every pipeline of that library switches to your copy.

Your copy is a library like any other: share it onwards and the next person installs only yours. It records where it came from (`derived_from = ["emg_tools"]`), never who made it, and doesn't depend on the original.

/// info | 📷 Screenshot
**Show:** the **Make my own copy** prompt for a library with the new name filled in, and afterwards the copy in **Libraries** marked *yours* with ✏ rows.
///

## Next steps

- [What Can I Share?](overview.md): which way of sharing fits, and what stays linked.
- [Export & Import Projects](export-import.md): the **Wheelhouse** option sends libraries along with a project.
