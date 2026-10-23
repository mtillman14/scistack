# Libraries

<!-- Ground truth: scistack-gui Sidebar/ShareLibraryButton.tsx (⇪ Share as
     library; report.install = pip install -e), services/library_share.py
     (what goes in; refusals), scidb/library.py create_library (folder
     layout: pyproject.toml, src/<lib>/, pipelines/*.json, entities file,
     matlab/+<lib>/), library_cli.py (`scistack library build`),
     Sidebar/LibrariesSection.tsx (+ / ✎ Make my own copy / × / 🔒 rows),
     services/library_service.py (seed, re-sync on definition_hash change,
     placements + bindings kept, orphans left), library_lock.py (read-only;
     what the project may still change), services/library_copy.py
     (all-or-nothing copy, names stay lib.fn, stays installed),
     scidb/bundle.py wheelhouse, design docs/claude/portability.md
     "Reusing code". -->

A **library** is a submodule packaged as an ordinary Python package, so that any number of projects can install it and use it. Use one when a group of steps (your lab's EMG preprocessing, say) belongs in several projects and you'll keep improving it: everyone who uses it gets your new version by installing it, the same way they'd update `numpy`.

Compared with sending a [submodule file](pipelines.md):

| | Submodule file (`.json`) | Library |
| --- | --- | --- |
| Carries the code | no | yes (Python and MATLAB) |
| Recipient can edit it | yes, it's their copy | not until they make their own copy |
| Gets your later changes | no, a file is a snapshot | yes, when they install the new version |

## Share a submodule as a library

1. In the sidebar's **Edit** tab, find the submodule under **Submodules** and click **⇪** on its row.
2. Enter the **library name**: the name people will install and that its functions are called by (`emg_tools` → `emg_tools.bandpass`). Lowercase letters, digits and underscores, starting with a letter.
3. Enter a **new folder** for it (a full path, to a folder that is empty or doesn't exist yet). Keep it outside your project folder.
4. Click **Share**. A report lists what was written and the command to install it.

/// info | 📷 Screenshot
**Show:** the Submodules list in the Edit tab with the **⇪** button on one row highlighted, and the **Share as library** popover open with the name and folder filled in.
///

/// info | 📷 Screenshot
**Show:** the **Share as library** report: files written, functions renamed `emg_tools.…`, dependencies, and the install command.
///

### What goes in

- The submodule's steps and wiring, including any submodules inside it.
- The code those steps run: each Python function's file, plus the project files it imports; each MATLAB `.m` file, plus the project helpers it calls.
- The Variables it uses.
- Its Parameters and path inputs, as **defaults**. A project that uses the library declares its own (its own data lives in its own folders).
- The packages its code imports, as the library's dependencies.

What a library never holds: which subjects or sessions you ran it on, your run history, or your data.

Some submodules can't be shared yet, and the message says why: one whose function code isn't found, one with another library's pipeline placed inside it, or one with a [glue](../gui/process.md) step in it.

### The library folder

```
emg_tools/
├── pyproject.toml              name, version (starts at 0.1.0), dependencies
└── src/emg_tools/
    ├── __init__.py
    ├── filters.py              your Python code, imports rewritten to emg_tools
    ├── scistack_entities.toml  its Variables, and Parameter / path input defaults
    ├── pipelines/
    │   └── preprocessing.json  the submodule itself
    └── matlab/+emg_tools/      your MATLAB code, if any
```

It's a standard Python package. Before sending it out, open `pyproject.toml` and check the dependency list, and add a `description` if you like.

## Package and distribute it

Pick whichever fits who you're sharing with.

| To share with… | Do this | They install with |
| --- | --- | --- |
| Yourself, on this computer | nothing more | `python -m pip install -e "<library folder>"` (the command in the report) |
| Your lab, through Git | put the library folder in its own Git repository (GitHub, GitLab…) | `python -m pip install "git+https://github.com/your-lab/emg_tools"` |
| Anyone, as one file | build a wheel: in a terminal, `scistack library build "<library folder>"`. The `.whl` file appears in the folder's `dist/`. | `python -m pip install emg_tools-0.1.0-py3-none-any.whl` |
| Everyone | publish it to PyPI, like any Python package ([packaging.python.org](https://packaging.python.org/en/latest/tutorials/packaging-projects/)) | `python -m pip install emg_tools` |
| Someone you're sending a whole project | tick **Wheelhouse** when you [export the project](export-import.md): a wheel of every library the project uses goes inside the `.scistack` file and is installed on import | nothing extra |

Always install into the **project's** `.venv`: open a terminal in VS Code with the project open (the `.venv` is active there) and run the command.

## Use a library in a project

1. Install it into the project's `.venv` (above).
2. In the sidebar's **Edit** tab, under **Libraries**, click **+**, type the library's name and press **Enter**.
3. Its pipelines appear under it with a 🔒. Drag one onto the canvas to place it, and connect its inputs like any submodule. Click it in the list to look inside.

If a pipeline shows faded ("Not loaded yet"), click **Refresh**.

/// info | 📷 Screenshot
**Show:** the **Libraries** section with one library (📦 emg_tools) and its 🔒 ⧉ preprocessing pipeline, and the same pipeline placed on the canvas and connected.
///

Its functions are named after the library (`emg_tools.bandpass`), so they never clash with your own `bandpass`. Its Variables are not: a library's `RawEMG` and your project's `RawEMG` are the same Variable when they're declared the same way. If they're declared differently, SciStack says so and names both places.

MATLAB code in a library is added to MATLAB's path for you; call it as `emg_tools.bandpass`.

### Read-only, and why

A library's pipeline is **read-only** in your project. Anything you change inside it would be overwritten by the next version, so SciStack refuses the edit and says how to make it editable.

What you can still change, because it belongs to your project:

- where the pipeline is placed and what feeds it (its connections and input bindings),
- how its nodes are positioned,
- which subjects or sessions are excluded.

## When the library changes

Every time the project opens or you click **Refresh**, SciStack checks each library's pipelines against the installed version:

- **Unchanged:** nothing happens.
- **Changed** (you installed a newer version): the library's pipelines are replaced with the new ones. Your placements and connections stay.
- **A pipeline the new version no longer has:** it stays on your canvas as it was, and SciStack reports it. Nothing is deleted.

A new version whose **code** changed marks the steps that use it as out of date, as with any code change: run them to bring the results up to date.

To get a newer version, install it again (`python -m pip install --upgrade emg_tools`, or the new wheel), then **Refresh**. With an `-e` install, edits to the library's code take effect at the next **Refresh**, without reinstalling.

### Releasing a new version

You're the library's author: work on its code in the library folder (installed with `-e` in your project, so you see the change straight away). When it's ready for others, raise `version` in its `pyproject.toml`, then push to Git, build a new wheel, or publish again.

/// note | Changing a library's wiring
Changing which steps a library's pipeline contains, or how they connect, isn't possible from the canvas yet: its pipeline is read-only everywhere, the author's own project included. Today the way to do it is to edit the submodule in a project, share it again to a new folder, and copy the new `pipelines/` files over the library's.
///

## Stop following the library

Two ways to stop a library's updates from reaching your project, under **Libraries**:

| Button | What happens | Use it when |
| --- | --- | --- |
| **✎ Make my own copy** | The library's code is copied into your project, every place you used it switches to the copy, and its pipelines become ordinary, editable submodules. Functions keep their `emg_tools.…` names, so your results stay up to date. The library leaves your list (it stays installed). | You want to change it for this project. |
| **×** Stop using | The library leaves your list. Its pipelines stay on your canvases as they were, and no longer update. | You no longer need it. |

Both are **unlinking**: from then on, new versions of the library don't reach this project. Make my own copy is all or nothing: every pipeline of that library becomes your own copy, not just one.

/// info | 📷 Screenshot
**Show:** the **Make my own copy** confirmation for a library, and afterwards the same pipeline under **Submodules** without the 🔒.
///

## Using your own library in the project it came from

After sharing, the original submodule is still an ordinary part of your project, and the library is a separate copy of it. To keep one copy, install the library into your project, add it under **Libraries**, place its pipeline where the original submodule was, and hide the original. Its functions are now named `emg_tools.…`, so their steps run once more under the new names.

## Next steps

- [What Can I Share?](overview.md): which way of sharing fits, and what stays linked.
- [Export & Import Projects](export-import.md): the **Wheelhouse** option sends libraries along with a project.
