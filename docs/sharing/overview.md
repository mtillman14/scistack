# What Can I Share?

<!-- Code variant of sharing/overview.ui.md. Ground truth:
     scistack-gui/scistack_gui/bundle_cli.py (scistack export / import),
     scidb/run/pipeline.py (Pipeline, use, bind: composition never copies,
     bindings never modify the source), scifor/project/names.py
     function_name (a library's functions are lib.fn, the project's own
     code and copied files keep bare names),
     scistack-gui/scistack_gui/library_cli.py (scistack library add / create
     / copy), design docs/claude/portability.md "Reusing code". -->

There are three sizes of SciStack work to share, and each has its own way:

```
Project                    everything: code, settings, pipelines, run history
└── Pipeline               a group of for_each steps (db.pipeline / scidb.Pipeline)
    └── Function           one step's code
```

| You want to share… | Use | What the other person gets | Page |
| --- | --- | --- | --- |
| **A whole project**: hand off a study, move computers, publish an analysis | `scistack export` → a `.scistack` file | A new project folder with your code, settings, pipelines and (optionally) your run history and results | [Export & Import Projects](export-import.md) |
| **One pipeline** with the functions and Variables it uses | The `.py` (or `.m`) files that define them | A copy they run inside **their** project, adapted to their schema keys with `bind` | [Share Part of a Project](pipelines.md) |
| **Functions or a pipeline that several projects reuse**, and that you'll keep improving | A library: an installable package | Code they install and use, and that updates when they install your new version | [Libraries](libraries.md) |
| **Plain functions**, for someone who doesn't use SciStack | The functions' file | Ordinary Python or MATLAB functions | [Share Part of a Project](pipelines.md#for-someone-who-doesnt-use-scistack) |

## What travels with each

Your **raw data files** never travel, whichever way you share. The person you share with keeps their own copy and tells SciStack where it is.

| | Project (`.scistack`) | Copied files | Library (package) |
| --- | --- | --- | --- |
| Your function code (Python and MATLAB) | ✅ | ✅ the files you send | ✅ |
| Pipelines (`for_each` calls) | ✅ | ✅ when the file defines them | ✅ |
| Variables | ✅ | ✅ when the file declares them | ✅ |
| Parameter and path input values from the entities file | ✅ | ❌ they set their own | ✅ as defaults; their project's values win |
| Packages your code needs | ✅ listed, installed on import | ❌ they install them | ✅ listed as dependencies |
| `scistack.toml` settings | ✅ | ❌ | ❌ |
| Saved plots and presets | ✅ | ❌ | ❌ |
| Run history | on by default | ❌ | ❌ |
| Saved results (the data) | off by default | ❌ | ❌ |
| Its functions are recorded as | your names (`bandpass`) | the recipient's own (`bandpass`) | the library's (`emg_tools.bandpass`) |

The last row matters when names meet. A copied file's functions become the recipient's own, so a `bandpass` in your file and a `bandpass` they already have would both be recorded as `bandpass`. A library's functions carry its name, so they never clash.

## Linked or independent?

Some ways of reusing code keep it **linked** to where it came from: change the original and the reuse changes too. Others make an **independent copy** that never changes again unless you change it.

| How the code got there | Linked to | When it stops following its source |
| --- | --- | --- |
| `pipe.use(other)` or `uses=[other]` inside one project | `other` itself: its steps join the graph, nothing is copied | never: an edit to `other`'s steps shows everywhere it's used |
| `pipe.use(other.bind(key_map=..., params=...))` | `other`, adapted for this one use | never for its steps. `bind` never changes `other` itself, so two pipelines can bind it differently. |
| Copied `.py` / `.m` files | nothing | at once. Your later edits don't reach their copy. |
| An imported `.scistack` project | nothing | at once: it's a new project, a copy. |
| An installed library | the version they installed | when they run `scistack library copy` to make their own copy. Until then they edit nothing in it, and installing your new version brings your changes. |

Results are a different matter. SciStack never runs the same step twice on the same inputs with the same code and settings: a reused pipeline that matches what has already run shows as up to date. Linking is only about the **code**: whether an edit made here also changes it there.

## Next steps

- [Export & Import Projects](export-import.md): the whole project as one file. [Export & Import Walkthrough](export-import-walkthrough.md) shows three hand-offs of one project.
- [Share Part of a Project](pipelines.md): one pipeline, or a few functions.
- [Libraries](libraries.md): package code so many projects can use it and get your updates.
