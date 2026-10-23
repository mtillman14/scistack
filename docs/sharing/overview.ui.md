# What Can I Share?

<!-- Ground truth: scistack-gui HypothesisTabs.tsx (⎘ ⇩ </> per tab, ⇧ import,
     ⇩ export project, ✓ verify), DAG/PipelineNode.tsx (⎘ Duplicate, ⇩, </>),
     Sidebar/EditTab.tsx + ShareLibraryButton.tsx (⇪ Share as library),
     Sidebar/LibrariesSection.tsx (+, ✎ Make my own copy, ×),
     services/portability_service.py (reuse/fork by pipeline_id),
     services/library_service.py (seed / lock / re-sync), library_copy.py,
     scope_service.duplicate_pipeline (child placements stay shared),
     design docs/claude/portability.md. -->

SciStack work nests in three sizes, and each size has its own way to be shared:

```
Project                     everything: code, settings, every hypothesis, run history
└── Hypothesis (a tab)      one research question and its canvas
    └── Submodule (⧉)       a group of steps packed into one node
```

| You want to share… | Use | What the other person gets | Page |
| --- | --- | --- | --- |
| **A whole project**: hand off a study, move computers, publish an analysis | **⇩ export project** → a `.scistack` file | A new project folder with your code, settings, canvases, saved plots and (optionally) your run history and results | [Export & Import Projects](export-import.md) |
| **One hypothesis** with everything it uses | **⇩** on the hypothesis tab → a `.json` file | A new tab in **their** project, wired exactly like yours | [Share a Hypothesis or Submodule](pipelines.md) |
| **One submodule** with everything it uses | **⇩** on the submodule node → a `.json` file | A new submodule in **their** project | [Share a Hypothesis or Submodule](pipelines.md) |
| **A submodule that several projects reuse** and that you'll keep improving | **⇪ Share as library** → an installable package | A read-only submodule that updates when they install your new version | [Libraries](libraries.md) |
| **Plain code**, for someone who doesn't use SciStack | **`</>`** on a hypothesis tab or submodule node | A standalone Python or MATLAB script | [Share a Hypothesis or Submodule](pipelines.md#a-plain-script-instead) |

/// info | 📷 Screenshot
**Show:** the top of the window with the hypothesis tab strip, one tab hovered so its ⎘ ⇩ `</>` ✎ buttons show, and the **⇧ import**, **⇩ export project** and **✓ verify** buttons at the right end, each labelled.
///

## What travels with each

Your **raw data files** never travel, whichever way you share. The person you share with keeps their own copy and tells SciStack where it is.

| | Project (`.scistack`) | Hypothesis / submodule (`.json`) | Library (package) |
| --- | --- | --- | --- |
| Canvas: nodes, connections, settings, positions | ✅ | ✅ (that tab or submodule, plus every submodule inside it) | ✅ (the submodule) |
| Your function code (Python and MATLAB) | ✅ | ❌ the recipient needs it already | ✅ |
| Variables, Parameters, path inputs | ✅ | by name; a missing path input or sweep is added to their project | ✅ (Parameters and path inputs as defaults) |
| Packages your code needs | ✅ listed, installed on import | ❌ | ✅ listed as dependencies |
| `scistack.toml` settings | ✅ | ❌ | ❌ |
| Saved plots and presets | ✅ | ❌ | ❌ |
| Run history | on by default | ❌ | ❌ |
| Saved results (the data) | off by default | ❌ | ❌ |

So a hypothesis or submodule file is the right choice when the other person already has the code (a labmate working in a copy of the same project, or someone who installed the same library). When they don't, export the whole project or share the submodule as a library.

## Linked or independent?

Some ways of reusing a pipeline keep it **linked** to where it came from: change the original and the reuse changes too. Others make an **independent copy** that never changes again unless you change it. Knowing which is which avoids surprises.

| How the pipeline got there | Linked to | When it stops following its source |
| --- | --- | --- |
| The same submodule placed twice (dragged from **Submodules** twice, or into two hypotheses) | the one submodule: both nodes are the same thing | never: an edit inside shows in every place it's used. Use **⎘ Duplicate** to split one off. |
| **⎘ Duplicate** on a submodule node | nothing: its own steps are copied | at once. Submodules *inside* it are still the shared ones. |
| **⎘** on a hypothesis tab | nothing: its own steps are copied | at once. Submodules placed in it are still the shared ones, so editing inside one changes both hypotheses. Duplicate the submodule too if you want them to differ. |
| Imported `.json` file | nothing | at once. The file is a snapshot: importing a newer version of it later makes a second copy named "… (imported)" instead of updating yours. |
| Imported `.scistack` project | nothing | at once: it's a new project, a copy. |
| A library's pipeline (🔒 under **Libraries**) | the installed library | when you click **✎ Make my own copy** (or **×** stop using the library). Until then it's read-only and follows every new version you install. |

/// info | 📷 Screenshot
**Show:** one submodule placed in two hypothesis tabs, side by side, with an edit inside it showing in both; caption "one submodule, used twice".
///

Results are a different matter. SciStack never runs the same step twice on the same inputs with the same code and settings, however many places it appears: a duplicated or imported copy that matches what has already run shows as up to date. Linking is only about the **canvas**: whether an edit made here also changes it there.

## Next steps

- [Export & Import Projects](export-import.md): the whole project as one file.
- [Share a Hypothesis or Submodule](pipelines.md): one tab or one submodule.
- [Libraries](libraries.md): package a submodule so many projects can use it and get your updates.
