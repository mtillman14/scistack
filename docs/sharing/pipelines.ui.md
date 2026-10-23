# Share a Hypothesis or Submodule

<!-- Ground truth: scistack-gui HypothesisTabs.tsx (⎘ duplicate, ⇩ export,
     </> script, ⇧ import + its report alert), DAG/PipelineNode.tsx
     (⎘ Duplicate, ⇩, </>), services/portability_service.py (closure export,
     reuse identical / fork "(imported)" by pipeline_id, globals reused by
     name, missing PathInput/Sweep materialised, hides not applied, no
     code/data/history), scope_service.duplicate_pipeline,
     code_export_service.py. -->

You don't have to share a whole project to share a piece of it. A **hypothesis** (one tab) or a **submodule** (one ⧉ node) can be saved as a small `.json` file and imported into someone else's project, with every submodule it uses inside it.

/// warning | The file holds the wiring, not the code
A hypothesis or submodule file holds the canvas: which steps, how they're connected, and their settings. It does **not** hold your functions' code, run history or results. The person importing needs the same functions in their project already: a labmate working in a copy of the same project, or someone who installed the same [library](libraries.md). If they don't have the code, [export the whole project](export-import.md) or [share the submodule as a library](libraries.md) instead.
///

## Export a hypothesis

Hover over the hypothesis tab and click **⇩**. The file is saved with the hypothesis's name, and a message says where.

The file carries the tab's research question and hypothesis statement too, so it arrives as a hypothesis tab.

/// info | 📷 Screenshot
**Show:** a hypothesis tab hovered, with the **⇩** button highlighted and its tooltip ("Export this hypothesis…") visible.
///

## Export a submodule

Click **⇩** in the row of buttons on the submodule node (next to **▶ Run** and **⎘ Duplicate**). It arrives on the other end as a submodule, listed under **Submodules** in the sidebar, ready to drag onto a canvas.

/// info | 📷 Screenshot
**Show:** a submodule node with its **▶ Run**, **⎘ Duplicate**, **⇩** and **`</>`** buttons, **⇩** highlighted.
///

## Import

Click **⇧ import** in the hypothesis tab strip and pick the `.json` file. SciStack adds it to your project and opens it.

/// info | 📷 Screenshot
**Show:** the message shown after an import that reused one submodule and found one function name missing.
///

What happens to each part:

| In the file | In your project |
| --- | --- |
| The hypothesis or submodule | Added. If you already have one with the same name, the new one is named "… (imported)". |
| A submodule you **already have, unchanged** (you imported it before, or it came from the same project) | Your existing one is used; no second copy. |
| A submodule you already have but **have since changed** | Imported as a separate "… (imported)" copy. Yours is left alone. |
| A Parameter, path input or sweep with a name you already use | Yours is used, with your values. |
| A path input or sweep you don't have | Added to your project. Check its folder: it's the sender's. |
| A function or variable name your project doesn't have | Listed in the message after the import. The node is on the canvas but can't run until the code is in your project. |
| Connections the sender had hidden | Not applied. They belonged to the sender's run history. |

Nothing is run, and nothing in your database changes, until you run it.

/// tip | A file is a snapshot
An imported hypothesis or submodule isn't linked to the sender's. When they send an updated file, importing it gives you a second copy ("… (imported)") next to the one you have, so you can compare the two; hide the one you don't want. If you want updates to arrive by themselves, share the submodule as a [library](libraries.md) instead.
///

## Duplicate inside your own project

To try a variation without touching the original, duplicate it:

- **⎘** on a hypothesis tab makes a new tab with the same steps. Use it for a sibling question (gait symmetry → gait speed).
- **⎘ Duplicate** on a submodule node makes a new, independent submodule with the same steps.

A duplicate copies the steps that belong to that tab or submodule. **Submodules placed inside it are not copied**: both the original and the duplicate use the same ones, so editing inside such a submodule changes both. To change it in one place only, duplicate that submodule too and use the duplicate there. See [Linked or independent?](overview.md#linked-or-independent).

Duplicates don't recompute anything that has already run: a step with the same inputs, code and settings shows as up to date in both.

/// info | 📷 Screenshot
**Show:** a hypothesis duplicated into a second tab, both tabs visible, the duplicate's canvas showing the same submodule node as the original.
///

## A plain script instead

**`</>`** on a hypothesis tab or a submodule node translates it into a standalone Python or MATLAB script (whichever language its functions are written in) that someone can read or run without SciStack. The script calls your functions, so they need your code too.

## Next steps

- [Libraries](libraries.md): when several projects should reuse a submodule and get your updates.
- [Export & Import Projects](export-import.md): share everything at once, code included.
