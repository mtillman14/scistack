# Figure size / aspect ratio setting (2026-09-16)

Goal: let the user set the SAVED matplotlib figure's size (inches) and aspect
ratio from Plot Studio. The GUI preview keeps filling the pane.

## Existing
- `StyleOptions.width/height` (spec.py) — consumed by render/mpl.py (figsize),
  codegen.py (set_size_inches / figsize) and the pipeline export. Not exposed.
- `style` is plan-irrelevant (reduce.py `_PLAN_IRRELEVANT_FIELDS`), so a size
  change re-renders plotly only.
- Save uses `bbox_inches="tight"` → file ratio approximate. DECISION PENDING.

## Stages
1. scistackplot/figsize.py: ASPECT_PRESETS (16:9, 4:3, 3:2, 1:1, golden, 2:1,
   9:16, 3:4, custom), height_for(width, ratio), aspect_name(width, height).
   mpl render logs figsize; plotly meta.figure_size = {width, height, aspect}.
   Tests: presets, aspect_name tolerance, mpl get_size_inches, codegen.
2. plot_service.describe: `figure_presets` next to `image_formats`.
3. PlotStudio.tsx "Figure size": Aspect select + Width/Height inputs (in);
   ratio pick keeps width, recomputes height; height edit → custom; px readout
   at save dpi. Preview unchanged.

## Status 2026-09-16 — all three stages built, bbox_inches="tight" kept
- scistackplot/figsize.py + tests/test_figsize.py; exports in __init__;
  render/mpl.py logs "figure size W x H in (aspect)"; render/plotly_.py
  meta.figure_size.
- plot_service._describe → figure_presets; test in tests/test_plot_service.py.
- PlotStudio.tsx "Figure size" section (Aspect / Width / Height / px readout),
  InchInput, setStyle; figureSize.ts + figureSize.test.ts (node --test, 86 pass);
  both vite bundles rebuilt.
- Not yet: Python tests run (user runs them); visual check in the panel.
