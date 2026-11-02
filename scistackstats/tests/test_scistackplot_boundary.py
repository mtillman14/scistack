"""scistackstats imports scistackplot only through its top level and model.

Guards design review 06 (P1): the stats tower may depend on the plot spec
model, not on how plots are reduced or drawn. ``TODAY`` lists the deeper
modules it still imports; the list may only shrink, so a new deep import
fails and a removed one must be deleted from ``TODAY``.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
# Appended, not prepended: the repo root holds a directory per package, and
# those must not shadow the installed packages.
sys.path.append(str(REPO))

from tools.import_layers import (  # noqa: E402
    load_config,
    module_imports,
    package_modules,
)

PLOT_SRC = REPO / "scistackplot" / "src"
PLOT_LAYERS = load_config(REPO / "scistackplot" / "tests" / "import_layers.toml")

#: Deeper scistackplot modules imported today, and why.
TODAY: dict[str, str] = {
    "scistackplot.text.aliases": "DisplayText / display_text for stats table labels",
    "scistackplot.reduce.diffbars": "DifferenceBar: stats results become plot overlays",
    "scistackplot.export.plot_data": "sample_curves for band figures",
    "scistackplot.reduce.groups": "apply_level_groups, shared with plots",
    "scistackplot.reduce.variants": "set_name / apply_variant_sets, shared with plots",
}


def _deep_imports() -> set[str]:
    edges = module_imports(
        REPO / "scistackstats" / "src",
        "scistackstats",
        other_modules=package_modules(PLOT_SRC, "scistackplot"),
    )
    return {
        e.dst
        for e in edges
        if e.dst.startswith("scistackplot.")
        and PLOT_LAYERS.layer_of(e.dst) != "model"
    }


def test_no_new_deep_scistackplot_imports() -> None:
    assert _deep_imports() - TODAY.keys() == set()


def test_today_list_has_no_stale_entries() -> None:
    assert TODAY.keys() - _deep_imports() == set(), "delete these from TODAY"
