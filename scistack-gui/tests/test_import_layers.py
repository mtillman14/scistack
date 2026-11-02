"""scistack-gui's layers hold: no new upward import, no new lazy import.

Layers and the allow-list live beside this file (``import_layers.toml``,
``import_allowlist.txt``); the checker is ``tools/import_layers.py``.
Guards the runtime -> stores -> project/matlab -> domain -> services -> api
order (design review 07, G1/G2): services never reach up into the api
handler modules. Fix a failure by importing downward, or move the module;
see the checker's docstring before touching the allow-list.
"""

import sys
from pathlib import Path

import pytest

# Appended, not prepended: the repo root holds a directory per package, and
# those must not shadow the installed packages.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from tools.import_layers import LayerReport, layer_report, load_config  # noqa: E402

CONFIG = Path(__file__).with_name("import_layers.toml")


@pytest.fixture(scope="module")
def report() -> LayerReport:
    return layer_report(load_config(CONFIG))


def test_no_new_upward_imports(report: LayerReport) -> None:
    assert report.violations == []


def test_allowlist_has_no_stale_entries(report: LayerReport) -> None:
    assert report.stale == [], "delete these lines from import_allowlist.txt"


def test_allowlist_entries_have_reasons(report: LayerReport) -> None:
    assert report.missing_reason == []


def test_every_module_has_a_layer(report: LayerReport) -> None:
    assert report.unclassified == [], "add these modules to import_layers.toml"


def test_lazy_imports_do_not_grow(report: LayerReport) -> None:
    assert report.lazy_count <= report.lazy_baseline, (
        f"{report.lazy_count} function-level imports > baseline "
        f"{report.lazy_baseline}; import at module top instead"
    )
    if report.lazy_count < report.lazy_baseline:
        print(f"lazy imports dropped to {report.lazy_count}: lower lazy_baseline")
