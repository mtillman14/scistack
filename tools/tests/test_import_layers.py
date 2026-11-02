"""The import-layer checker reads imports correctly and flags upward ones.

Guards the one owner every package's layer and boundary test relies on: a
mis-resolved relative import or a missed lazy import would silently hide a
layering violation in every package at once.
"""

from pathlib import Path

import pytest

from tools.import_layers import (
    check,
    imports_outside,
    lazy_imports,
    load_config,
    module_imports,
    read_allowlist,
    stale_allowed,
    unclassified,
    write_allowlist,
)

ORDER = ["low", ["mid", "peer"], "high"]
LAYERS = {
    "low": ("pkg.low",),
    "mid": ("pkg.mid",),
    "peer": ("pkg.peer",),
    "high": ("pkg.high",),
}


def _layer_of(module: str) -> str | None:
    for layer, prefixes in LAYERS.items():
        if any(module == p or module.startswith(p + ".") for p in prefixes):
            return layer
    return None


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    pkg = tmp_path / "pkg"
    (pkg / "high").mkdir(parents=True)
    (pkg / "__init__.py").write_text("from . import low\n")
    (pkg / "low.py").write_text(
        "import os\n"
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from .high import api\n"
        "def f():\n"
        "    from .mid import thing\n"
    )
    (pkg / "mid.py").write_text("from .low import x\nfrom .peer import y\n")
    (pkg / "peer.py").write_text("from pkg.mid import z\n")
    (pkg / "high" / "__init__.py").write_text("from . import api\n")
    (pkg / "high" / "api.py").write_text("from ..low import x\nfrom .. import mid\n")
    return tmp_path


def test_relative_imports_resolve_to_modules(tree: Path) -> None:
    pairs = {(e.src, e.dst) for e in module_imports(tree, "pkg")}
    assert ("pkg", "pkg.low") in pairs  # `from . import low` in __init__
    assert ("pkg.high", "pkg.high.api") in pairs  # level 1 in a package
    assert ("pkg.high.api", "pkg.low") in pairs  # level 2 from a module
    assert ("pkg.high.api", "pkg.mid") in pairs  # `from .. import mid`
    assert ("pkg.low", "os") in pairs


def test_lazy_and_type_only_are_flagged(tree: Path) -> None:
    edges = module_imports(tree, "pkg")
    assert lazy_imports(edges) == {("pkg.low", 6)}
    type_only = [e for e in edges if e.type_only]
    assert [(e.src, e.dst) for e in type_only] == [("pkg.low", "pkg.high.api")]


def test_upward_import_is_a_violation_and_peers_are_not(tree: Path) -> None:
    edges = module_imports(tree, "pkg")
    violations = check(edges, _layer_of, ORDER, allow=set())
    # The lazy low -> mid import is upward; the TYPE_CHECKING one is not run.
    assert violations == ["pkg.low:6 imports pkg.mid (low -> mid)"]


def test_allow_list_hit_and_stale_entry(tree: Path) -> None:
    edges = module_imports(tree, "pkg")
    allow = {("pkg.low", "pkg.mid"), ("pkg.mid", "pkg.high")}
    assert check(edges, _layer_of, ORDER, allow) == []
    assert stale_allowed(edges, _layer_of, ORDER, allow) == {("pkg.mid", "pkg.high")}


def test_unclassified_modules_are_reported() -> None:
    assert unclassified(["pkg.low", "pkg.other"], _layer_of) == ["pkg.other"]


def test_imports_outside_a_supported_surface(tree: Path) -> None:
    edges = module_imports(tree, "pkg")
    outside = imports_outside(edges, "pkg", supported={"pkg", "pkg.low", "pkg.mid"})
    assert "pkg.high:1 imports pkg.high.api" in outside
    assert not any("imports os" in line for line in outside)


def test_allowlist_round_trip_and_config(tmp_path: Path) -> None:
    path = tmp_path / "import_allowlist.txt"
    write_allowlist(path, {("a.b", "a.c"): "cycle via X", ("a.d", "a.e"): ""})
    assert read_allowlist(path) == {("a.b", "a.c"): "cycle via X", ("a.d", "a.e"): "TODO"}

    toml = tmp_path / "import_layers.toml"
    toml.write_text(
        'package = "pkg"\nsrc = "."\nlazy_baseline = 3\n'
        'order = ["low", ["mid", "peer"]]\n'
        '[layers]\nlow = ["pkg.low"]\nmid = ["pkg.mid"]\npeer = ["pkg.peer", "pkg.peer.deep.*"]\n'
    )
    cfg = load_config(toml)
    assert cfg.order == ("low", ("mid", "peer"))
    assert cfg.layer_of("pkg.peer.deep.x") == "peer"
    assert cfg.layer_of("pkg.peer.x") is None  # plain entries match exactly
    assert cfg.layer_of("pkg.nowhere") is None
    assert cfg.allowlist_path == path


def test_from_import_of_another_packages_module(tmp_path: Path) -> None:
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "__init__.py").write_text("from lib import mod, Thing\n")
    plain = {e.dst for e in module_imports(tmp_path, "app")}
    assert plain == {"lib"}
    resolved = {e.dst for e in module_imports(tmp_path, "app", other_modules={"lib.mod"})}
    assert resolved == {"lib", "lib.mod"}
