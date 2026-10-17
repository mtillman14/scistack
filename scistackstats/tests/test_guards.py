"""Import direction (S8) and the engine seam (P9), by AST."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.add(node.module)
    return out


def _sources(package_dir: str) -> list[Path]:
    return sorted((ROOT / package_dir / "src").rglob("*.py"))


def test_the_plot_layer_never_imports_statistics():
    offenders = []
    for package in ("scistackplot", "scistackplotdb"):
        for path in _sources(package):
            if any(name.split(".")[0] in ("scistackstats", "scistackstatsdb") for name in _imports(path)):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], f"stats -> plot only (S8): {offenders}"


def test_only_the_engine_imports_csvstats():
    offenders = []
    for path in _sources("scistackstats"):
        if path.name == "engine.py":
            continue
        if any(name.split(".")[0] == "csvstats" for name in _imports(path)):
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], f"csvstats is imported only by scistackstats/engine.py (P9): {offenders}"


def test_scistackstats_never_imports_its_db_layer():
    for path in _sources("scistackstats"):
        assert not any(n.split(".")[0] == "scistackstatsdb" for n in _imports(path)), path
