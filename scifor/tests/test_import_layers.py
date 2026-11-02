"""``scifor.project`` never imports the scifor engine.

``scifor.project`` is the model-free half of project runtime (root, machine
roots, discovery, ``scistack.toml`` and its grammars). Callers such as the
GUI's config loader and scidb's display modules import it without pulling in
``for_each``, ``PathInput`` or locations, and the engine may import it freely.
Guards against the first engine import creeping in (design review 09, T1;
plan P3 stage 1). Fix a failure by moving the needed code into
``scifor.project`` or by passing the value in from the caller.

Every import counts, including function-level ones: a lazy import of the
engine is still a dependency on it.
"""

import sys
from pathlib import Path

# Appended, not prepended: the repo root holds a directory per package, and
# those must not shadow the installed packages.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from tools.import_layers import module_imports, package_modules  # noqa: E402

SRC = Path(__file__).resolve().parents[1] / "src"


def _in_project(module: str) -> bool:
    return module == "scifor.project" or module.startswith("scifor.project.")


def test_project_imports_no_engine_module() -> None:
    edges = module_imports(SRC, "scifor")
    engine = [
        f"{e.src}:{e.lineno} imports {e.dst}"
        for e in edges
        if _in_project(e.src)
        and (e.dst == "scifor" or e.dst.startswith("scifor."))
        and not _in_project(e.dst)
    ]
    assert engine == []


def test_project_package_exists() -> None:
    """The check above is vacuous if the subpackage moved away."""
    modules = package_modules(SRC, "scifor")
    assert "scifor.project.root" in modules
    assert "scifor.project.discovery" in modules
