"""The GUI never parses TOML itself.

``scistack_gui.project.config`` used to open ``scistack.toml`` twice with
``tomllib.load`` and decide on its own what ``modules``, ``packages`` and
``[matlab]`` meant (design review 07, G3). Reading and the meaning of each
section are ``scifor.project.config``'s; the GUI only turns those entries
into the files its registry discovers. Guards against a direct read coming
back. Fix a failure by calling ``read_project_config`` /
``load_config_file`` / ``read_scistack_section``.
"""

import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "scistack_gui"
_TOML = re.compile(r"^\s*(?:import|from)\s+toml(?:lib|i)\b|\btoml(?:lib|i)\.loads?\(", re.M)


def test_gui_package_does_not_read_toml() -> None:
    readers = sorted(
        p.relative_to(PACKAGE).as_posix()
        for p in PACKAGE.rglob("*.py")
        if "__pycache__" not in p.parts and _TOML.search(p.read_text(encoding="utf-8"))
    )
    assert readers == []
