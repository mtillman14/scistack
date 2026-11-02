"""Every entry of ``scidb.SUPPORTED_MODULES`` names a real scidb module.

Guards the declared public surface (design review 04a, B4) against entries
left behind when a module is renamed or removed.
"""

from pathlib import Path

import scidb

SRC = Path(__file__).resolve().parents[1] / "src"


def _exists(module: str) -> bool:
    path = SRC.joinpath(*module.split("."))
    return path.with_suffix(".py").is_file() or (path / "__init__.py").is_file()


def test_supported_modules_exist() -> None:
    assert [m for m in scidb.SUPPORTED_MODULES if not _exists(m)] == []


def test_supported_modules_have_no_duplicates() -> None:
    assert len(set(scidb.SUPPORTED_MODULES)) == len(scidb.SUPPORTED_MODULES)
