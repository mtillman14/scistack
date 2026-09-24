"""Plots see the project's ``[aliases]`` — live, and without rebuilding tables.

The opposite cache rule to ``[schema_keys]`` (test_declared_level_order.py):
level ORDER is baked into a built table, so an edit to it drops the table;
an ALIAS is display only, so an edit must reach the next figure while the
built table — and the plan cached on its identity — stays. The table carries
a reader (``LongTable.aliases_source``), not a copy. Stage 3 of
``.claude/plan-plot-text-sizes-and-aliases.md``.
"""

from __future__ import annotations

import os

import pytest
from scidb import aliases, schema_order
from scifor.pathinput import clear_project_root
from scistackplot import Alias
from scistackplotdb import ScidbSource


def _declare(root, body: str) -> None:
    config = root / "scistack.toml"
    existed = config.exists()
    config.write_text(f"modules = []\n\n{body}", encoding="utf-8")
    if existed:
        stat = config.stat()
        os.utime(config, (stat.st_atime + 5, stat.st_mtime + 5))
    else:
        # A NEW config inside schema_order.LOCATE_TTL of the "none found"
        # answer; forget that answer rather than sleep (see
        # test_declared_level_order._declare).
        schema_order.clear_cache()


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    clear_project_root()
    schema_order.clear_cache()
    aliases.clear_cache()
    yield tmp_path
    schema_order.clear_cache()
    aliases.clear_cache()


@pytest.fixture
def source(seeded):
    return ScidbSource(seeded)


def test_no_aliases_table_means_none(project, source):
    table = source.get_table(["StepLength"])
    assert table.project_aliases() == {}


def test_the_table_carries_the_project_aliases(project, source):
    _declare(
        project,
        '[aliases.session]\nname = "Session"\n\n[aliases.session.levels]\n'
        '"pre" = "Before"\n"post" = "After"\n',
    )
    table = source.get_table(["StepLength"])
    assert table.project_aliases() == {
        "session": Alias(name="Session", levels={"pre": "Before", "post": "After"})
    }


def test_an_alias_edit_keeps_the_built_table(project, source):
    """Unlike a level-order edit: no rebuild, same object, new aliases."""
    _declare(project, '[aliases.session]\nname = "Session"\n')
    before = source.get_table(["StepLength"])
    assert before.project_aliases()["session"].name == "Session"

    _declare(project, '[aliases.session]\nname = "Visit"\n')
    after = source.get_table(["StepLength"])
    assert after is before
    assert after.project_aliases()["session"].name == "Visit"


def test_a_level_order_edit_still_rebuilds_and_keeps_the_reader(project, source):
    _declare(project, '[aliases.session]\nname = "Session"\n')
    before = source.get_table(["StepLength"])
    _declare(
        project,
        '[schema_keys]\nsession = ["pre", "post"]\n\n[aliases.session]\nname = "Session"\n',
    )
    after = source.get_table(["StepLength"])
    assert after is not before
    assert after.project_aliases()["session"].name == "Session"
