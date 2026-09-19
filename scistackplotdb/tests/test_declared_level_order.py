"""Plots follow the project's declared ``[schema_keys]`` level order — live.

``ScidbSource`` bakes each factor's level order into the tables it builds, and
caches those tables on the variable's content fingerprint. An edit to
scistack.toml moves no fingerprint, so before the cache learned about the
declaration a panel kept drawing the old order until it was reopened — and
before ``DatabaseManager.dataset_schema_key_order`` became a live read, until
the whole GUI was restarted.

Sessions here are ``pre``/``post``: alphabetical puts ``post`` first, which is
exactly the chronology a declaration exists to fix.
"""

from __future__ import annotations

import os

import pytest
from scidb import schema_order
from scifor.pathinput import clear_project_root
from scistackplot import PlotKind, PlotSpec, Role, render_plotly, resolve
from scistackplotdb import ScidbSource


def _declare(root, body: str) -> None:
    config = root / "scistack.toml"
    existed = config.exists()
    config.write_text(f"modules = []\n\n[schema_keys]\n{body}", encoding="utf-8")
    if existed:
        # Two writes inside one filesystem tick look like one version of the
        # file to the mtime cache; move the clock on explicitly.
        stat = config.stat()
        os.utime(config, (stat.st_atime + 5, stat.st_mtime + 5))
    else:
        # A NEW config: opening the database already looked for one, found
        # none, and that answer is trusted for schema_order.LOCATE_TTL
        # seconds (a newly created file is found within that window in real
        # use). Forget it rather than sleep. An EDIT, above, needs no such
        # help — that goes through the mtime check, which is the live path
        # these tests are about.
        schema_order.clear_cache()


@pytest.fixture
def project(tmp_path, monkeypatch):
    """The seeded database's folder, as the cwd's project."""
    monkeypatch.chdir(tmp_path)
    clear_project_root()
    schema_order.clear_cache()
    yield tmp_path
    schema_order.clear_cache()


@pytest.fixture
def source(seeded):
    return ScidbSource(seeded)


def _bar_by_session():
    return PlotSpec(
        measures=["StepLength"],
        kind=PlotKind.BAR,
        roles={"session": Role.GROUP},
    )


def test_undeclared_sessions_are_alphabetical(project, source):
    """The baseline the declaration exists to override."""
    table = source.get_table(["StepLength"])
    assert table.factor("session").levels == ["post", "pre"]


def test_table_levels_follow_the_declaration(project, source):
    _declare(project, 'session = ["pre", "post"]\n')
    table = source.get_table(["StepLength"])
    assert table.factor("session").levels == ["pre", "post"]


def test_the_x_axis_follows_the_declaration(project, source):
    _declare(project, 'session = ["pre", "post"]\n')
    figure = resolve(_bar_by_session(), source.get_table(["StepLength"]))[0]
    assert [str(v) for v in figure.x_order] == ["pre", "post"]

    payload = render_plotly(figure)
    axis = payload["layout"]["xaxis"]
    assert axis["categoryarray"] == ["pre", "post"]


def test_an_edit_rebuilds_the_cached_table(project, source):
    _declare(project, 'session = ["pre", "post"]\n')
    first = source.get_table(["StepLength"])
    assert first.factor("session").levels == ["pre", "post"]

    _declare(project, 'session = ["post", "pre"]\n')
    second = source.get_table(["StepLength"])
    assert second is not first
    assert second.factor("session").levels == ["post", "pre"]


def test_an_unchanged_declaration_keeps_the_cache(project, source):
    _declare(project, 'session = ["pre", "post"]\n')
    first = source.get_table(["StepLength"])
    assert source.get_table(["StepLength"]) is first


def test_the_variant_table_follows_an_edit_too(project, source):
    """``variant_table`` shares the memo, so it must share the check."""
    _declare(project, 'session = ["pre", "post"]\n')
    first = source.variant_table("StepLength")
    _declare(project, 'session = ["post", "pre"]\n')
    assert source.variant_table("StepLength") is not first


def test_the_schema_panel_lists_levels_in_the_declared_order(project, source):
    _declare(project, 'session = ["pre", "post"]\n')
    factors = {f["name"]: f for f in source.describe()["factors"]}
    assert factors["session"]["levels"] == ["pre", "post"]
