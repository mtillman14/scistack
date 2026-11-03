"""
The layout caches are keyed on the figure's CONTENT (``ResolvedPlot.layout_key``).

scistack-debug.log 2026-11-03: every GUI request rebuilds the figure from the
plan cache, so the old ``id()`` key never matched across requests, and an
identical request re-ran the whole auto-size search (5 layouts, 22 s, on a
105-position bar chart). Pinned here:

- an EQUAL figure from a second resolve reuses the decisions: no matplotlib
  layout at all, and the reuse is logged;
- ANY change (data, size, the chosen reference) is a new key: never a wrong hit;
- the cache hands out copies;
- a figure that cannot be digested is laid out every time, and that is WARNed.
"""

from __future__ import annotations

import logging
from dataclasses import replace

import pytest

pytest.importorskip("matplotlib")

from scistackplot import layout_decisions  # noqa: E402
from scistackplot.render.mpl import LAYER  # noqa: E402
from scistackplot.text.autosize import settle  # noqa: E402
from test_mpl_label_fit import _graph2, gait_table  # noqa: E402,F401
from test_preview_decisions import _auto, _count_renders, _one  # noqa: E402


def _equal_pair(gait_table):
    """Two resolves of one spec and table: equal content, distinct objects."""
    spec = _auto(_graph2(width=16.0))
    return _one(spec, gait_table), _one(spec, gait_table)


def _shifted(resolved, by: float = 1.0):
    """``resolved`` with every y value moved: same shape, different content."""
    y = resolved.encoding.y
    return replace(
        resolved,
        panels=[replace(p, frame=p.frame.assign(**{y: p.frame[y] + by})) for p in resolved.panels],
    )


# ---------------------------------------------------------------------------
# The key


def test_equal_content_gives_an_equal_key(gait_table):
    first, second = _equal_pair(gait_table)
    assert first is not second
    assert first.layout_key() is not None
    assert first.layout_key() == second.layout_key()


def test_data_size_and_reference_each_change_the_key(gait_table):
    first, _ = _equal_pair(gait_table)
    key = first.layout_key()
    assert _shifted(first).layout_key() != key
    wider = replace(first, spec=replace(first.spec, style=replace(first.spec.style, width=20.0)))
    assert wider.layout_key() != key
    settled = settle(first)
    assert settled.auto_text is not None
    assert settled.layout_key() != key  # the reference is part of a layout


# ---------------------------------------------------------------------------
# layout_decisions


def test_an_equal_figure_from_another_resolve_is_not_laid_out_again(
    gait_table, monkeypatch, caplog
):
    first, second = _equal_pair(gait_table)
    decided = layout_decisions(first)
    calls = _count_renders(monkeypatch)
    with caplog.at_level(logging.INFO, logger=LAYER):
        again = layout_decisions(second)
    assert calls == []
    assert again == decided
    assert any(
        "reused the layout of an identical figure" in r.getMessage() for r in caplog.records
    )


def test_changed_data_is_laid_out_again(gait_table, monkeypatch):
    first, _ = _equal_pair(gait_table)
    layout_decisions(first)
    calls = _count_renders(monkeypatch)
    layout_decisions(_shifted(first))
    assert calls, "a figure with different values must not reuse a cached layout"


def test_another_size_is_laid_out_again(gait_table, monkeypatch):
    first, _ = _equal_pair(gait_table)
    layout_decisions(first, width_in=16.0, height_in=5.0)
    calls = _count_renders(monkeypatch)
    wide = layout_decisions(first, width_in=20.0, height_in=5.0)
    assert calls
    assert wide["width_in"] == 20.0


def test_the_cache_hands_out_copies(gait_table):
    first, second = _equal_pair(gait_table)
    decided = layout_decisions(first)
    decided["width_in"] = -1.0
    decided.clear()
    again = layout_decisions(second)
    assert again["width_in"] == 16.0


def test_settle_reuses_the_size_chosen_for_an_equal_figure(gait_table, monkeypatch):
    first, second = _equal_pair(gait_table)
    chosen = settle(first).auto_text
    calls = _count_renders(monkeypatch)
    assert settle(second).auto_text.reference == chosen.reference
    assert calls == []


# ---------------------------------------------------------------------------
# No key: the old behaviour, said out loud


def test_an_undigestible_figure_is_laid_out_every_time_and_warned(
    gait_table, monkeypatch, caplog
):
    from scistackplot.reduce import resolved as resolved_module

    monkeypatch.setattr(resolved_module, "_LAYOUT_KEY_FAILURES", set())
    first, _ = _equal_pair(gait_table)
    # The GUI's Labels list is never drawn; a lambda in it cannot be pickled.
    odd = replace(first, labelable=[{"cannot pickle": lambda: None}])
    with caplog.at_level(logging.WARNING, logger=LAYER):
        assert odd.layout_key() is None
        assert odd.layout_key() is None
    warned = [r for r in caplog.records if "layout key: cannot digest" in r.getMessage()]
    assert len(warned) == 1  # once per reason, not per request
    layout_decisions(odd)
    calls = _count_renders(monkeypatch)
    layout_decisions(odd)
    assert calls, "no key means no cache: laid out again"
