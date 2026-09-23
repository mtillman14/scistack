"""Which rows form one line is decided once, in ``render.base.series_groups``.

It was a byte-identical ``_series_groups`` in both renderers until
2026-09-23 -- free to drift, so matplotlib and plotly could draw different
lines from the same resolved plot.
"""

import types

import pandas as pd

from scistackplot.render import base, mpl, plotly_


def test_both_renderers_use_the_one_owner():
    assert mpl.series_groups is base.series_groups
    assert plotly_.series_groups is base.series_groups
    assert not hasattr(mpl, "_series_groups")
    assert not hasattr(plotly_, "_series_groups")


def _resolved(series):
    return types.SimpleNamespace(encoding=types.SimpleNamespace(series=series))


def test_groups_by_the_series_column_in_first_seen_order():
    frame = pd.DataFrame({"subject": ["b", "a", "b"], "y": [1, 2, 3]})
    groups = base.series_groups(frame, _resolved("subject"))
    assert [sid for sid, _ in groups] == ["b", "a"]
    assert list(groups[0][1]["y"]) == [1, 3]


def test_no_series_column_is_one_group():
    frame = pd.DataFrame({"y": [1, 2]})
    assert [sid for sid, _ in base.series_groups(frame, _resolved(None))] == [None]
    assert [sid for sid, _ in base.series_groups(frame, _resolved("absent"))] == [None]
