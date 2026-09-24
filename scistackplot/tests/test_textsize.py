"""textsize: one owner resolves every text element's size (pure)."""

from __future__ import annotations

import pytest

from scistackplot import StyleOptions, TextSizes, rc_params, resolve_sizes
from scistackplot.textsize import LARGE, MEDIUM, SMALL


def test_unset_sizes_are_what_matplotlibs_relative_names_resolve_to():
    """The regression guard: a spec that sets only ``base`` draws exactly what
    the single ``font_size`` knob drew (every other size was a relative name
    resolved against font.size)."""
    matplotlib = pytest.importorskip("matplotlib")
    from matplotlib.font_manager import FontProperties

    sizes = resolve_sizes(TextSizes(base=14.0))
    with matplotlib.rc_context({"font.size": 14.0}):

        def pt(name):
            return FontProperties(size=name).get_size_in_points()

        assert sizes.title == pytest.approx(pt("large"))
        assert sizes.x_label == pytest.approx(pt("medium"))
        assert sizes.y_label == pytest.approx(pt("medium"))
        assert sizes.x_ticks == pytest.approx(pt("medium"))
        assert sizes.y_ticks == pytest.approx(pt("medium"))
        assert sizes.legend == pytest.approx(pt("medium"))
        # The brackets were drawn "small".
        assert sizes.groups == pytest.approx(pt("small"), abs=1e-3)


def test_ratios_are_matplotlibs_font_scalings():
    matplotlib = pytest.importorskip("matplotlib")
    from matplotlib.font_manager import font_scalings

    assert (MEDIUM, LARGE, SMALL) == (
        font_scalings["medium"],
        font_scalings["large"],
        font_scalings["small"],
    )
    del matplotlib


def test_the_default_base_is_14():
    """10 pt at 8 x 6 in was unreadable on a slide (user, 2026-09-16)."""
    assert TextSizes().base == 14.0
    assert StyleOptions().text == TextSizes()


def test_a_fixed_size_is_used_as_given_and_pinned():
    sizes = resolve_sizes(TextSizes(base=10.0, y_label=17.0, legend=9.0))
    assert sizes.y_label == 17.0
    assert sizes.x_label == 10.0  # still derived
    assert sizes.pinned == frozenset({"y_label", "legend"})
    assert sizes.is_pinned("legend") and not sizes.is_pinned("x_ticks")


def test_unset_groups_follow_the_x_ticks_even_when_those_are_fixed():
    """Brackets are read with the ticks above them."""
    sizes = resolve_sizes(TextSizes(base=10.0, x_ticks=20.0))
    assert sizes.groups == pytest.approx(20.0 * SMALL)
    assert not sizes.is_pinned("groups")


def test_groups_have_their_own_size_when_fixed():
    sizes = resolve_sizes(TextSizes(base=10.0, x_ticks=20.0, groups=7.0))
    assert sizes.groups == 7.0
    assert sizes.is_pinned("groups")


def test_unset_legend_title_follows_the_entries_wherever_they_end_up():
    sizes = resolve_sizes(TextSizes(base=12.0))
    assert sizes.legend_title is None
    assert sizes.legend_title_for(9.5) == 9.5  # after a shrink
    fixed = resolve_sizes(TextSizes(base=12.0, legend_title=15.0))
    assert fixed.legend_title_for(9.5) == 15.0


def test_rc_params_state_every_size_in_points():
    params = rc_params(resolve_sizes(TextSizes(base=12.0, x_label=13.0, legend=11.0)))
    assert params == {
        "font.size": 12.0,
        "figure.titlesize": pytest.approx(14.4),
        "axes.titlesize": pytest.approx(14.4),
        "axes.labelsize": 13.0,
        "xtick.labelsize": 12.0,
        "ytick.labelsize": 12.0,
        "legend.fontsize": 11.0,
    }
    # Unset, matplotlib's own None already means "the entries' size".
    assert "legend.title_fontsize" not in params
    fixed = rc_params(resolve_sizes(TextSizes(legend_title=8.0)))
    assert fixed["legend.title_fontsize"] == 8.0


def test_rc_params_are_accepted_by_matplotlib():
    matplotlib = pytest.importorskip("matplotlib")
    params = rc_params(resolve_sizes(TextSizes(base=11.0, legend_title=9.0)))
    with matplotlib.rc_context(params):
        assert matplotlib.rcParams["xtick.labelsize"] == 11.0


def test_to_dict_and_describe_for_the_gui_and_the_log():
    sizes = resolve_sizes(StyleOptions(text=TextSizes(base=10.0, title=20.0)))
    payload = sizes.to_dict()
    assert payload["title"] == 20.0 and payload["base"] == 10.0
    assert payload["legend_title"] is None
    assert payload["pinned"] == ["title"]
    line = sizes.describe()
    assert "title=20*" in line and "legend_title=legend" in line and "(* fixed)" in line


def test_text_sizes_round_trip_through_the_spec():
    from scistackplot import PlotSpec

    spec = PlotSpec(
        measures=["m"],
        style=StyleOptions(text=TextSizes(base=11.0, y_ticks=8.0, groups=7.0)),
    )
    again = PlotSpec.from_dict(spec.to_dict())
    assert again.style.text == spec.style.text


def test_the_strict_reader_refuses_the_old_flat_keys():
    """Clean break (beta): `font_size` is not silently mapped to `text.base`;
    saved plots go through restore_spec, which notes it."""
    from scistackplot import PlotSpec

    with pytest.raises(TypeError):
        PlotSpec.from_dict({"measures": ["m"], "style": {"font_size": 12.0}})
