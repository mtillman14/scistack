"""textsize: one owner resolves every text element's size (pure).

One dial, fixed proportions (user, 2026-10-14): drawn = clamp(reference x
ratio, element floor, element ceiling); the ratios are intent and are never
altered by sizing.
"""

from __future__ import annotations

import pytest

from scistackplot import (
    DEFAULT_TEXT_RATIOS,
    TEXT_ELEMENTS,
    TEXT_TARGETS,
    StyleOptions,
    TextSizes,
    drawn_sizes,
    rc_params,
    resolve_sizes,
)
from scistackplot.textsize import (
    BANDS,
    CEILING,
    FLOOR,
    auto_elements,
    default_bounds,
    element_bounds,
    reference_range,
    text_ratios,
)

# --- the table -----------------------------------------------------------------


def test_one_ratio_per_element_and_one_band_per_target():
    assert tuple(DEFAULT_TEXT_RATIOS) == TEXT_ELEMENTS
    assert set(BANDS) == set(TEXT_TARGETS)


def test_the_default_table_the_user_set():
    assert DEFAULT_TEXT_RATIOS == {
        "title": 1.44,
        "x_label": 1.2,
        "y_label": 1.2,
        "x_ticks": 1.0,
        "y_ticks": 1.0,
        "groups": 1.0,
        "legend": 1.0,
        "legend_title": 1.2,
        "differences": 1.0,
    }
    assert (BANDS["print"].floor_pt, BANDS["print"].ceiling_pt) == (8.0, 12.0)
    assert (BANDS["slide"].floor_pt, BANDS["slide"].ceiling_pt) == (14.0, 28.0)


def test_default_bands_are_the_target_band_times_the_ratio():
    bounds = default_bounds(TextSizes())
    assert bounds["x_ticks"] == (8.0, 12.0)
    assert bounds["x_label"] == pytest.approx((9.6, 14.4))
    assert bounds["title"] == pytest.approx((11.52, 17.28))
    slide = default_bounds(TextSizes(target="slide"))
    assert slide["title"] == pytest.approx((20.16, 40.32))


def test_with_the_defaults_the_reference_runs_exactly_over_the_band():
    assert reference_range(TextSizes()) == (8.0, 12.0)
    assert reference_range(TextSizes(target="slide")) == (14.0, 28.0)


# --- drawn sizes ---------------------------------------------------------------


def test_drawn_sizes_keep_the_ratios_inside_the_band():
    sizes, clamped = drawn_sizes(10.0, TextSizes())
    assert clamped == {}
    for element in TEXT_ELEMENTS:
        assert sizes[element] == pytest.approx(10.0 * DEFAULT_TEXT_RATIOS[element])


def test_a_clamp_changes_what_is_drawn_never_the_stored_ratio():
    text = TextSizes(ratios={"title": 2.0})
    sizes, clamped = drawn_sizes(12.0, text)
    # The title's own default band is 16-24 (8-12 x 2.0): 24 fits.
    assert sizes["title"] == 24.0 and "title" not in clamped
    capped = TextSizes(ratios={"title": 2.0}, ceilings={"title": 20.0})
    sizes, clamped = drawn_sizes(12.0, capped)
    assert sizes["title"] == 20.0 and clamped["title"] == CEILING
    assert capped.ratios == {"title": 2.0}
    assert text_ratios(capped)["title"] == 2.0


def test_resettling_at_a_larger_size_restores_the_proportions():
    """Held at a floor at a small reference; at a larger one the element is
    back on its ratio, because nothing was written back."""
    text = TextSizes(floors={"y_ticks": 10.0})
    small, clamped = drawn_sizes(8.0, text)
    assert small["y_ticks"] == 10.0 and clamped == {"y_ticks": FLOOR}
    large, clamped = drawn_sizes(11.0, text)
    assert large["y_ticks"] == 11.0 and clamped == {}
    assert large["x_label"] / large["x_ticks"] == pytest.approx(1.2)


def test_a_fixed_element_is_outside_the_scaling():
    text = TextSizes(y_ticks=7.0)
    for reference in (8.0, 12.0):
        sizes, clamped = drawn_sizes(reference, text)
        assert sizes["y_ticks"] == 7.0 and "y_ticks" not in clamped
    assert "y_ticks" not in auto_elements(text)


def test_user_pt_bounds_are_honoured():
    text = TextSizes(floors={"legend": 9.0}, ceilings={"x_ticks": 10.0})
    bounds = element_bounds(text)
    assert bounds["legend"] == (9.0, 12.0)
    assert bounds["x_ticks"] == (8.0, 10.0)
    sizes, clamped = drawn_sizes(11.5, text)
    assert sizes["x_ticks"] == 10.0 and clamped["x_ticks"] == CEILING
    sizes, clamped = drawn_sizes(8.0, text)
    assert sizes["legend"] == 9.0 and clamped["legend"] == FLOOR


def test_a_user_floor_above_the_default_ceiling_wins():
    bounds = element_bounds(TextSizes(floors={"x_ticks": 13.0}))
    assert bounds["x_ticks"] == (13.0, 13.0)


def test_the_range_spans_every_scaled_element():
    """The reference runs from the smallest value at which some element is
    still above its floor to the largest at which some is below its ceiling."""
    text = TextSizes(ceilings={"y_ticks": 18.0})
    assert reference_range(text) == (8.0, 18.0)
    assert reference_range(TextSizes(**{e: 10.0 for e in TEXT_ELEMENTS})) is None


# --- resolve_sizes -------------------------------------------------------------


def test_the_default_size_is_auto_for_print():
    assert TextSizes().base is None
    assert TextSizes().target == "print"
    assert StyleOptions().text == TextSizes()


def test_a_fixed_base_uses_the_ratios_and_is_never_clamped():
    sizes = resolve_sizes(TextSizes(base=30.0))
    assert sizes.base == 30.0
    assert sizes.title == pytest.approx(43.2)
    assert sizes.x_label == pytest.approx(36.0)
    assert sizes.groups == 30.0
    assert sizes.clamped == {} and sizes.bounds == {}


def test_the_auto_reference_is_used_and_fixed_sizes_win():
    sizes = resolve_sizes(TextSizes(y_label=17.0), reference=9.0)
    assert (sizes.base, sizes.x_ticks, sizes.y_ticks) == (9.0, 9.0, 9.0)
    assert sizes.x_label == pytest.approx(10.8)
    assert sizes.y_label == 17.0 and sizes.pinned == frozenset({"y_label"})


def test_the_auto_reference_is_ignored_when_base_is_fixed():
    sizes = resolve_sizes(TextSizes(base=10.0), reference=20.0)
    assert sizes.base == 10.0 and sizes.x_ticks == 10.0


def test_an_unsettled_auto_spec_sizes_at_the_top_of_its_range():
    assert resolve_sizes(TextSizes()).base == 12.0


def test_target_and_tables_are_not_pins():
    sizes = resolve_sizes(TextSizes(base=12.0, target="slide", ratios={"title": 2.0}))
    assert sizes.pinned == frozenset()


def test_an_unknown_target_or_element_is_refused():
    with pytest.raises(ValueError, match="text target"):
        TextSizes(target="poster")
    with pytest.raises(ValueError, match="not a text element"):
        TextSizes(ratios={"subtitle": 1.0})
    with pytest.raises(ValueError, match="positive"):
        TextSizes(floors={"title": 0})
    with pytest.raises(ValueError, match="floors above ceilings"):
        TextSizes(floors={"title": 20.0}, ceilings={"title": 10.0})


def test_text_sizes_with_tables_are_hashable():
    """The autosize memo keys on the text."""
    a = TextSizes(ratios={"title": 2.0})
    assert hash(a) == hash(TextSizes(ratios={"title": 2.0}))
    assert a != TextSizes()


def test_the_legend_title_keeps_its_ratio_through_a_legend_shrink():
    sizes = resolve_sizes(TextSizes(base=10.0))
    assert sizes.legend_title == 12.0
    assert sizes.legend_title_for(8.0) == pytest.approx(9.6)
    fixed = resolve_sizes(TextSizes(base=10.0, legend_title=15.0))
    assert fixed.legend_title_for(8.0) == 15.0


def test_rc_params_state_every_size_in_points():
    params = rc_params(resolve_sizes(TextSizes(base=10.0, x_label=13.0, legend=11.0)))
    assert params == {
        "font.size": 10.0,
        "figure.titlesize": pytest.approx(14.4),
        "axes.titlesize": pytest.approx(14.4),
        "axes.labelsize": 13.0,
        "xtick.labelsize": 10.0,
        "ytick.labelsize": 10.0,
        "legend.fontsize": 11.0,
        "legend.title_fontsize": pytest.approx(12.0),
    }


def test_rc_params_are_accepted_by_matplotlib():
    matplotlib = pytest.importorskip("matplotlib")
    params = rc_params(resolve_sizes(TextSizes(base=11.0, legend_title=9.0)))
    with matplotlib.rc_context(params):
        assert matplotlib.rcParams["xtick.labelsize"] == 11.0


def test_to_dict_and_describe_for_the_gui_and_the_log():
    sizes = resolve_sizes(TextSizes(title=20.0, floors={"y_ticks": 10.0}), reference=8.0)
    payload = sizes.to_dict()
    assert payload["title"] == 20.0 and payload["base"] == 8.0
    assert payload["pinned"] == ["title"]
    assert payload["clamped"] == {"y_ticks": "floor"}
    assert payload["ratios"]["x_label"] == 1.2
    assert payload["bounds"]["y_ticks"] == [10.0, 12.0]
    line = sizes.describe()
    assert "title=20*" in line and "y_ticks=10(floor)" in line and "reference=8" in line


def test_text_sizes_round_trip_through_the_spec():
    from scistackplot import PlotSpec

    spec = PlotSpec(
        measures=["m"],
        style=StyleOptions(
            text=TextSizes(
                base=11.0, y_ticks=8.0, ratios={"title": 1.6}, floors={"legend": 9.0},
                ceilings={"x_ticks": 11.0},
            )
        ),
    )
    again = PlotSpec.from_dict(spec.to_dict())
    assert again.style.text == spec.style.text


def test_empty_tables_are_not_written():
    """Like a null size: a panel that clears its last ratio matches the save."""
    from scistackplot import PlotSpec

    text = PlotSpec(measures=["m"]).to_dict()["style"]["text"]
    assert not {"ratios", "floors", "ceilings"} & set(text)


def test_the_strict_reader_refuses_the_old_flat_keys():
    from scistackplot import PlotSpec

    with pytest.raises(TypeError):
        PlotSpec.from_dict({"measures": ["m"], "style": {"font_size": 12.0}})
