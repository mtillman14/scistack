"""Fitting axis labels into their room: the pure decision (scistackplot.ticklabels).

A fake ``measure`` makes the geometry exact: every character is 0.6 em wide and
a line is 1 em tall, so each expected outcome below can be checked by hand.
The renderers' real measurements are exercised separately (stage 2).
"""

from __future__ import annotations

import pytest

from scistackplot import (
    BRACKET_POLICY,
    LabelPolicy,
    LabelRow,
    fit_labels,
    slot_row,
)
from scistackplot.ticklabels import is_numbered, strip_prefix, wrap_label

#: Names (not numbered, so never stripped or thinned), no break point: only
#: shrink and rotate can help.
PLAIN = ["ABCDEFGHIJ", "KLMNOPQRST"]


def measure(text: str, font_pt: float) -> tuple[float, float]:
    return 0.6 * font_pt * len(text), font_pt


# --- the ladder, step by step ------------------------------------------------


def test_labels_that_fit_are_left_alone():
    fit = fit_labels(slot_row(["A", "B", "C"], 50), 10, measure)
    assert fit.rows == (("A", "B", "C"),)
    assert (fit.font_pt, fit.rotation, fit.every) == (10, 0, 1)
    assert fit.steps == ()
    assert fit.fits


def test_wrap_before_shrinking():
    # Upright needs (90 + 90) / 2 + 2 = 92pt; wrapped, (66 + 54) / 2 + 2 = 62.
    fit = fit_labels(slot_row(["Six_Minute_Walk", "Timed_Up_and_Go"], 70), 10, measure)
    assert fit.steps == ("wrap",)
    assert fit.wrapped and fit.font_pt == 10 and fit.rotation == 0
    assert all("\n" in label for label in fit.rows[0])


def test_shrink_picks_the_largest_font_that_fits():
    # 6f + 2 <= 58  ->  f <= 9.33, and fonts step by 0.5.
    fit = fit_labels(slot_row(PLAIN, 58), 10, measure)
    assert fit.steps == ("shrink",)
    assert fit.font_pt == 9.0 and fit.rotation == 0


def test_rotate_45_when_even_the_floor_font_overlaps():
    # Floor 8pt upright needs 50pt > 30. At 45 deg only heights compete:
    # (10 + 2) / sin 45 = 17pt.
    fit = fit_labels(slot_row(PLAIN, 30), 10, measure)
    assert fit.steps == ("rotate_45",)
    assert (fit.rotation, fit.font_pt) == (45, 10)


def test_rotate_90_when_45_is_not_enough():
    # 45 deg needs 17pt at 10pt, 14.1 at the 8pt floor; 90 deg needs 12.
    fit = fit_labels(slot_row(PLAIN, 14), 10, measure)
    assert (fit.rotation, fit.font_pt) == (90, 10)


def test_thin_keeps_the_ends_and_uses_the_smallest_step():
    labels = [f"{i:02d}" for i in range(1, 21)]
    fit = fit_labels(slot_row(labels, 5), 10, measure)
    assert fit.fits
    assert fit.every == 2 and fit.rotation == 90
    shown = fit.rows[0]
    assert shown[0] == "01" and shown[-1] == "20"
    assert "thin" in fit.steps


def test_names_are_never_thinned():
    # A hidden number can be read off its neighbours; a hidden name cannot.
    # So names stop at 90 degrees and report that they still overlap.
    names = ["Apple", "Berry", "Cherry", "Kiwi", "Lemon", "Mango", "Olive", "Plum"]
    fit = fit_labels(slot_row(names, 5), 10, measure)
    assert fit.every == 1
    assert all(fit.rows[0])
    assert not fit.fits and fit.rotation == 90


def test_thin_keeps_each_groups_ends():
    labels = [f"{i:02d}" for i in range(5)] + [""] + [f"{i:02d}" for i in range(5, 10)]
    row = slot_row(labels, 5, groups=[(0, 4), (6, 10)])
    fit = fit_labels(row, 10, measure)
    shown = fit.rows[0]
    for start, end in [(0, 4), (6, 10)]:
        assert shown[start] and shown[end]
    assert shown[5] == ""


def test_spacers_give_their_room_to_the_neighbours():
    # Adjacent at 10pt slots these collide (6*10 = 60 > 10); two spacers
    # apart they are 30pt apart and a 2-char label needs 14.
    fit = fit_labels(slot_row(["ab", "", "", "cd"], 10), 10, measure)
    assert fit.steps == () and fit.fits


# --- one decision per figure -------------------------------------------------


def test_the_most_crowded_panel_decides_for_every_panel():
    roomy, crowded = slot_row(PLAIN, 100), slot_row(PLAIN, 30)
    assert fit_labels(roomy, 10, measure).rotation == 0
    fit = fit_labels([roomy, crowded], 10, measure)
    assert fit.rotation == 45
    assert len(fit.rows) == 2


# --- prefix stripping (numbered labels only) --------------------------------


def test_strip_a_shared_id_prefix():
    fit = fit_labels(slot_row(["SS01", "SS03", "SS09"], 100), 10, measure)
    assert fit.rows == (("01", "03", "09"),)
    assert fit.prefix == "SS" and fit.steps == ("strip_prefix",)


@pytest.mark.parametrize(
    "labels, expected, prefix",
    [
        (["SS01", "SS03"], ["01", "03"], "SS"),
        (["SS01", "SS10"], ["01", "10"], "SS"),
        (["Subject_01", "Subject_02"], ["01", "02"], "Subject_"),
        (["Trial_A1", "Trial_B1"], ["A1", "B1"], "Trial_"),
        (["SS01", "", "SS02"], ["01", "", "02"], "SS"),
        # Names are never stripped, however much they share.
        (["L_HAM", "R_HAM"], ["L_HAM", "R_HAM"], ""),
        (["RHAM", "RTA"], ["RHAM", "RTA"], ""),
        # Every label must be numbered, not just most of them.
        (["SS01", "SS02", "SSx"], ["SS01", "SS02", "SSx"], ""),
        # Never mid-word or inside a number.
        (["Pre1", "Post1"], ["Pre1", "Post1"], ""),
        (["01", "02", "10"], ["01", "02", "10"], ""),
        # One distinct level: nothing is redundant about it.
        (["SS01", "SS01"], ["SS01", "SS01"], ""),
        # Numbered, but no shared prefix.
        (["MID24", "POST24"], ["MID24", "POST24"], ""),
    ],
)
def test_strip_prefix(labels, expected, prefix):
    assert strip_prefix(labels) == (expected, prefix)


@pytest.mark.parametrize(
    "labels, numbered",
    [
        (["01", "02"], True),
        (["SS01", "Trial_12", ""], True),
        (["SS01", "SSx"], False),
        (["Digitimer", "Sham"], False),
        (["1a", "2a"], False),
        ([""], False),
    ],
)
def test_is_numbered(labels, numbered):
    assert is_numbered(labels) is numbered


def test_stripping_can_be_turned_off():
    policy = LabelPolicy(strip_prefix=False)
    fit = fit_labels(slot_row(["SS01", "SS02"], 100), 10, measure, policy)
    assert fit.rows == (("SS01", "SS02"),) and fit.prefix == ""


# --- wrapping ----------------------------------------------------------------


def test_wrap_leaves_unbreakable_labels_alone():
    assert wrap_label("Digitimer", 10, measure) == "Digitimer"


def test_wrap_drops_the_space_at_a_break():
    wrapped = wrap_label("Timed Up and Go", 10, measure)
    assert "\n" in wrapped
    assert all(line == line.strip() for line in wrapped.split("\n"))


# --- brackets and failure ----------------------------------------------------


def test_brackets_never_rotate_or_thin_and_report_the_overlap():
    fit = fit_labels(slot_row(PLAIN, 20), 10, measure, BRACKET_POLICY)
    assert not fit.fits
    assert fit.rotation == 0 and fit.every == 1
    assert fit.worst_overlap_pt > 0
    # The least-overlapping attempt: the floor font.
    assert fit.font_pt == BRACKET_POLICY.font_floor(10)
    assert "STILL OVERLAPS" in fit.describe()


@pytest.mark.parametrize("font, floor", [(14, 9.8), (10, 8.0), (6, 6.0)])
def test_font_floor(font, floor):
    assert LabelPolicy().font_floor(font) == pytest.approx(floor)


def test_row_lengths_must_agree():
    with pytest.raises(ValueError):
        LabelRow(labels=["a", "b"], positions=[0.0])


def test_describe_names_the_decision():
    fit = fit_labels(slot_row(PLAIN, 30), 10, measure)
    assert "rotate 45°" in fit.describe()


# --- settings the user fixed (StyleOptions.tick_*) --------------------------


def test_a_pinned_rotation_is_used_even_where_upright_fits():
    fit = fit_labels(slot_row(["A", "B"], 50), 10, measure, LabelPolicy(pin_rotation=90))
    assert fit.rotation == 90 and fit.fits
    assert "pinned" in fit.steps


def test_a_pinned_font_is_used_as_given():
    fit = fit_labels(slot_row(["A", "B"], 50), 10, measure, LabelPolicy(pin_font_pt=6.0))
    assert fit.font_pt == 6.0


def test_a_pinned_every_thins_even_names():
    """Auto never thins names; asked to, it does."""
    names = ["Apple", "Berry", "Cherry", "Kiwi"]
    fit = fit_labels(slot_row(names, 100), 10, measure, LabelPolicy(pin_every=2))
    assert fit.every == 2
    assert fit.rows[0][1] == "" and fit.rows[0][0] == "Apple" and fit.rows[0][-1] == "Kiwi"


def test_a_pin_that_overlaps_is_kept_and_reported():
    fit = fit_labels(slot_row(PLAIN, 14), 10, measure, LabelPolicy(pin_rotation=0))
    assert fit.rotation == 0
    assert not fit.fits and fit.worst_overlap_pt > 0


def test_what_is_not_pinned_is_still_fitted():
    """Rotation fixed upright, crowded numbered labels: thinning does the rest."""
    labels = [f"{i:02d}" for i in range(1, 21)]
    fit = fit_labels(slot_row(labels, 8), 10, measure, LabelPolicy(pin_rotation=0))
    assert fit.rotation == 0 and fit.every > 1 and fit.fits
