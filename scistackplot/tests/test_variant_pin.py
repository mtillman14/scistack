"""Named variants: selection, the ``Variant`` factor, and the picker's data model.

A variant is a **named** region of variant space. One is a pin ("show me the
current results"); several are a comparison, and they collapse into one ordinary
factor whose levels are the names — which is what makes "v1 against v3" a figure
rather than two figures.

Selection values may be a list of levels, not just one. That is what turns "show
me this one variant" into "show me every variant where bandpass=v1" — the
question a project with several versioned layers actually asks, and one that
cannot be expressed a single scalar at a time.

``variant_summary`` is the other half: the rows the GUI renders plus the
combination counts it reads. Both go through ``variant_set_mask``, so the number
on screen and the rows in the figure cannot disagree.

See docs/claude/variant-selection.md §5 and .claude/plan-plot-variant-rows.md.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest
from scistackplot import (
    VARIANT_FACTOR,
    LongTable,
    PlotSpec,
    Role,
    RoleError,
    apply_variant_sets,
    auto_label,
    variant_set_mask,
    variant_summary,
)
from scistackplot.spec import VariantPolicy, VariantSet


def _table(
    frame: pd.DataFrame, variants: list[str], *, latest_column: str | None = None
) -> LongTable:
    factors = [c for c in frame.columns if c not in ("value", latest_column)]
    return LongTable.from_frame(
        frame,
        factors=factors,
        measures=["value"],
        variant_factors=variants,
        name="value",
        latest_column=latest_column,
    )


# --- the mask -------------------------------------------------------------


def test_a_level_set_keeps_any_of_them():
    frame = pd.DataFrame({"Code:f": ["v1", "v2", "v3"], "value": [1, 2, 3]})

    kept = frame[variant_set_mask(frame, {"Code:f": ["v1", "v3"]})]

    assert kept["value"].tolist() == [1, 3]


def test_a_scalar_still_works():
    frame = pd.DataFrame({"Code:f": ["v1", "v2"], "value": [1, 2]})

    kept = frame[variant_set_mask(frame, {"Code:f": "v2"})]

    assert kept["value"].tolist() == [2]


def test_an_unknown_column_is_ignored_not_matched_against():
    """A spec outlives the table it was written against — a two-measure join
    drops columns, a reload may find a factor gone. A stale key must not
    silently empty the figure."""
    frame = pd.DataFrame({"Code:f": ["v1", "v2"], "value": [1, 2]})

    kept = frame[variant_set_mask(frame, {"gone": "v9"})]

    assert len(kept) == 2


def test_two_dimensions_select_a_subcube_not_a_point():
    """Neither dimension alone identifies a row; selecting one leaves the other
    free, which is exactly 'every variant where filter=v1'."""
    frame = pd.DataFrame(
        {
            "Code:load": ["v1", "v1", "v2", "v2"],
            "Code:filter": ["v1", "v2", "v1", "v2"],
            "value": [1, 2, 3, 4],
        }
    )

    kept = frame[variant_set_mask(frame, {"Code:filter": ["v1"]})]

    assert kept["value"].tolist() == [1, 3], "both load versions must survive"


def test_an_empty_level_list_keeps_nothing():
    """A legitimate state to be IN (everything unchecked) even though it is not
    one to render. The picker reports it; validate refuses it."""
    frame = pd.DataFrame({"Code:f": ["v1", "v2"], "value": [1, 2]})

    assert len(frame[variant_set_mask(frame, {"Code:f": []})]) == 0


# --- "latest" -------------------------------------------------------------
#
# The two resolutions are the subtlest thing in this feature, and getting the
# first one wrong loses whole subjects from a figure without saying so.


def _latest_frame() -> pd.DataFrame:
    # Subject 02 was never re-run, so its only record is v1 — and it is the
    # newest AT ITS OWN LOCATION, which is what CodeIsLatest records.
    return pd.DataFrame(
        {
            "subject": ["01", "01", "02"],
            "Code:f": ["v1", "v2", "v1"],
            "CodeIsLatest": [False, True, True],
            "value": [1.0, 2.0, 3.0],
        }
    )


def test_latest_alone_uses_the_per_location_flag():
    frame = _latest_frame()

    kept = frame[
        variant_set_mask(frame, {"Code:f": "latest"}, latest_column="CodeIsLatest")
    ]

    assert kept["value"].tolist() == [2.0, 3.0], (
        "subject 02 never ran v2; resolving 'latest' to the highest ordinal "
        "would drop it from the figure entirely"
    )


def test_latest_beside_a_named_pin_resolves_to_the_highest_ordinal():
    """The flag is unusable once something is pinned to an old version — a v1
    record is by definition not the latest — so the remaining axes take the
    highest ordinal instead. The GUI shows this as 'latest (v2)'."""
    frame = pd.DataFrame(
        {
            "Code:load": ["v1", "v1", "v2", "v2"],
            "Code:filter": ["v1", "v2", "v1", "v2"],
            "CodeIsLatest": [False, False, False, True],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )

    kept = frame[
        variant_set_mask(
            frame,
            {"Code:load": "v1", "Code:filter": "latest"},
            latest_column="CodeIsLatest",
        )
    ]

    assert kept["value"].tolist() == [2.0]


def test_highest_ordinal_is_numeric_not_lexical():
    frame = pd.DataFrame({"Code:f": ["v9", "v10"], "other": ["a", "a"], "value": [1, 2]})

    kept = frame[variant_set_mask(frame, {"Code:f": "latest", "other": "a"})]

    assert kept["value"].tolist() == [2], "v10 beats v9"


def test_a_missing_version_level_never_wins_latest():
    """scidb writes '(n/a)' where a record's chain never ran the function; it is
    not a version and must not be chosen as the newest one."""
    frame = pd.DataFrame(
        {"Code:f": ["(n/a)", "v1"], "other": ["a", "a"], "value": [1, 2]}
    )

    kept = frame[variant_set_mask(frame, {"Code:f": "latest", "other": "a"})]

    assert kept["value"].tolist() == [2]


# --- the Variant factor ---------------------------------------------------


def _two_version_table() -> LongTable:
    return _table(
        pd.DataFrame(
            {
                "Code:filter": ["v1", "v2", "v1", "v2"],
                "session": ["pre", "pre", "post", "post"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ["Code:filter"],
    )


def test_one_variant_is_a_pin():
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("baseline", {"Code:filter": "v1"})],
    )

    derived = apply_variant_sets(spec, table)

    assert derived.frame["value"].tolist() == [1.0, 3.0]
    assert derived.factor(VARIANT_FACTOR).levels == ["baseline"]


def test_two_variants_become_the_levels_of_one_factor():
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet("new filter", {"Code:filter": "v2"}),
        ],
    )

    derived = apply_variant_sets(spec, table)

    assert derived.factor(VARIANT_FACTOR).levels == ["baseline", "new filter"]
    assert sorted(derived.frame[VARIANT_FACTOR].unique()) == ["baseline", "new filter"]


def test_a_selected_column_stops_being_its_own_factor():
    """Otherwise the same information is encoded twice — and with two variants
    the leftover column is an unassigned two-level variant factor, so validate
    would refuse the very figure the user just asked for."""
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet("new filter", {"Code:filter": "v2"}),
        ],
    )

    derived = apply_variant_sets(spec, table)

    assert "Code:filter" not in derived.factor_names
    assert "session" in derived.factor_names, "untouched factors are left alone"


def test_the_latest_flag_answers_every_code_axis():
    """Reported 2026-09-09: `Code:loadDelsysEMGOneFile` sat in Factors beside a
    variant that had already decided it.

    The opening selection is `CodeIsLatest`, which is not a code *column*, so a
    name-matching test never saw it. But that flag IS the code selection — one
    flag for the whole chain, which is what keeps "current results" a single
    row however many functions were edited.
    """
    table = _table(
        pd.DataFrame(
            {
                "Code:load": ["v1", "v2", "v1"],
                "Code:filter": ["v1", "v1", "v2"],
                "CodeIsLatest": [False, True, True],
                "value": [1.0, 2.0, 3.0],
            }
        ),
        ["Code:load", "Code:filter"],
        latest_column="CodeIsLatest",
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )

    derived = apply_variant_sets(spec, table)

    assert derived.factor_names == [VARIANT_FACTOR]


def test_current_may_hold_several_ordinals_and_that_is_not_an_overplot():
    """The flag is per schema LOCATION, so one variant legitimately holds rows
    built by different versions — each its own location's newest. Collapsing
    them is the point; it is not the thing the pooling guard protects against."""
    table = _table(
        pd.DataFrame(
            {
                "subject": ["01", "01", "02"],
                "Code:load": ["v1", "v2", "v1"],
                "CodeIsLatest": [False, True, True],
                "value": [1.0, 2.0, 3.0],
            }
        ),
        ["Code:load"],
        latest_column="CodeIsLatest",
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )

    derived = apply_variant_sets(spec, table)

    assert derived.frame["value"].tolist() == [2.0, 3.0]
    assert "Code:load" not in derived.factor_names


def test_a_branch_param_only_some_variants_answer_stays_a_factor():
    """The intersection rule, which still applies to branch params: nothing
    about "current code" decides which filter cutoff to plot."""
    table = _table(
        pd.DataFrame(
            {
                "Code:filter": ["v1", "v1", "v2", "v2"],
                "bandpass.low_hz": ["20", "50", "20", "50"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ["Code:filter", "bandpass.low_hz"],
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("v1 at 20", {"Code:filter": "v1", "bandpass.low_hz": "20"}),
            VariantSet("v2, any cutoff", {"Code:filter": "v2"}),
        ],
    )

    derived = apply_variant_sets(spec, table)

    assert "bandpass.low_hz" in derived.factor_names
    assert "Code:filter" not in derived.factor_names


# --- a row that has not been filled in yet --------------------------------
#
# Reported 2026-09-09: clicking "+ Add variant" immediately produced a
# RoleError and dropped `Code:<fn>` back into Factors. Adding a row is not a
# statement about the data; nothing should happen until it says something.


def test_an_unfilled_variant_changes_nothing():
    table = _two_version_table()
    filled = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("baseline", {"Code:filter": "v1"})],
    )
    just_added = PlotSpec(
        measures=["value"],
        variant_sets=[*filled.variant_sets, VariantSet(None, {})],
    )

    before = apply_variant_sets(filled, table)
    after = apply_variant_sets(just_added, table)

    assert after.frame["value"].tolist() == before.frame["value"].tolist()
    assert after.factor(VARIANT_FACTOR).levels == ["baseline"]


def test_an_unfilled_variant_does_not_resurrect_the_code_factor():
    """The reported symptom: an empty row answered nothing, so under an
    intersection rule it un-answered the code axis for every other row."""
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet(None, {}),
        ],
    )

    assert "Code:filter" not in apply_variant_sets(spec, table).factor_names


def test_an_unfilled_variant_does_not_make_the_figure_unrenderable():
    from scistackplot.roles import validate

    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        roles={"session": Role.X},
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet(None, {}),
        ],
    )

    validate(spec, apply_variant_sets(spec, table))  # must not raise


def test_an_unfilled_variant_reads_as_unset_not_as_matching_nothing():
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet(None, {}),
        ],
    )

    entry = variant_summary(spec, table)["sets"][1]

    assert entry["defined"] is False
    assert entry["auto_label"] == "(not set 2)"


def test_only_unfilled_variants_is_the_same_as_none():
    table = _two_version_table()
    spec = PlotSpec(measures=["value"], variant_sets=[VariantSet(None, {})])

    assert apply_variant_sets(spec, table) is table


# --- code axes leave Factors unconditionally, and say when that pools ------


def test_a_variant_that_pools_versions_says_so_on_its_own_row():
    """Code axes are the Variants section's business, so the column never comes
    back as a factor. A variant straddling two versions is reported where the
    fix is — on the row — rather than by asking the user to answer the same
    question again in Factors."""
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("everything", {"session": ["pre", "post"]})],
    )

    entry = variant_summary(spec, table)["sets"][0]

    assert set(entry["spans"]) == {"Code:filter"}
    assert entry["spans"]["Code:filter"]["versions"] == {"v1": 2, "v2": 2}
    assert "Code:filter" not in apply_variant_sets(spec, table).factor_names


def test_latest_spanning_ordinals_IS_reported_as_pooling():
    """Reversed 2026-09-11, by the user, deliberately.

    This test used to assert the opposite: a selection resolving through the
    per-location ``CodeIsLatest`` flag was never counted as spanning, because
    spanning ordinals is what per-location "latest" *means* and reporting it
    would cry wolf on the most ordinary state in the system.

    That argument is about frequency, and the thing it stayed silent about is a
    figure whose points were computed by **different versions of the same
    function** — which a reader cannot see and must not have to assume away. The
    requirement now is the opposite: if the body actually used differs between
    schema locations, say so prominently.

    What makes it actionable rather than noisy is naming the locations, which is
    why ``spans`` carries them; see
    ``test_a_span_names_which_locations_hold_which_version``.

    Do not re-exempt the flag without re-reading
    ``docs/claude/plot-variant-rows.md`` §3, which argued for the old rule.
    """
    table = _table(
        _latest_frame(), ["Code:f"], latest_column="CodeIsLatest"
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )

    spans = variant_summary(spec, table)["sets"][0]["spans"]

    # Subject 01 contributes its v2 record, subject 02 its v1 — both current at
    # their own location, and two different bodies in one figure.
    assert set(spans) == {"Code:f"}
    assert spans["Code:f"]["versions"] == {"v1": 1, "v2": 1}


def test_the_latest_flag_does_not_answer_branch_params():
    """Nothing about 'current code' decides which filter cutoff to plot."""
    table = _table(
        pd.DataFrame(
            {
                "Code:load": ["v1", "v2"],
                "bandpass.low_hz": ["20", "50"],
                "CodeIsLatest": [True, True],
                "value": [1.0, 2.0],
            }
        ),
        ["Code:load", "bandpass.low_hz"],
        latest_column="CodeIsLatest",
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )

    derived = apply_variant_sets(spec, table)

    assert "bandpass.low_hz" in derived.factor_names
    assert "Code:load" not in derived.factor_names


def test_defaults_never_assign_a_role_to_an_answered_column():
    """The crash this pair of rules would otherwise cause.

    `default_roles` puts a multi-level `Code:<fn>` on COLOUR; the table then
    opens on the "current" variant, which answers that column; `validate` then
    calls the role an unknown factor and refuses to draw anything.
    """
    from scistackplot import default_spec
    from scistackplot.roles import validate

    table = _table(
        pd.DataFrame(
            {
                "subject": ["01", "01", "02"],
                "Code:load": ["v1", "v2", "v1"],
                "CodeIsLatest": [False, True, True],
                "value": [1.0, 2.0, 3.0],
            }
        ),
        ["Code:load"],
        latest_column="CodeIsLatest",
    )
    table = replace(table, default_pin={"CodeIsLatest": True})

    spec = default_spec(table, "value")

    assert "Code:load" not in spec.roles
    validate(spec, apply_variant_sets(spec, table))  # must not raise


def test_a_stale_role_from_a_saved_spec_is_dropped_not_fatal():
    """Assign a code axis to a facet, then add a variant that pins it — an
    ordinary sequence in the GUI, and the spec outlives the table either way."""
    from scistackplot.variants import strip_answered_roles

    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        roles={"Code:filter": Role.FACET, "session": Role.X},
        variant_sets=[VariantSet("v1 only", {"Code:filter": "v1"})],
    )
    derived = apply_variant_sets(spec, table)

    cleaned = strip_answered_roles(spec, table, derived)

    assert "Code:filter" not in cleaned.roles
    assert cleaned.roles["session"] is Role.X


def test_a_role_naming_nothing_at_all_is_still_an_error():
    """The stale-role drop must not swallow typos."""
    from scistackplot.roles import validate

    table = _two_version_table()
    spec = PlotSpec(measures=["value"], roles={"sesion": Role.X})

    with pytest.raises(RoleError, match="unknown factor"):
        validate(spec, table)


def test_an_unselected_code_axis_is_reported_on_the_ROW_not_resurrected():
    """A code axis a variant left open still has to be surfaced — but where?

    This test used to assert the column came back as a factor. That was the
    earlier rule, and `_answered` deliberately replaced it: **every** code axis
    is answered once any variant is defined, because "which version of the code"
    is the question the Variants section exists to answer, and offering it again
    in Factors asks the user to decide the same thing twice with no way to tell
    which answer wins.

    The distinction is not lost, it moved. `spanned_code_axes` reports it on the
    row that pooled it — where the fix is (pin a version there, or split the row
    in two), and where the GUI shows it as "pools 2 versions".
    """
    from scistackplot.capability import variant_summary

    table = _table(
        pd.DataFrame(
            {
                "Code:load": ["v1", "v2", "v1", "v2"],
                "Code:filter": ["v1", "v1", "v2", "v2"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ["Code:load", "Code:filter"],
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("filter v1", {"Code:filter": "v1"})],
    )

    derived = apply_variant_sets(spec, table)

    assert "Code:load" not in derived.factor_names, (
        "code axes belong to the Variants section entirely — see _answered"
    )
    spans = variant_summary(spec, table)["sets"][0]["spans"]
    assert set(spans) == {"Code:load"}, (
        "the variant pools two versions of the loader, and must say so"
    )
    assert spans["Code:load"]["versions"] == {"v1": 1, "v2": 1}


def test_rows_matching_no_variant_are_dropped():
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"], variant_sets=[VariantSet("only v1", {"Code:filter": "v1"})]
    )

    assert len(apply_variant_sets(spec, table).frame) == 2


def test_a_row_matching_two_variants_goes_to_the_first():
    """Overlapping selections are legal ('all where low_hz=20' and 'all where
    code=v1' genuinely intersect); duplicating the row would double-count it in
    every mean."""
    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        # Both sessions is every row, so the second variant's rows are a subset
        # of the first's. (An EMPTY selection would not do here: an unfilled row
        # is inert by design — see the "not filled in yet" tests.)
        variant_sets=[
            VariantSet("everything", {"session": ["pre", "post"]}),
            VariantSet("just v1", {"Code:filter": "v1"}),
        ],
    )

    derived = apply_variant_sets(spec, table)

    assert derived.frame[VARIANT_FACTOR].tolist() == ["everything"] * 4
    assert derived.factor(VARIANT_FACTOR).levels == ["everything"], (
        "a variant that claimed no rows contributes no level"
    )


def test_no_variant_sets_leaves_the_table_untouched():
    table = _two_version_table()

    assert apply_variant_sets(PlotSpec(measures=["value"]), table) is table


def test_a_multi_level_variant_factor_defaults_to_colour_not_pooling():
    """Two named variants left FREE would overplot — the exact failure this
    feature exists to prevent — so an unassigned Variant factor is coloured."""
    from scistackplot.roles import complete_roles

    table = _two_version_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet("new", {"Code:filter": "v2"}),
        ],
    )
    derived = apply_variant_sets(spec, table)

    assert complete_roles(spec, derived)[VARIANT_FACTOR] is Role.COLOR


def test_pooling_still_has_to_be_asked_for():
    from scistackplot.roles import validate

    table = _table(
        pd.DataFrame(
            {"Code:filter": ["v1", "v2"], "value": [1.0, 2.0]}, index=[0, 1]
        ),
        ["Code:filter"],
    )
    spec = PlotSpec(measures=["value"], roles={"Code:filter": Role.FREE})

    with pytest.raises(RoleError, match="pooled"):
        validate(spec, table)


# --- labels ----------------------------------------------------------------


def test_the_auto_label_names_the_coordinates():
    assert auto_label({"Code:bandpass": "v1", "bandpass.low_hz": "20"}) == (
        "bandpass v1 · low_hz=20"
    )


def test_an_unfilled_variant_is_labelled_as_unset():
    """Not "all variants": an unfilled row is inert, so a label promising every
    variant would describe a row that contributes nothing."""
    assert auto_label({}) == "(not set)"


def test_a_level_list_reads_as_alternatives():
    assert auto_label({"bandpass.low_hz": ["20", "50"]}) == "low_hz=20+50"


# --- the picker's data model ---------------------------------------------


def _two_axis_table() -> LongTable:
    return _table(
        pd.DataFrame(
            {
                "Code:load": ["v1", "v1", "v2", "v2"],
                "Code:filter": ["v1", "v2", "v1", "v2"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ["Code:load", "Code:filter"],
    )


def test_summary_lists_every_variant_factor_and_its_levels():
    table = _two_axis_table()
    spec = PlotSpec(measures=["value"], roles={}, variant_policy=VariantPolicy.FACET)

    summary = variant_summary(spec, table)

    assert [f["name"] for f in summary["factors"]] == ["Code:load", "Code:filter"]
    assert summary["factors"][0]["levels"] == ["v1", "v2"]
    assert all(f["is_code"] for f in summary["factors"])


def test_summary_axes_survive_being_selected():
    """The axis a variant already pinned is exactly the one the user needs to
    reopen and change, so the picker is given the pre-selection view."""
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("v1s", {"Code:filter": "v1"})],
    )

    names = [f["name"] for f in variant_summary(spec, table)["factors"]]

    assert "Code:filter" in names


def test_unselected_reports_everything_selected():
    table = _two_axis_table()
    spec = PlotSpec(measures=["value"], roles={}, variant_policy=VariantPolicy.FACET)

    summary = variant_summary(spec, table)

    assert summary["total_combinations"] == 4
    assert summary["selected_combinations"] == 4


def test_a_partial_selection_reports_the_surviving_combinations():
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("filter v1", {"Code:filter": ["v1"]})],
    )

    summary = variant_summary(spec, table)

    assert summary["selected_combinations"] == 2
    assert summary["total_combinations"] == 4
    by_name = {f["name"]: f for f in summary["factors"]}
    assert by_name["Code:filter"]["selected"] == ["v1"]
    assert by_name["Code:load"]["selected"] == ["v1", "v2"], (
        "the unselected dimension stays fully selected — that is what makes the "
        "selection partial, and the readout has to show it"
    )


def test_each_variant_reports_the_rows_it_contributes():
    """The number that catches real mistakes: a variant selecting a combination
    nobody ever ran looks exactly like a working one until its series is
    silently missing from the figure."""
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("filter v1", {"Code:filter": "v1"}),
            VariantSet("never ran", {"Code:filter": "v9"}),
        ],
    )

    sets = variant_summary(spec, table)["sets"]

    assert [s["row_count"] for s in sets] == [2, 0]
    assert [s["name"] for s in sets] == ["filter v1", "never ran"]


def test_row_counts_follow_first_match_wins():
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("everything", {"Code:load": ["v1", "v2"]}),
            VariantSet("filter v1", {"Code:filter": "v1"}),
        ],
    )

    assert [s["row_count"] for s in variant_summary(spec, table)["sets"]] == [4, 0]


def test_an_unnamed_variant_still_reports_a_label():
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"], variant_sets=[VariantSet(None, {"Code:filter": "v1"})]
    )

    entry = variant_summary(spec, table)["sets"][0]

    assert entry["explicit_name"] is None
    assert entry["name"] == entry["auto_label"] == "filter v1"


def test_counts_are_measured_not_multiplied():
    """Real data is ragged: a location never re-run under the newest code has
    no row for that combination. Multiplying level counts would claim 4."""
    table = _table(
        pd.DataFrame(
            {
                "Code:load": ["v1", "v1", "v2"],
                "Code:filter": ["v1", "v2", "v1"],
                "value": [1.0, 2.0, 3.0],
            }
        ),
        ["Code:load", "Code:filter"],
    )
    spec = PlotSpec(measures=["value"], roles={}, variant_policy=VariantPolicy.FACET)

    assert variant_summary(spec, table)["total_combinations"] == 3


def test_selecting_nothing_is_reported_not_hidden():
    table = _two_axis_table()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("nothing", {"Code:filter": []})],
    )

    assert variant_summary(spec, table)["selected_combinations"] == 0


def test_a_table_without_variants_summarises_to_nothing():
    table = _table(
        pd.DataFrame({"subject": ["01", "02"], "value": [1.0, 2.0]}), []
    )
    spec = PlotSpec(measures=["value"], roles={"subject": Role.X})

    summary = variant_summary(spec, table)

    assert summary["factors"] == []
    assert summary["total_combinations"] == 0


# --- serialization --------------------------------------------------------


def test_variant_sets_round_trip_through_json():
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("baseline", {"Code:filter": "v1"}),
            VariantSet(None, {"bandpass.low_hz": ["20", "50"]}),
        ],
    )

    restored = PlotSpec.from_json(spec.to_json())

    assert restored.variant_sets == spec.variant_sets
