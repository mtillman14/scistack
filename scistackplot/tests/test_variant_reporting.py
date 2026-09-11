"""
What a variant row reports about the rows it actually got.

Two states need explaining, and both are the same question — *what did this pin
resolve to against the data?* — so they come off one summary rather than two
unrelated warnings:

* the pin **spans code versions**: a figure that IS drawn, but whose points were
  computed by more than one body of the same function;
* the pin **matched nothing**: no figure at all, from controls that look
  correctly filled in.

The second is reachable by design. ``variants.default_selection`` applies its pin
blindly — a rule that quietly picks a different value to avoid an empty figure is
no longer a rule anyone can predict — so a combination nobody ever ran selects
zero rows. That is only defensible if the empty state says what was attempted
and what exists instead.

See ``.claude/plan-default-variant-selection.md``, "Where each report lives".
"""

from __future__ import annotations

import pandas as pd
from scistackplot import LongTable, PlotSpec
from scistackplot.capability import variant_summary
from scistackplot.spec import VariantSet
from scistackplot.variants import SPAN_LOCATION_LIMIT, describe_span


def _table(
    frame: pd.DataFrame,
    variants: list[str],
    *,
    latest_column: str | None = None,
    schema_levels: list[str] | None = None,
) -> LongTable:
    factors = [c for c in frame.columns if c not in ("value", latest_column)]
    return LongTable.from_frame(
        frame,
        factors=factors,
        measures=["value"],
        variant_factors=variants,
        name="value",
        latest_column=latest_column,
        schema_levels=schema_levels or [],
    )


def _ragged_latest() -> LongTable:
    """Three subjects; only 01 was re-run under the edited body.

    Every row is the newest AT ITS OWN LOCATION, so the per-location flag keeps
    all three subjects — and the figure ends up holding two versions of ``f``.
    """
    return _table(
        pd.DataFrame(
            {
                "subject": ["01", "01", "02", "03"],
                "Code:f": ["v1", "v2", "v1", "v1"],
                "CodeIsLatest": [False, True, True, True],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ["Code:f"],
        latest_column="CodeIsLatest",
        schema_levels=["subject"],
    )


def _current_spec() -> PlotSpec:
    return PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )


# --- spanning versions ----------------------------------------------------


def test_a_span_names_which_locations_hold_which_version():
    """"Pools 2 versions" is not actionable; "v2 is only subject 01" is.

    This is what makes reporting the ordinary per-location "latest" state
    tolerable rather than crying wolf — the sentence tells the user whether they
    have a problem (one subject left behind) or not.
    """
    span = variant_summary(_current_spec(), _ragged_latest())["sets"][0]["spans"]

    assert span["Code:f"]["versions"] == {"v1": 2, "v2": 1}
    assert span["Code:f"]["locations"] == {"v1": ["02", "03"], "v2": ["01"]}
    assert span["Code:f"]["schema_levels"] == ["subject"]
    assert span["Code:f"]["function"] == "f"
    assert span["Code:f"]["truncated"] is False


def test_a_location_label_joins_the_whole_schema_path():
    """A location is the full nesting, outermost first — ``01`` alone is
    ambiguous once a subject has sessions."""
    table = _table(
        pd.DataFrame(
            {
                "subject": ["01", "01"],
                "session": ["pre", "post"],
                "Code:f": ["v1", "v2"],
                "CodeIsLatest": [True, True],
                "value": [1.0, 2.0],
            }
        ),
        ["Code:f"],
        latest_column="CodeIsLatest",
        schema_levels=["subject", "session"],
    )

    span = variant_summary(_current_spec(), table)["sets"][0]["spans"]["Code:f"]

    assert span["locations"] == {"v1": ["01/pre"], "v2": ["01/post"]}


def test_a_long_location_list_is_capped_and_says_so():
    n = SPAN_LOCATION_LIMIT + 4
    table = _table(
        pd.DataFrame(
            {
                "subject": [f"{i:02d}" for i in range(1, n + 1)] + ["99"],
                "Code:f": ["v1"] * n + ["v2"],
                "CodeIsLatest": [True] * (n + 1),
                "value": [float(i) for i in range(n + 1)],
            }
        ),
        ["Code:f"],
        latest_column="CodeIsLatest",
        schema_levels=["subject"],
    )

    span = variant_summary(_current_spec(), table)["sets"][0]["spans"]["Code:f"]

    assert span["versions"] == {"v1": n, "v2": 1}
    assert len(span["locations"]["v1"]) == SPAN_LOCATION_LIMIT
    assert span["truncated"] is True
    assert "…" in describe_span(span)


def test_locations_sort_naturally_not_lexically():
    table = _table(
        pd.DataFrame(
            {
                "subject": ["10", "2", "1"],
                "Code:f": ["v1", "v1", "v2"],
                "CodeIsLatest": [True, True, True],
                "value": [1.0, 2.0, 3.0],
            }
        ),
        ["Code:f"],
        latest_column="CodeIsLatest",
        schema_levels=["subject"],
    )

    span = variant_summary(_current_spec(), table)["sets"][0]["spans"]["Code:f"]

    assert span["locations"]["v1"] == ["2", "10"]


def test_a_table_with_no_schema_still_reports_the_versions():
    """A CSV has nowhere to place a version, but "this pools two bodies" is
    still worth saying."""
    table = _table(
        pd.DataFrame(
            {
                "Code:f": ["v1", "v2"],
                "session": ["pre", "post"],
                "value": [1.0, 2.0],
            }
        ),
        ["Code:f"],
    )
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("all", {"session": ["pre", "post"]})],
    )

    span = variant_summary(spec, table)["sets"][0]["spans"]["Code:f"]

    assert span["versions"] == {"v1": 1, "v2": 1}
    assert span["locations"] == {"v1": [], "v2": []}
    assert "row(s)" in describe_span(span)


def test_one_version_everywhere_is_not_a_span():
    """The state the warning must stay quiet about, or it is worthless."""
    table = _table(
        pd.DataFrame(
            {
                "subject": ["01", "02"],
                "Code:f": ["v2", "v2"],
                "CodeIsLatest": [True, True],
                "value": [1.0, 2.0],
            }
        ),
        ["Code:f"],
        latest_column="CodeIsLatest",
        schema_levels=["subject"],
    )

    assert variant_summary(_current_spec(), table)["sets"][0]["spans"] == {}


# --- the empty selection --------------------------------------------------


def _ragged_combinations() -> LongTable:
    """Parameter1=1 only ever ran under v1; the current body only ran value 2."""
    return _table(
        pd.DataFrame(
            {
                "subject": ["01", "01"],
                "Code:f": ["v1", "v2"],
                "f.Parameter1": ["1", "2"],
                "CodeIsLatest": [False, True],
                "value": [1.0, 2.0],
            }
        ),
        ["Code:f", "f.Parameter1"],
        latest_column="CodeIsLatest",
        schema_levels=["subject"],
    )


def test_a_row_that_matched_nothing_lists_what_does_exist():
    """The empty figure has to be self-explaining, because the pin that caused
    it is applied blindly and looks perfectly well-formed on screen."""
    table = _ragged_combinations()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("current", {"CodeIsLatest": True, "f.Parameter1": "1"})
        ],
    )

    entry = variant_summary(spec, table)["sets"][0]

    assert entry["row_count"] == 0
    assert entry["available"] == [
        {"Code:f": "v1", "f.Parameter1": "1"},
        {"Code:f": "v2", "f.Parameter1": "2"},
    ]


def test_the_row_also_reports_what_its_selection_resolved_to():
    """"What exists" is only half the sentence; the other half is what was
    attempted, and the raw selection is not that — ``latest`` resolves."""
    table = _ragged_combinations()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[VariantSet("old", {"Code:f": "latest"})],
    )

    entry = variant_summary(spec, table)["sets"][0]

    # A lone "latest" resolves through the per-location flag, not to an ordinal.
    assert entry["resolved"] == {"CodeIsLatest": True}


def test_a_row_that_matched_rows_does_not_pay_for_the_list():
    """The common path. Computing "what else exists" when the answer to "what
    did I get" is not *nothing* is pure cost and pure noise."""
    table = _ragged_combinations()
    spec = PlotSpec(
        measures=["value"],
        variant_sets=[
            VariantSet("current", {"CodeIsLatest": True, "f.Parameter1": "2"})
        ],
    )

    entry = variant_summary(spec, table)["sets"][0]

    assert entry["row_count"] == 1
    assert entry["available"] == []


def test_an_unfilled_row_reports_neither():
    """An unfilled row is inert — it claims nothing, so it has not "matched
    nothing" and must not be decorated as though it had."""
    spec = PlotSpec(measures=["value"], variant_sets=[VariantSet(None, {})])

    entry = variant_summary(spec, _ragged_combinations())["sets"][0]

    assert entry["defined"] is False
    assert entry["available"] == []
    assert entry["resolved"] == {}
    assert entry["spans"] == {}
