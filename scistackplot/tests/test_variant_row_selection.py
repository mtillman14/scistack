"""
What a variant row selects — ONE owner (``variants.row_mask``) for the figure,
the row counts and the span banner.

Found 2026-10-15 (scidb.log): a saved plot stored ``variant_sets=[{selection:
{}}]``. After ``load_cbrace.m`` was edited and re-run, the FIGURE applied the
source default and drew v2 only, while ``capability.variant_summary`` measured
the empty row 0 as "every row" and its banner read "CosmedData mixes v1 | v2".
A leftover ``Code:load_cbrace`` factor then defaulted to ITERATE and titled the
figure ``Code:load_cbrace=v2``.

Also here: frozen rows (a reopened saved plot's versions), ``follow_latest``,
the export's selection, and the title rule for a single variant.

See ``docs/claude/variant-row-selection.md``.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scistackplot import LongTable, PlotSpec, VariantSet, resolve
from scistackplot.capability import variant_summary
from scistackplot.roles import Role
from scistackplot.variants import (
    SOURCE_DEFAULT,
    SOURCE_FROZEN,
    SOURCE_STATED,
    VARIANT_FACTOR,
    apply_variant_sets,
    effective_selection_with_source,
    export_selection,
    row_mask,
)

LATEST = "CodeIsLatest"


def _table(rows: list[dict], *, pin: bool = True, extra_variants=()) -> LongTable:
    frame = pd.DataFrame(rows)
    factors = [c for c in frame.columns if c not in ("value", LATEST)]
    return LongTable.from_frame(
        frame,
        factors=factors,
        measures=["value"],
        variant_factors=["Code:f", *extra_variants],
        name="value",
        latest_column=LATEST,
        default_pin={LATEST: True} if pin else None,
        schema_levels=["subject"],
    )


def _rerun_table() -> LongTable:
    """Every subject re-run under v2: v1 superseded everywhere (the CosmedData
    state after the 12:51 re-run)."""
    rows = []
    for subject in ("01", "02"):
        for code in ("v1", "v2"):
            rows.append(
                {
                    "subject": subject,
                    "Code:f": code,
                    LATEST: code == "v2",
                    "value": 1.0 if code == "v1" else 2.0,
                }
            )
    return _table(rows)


def _three_version_table() -> LongTable:
    """01 ran v1, v2, v3; 02 ran v1 only (never re-run)."""
    rows = [
        {"subject": "01", "Code:f": "v1", LATEST: False, "value": 1.0},
        {"subject": "01", "Code:f": "v2", LATEST: False, "value": 2.0},
        {"subject": "01", "Code:f": "v3", LATEST: True, "value": 3.0},
        {"subject": "02", "Code:f": "v1", LATEST: True, "value": 10.0},
    ]
    return _table(rows)


def _spec(*sets: VariantSet, roles=None) -> PlotSpec:
    return PlotSpec(measures=["value"], variant_sets=list(sets), roles=roles or {})


# --- the bug: banner and figure disagreed about an empty row 0 -------------


def test_an_empty_row_zero_reports_no_span_when_the_figure_draws_one_version():
    table = _rerun_table()
    spec = _spec(VariantSet())

    entry = variant_summary(spec, table)["sets"][0]
    derived = apply_variant_sets(spec, table)

    assert entry["spans"] == {}, "the figure draws v2 only — no 'mixes' banner"
    assert entry["row_count"] == len(derived.frame) == 2
    assert set(derived.frame["Code:f"]) == {"v2"}


def test_the_default_pin_answers_the_code_axis_so_it_is_no_longer_a_factor():
    """Left in, ``Code:f`` (filtered to one version already) got ITERATE and
    titled the figure ``Code:f=v2``."""
    table = _rerun_table()

    derived = apply_variant_sets(_spec(VariantSet()), table)

    assert "Code:f" not in derived.factor_names
    assert apply_variant_sets(_spec(), table).factor_names == derived.factor_names


def test_a_stale_code_role_from_a_saved_plot_is_dropped_not_iterated():
    table = _rerun_table()
    spec = _spec(VariantSet(), roles={"Code:f": Role.ITERATE, "subject": Role.GROUP})

    figures = resolve(spec, table)

    assert len(figures) == 1
    assert "Code:f" not in figures[0].figure_key
    assert "Code:f" not in (figures[0].labels.title or "")


def test_per_location_latest_spanning_versions_is_still_reported_honestly():
    """The genuine mix (02 never re-run) must still show the banner."""
    table = _three_version_table()

    spans = variant_summary(_spec(VariantSet()), table)["sets"][0]["spans"]

    assert spans["Code:f"]["versions"] == {"v1": 1, "v3": 1}


# --- one owner: counts always equal the figure ------------------------------


@pytest.mark.parametrize(
    "sets",
    [
        [VariantSet()],
        [VariantSet(None, {LATEST: True})],
        [VariantSet(None, {"Code:f": "v1"})],
        [VariantSet("a", {"Code:f": "v1"}), VariantSet("b", {"Code:f": "v2"})],
        [VariantSet(None, {}, frozen={"Code:f": ["v1"]})],
        [VariantSet(None, {"subject": "01"})],
    ],
)
def test_the_summary_counts_what_the_figure_draws(sets):
    table = _rerun_table()
    spec = _spec(*sets)

    summary = variant_summary(spec, table)["sets"]
    derived = apply_variant_sets(spec, table)

    assert sum(entry["row_count"] for entry in summary) == len(derived.frame)


# --- no pooling unless the selection names it -------------------------------


def test_a_row_naming_no_code_draws_current_code():
    table = _rerun_table()
    variant = VariantSet(None, {"subject": ["01", "02"]})

    selection, source = effective_selection_with_source(variant, table)

    assert source == SOURCE_DEFAULT
    assert selection == {LATEST: True, "subject": ["01", "02"]}
    assert set(table.frame[row_mask(table, _spec(variant), variant)]["Code:f"]) == {"v2"}


def test_naming_several_versions_pools_them_as_asked():
    """The one way to pool: name the versions. It is the user's explicit
    choice, so `spanned_code_axes` does not warn about the axis the row itself
    selected (it reports only axes a row left OPEN)."""
    table = _rerun_table()
    variant = VariantSet("both", {"Code:f": ["v1", "v2"]})

    assert effective_selection_with_source(variant, table)[1] == SOURCE_STATED
    entry = variant_summary(_spec(variant), table)["sets"][0]
    assert entry["row_count"] == 4
    assert entry["spans"] == {}


def test_a_source_without_a_default_is_used_as_written():
    table = _table(
        [{"subject": "01", "Code:f": c, LATEST: True, "value": 1.0} for c in ("v1", "v2")],
        pin=False,
    )

    assert effective_selection_with_source(VariantSet(), table) == ({}, SOURCE_STATED)


# --- frozen rows -------------------------------------------------------------


def test_a_frozen_row_draws_its_version_even_though_newer_exists():
    table = _rerun_table()
    variant = VariantSet(None, {LATEST: True}, frozen={"Code:f": ["v1"]})

    selection, source = effective_selection_with_source(variant, table)
    kept = table.frame[row_mask(table, _spec(variant), variant)]

    assert source == SOURCE_FROZEN
    assert LATEST not in selection, "'current' AND 'v1' would select nothing"
    assert set(kept["Code:f"]) == {"v1"}
    assert len(kept) == 2


def test_a_frozen_empty_row_zero_is_a_statement():
    """A reopened saved plot's row 0 is ``selection={}`` + a freeze. It must
    fold as a defined row, not fall back to current."""
    table = _rerun_table()
    spec = _spec(VariantSet(None, {}, frozen={"Code:f": ["v1"]}))

    derived = apply_variant_sets(spec, table)

    assert set(derived.frame["Code:f"]) == {"v1"}


def test_a_multi_version_freeze_keeps_the_highest_per_location():
    """Saved while 01 was on v2 and 02 on v1; reopened after 01 ran v3."""
    table = _three_version_table()
    variant = VariantSet(None, {}, frozen={"Code:f": ["v1", "v2"]})

    kept = table.frame[row_mask(table, _spec(variant), variant)]

    assert sorted(zip(kept["subject"], kept["Code:f"])) == [("01", "v2"), ("02", "v1")]


def test_follow_latest_ignores_a_freeze():
    table = _rerun_table()
    variant = VariantSet(None, {}, frozen={"Code:f": ["v1"]}, follow_latest=True)

    kept = table.frame[row_mask(table, _spec(variant), variant)]

    assert set(kept["Code:f"]) == {"v2"}


def test_a_frozen_column_the_table_lacks_is_ignored():
    """The function is back to one version: no axis, nothing to choose."""
    table = _rerun_table()
    variant = VariantSet(None, {}, frozen={"Code:gone": ["v1"]})

    assert effective_selection_with_source(variant, table)[1] == SOURCE_DEFAULT


def test_a_frozen_row_is_labelled_as_saved():
    table = _rerun_table()
    spec = _spec(VariantSet(None, {LATEST: True}, frozen={"Code:f": ["v1"]}))

    entry = variant_summary(spec, table)["sets"][0]

    assert entry["auto_label"] == "value · f v1 (saved)"
    assert entry["frozen"] == {"Code:f": ["v1"]}


def test_frozen_round_trips_through_to_dict_and_is_omitted_when_unset():
    plain = VariantSet("a", {"Code:f": "v1"})
    frozen = VariantSet(None, {}, frozen={"Code:f": ["v1"]}, follow_latest=True)

    assert plain.to_dict() == {"name": "a", "selection": {"Code:f": "v1"}, "variable": None}
    assert VariantSet.from_dict(frozen.to_dict()) == frozen


# --- export loads what the preview draws -------------------------------------


def test_export_selection_matches_the_preview_rule():
    pin = {"latest_column": LATEST, "default_pin": {LATEST: True}}
    assert export_selection(VariantSet(), **pin) == {LATEST: True}
    assert export_selection(VariantSet(None, {"f.low_hz": "20"}), **pin) == {
        LATEST: True,
        "f.low_hz": "20",
    }
    assert export_selection(VariantSet(None, {"Code:f": "v1"}), **pin) == {"Code:f": "v1"}
    assert export_selection(
        VariantSet(None, {LATEST: True}, frozen={"Code:f": ["v1"]}), **pin
    ) == {"Code:f": "v1"}


def test_export_selection_adds_nothing_when_the_table_has_no_default():
    """A stacked multi-variable table has no default pin; the preview draws
    its rows as written, so the export must too (it once added
    code_version='latest' here — test_variant_rows_multivariable caught it)."""
    assert export_selection(VariantSet(), latest_column=LATEST, default_pin=None) == {}
    assert export_selection(
        VariantSet(None, {}, variable="Scaled"), latest_column=LATEST, default_pin=None
    ) == {}


# --- one variant: no variant in the title ------------------------------------


def test_a_single_variant_is_not_in_the_title():
    table = _rerun_table()
    spec = _spec(
        VariantSet(None, {"Code:f": "v2"}),
        roles={VARIANT_FACTOR: Role.ITERATE, "subject": Role.GROUP},
    )

    figures = resolve(spec, table)

    assert len(figures) == 1
    assert "Variant=" not in (figures[0].labels.title or "")
    assert VARIANT_FACTOR in figures[0].figure_key, "file names still use it"


def test_two_iterated_variants_keep_the_variant_in_each_title():
    table = _rerun_table()
    spec = _spec(
        VariantSet("old", {"Code:f": "v1"}),
        VariantSet("new", {"Code:f": "v2"}),
        roles={VARIANT_FACTOR: Role.ITERATE, "subject": Role.GROUP},
    )

    titles = [figure.labels.title for figure in resolve(spec, table)]

    assert titles == ["Variant=old", "Variant=new"]
