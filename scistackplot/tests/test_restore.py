"""Restoring a stored spec whose shape may have drifted — salvage, never migrate.

Three guarantees (``.claude/plan-saved-plots.md`` Stage 1):

* **Coverage.** Every field of the spec, however deeply nested, survives
  ``to_dict -> restore_spec`` unchanged. The fixture sets every field to a
  non-default value and a walker checks that it did, so a field added to the
  spec fails here until the fixture — and the round trip — covers it.
* **Drift never raises.** Unknown keys, bad values, old vocabularies and wrong
  types each become a note and a default; everything else is kept.
* **One re-save clears the notes.** A restored spec, saved again, restores
  with no notes and carries no key the spec does not have.
"""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass, MISSING

import pytest

from scistackplot import (
    Aggregation,
    Alias,
    ErrorBand,
    FacetOptions,
    FactorVariable,
    Filter,
    LegacySpecError,
    LevelGroup,
    LocationFilter,
    Matcher,
    MatchOp,
    NoteKind,
    PlotKind,
    PlotSpec,
    RestoreError,
    Role,
    Statistic,
    StyleOptions,
    TextSizes,
    VariantSet,
    YAxis,
    reconcile,
    restore_spec,
)
from scistackplot.variants import stale_role_names, strip_answered_roles


def full_spec() -> PlotSpec:
    """Every field of every nested dataclass at a NON-default value."""
    return PlotSpec(
        measures=["StepLength"],
        x_measure="Cadence",
        roles={
            "session": Role.GROUP,
            "limb": Role.GROUP,
            "subject": Role.COLLAPSE,
            "trial": Role.FACET,
        },
        groups=["limb", "session"],
        color="limb",
        kind=PlotKind.BOX,
        aggregate=Aggregation(
            statistic=Statistic.MEDIAN, error=ErrorBand.SEM, pooled=True
        ),
        cell_statistic=Statistic.MEDIAN,
        index_column="time",
        show_sample=["trial"],
        join_sample=True,
        sample_color="subject",
        sample_in_legend=False,
        facet=FacetOptions(
            n_rows=2,
            n_cols=3,
            rows=[Matcher(op=MatchOp.STARTS_WITH, value="R", label="Right")],
            cols=[Matcher(op=MatchOp.REGEX, value="TA$", label="Tib")],
            share_x=False,
        ),
        y_axis=YAxis(scope=["subject"], minimum=0.0, maximum=2.5),
        style=StyleOptions(
            palette="viridis",
            width=7.2,
            height=4.5,
            text=TextSizes(
                base=10.0,
                title=16.0,
                x_label=11.0,
                y_label=12.0,
                x_ticks=9.0,
                y_ticks=8.5,
                groups=7.5,
                legend=9.5,
                legend_title=10.5,
            ),
            log_x=True,
            log_y=True,
            title="Step length",
            x_label="Session",
            y_label="m",
            marker_size=12.0,
            alpha=0.5,
            hide_legend_ticks=True,
            tick_rotation=45,
            tick_every=2,
        ),
        aliases={"session": Alias(name="Visit", levels={"01": "One"})},
        filters=[
            Filter(
                column="subject",
                include=["01", "02"],
                exclude=["03"],
                minimum=0.5,
                maximum=1.5,
            )
        ],
        location_filter=LocationFilter(
            keys=["subject", "session"],
            include=[[["subject", "01"]], [["subject", "02"], ["session", "pre"]]],
            exclude_levels={"session": ["post"]},
        ),
        factor_variables=[
            FactorVariable("Demographics", "Sex", {"CodeIsLatest": True})
        ],
        level_groups=[
            LevelGroup("Phase", "session", {"pre": "base", "post": "after"}, "other")
        ],
        variant_sets=[
            VariantSet(
                name="baseline",
                selection={"Code:bandpass": "v1", "bandpass.low_hz": ["20", "50"]},
                variable="Filtered",
            )
        ],
    )


def _defaults_left(obj, path: str = "") -> list[str]:
    """Paths of fields in *obj* (recursively) still at their default."""
    left = []
    for f in fields(obj):
        value = getattr(obj, f.name)
        sub = f"{path}.{f.name}" if path else f.name
        if f.default is not MISSING:
            if value == f.default:
                left.append(sub)
        elif f.default_factory is not MISSING:
            if value == f.default_factory():
                left.append(sub)
        if is_dataclass(value):
            left.extend(_defaults_left(value, sub))
        elif isinstance(value, list) and value and is_dataclass(value[0]):
            left.extend(_defaults_left(value[0], f"{sub}[0]"))
    return left


def _through_json(spec: PlotSpec) -> dict:
    return json.loads(spec.to_json())


def _paths(notes, kind=None) -> list[str]:
    return [n.path for n in notes if kind is None or n.kind is kind]


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


def test_fixture_sets_every_field_to_a_non_default_value():
    # A new PlotSpec field fails HERE first: give it a non-default value in
    # full_spec(), and the round trip below then proves to_dict/restore carry it.
    assert _defaults_left(full_spec()) == []


def test_every_field_round_trips_with_no_notes():
    spec = full_spec()
    restored = restore_spec(_through_json(spec))
    assert restored.notes == []
    assert restored.spec == spec


def test_restore_agrees_with_strict_from_dict_on_a_current_spec():
    raw = _through_json(full_spec())
    assert restore_spec(raw).spec == PlotSpec.from_dict(raw)


# ---------------------------------------------------------------------------
# Drift
# ---------------------------------------------------------------------------


def test_unknown_top_level_setting_is_ignored_with_a_note():
    raw = _through_json(full_spec())
    raw["smoothing"] = 3
    restored = restore_spec(raw)
    assert _paths(restored.notes, NoteKind.UNKNOWN_SETTING) == ["smoothing"]
    assert restored.spec == full_spec()


def test_unknown_style_key_keeps_the_rest_of_style():
    raw = _through_json(full_spec())
    raw["style"]["widht"] = 3
    with pytest.raises(TypeError):
        PlotSpec.from_dict(raw)  # the strict reader: one bad key loses the lot
    restored = restore_spec(raw)
    assert _paths(restored.notes) == ["style.widht"]
    assert restored.spec.style == full_spec().style


def test_pre_text_sizes_font_keys_revert_with_a_note_each():
    """`font_size` / `tick_font_size` became `style.text` (2026-09-24), a clean
    break: a plot saved before then opens with default sizes, is told which
    keys were dropped, and keeps every other style setting."""
    raw = _through_json(full_spec())
    del raw["style"]["text"]
    raw["style"]["font_size"] = 18.0
    raw["style"]["tick_font_size"] = 9.0
    restored = restore_spec(raw)
    assert sorted(_paths(restored.notes, NoteKind.UNKNOWN_SETTING)) == [
        "style.font_size",
        "style.tick_font_size",
    ]
    assert restored.spec.style.text == TextSizes()
    assert restored.spec.style.width == full_spec().style.width


def test_a_bad_text_size_defaults_only_that_size():
    raw = _through_json(full_spec())
    raw["style"]["text"]["legend"] = "huge"
    restored = restore_spec(raw)
    assert _paths(restored.notes) == ["style.text.legend"]
    assert restored.spec.style.text.legend is None
    assert restored.spec.style.text.title == full_spec().style.text.title


def test_a_setting_moved_out_of_a_block_is_reported_by_its_old_path():
    raw = _through_json(full_spec())
    raw["facet"]["share_y"] = False  # moved to y_axis on 2026-09-11
    restored = restore_spec(raw)
    assert _paths(restored.notes, NoteKind.UNKNOWN_SETTING) == ["facet.share_y"]
    assert restored.spec.facet == full_spec().facet


def test_invalid_enum_value_falls_back_to_the_default():
    raw = _through_json(full_spec())
    raw["kind"] = "pie"
    restored = restore_spec(raw)
    assert _paths(restored.notes, NoteKind.INVALID_VALUE) == ["kind"]
    assert restored.spec.kind is PlotKind.SCATTER
    assert "'pie'" in restored.notes[0].message


def test_nested_invalid_value_defaults_only_that_value():
    raw = _through_json(full_spec())
    raw["facet"]["rows"][0]["op"] = "fuzzy"
    restored = restore_spec(raw)
    assert _paths(restored.notes) == ["facet.rows[0].op"]
    row = restored.spec.facet.rows[0]
    assert row.op is MatchOp.CONTAINS
    assert (row.value, row.label) == ("R", "Right")


def test_wrong_scalar_types():
    raw = _through_json(full_spec())
    raw["style"]["width"] = "wide"
    raw["style"]["tick_rotation"] = 45.0  # integral float: not a problem
    raw["sample_in_legend"] = "no"
    restored = restore_spec(raw)
    assert sorted(_paths(restored.notes, NoteKind.INVALID_VALUE)) == [
        "sample_in_legend",
        "style.width",
    ]
    assert restored.spec.style.width == 8.0
    assert restored.spec.style.tick_rotation == 45
    assert restored.spec.sample_in_legend is True


def test_old_role_vocabulary_is_salvaged_not_refused():
    raw = _through_json(full_spec())
    raw["roles"] = {"subject": "x", "trial": "collapse"}
    raw["x_layers"] = ["session"]
    raw["collapse_statistic"] = "mean"
    with pytest.raises(LegacySpecError):
        PlotSpec.from_dict(raw)
    restored = restore_spec(raw)
    assert restored.spec.roles == {"trial": Role.COLLAPSE}
    assert _paths(restored.notes, NoteKind.DROPPED_ENTRY) == ["roles.subject"]
    assert sorted(_paths(restored.notes, NoteKind.UNKNOWN_SETTING)) == [
        "collapse_statistic",
        "x_layers",
    ]


def test_list_entries_are_salvaged_one_by_one():
    raw = _through_json(full_spec())
    raw["filters"] = [
        {"column": "subject", "include": ["01"], "regex": "^0"},  # unknown key
        {"include": ["02"]},  # no column: cannot be a filter
    ]
    raw["factor_variables"] = ["Demographics", {"variable": "Condition"}]
    restored = restore_spec(raw)
    assert restored.spec.filters == [Filter(column="subject", include=["01"])]
    assert restored.spec.factor_variables == [FactorVariable("Condition")]
    assert sorted(_paths(restored.notes)) == [
        "factor_variables[0]",
        "filters[0].regex",
        "filters[1]",
    ]


def test_a_dropped_entry_reports_once_not_per_field():
    raw = _through_json(full_spec())
    raw["level_groups"] = [{"source": "session", "bogus": 1, "unmatched": 5}]
    restored = restore_spec(raw)
    assert restored.spec.level_groups == []
    assert _paths(restored.notes) == ["level_groups[0]"]


def test_missing_measure_uses_the_fallback():
    raw = _through_json(full_spec())
    del raw["measures"]
    with pytest.raises(RestoreError):
        restore_spec(raw)
    restored = restore_spec(raw, fallback_measure="StepLength")
    assert restored.spec.measures == ["StepLength"]
    assert _paths(restored.notes, NoteKind.MEASURE_REPLACED) == ["measures"]


def test_non_table_spec_opens_on_defaults_with_a_fallback():
    with pytest.raises(RestoreError):
        restore_spec(["not", "a", "spec"])
    restored = restore_spec(["not", "a", "spec"], fallback_measure="StepLength")
    assert restored.spec == PlotSpec(measures=["StepLength"])
    assert restored.notes


@pytest.mark.parametrize(
    "garbage", [None, 42, "text", [1, 2], {"?": 1}, True, 1.5]
)
def test_no_field_can_make_restore_raise(garbage):
    base = _through_json(full_spec())
    for name in base:
        if name == "measures":
            continue
        raw = dict(base, **{name: garbage})
        restored = restore_spec(raw)
        # Whatever came back, the strict reader accepts it: restore never
        # produces a spec the rest of the package would refuse to read.
        assert PlotSpec.from_dict(restored.spec.to_dict()) == restored.spec


# ---------------------------------------------------------------------------
# Re-save clears the notes
# ---------------------------------------------------------------------------


def test_one_resave_writes_only_the_current_shape():
    raw = _through_json(full_spec())
    raw["facet"]["share_y"] = True
    raw["x_layers"] = ["session"]
    raw["style"]["widht"] = 3
    raw["kind"] = "pie"
    first = restore_spec(raw)
    assert first.notes

    resaved = _through_json(first.spec)
    second = restore_spec(resaved)
    assert second.notes == []
    assert second.spec == first.spec
    spec_fields = {f.name for f in fields(PlotSpec)}
    assert set(resaved) <= spec_fields
    assert "share_y" not in resaved["facet"]
    assert "widht" not in resaved["style"]


# ---------------------------------------------------------------------------
# Reconcile against today's data
# ---------------------------------------------------------------------------


def _scalar_spec(**changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BOX,
    )
    base.update(changes)
    return PlotSpec(**base)


def test_reconcile_clean_spec_has_no_notes(scalar_table):
    spec = _scalar_spec(show_sample=["trial"], sample_color="subject")
    result = reconcile(spec, scalar_table)
    assert result.notes == []
    assert result.spec == spec


def test_reconcile_reports_a_missing_factors_role_but_keeps_it(scalar_table):
    spec = _scalar_spec(roles={"session": Role.GROUP, "limb": Role.FACET})
    result = reconcile(spec, scalar_table)
    assert _paths(result.notes, NoteKind.NOT_IN_DATA) == ["roles.limb"]
    # strip_answered_roles owns dropping it at resolve; if `limb` comes back,
    # so does its role.
    assert result.spec.roles == spec.roles


def test_reconcile_drops_shown_sample_keys_and_colour_the_data_lacks(scalar_table):
    spec = _scalar_spec(show_sample=["trial", "cycle"], sample_color="cycle")
    result = reconcile(spec, scalar_table)
    assert result.spec.show_sample == ["trial"]
    assert result.spec.sample_color is None
    assert sorted(_paths(result.notes)) == ["sample_color", "show_sample"]


def test_reconcile_reports_a_missing_measure(scalar_table):
    result = reconcile(_scalar_spec(measures=["Cadence"]), scalar_table)
    assert _paths(result.notes, NoteKind.NOT_IN_DATA) == ["measures"]


def test_reconcile_counts_derived_factors_as_present(scalar_table):
    spec = _scalar_spec(
        roles={"Phase": Role.FACET, "subject": Role.COLLAPSE},
        level_groups=[LevelGroup("Phase", "session", {"pre": "a", "post": "b"})],
    )
    assert reconcile(spec, scalar_table).notes == []


def test_stale_role_names_is_what_strip_answered_roles_drops(scalar_table):
    spec = _scalar_spec(roles={"session": Role.GROUP, "limb": Role.FACET})
    answered, missing = stale_role_names(spec, scalar_table, scalar_table)
    stripped = strip_answered_roles(spec, scalar_table, scalar_table)
    assert set(spec.roles) - set(stripped.roles) == set(answered) | set(missing)
    assert missing == ["limb"]
