"""ScidbSource: loading, shapes, ordering, joins, and the variant trap."""

from __future__ import annotations

import pytest
from scistackplot import PlotKind, PlotSpec, Role, RoleError, Shape, resolve, validate

from scistackplotdb import ScidbSource, join_kind, joinable, load_variable

from conftest import SUBJECTS


# --- loading ---------------------------------------------------------------


def test_load_variable_returns_long_rows(seeded):
    loaded = load_variable(seeded, "StepLength")

    assert len(loaded.frame) == 3 * 2 * 2
    assert loaded.levels == ["subject", "session", "trial"]
    assert set(loaded.frame.columns) >= {"subject", "session", "trial"}


def test_subject_level_variable_reports_shallower_levels(seeded):
    assert load_variable(seeded, "Mass").levels == ["subject"]


def test_shapes_are_classified_from_values(seeded):
    source = ScidbSource(seeded)
    described = {m["name"]: m["shape"] for m in source.describe()["measures"]}

    assert described["StepLength"] == str(Shape.SCALAR)
    assert described["Signal"] == str(Shape.SERIES_1D)


# --- ordering (the zero-padded trap) ---------------------------------------


def test_factor_levels_use_declared_order_not_lexicographic(seeded):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])

    assert table.factor("subject").levels == SUBJECTS


def test_wide_subject_ordering_is_numeric(db):
    """Ten subjects: lexicographic order would put '10' second."""
    from conftest import Mass

    for n in range(1, 11):
        Mass.save(float(n), subject=f"{n:02d}")

    table = ScidbSource(db).get_table(["Mass"])
    assert table.factor("subject").levels == [f"{n:02d}" for n in range(1, 11)]


# --- joins across schema depth ---------------------------------------------


def test_join_kind_classifies_prefixes():
    assert join_kind(["subject"], ["subject"]) == "identical"
    assert join_kind(["subject"], ["subject", "trial"]) == "broadcast"
    assert join_kind(["subject", "trial"], ["subject"]) == "broadcast"
    assert join_kind(["session"], ["subject", "trial"]) == "incompatible"
    assert joinable(["subject"], ["subject", "session", "trial"])


def test_shallow_variable_broadcasts_across_deeper_rows(seeded):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"], x_measure="Mass")

    # One subject-level Mass value reused across that subject's 4 trial rows.
    assert len(table.frame) == 12
    assert table.frame.groupby("subject")["Mass"].nunique().max() == 1


def test_join_keeps_both_measures_when_data_columns_share_a_name(seeded):
    """
    Two variables' data columns routinely have the SAME name ("value" is the
    default), so the join has to rename before merging — renaming afterwards
    lets one key shadow the other and the first measure's column disappears.
    """
    table = ScidbSource(seeded).get_table(["StepLength"], x_measure="Mass")

    assert {"StepLength", "Mass"} <= set(table.frame.columns)
    assert table.measure_names == ["StepLength", "Mass"]


def test_unknown_variable_raises_key_error(seeded):
    with pytest.raises(KeyError, match="Unknown variable"):
        ScidbSource(seeded).get_table(["NoSuchVariable"])


def test_joinable_with_lists_only_scalar_partners(seeded):
    partners = ScidbSource(seeded).joinable_with("StepLength")
    assert "Mass" in partners
    assert "Signal" not in partners  # 1-D cannot supply an x axis


# --- end-to-end resolution against real data -------------------------------


def test_scalar_variable_resolves_to_a_box_plot(seeded):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X, "subject": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    resolved = resolve(spec, table)[0]
    assert resolved.row_count == 12
    assert resolved.x_order == ["pre", "post"] or resolved.x_order == ["post", "pre"]


def test_1d_variable_explodes_into_samples(seeded):
    source = ScidbSource(seeded)
    table = source.get_table(["Signal"])
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.COLOR, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BAND,
    )

    resolved = resolve(spec, table)[0]
    # 8 samples x 2 sessions after summarizing across subjects and trials.
    assert resolved.row_count == 16


# --- dict/struct variables -------------------------------------------------


def test_struct_fields_become_a_factor(seeded):
    """
    A dict-valued variable is stored one column per key. Those columns are
    parallel quantities, so they melt into a field factor rather than the
    plot silently showing whichever column happened to come first.
    """
    table = ScidbSource(seeded).get_table(["Emg"])

    fields = table.field_factors
    assert len(fields) == 1
    assert set(fields[0].levels) == {"RHAM", "RTA", "LMG"}
    # 3 subjects x 2 sessions x 2 trials x 3 muscles
    assert len(table.frame) == 36
    assert table.measure_names == ["Emg"]


def test_struct_defaults_to_one_subplot_per_field(seeded):
    from scistackplot import default_spec

    table = ScidbSource(seeded).get_table(["Emg"])
    spec = default_spec(table, "Emg")
    resolved = resolve(spec, table)[0]

    assert len(resolved.panels) == 3
    assert {p.title for p in resolved.panels} == {"RHAM", "RTA", "LMG"}


def test_struct_shape_is_classified_from_a_field(seeded):
    table = ScidbSource(seeded).get_table(["Emg"])
    assert table.shape_of("Emg") is Shape.SERIES_1D


def test_struct_cannot_be_paired_with_an_x_measure(seeded):
    """PAIRING is the x-axis join: one x value per y, so a dict variable with
    13 fields has no single number to pair each x with."""
    with pytest.raises(ValueError, match="no single value to pair"):
        ScidbSource(seeded).get_table(["Emg"], x_measure="Mass")


def test_struct_cannot_stack_with_a_plain_value(seeded):
    """STACKING is the other operation, and it fails for its own reason: there
    is no correspondence between one number and a set of fields.

    (Two dict variables that SHARE fields do stack — see
    test_variant_rows_multivariable.py.)
    """
    with pytest.raises(ValueError, match="single value"):
        ScidbSource(seeded).get_table(["Emg", "Mass"])


def test_struct_is_not_offered_as_an_x_axis(seeded):
    assert "Emg" not in ScidbSource(seeded).joinable_with("StepLength")


# --- variants --------------------------------------------------------------


@pytest.fixture
def two_variants(seeded):
    """Run one step twice with different constants, producing two variants."""
    import numpy as np
    from scidb import for_each

    from conftest import Scaled, Signal

    def scale_signal(signal, factor):
        return float(np.mean(signal) * factor)

    for factor in (2, 3):
        for_each(
            scale_signal,
            inputs={"signal": Signal, "factor": factor},
            outputs=[Scaled],
            subject=[],
            session=[],
            trial=[],
        )
    return seeded


def test_variants_arrive_as_factor_columns(two_variants):
    loaded = load_variable(two_variants, "Scaled")

    assert loaded.variant_columns, "branch params should become columns"
    # Two records per schema combination — one per variant.
    assert len(loaded.frame) == 2 * 3 * 2 * 2


def test_unassigned_variant_is_refused_not_pooled(two_variants):
    """The correctness trap: two pipelines' results must not overplot as one."""
    table = ScidbSource(two_variants).get_table(["Scaled"])
    spec = PlotSpec(
        measures=["Scaled"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, table)


def test_variant_assigned_to_colour_resolves(two_variants):
    table = ScidbSource(two_variants).get_table(["Scaled"])
    variant = table.variant_factors[0].name
    spec = PlotSpec(
        measures=["Scaled"],
        roles={
            "session": Role.X,
            variant: Role.COLOR,
            "subject": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BOX,
    )

    resolved = resolve(spec, table)[0]
    assert len(resolved.color_order) == 2


# --- function-version variants ---------------------------------------------
#
# Two records produced by different versions of ONE function's source carry
# identical branch params, so before `fn_version` they reached the plot layer
# indistinguishable and were overplotted as replicates — the variant trap
# above, reached by the one route it did not cover.
# See docs/claude/function-version-variants.md.


@pytest.fixture
def two_code_versions(seeded):
    """Run one step twice with the same constants but a DIFFERENT body.

    ``__name__`` is what lands in ``_invocation.function_name`` while
    ``function_hash`` is an AST hash of the source, so this models "the user
    edited the function and hit Run" rather than "the user swept a constant".
    """
    import numpy as np
    from scidb import for_each

    from conftest import Scaled, Signal

    def scale_v1(signal):
        return float(np.mean(signal) * 2)

    def scale_v2(signal):
        return float(np.mean(signal) * 2 + 1)

    for body in (scale_v1, scale_v2):
        body.__name__ = "scale_signal"
        for_each(
            body,
            inputs={"signal": Signal},
            outputs=[Scaled],
            subject=[],
            session=[],
            trial=[],
        )
    return seeded


def test_code_version_becomes_a_variant_column(two_code_versions):
    loaded = load_variable(two_code_versions, "Scaled")

    assert "Code:scale_signal" in loaded.frame.columns
    assert "Code:scale_signal" in loaded.variant_columns, (
        "it must be a VARIANT column, not a plain one — that is what arms the "
        "pooling guard"
    )
    assert set(loaded.frame["Code:scale_signal"]) == {"v1", "v2"}
    assert len(loaded.frame) == 2 * 3 * 2 * 2


def test_single_code_version_attaches_no_column(two_variants):
    """The ordinary case stays exactly as it was: constants only, no version."""
    loaded = load_variable(two_variants, "Scaled")

    assert "Code:scale_signal" not in loaded.frame.columns
    assert all(not c.startswith("Code:") for c in loaded.variant_columns)


def test_two_code_versions_are_refused_not_pooled(two_code_versions):
    """The reported bug: these two silently overplotted as one line.

    The code axis is left UNASSIGNED, which is what makes the pooling silent —
    since Stage 8 the refusal is specifically about roles nobody chose, and
    naming it FREE here would be asking for the pooling this guards against.
    """
    table = ScidbSource(two_code_versions).get_table(["Scaled"])
    spec = PlotSpec(
        measures=["Scaled"],
        roles={
            "session": Role.X,
            "subject": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BOX,
    )

    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, table)


def test_code_version_assigned_to_colour_resolves(two_code_versions):
    table = ScidbSource(two_code_versions).get_table(["Scaled"])
    spec = PlotSpec(
        measures=["Scaled"],
        roles={
            "session": Role.X,
            "Code:scale_signal": Role.COLOR,
            "subject": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BOX,
    )

    resolved = resolve(spec, table)[0]
    assert len(resolved.color_order) == 2


@pytest.fixture
def constants_and_code_versions(seeded):
    """Two constants CROSSED with two function bodies — four records per combo,
    giving two genuinely multi-level variant factors at once."""
    import numpy as np
    from scidb import for_each

    from conftest import Scaled, Signal

    def scale_v1(signal, factor):
        return float(np.mean(signal) * factor)

    def scale_v2(signal, factor):
        return float(np.mean(signal) * factor + 1)

    for body in (scale_v1, scale_v2):
        body.__name__ = "scale_signal"
        for factor in (2, 3):
            for_each(
                body,
                inputs={"signal": Signal, "factor": factor},
                outputs=[Scaled],
                subject=[],
                session=[],
                trial=[],
            )
    return seeded


def test_default_roles_separate_both_variant_kinds(constants_and_code_versions):
    """A constant sweep AND a code edit on one variable must both stay visible.

    default_roles used to hand extra variants Role.FREE, which validate then
    refused — so the user got an error instead of a figure. Adding the code
    version is what made a two-variant table common enough to hit.
    """
    from scistackplot import default_roles

    table = ScidbSource(constants_and_code_versions).get_table(["Scaled"])
    multi = [f for f in table.variant_factors if len(f.levels) > 1]
    assert len(multi) == 2, "a constant variant and a code version, both live"

    roles = default_roles(table, "Scaled")
    assert Role.FREE not in {roles[f.name] for f in multi}, (
        "neither variant may be left pooled"
    )

    spec = PlotSpec(
        measures=["Scaled"], roles=roles, kind=PlotKind.BOX
    )
    validate(spec, table)  # must not raise


# --- pinning the current code version (Stage 4) -----------------------------


def _rows(resolved):
    """All of a resolved figure's rows as one frame.

    Panels carry the CANONICAL frame (``__x``/``__y``/``__color``), not the
    original factor columns — ``reduce`` projects them away. So assertions here
    go through ``resolved.encoding`` to name the column they want rather than
    reaching for ``subject`` or ``Code:scale_signal``, which are gone by this point.
    """
    import pandas as pd

    return pd.concat([p.frame for p in resolved.panels], ignore_index=True)


def test_table_carries_a_default_pin_for_the_latest_code(two_code_versions):
    table = ScidbSource(two_code_versions).get_table(["Scaled"])

    assert table.default_pin == {"CodeIsLatest": True}


def test_no_default_pin_without_code_versions(two_variants):
    """``default_pin`` is the SOURCE's opinion about which rows are *current*,
    and with no code versions it has none — so it stays None.

    It is no longer the whole opening selection. ``default_spec`` pins every
    variant axis on top of this (``variants.default_selection``), so a
    constants-only variable still opens on ONE of its variants; see
    ``test_default_spec_pins_a_swept_param_with_no_code_versions``. Keeping the
    two separate is the point: only scidb can say which records are current,
    while "which parameter value do we open on" is a plotting decision.
    """
    table = ScidbSource(two_variants).get_table(["Scaled"])

    assert table.default_pin is None


def test_default_spec_opens_on_one_named_variant(two_code_versions):
    """The panel always has a row to edit, and it starts on current results."""
    from scistackplot import apply_variant_sets, default_spec
    from scistackplot.variants import set_name

    table = ScidbSource(two_code_versions).get_table(["Scaled"])
    spec = default_spec(table, "Scaled")

    assert len(spec.variant_sets) == 1
    row = spec.variant_sets[0]
    assert row.selection == {"CodeIsLatest": True}
    # No hardcoded name any more: the row is named for what it plots, with the
    # latest FLAG spelled as "current" rather than as `CodeIsLatest=True`.
    assert row.name is None
    assert (
        set_name(row, 0, primary="Scaled", latest_column=table.latest_column)
        == "Scaled · current"
    )
    validate(spec, apply_variant_sets(spec, table))  # must not raise


def test_default_spec_pins_a_swept_param_with_no_code_versions(two_variants):
    """A constants-only variable opens on ONE of its variants.

    ``two_variants`` ran ``scale_signal`` at ``factor`` 2 and 3, so there are two
    records per location and no code edit anywhere. The source has no opinion
    about which is current (``default_pin is None``), but the panel must still
    open on one line rather than two: the second record is a different pipeline
    variant, and drawing both unasked makes them look like replicates.

    This is the case the old rule missed entirely — it pinned only code axes, so
    a swept parameter reached ``default_roles`` as an unanswered multi-level
    variant factor and became COLOUR.
    """
    from scistackplot import apply_variant_sets, default_spec

    table = ScidbSource(two_variants).get_table(["Scaled"])
    spec = default_spec(table, "Scaled")

    assert len(spec.variant_sets) == 1
    assert spec.variant_sets[0].selection == {"scale_signal.factor": "2"}
    assert spec.roles.get("scale_signal.factor") is None

    derived = apply_variant_sets(spec, table)
    assert "scale_signal.factor" not in derived.factor_names
    validate(spec, derived)  # must not raise

    # One record per location, not two.
    assert resolve(spec, table)[0].row_count == 3 * 2 * 2


def test_pinned_render_keeps_only_the_newest_rows(two_code_versions):
    from scistackplot import default_spec

    source = ScidbSource(two_code_versions)
    table = source.get_table(["Scaled"])
    spec = default_spec(table, "Scaled")

    resolved = resolve(spec, table)[0]
    # 3 subjects x 2 sessions x 2 trials, one row each — not two.
    assert resolved.row_count == 3 * 2 * 2

    # The code version is no longer an encoding channel: the "current" variant
    # answers that axis, so it leaves the factor list rather than being colour
    # with a single level (the 2026-09-09 report). What it must still be is
    # RIGHT — the VALUES have to be the new code's, not the old one's. scale_v2
    # adds 1 to scale_v1, so this fails loudly if the wrong record survived,
    # which is exactly the originally reported bug.
    rows = _rows(resolved)
    expected = table.frame[table.frame["Code:scale_signal"] == "v2"]["Scaled"]
    assert sorted(round(v, 9) for v in rows[resolved.encoding.y]) == sorted(
        round(v, 9) for v in expected
    )


def test_clearing_the_variant_brings_every_version_back(two_code_versions):
    """The opening variant is a starting point, not a lock — the old records are
    still there and deleting the row plots them."""
    from dataclasses import replace

    from scistackplot import default_spec

    table = ScidbSource(two_code_versions).get_table(["Scaled"])
    spec = default_spec(table, "Scaled")

    # Deleting the row brings the code axis back as an ordinary factor needing a
    # role. `session` has to give up colour to take it: with the variant
    # answering the code axis, the opening defaults put session there, and two
    # factors cannot share one channel.
    unpinned = replace(
        spec,
        variant_sets=[],
        roles={**spec.roles, "session": Role.FREE, "Code:scale_signal": Role.COLOR},
    )
    resolved = resolve(unpinned, table)[0]

    assert resolved.row_count == 2 * 3 * 2 * 2
    assert set(_rows(resolved)[resolved.encoding.color]) == {"v1", "v2"}


def test_pin_keeps_locations_never_rerun_under_the_newest_code(seeded):
    """The trap in pinning: a location the user did not re-run must NOT vanish.

    Version ordinals are numbered per function, so pinning `Code:fn == "v2"` would
    drop any subject still on v1. The pin is on the per-location `CodeIsLatest`
    flag precisely so each location contributes its own newest record.
    """
    import numpy as np
    from scidb import for_each
    from scistackplot import default_spec

    from conftest import Scaled, Signal

    def scale_v1(signal):
        return float(np.mean(signal) * 2)

    def scale_v2(signal):
        return float(np.mean(signal) * 2 + 1)

    scale_v1.__name__ = scale_v2.__name__ = "scale_signal"

    # Everyone gets v1; only subject 01 is then re-run with the edited body.
    for_each(
        scale_v1,
        inputs={"signal": Signal},
        outputs=[Scaled],
        subject=[], session=[], trial=[],
    )
    for_each(
        scale_v2,
        inputs={"signal": Signal},
        outputs=[Scaled],
        subject=[SUBJECTS[0]], session=[], trial=[],
    )

    source = ScidbSource(seeded)
    table = source.get_table(["Scaled"])
    resolved = resolve(default_spec(table, "Scaled"), table)[0]
    rows = _rows(resolved)

    # The load-bearing number. One row per location: 3 subjects x 2 sessions x
    # 2 trials. Pinning `Code:fn == "v2"` instead would have kept only
    # subject 01's four rows and quietly deleted the other two subjects.
    assert resolved.row_count == 3 * 2 * 2, (
        "pinning the latest must not delete the subjects that were never re-run"
    )
    # default_roles puts subject on x for a scalar measure.
    assert set(rows[resolved.encoding.x]) == set(SUBJECTS)

    # Subject 01 contributes the new code, the others still contribute what they
    # have. Checked against the frame rather than a colour channel: "current"
    # answers the code axis, so it is no longer an encoding — and one variant
    # legitimately spanning two ordinals, each newest at its own location, is
    # precisely what the per-location flag means.
    kept = table.frame[table.frame[table.latest_column]]
    by_subject = {
        subject: set(group["Code:scale_signal"])
        for subject, group in kept.groupby("subject")
    }
    assert by_subject[SUBJECTS[0]] == {"v2"}
    assert by_subject[SUBJECTS[1]] == {"v1"}
