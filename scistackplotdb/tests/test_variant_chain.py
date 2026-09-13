"""
Multi-layer variant identity — what a variable knows about code changes that
happened UPSTREAM of it.

Code versions used to be a **one-hop** discriminator: derived from the immediate
producing invocation only, while ``branch_params`` walked the whole chain. Two
records that differed solely by the version of some *upstream* function reached
the display layer indistinguishable and overplotted as replicates.

``code_versions_batch`` closes that — one ``Code:<fn>`` column per upstream
function holding more than one version. These tests were written first as
``xfail`` characterisations of the bug (2026-09-08) and flipped when Stage 1/2
landed; the "scenario is real" half stays because it is what gives the rest of
the file meaning.

See docs/claude/variant-selection.md §2.
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb import for_each
from scistackplot import PlotKind, PlotSpec, Role, RoleError, validate

from scistackplotdb import ScidbSource, load_variable

from conftest import Scaled, Signal, Summarized


@pytest.fixture
def upstream_code_versions(seeded):
    """Edit an UPSTREAM body, leave the downstream one alone, re-run both.

    The sequence matters and a shorter one does not reproduce the bug. Running
    the second layer only once is not enough: it consumes ``Scaled`` through the
    load path, which collapses to the latest record per variant group, so it
    would see one input and write one output. Two ``Summarized`` records only
    coexist once the whole pipeline is re-run after the edit — which is exactly
    what "the user edited a loader and hit Run" does.

    ``__name__`` is pinned across both bodies so this models a body *edit*
    (``function_name`` stable, ``function_hash`` different) rather than two
    differently-named functions.
    """

    def scale_v1(signal):
        return float(np.mean(signal) * 2)

    def scale_v2(signal):
        return float(np.mean(signal) * 2 + 1)

    def summarize(scaled):
        # NEVER edited. Its hash is identical in both passes, which is the
        # whole point: the only thing distinguishing the two Summarized records
        # is an input produced by code that changed.
        return float(scaled) * 10

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
        for_each(
            summarize,
            inputs={"scaled": Scaled},
            outputs=[Summarized],
            subject=[],
            session=[],
            trial=[],
        )
    return seeded


# --- the scenario is real -------------------------------------------------


def test_upstream_edit_produces_two_downstream_records(upstream_code_versions):
    """Precondition. Without this the rest of the file proves nothing."""
    loaded = load_variable(upstream_code_versions, "Summarized")

    # 3 subjects x 2 sessions x 2 trials = 12 locations, two records each.
    assert len(loaded.frame) == 2 * 3 * 2 * 2


def test_the_upstream_variable_itself_does_distinguish_them(upstream_code_versions):
    """One hop away it works — this is the 2026-09-06 fix, still holding."""
    loaded = load_variable(upstream_code_versions, "Scaled")

    assert "Code:scale_signal" in loaded.variant_columns
    assert set(loaded.frame["Code:scale_signal"]) == {"v1", "v2"}


def test_downstream_records_share_one_producing_hash(upstream_code_versions):
    """Why the discriminator goes missing: ``summarize`` was never edited, so
    the immediate producing invocation is the same for both records."""
    from scidb.provenance_query import producing_function_versions_batch

    loaded = load_variable(upstream_code_versions, "Summarized")
    versions = producing_function_versions_batch(
        upstream_code_versions._duck, loaded.frame["record_id"].tolist()
    )

    hashes = {info["fn_hash"] for info in versions.values()}
    assert len(hashes) == 1, (
        "the downstream function was not edited, so one hash is correct here — "
        "the bug is that nothing else is consulted"
    )


# --- what we actually want ------------------------------------------------


def test_upstream_code_version_reaches_the_downstream_table(upstream_code_versions):
    """The two Summarized records must be distinguishable by something."""
    loaded = load_variable(upstream_code_versions, "Summarized")

    assert "Code:scale_signal" in loaded.variant_columns, (
        "two records per location differ only by the code that produced their "
        "input; with no variant column they are replicates to every consumer"
    )
    assert set(loaded.frame["Code:scale_signal"]) == {"v1", "v2"}


def test_the_unedited_producer_contributes_no_column(upstream_code_versions):
    """``summarize`` was never edited, so it is not an axis and must not add a
    column whose only level is ``v1``."""
    loaded = load_variable(upstream_code_versions, "Summarized")

    assert "Code:summarize" not in loaded.frame.columns
    assert loaded.variant_columns == ["Code:scale_signal"]


def test_latest_is_chain_wide_not_one_hop(upstream_code_versions):
    """Half the rows are current. Under the one-hop rule every row was, because
    ``summarize``'s own hash never changed — which is what let the stale half
    into the default figure."""
    loaded = load_variable(upstream_code_versions, "Summarized")

    current = loaded.frame[loaded.frame[loaded.latest_column]]
    assert len(current) == 3 * 2 * 2
    assert set(current["Code:scale_signal"]) == {"v2"}


def test_downstream_pooling_is_refused(upstream_code_versions):
    """The guard that exists for exactly this, previously never armed one hop out.

    Mirrors ``test_two_code_versions_are_refused_not_pooled`` in test_source.py
    — same assertion, one layer downstream. The spec deliberately leaves the code
    column unassigned so it defaults to FREE, which is what pooling means here.
    """
    table = ScidbSource(upstream_code_versions).get_table(["Summarized"])
    spec = PlotSpec(
        measures=["Summarized"],
        roles={
            "session": Role.X,
            "subject": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BOX,
    )

    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, table)


# --- the picker's data model ----------------------------------------------
#
# Axis origin travels WITH the axis so nothing downstream has to parse
# "Code:scale_signal" or "scale_signal.factor" back into a function name. Those
# are scidb's namespacing conventions; a GUI re-deriving them by splitting
# strings would be the first thing to break when they change.


def test_axes_carry_the_function_that_produced_them(upstream_code_versions):
    loaded = load_variable(upstream_code_versions, "Summarized")

    by_column = {axis["column"]: axis for axis in loaded.variant_axes}

    assert by_column["Code:scale_signal"]["kind"] == "code"
    assert by_column["Code:scale_signal"]["function"] == "scale_signal"


@pytest.fixture
def param_variants(seeded):
    """One step run at two constants — a branch-param axis, no code edit."""

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


def test_param_axes_split_the_namespaced_name(param_variants):
    loaded = load_variable(param_variants, "Scaled")

    axis = next(a for a in loaded.variant_axes if a["kind"] == "param")

    assert axis["column"] == f"{axis['function']}.{axis['param']}"
    assert axis["function"] == "scale_signal"
    assert axis["param"] == "factor"


def test_axes_reach_the_table_as_factor_origins(upstream_code_versions):
    """What `capability.variant_summary` hands the GUI."""
    table = ScidbSource(upstream_code_versions).get_table(["Summarized"])

    origin = table.factor("Code:scale_signal").origin

    assert origin["kind"] == "code"
    assert origin["function"] == "scale_signal"


def test_variant_graph_lists_levels_and_versions(upstream_code_versions):
    source = ScidbSource(upstream_code_versions)

    graph = source.variant_graph("Summarized")

    axis = next(a for a in graph["axes"] if a["column"] == "Code:scale_signal")
    assert axis["levels"] == ["v1", "v2"]
    assert [v["version"] for v in graph["versions"]["scale_signal"]] == ["v1", "v2"]


def test_variant_graph_covers_single_version_functions_in_the_chain(
    upstream_code_versions,
):
    """`summarize` was never edited, so it is not an axis — but the popup still
    has to offer its one version, or the dropdown is empty for the commonest
    case in any project."""
    graph = ScidbSource(upstream_code_versions).variant_graph("Summarized")

    assert "summarize" in graph["chain_functions"]
    assert [v["version"] for v in graph["versions"]["summarize"]] == ["v1"]
    assert not any(a["function"] == "summarize" for a in graph["axes"])


def test_variant_graph_answers_for_functions_outside_the_chain(
    upstream_code_versions,
):
    """The popup mirrors the whole canvas, so a node the plotted measure does
    not depend on still has to be able to say what it has run."""
    graph = ScidbSource(upstream_code_versions).variant_graph(
        "Scaled", ["summarize", "never_ran"]
    )

    assert [v["version"] for v in graph["versions"]["summarize"]] == ["v1"]
    assert "never_ran" not in graph["versions"]


# --- writing the same selection by hand -----------------------------------


def test_variant_set_translates_a_scidb_variant(upstream_code_versions):
    """The selector a scientist already writes in for_each, meaning the same
    thing in a figure."""
    from scidb import Variant

    from scistackplotdb import variant_set

    table = ScidbSource(upstream_code_versions).get_table(["Summarized"])

    built = variant_set("baseline", Variant(Summarized, code_version="v1"), table)

    assert built.name == "baseline"
    assert built.selection == {"Code:scale_signal": "v1"}


def test_a_bare_code_version_is_refused_when_it_is_ambiguous(
    upstream_code_versions,
):
    from scistackplotdb.variants import _bare_code_column

    with pytest.raises(ValueError, match="ambiguous"):
        _bare_code_column(
            [
                {"column": "Code:a", "kind": "code", "function": "a"},
                {"column": "Code:b", "kind": "code", "function": "b"},
            ]
        )


def test_a_named_variant_selects_the_same_rows_the_figure_will_show(
    upstream_code_versions,
):
    """The point of translating rather than inventing a second dialect: what the
    selector picks and what the figure draws are the same set of records."""
    from scidb import Variant
    from scistackplot import apply_variant_sets

    from scistackplotdb import variant_set

    source = ScidbSource(upstream_code_versions)
    table = source.get_table(["Summarized"])
    spec = PlotSpec(
        measures=["Summarized"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        variant_sets=[
            variant_set("old code", Variant(Summarized, code_version="v1"), table)
        ],
    )

    derived = apply_variant_sets(spec, table)

    assert len(derived.frame) == 3 * 2 * 2
    assert set(derived.frame["Code:scale_signal"]) == {"v1"}


# --- the inverse: asking scidb about a selection the plotting layer holds ----


class TestBranchParamsFor:
    """``selection_for`` run backwards, for the schema location picker.

    The picker holds a column-keyed selection and has to ask
    ``scidb.locations.location_states`` — which takes a ``branch_params_filter``
    — which locations have that variant. Round-tripping is the guarantee that
    the dots and a ``Variant(...).load()`` name the same records.
    """

    def test_code_columns_become_the_code_namespace(self):
        from scistackplotdb import branch_params_for

        assert branch_params_for({"Code:bandpass": "v1"}) == {"__code__.bandpass": "v1"}

    def test_branch_params_pass_through_untouched(self):
        """They are already scidb's own namespacing."""
        from scistackplotdb import branch_params_for

        assert branch_params_for({"bandpass.low_hz": 20}) == {"bandpass.low_hz": 20}

    def test_a_list_survives_as_a_list(self):
        """scidb reads a list-valued branch param as membership, so the
        picker's multi-checkbox needs no new scidb work."""
        from scistackplotdb import branch_params_for

        assert branch_params_for({"bandpass.low_hz": [20, 50]}) == {
            "bandpass.low_hz": [20, 50]
        }

    def test_the_latest_flag_becomes_the_per_location_code_pin(self):
        """Both spell the same rule: each location contributes its own newest
        record, NOT the global highest ordinal."""
        from scidb.variant import CODE_PIN_PREFIX, LATEST_VERSION
        from scistackplotdb import branch_params_for
        from scistackplotdb.load import LATEST_COLUMN

        assert branch_params_for({LATEST_COLUMN: True}) == {
            CODE_PIN_PREFIX: LATEST_VERSION
        }

    def test_latest_false_is_dropped_rather_than_inverted(self):
        """scidb has no 'not the latest' pin, and guessing one would be worse."""
        from scistackplotdb import branch_params_for
        from scistackplotdb.load import LATEST_COLUMN

        assert branch_params_for({LATEST_COLUMN: False, "f.p": 1}) == {"f.p": 1}

    def test_the_round_trip_resolves_abbreviations_rather_than_echoing_them(
        self, upstream_code_versions
    ):
        """A bare ``code_version=`` comes back **qualified**, and that is right.

        scidb lets you write ``Variant(X, code_version="v1")`` and resolves
        "which function?" against the data at load time. ``selection_for``
        resolves it once, against the table; coming back the other way there is
        nothing left to abbreviate, so the filter names the function outright.
        Echoing the bare form would hand scidb an ambiguity it has already
        settled.
        """
        from scidb import Variant

        from scistackplotdb import branch_params_for, variant_set

        table = ScidbSource(upstream_code_versions).get_table(["Summarized"])
        variant = Variant(Summarized, code_version="v1")
        selection = variant_set("baseline", variant, table).selection

        assert variant.branch_params == {"__code__": "v1"}  # the bare form in
        assert selection == {"Code:scale_signal": "v1"}  # resolved once
        assert branch_params_for(selection) == {"__code__.scale_signal": "v1"}

    def test_an_empty_selection_is_an_empty_filter(self):
        from scistackplotdb import branch_params_for

        assert branch_params_for({}) == {}
        assert branch_params_for(None) == {}
