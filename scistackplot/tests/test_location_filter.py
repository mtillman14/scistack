"""
``LocationFilter``: the schema location picker's storage.

Stage 1b of ``.claude/plan-schema-location-picker.md``. The picker REPLACES the
flat per-key pickers in Plot Studio, and it could not reuse ``Filter`` to do it:
a set of ``Filter``s is one include-list per column, so it can only express a
Cartesian product, while a tree of checkboxes means something ragged. The first
test here is that difference, stated as an assertion.

The second load-bearing test is :func:`test_generated_code_matches_the_mask` —
a ragged selection cannot be written as ``for_each(subject=[…], trial=[…])``,
whose keys cross-product, so the export carries it as a mask inside the function
body. Two implementations of one rule will drift unless something compares them.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, capabilities, resolve
from scistackplot.codegen import _location_lines, extract_spec, generate_plot_function
from scistackplot.reduce import apply_filters
from scistackplot.spec import Filter, LocationFilter

KEYS = ["subject", "session", "trial"]


def _loc(*prefixes: tuple[str, ...]) -> LocationFilter:
    """Build a filter from positional value-tuples, for brevity in tests.

    Zips each tuple against :data:`KEYS`, which is exactly what a *contiguous*
    selection means. ``TestNonContiguousLocations`` builds its prefixes by hand
    instead — that is the case positional shorthand cannot express, and the
    reason ``include`` carries key names at all.
    """
    return LocationFilter(
        keys=KEYS,
        include=[[[k, v] for k, v in zip(KEYS, p)] for p in prefixes],
    )


def _spec(loc: LocationFilter, **kwargs) -> PlotSpec:
    return PlotSpec(measures=["StepLength"], location_filter=loc, **kwargs)


def _locations(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    return set(map(tuple, frame[KEYS].astype(str).to_numpy()))


# --- the reason this type exists -------------------------------------------


def test_a_ragged_selection_is_not_a_cartesian_product(scalar_table):
    """All of subject 01, plus ONE trial of subject 02.

    No set of per-column ``Filter``s can say this: ``subject in {01,02}`` x
    ``trial in {3}`` would drop seven of subject 01's rows and keep both of
    subject 02's sessions.
    """
    spec = _spec(_loc(("01",), ("02", "pre", "3")))

    kept = apply_filters(scalar_table.frame, spec)

    assert len(kept) == 9  # 8 rows of subject 01 + 1 trial of subject 02
    assert _locations(kept) - {("02", "pre", "3")} == {
        ("01", session, trial)
        for session in ("pre", "post")
        for trial in ("1", "2", "3", "4")
    }

    cartesian = PlotSpec(
        measures=["StepLength"],
        filters=[
            Filter("subject", include=["01", "02"]),
            Filter("trial", include=["3"]),
        ],
    )
    assert len(apply_filters(scalar_table.frame, cartesian)) == 4
    assert _locations(apply_filters(scalar_table.frame, cartesian)) != _locations(kept)


def test_a_prefix_keeps_its_whole_subtree(scalar_table):
    kept = apply_filters(scalar_table.frame, _spec(_loc(("01",))))

    assert set(kept["subject"]) == {"01"}
    assert len(kept) == 8


def test_a_full_depth_prefix_is_one_location(scalar_table):
    kept = apply_filters(scalar_table.frame, _spec(_loc(("03", "post", "2"))))

    assert len(kept) == 1
    assert _locations(kept) == {("03", "post", "2")}


def test_prefixes_union_rather_than_intersect(scalar_table):
    kept = apply_filters(scalar_table.frame, _spec(_loc(("01",), ("03",))))

    assert set(kept["subject"]) == {"01", "03"}
    assert len(kept) == 16


# --- inert and degrading states --------------------------------------------


def test_an_empty_selection_is_inert(scalar_table):
    """Opening a picker is not a statement about the data.

    The same rule an unfilled variant row follows — see
    docs/claude/plot-variant-rows.md §3.
    """
    frame = scalar_table.frame
    assert len(apply_filters(frame, _spec(LocationFilter()))) == len(frame)
    assert len(apply_filters(frame, _spec(_loc()))) == len(frame)
    assert len(apply_filters(frame, _spec(LocationFilter(keys=KEYS)))) == len(frame)
    assert LocationFilter(keys=KEYS).is_empty()
    # keys is display order only — a prefix with no keys list still matches.
    bare = LocationFilter(include=[[["subject", "01"]]])
    assert not bare.is_empty()
    assert len(apply_filters(frame, _spec(bare))) == 8


def test_a_key_the_frame_lacks_goes_unconstrained(scalar_frame):
    """A shallower variable still contributes its one value.

    Subject-level Mass against a trial-level selection: the subject part binds,
    the trial part cannot, and broadcasting is what the join already does
    (scistackplotdb.hierarchy).
    """
    frame = scalar_frame.groupby("subject", as_index=False)["StepLength"].mean()
    table = LongTable.from_frame(
        frame,
        factors=["subject"],
        measures=["StepLength"],
        schema_levels=["subject"],
    )

    kept = apply_filters(table.frame, _spec(_loc(("02", "pre", "3"))))

    assert list(kept["subject"]) == ["02"]


def test_a_filter_naming_no_known_column_is_ignored(scalar_table):
    """A spec outlives the table it was written against."""
    stale = LocationFilter(keys=["cohort"], include=[[["cohort", "A"]]])

    kept = apply_filters(scalar_table.frame, _spec(stale))

    assert len(kept) == len(scalar_table.frame)


class TestNonContiguousLocations:
    """A saved location need not fill the schema.

    ``subject`` + ``speed`` with ``timepoint`` NULL is a supported shape
    (docs/claude/schema-hierarchy-contiguity.md), and it is why a prefix names
    its keys. Read positionally, ``["01", "SSV"]`` against
    ``[subject, timepoint, speed]`` would match ``SSV`` against ``timepoint``
    and select nothing, in silence.
    """

    @pytest.fixture
    def crosscutting_table(self):
        frame = pd.DataFrame(
            [
                {"subject": "01", "timepoint": "T1", "speed": "SSV", "D": 1.0},
                {"subject": "01", "timepoint": "T2", "speed": "FST", "D": 2.0},
                {"subject": "02", "timepoint": "T1", "speed": "SSV", "D": 3.0},
            ]
        )
        return LongTable.from_frame(
            frame,
            factors=["subject", "timepoint", "speed"],
            measures=["D"],
            schema_levels=["subject", "timepoint", "speed"],
        )

    def test_a_hole_in_the_middle_matches_the_named_keys(self, crosscutting_table):
        loc = LocationFilter(
            keys=["subject", "timepoint", "speed"],
            include=[[["subject", "01"], ["speed", "SSV"]]],
        )

        kept = apply_filters(
            crosscutting_table.frame, PlotSpec(measures=["D"], location_filter=loc)
        )

        assert len(kept) == 1
        assert kept.iloc[0]["timepoint"] == "T1"  # not constrained, and not misread

    def test_generated_code_agrees(self, crosscutting_table):
        loc = LocationFilter(
            keys=["subject", "timepoint", "speed"],
            include=[[["subject", "01"], ["speed", "SSV"]]],
        )
        spec = PlotSpec(measures=["D"], location_filter=loc)
        namespace: dict = {"df": crosscutting_table.frame.copy(), "pd": pd}

        exec("\n".join(_location_lines(spec)), namespace)  # noqa: S102

        expected = apply_filters(crosscutting_table.frame, spec)
        assert len(namespace["df"]) == len(expected) == 1


def test_a_selection_matching_nothing_yields_an_empty_figure(scalar_table):
    spec = _spec(
        _loc(("99",)),
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    figures = resolve(spec, scalar_table)

    assert len(figures) == 1
    assert sum(len(panel.frame) for panel in figures[0].panels) == 0


def test_it_composes_with_ordinary_filters(scalar_table):
    spec = _spec(_loc(("01",), ("02",)), filters=[Filter("session", include=["post"])])

    kept = apply_filters(scalar_table.frame, spec)

    assert set(kept["subject"]) == {"01", "02"}
    assert set(kept["session"]) == {"post"}
    assert len(kept) == 8


def test_values_match_as_text(scalar_frame):
    """Zero-padded keys are this project's standing trap."""
    frame = scalar_frame.assign(trial=scalar_frame["trial"].astype(int))
    table = LongTable.from_frame(
        frame,
        factors=KEYS,
        measures=["StepLength"],
        schema_levels=KEYS,
    )

    kept = apply_filters(table.frame, _spec(_loc(("01", "pre", "3"))))

    assert len(kept) == 1


# --- serialization ----------------------------------------------------------


class TestRoundTrip:
    def test_json(self):
        spec = _spec(_loc(("01",), ("02", "pre", "3")))

        restored = PlotSpec.from_json(spec.to_json())

        assert restored.location_filter == spec.location_filter
        assert restored.location_filter.prefixes() == [
            (("subject", "01"),),
            (("subject", "02"), ("session", "pre"), ("trial", "3")),
        ]

    def test_tuples_survive_as_lists(self):
        """JSON has no tuple; ``prefixes()`` is the tuple view, not the storage."""
        raw = _spec(_loc(("01",))).to_dict()

        assert raw["location_filter"] == {
            "keys": KEYS,
            "include": [[["subject", "01"]]],
        }

    def test_through_generated_source(self, scalar_table):
        spec = _spec(
            _loc(("01",), ("02", "pre", "3")),
            roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
            kind=PlotKind.BOX,
        )

        recovered = extract_spec(generate_plot_function(spec, scalar_table))

        assert recovered is not None
        assert recovered.location_filter == spec.location_filter

    def test_an_empty_filter_round_trips_to_empty(self):
        restored = PlotSpec.from_json(PlotSpec(measures=["X"]).to_json())

        assert restored.location_filter.is_empty()


# --- the exported figure is the previewed figure ----------------------------


@pytest.mark.parametrize(
    "prefixes",
    [
        (("01",),),
        (("01",), ("02", "pre", "3")),
        (("03", "post", "2"),),
        (("01",), ("02",), ("03", "pre")),
        (("99",),),
    ],
)
def test_generated_code_matches_the_mask(scalar_table, prefixes):
    """The emitted pandas and ``reduce`` must keep the same rows.

    Run the generated preamble against the same frame and compare. Without this
    the two implementations of one rule drift, and the drift shows up as an
    exported figure that is not the one the user approved.
    """
    spec = _spec(_loc(*prefixes))
    namespace: dict = {"df": scalar_table.frame.copy(), "pd": pd}

    exec("\n".join(_location_lines(spec)), namespace)  # noqa: S102

    expected = apply_filters(scalar_table.frame, spec)
    assert _locations(namespace["df"]) == _locations(expected)
    assert len(namespace["df"]) == len(expected)


def test_an_inert_filter_emits_nothing(scalar_table):
    assert _location_lines(_spec(LocationFilter())) == []
    assert _location_lines(_spec(_loc())) == []


def test_generated_code_guards_absent_keys(scalar_frame):
    """The emitted ``if _k in df.columns`` is the rule, not defensive clutter."""
    frame = scalar_frame.groupby("subject", as_index=False)["StepLength"].mean()
    spec = _spec(_loc(("02", "pre", "3")))
    namespace: dict = {"df": frame.copy(), "pd": pd}

    exec("\n".join(_location_lines(spec)), namespace)  # noqa: S102

    assert list(namespace["df"]["subject"]) == ["02"]


# --- what the panel is told -------------------------------------------------


def test_capabilities_report_which_levels_survive(scalar_table):
    spec = _spec(_loc(("01",), ("03",)), roles={"session": Role.X})

    report = capabilities(spec, scalar_table)
    by_name = {f["name"]: f for f in report["factors"]}

    assert by_name["subject"]["levels"] == ["01", "02", "03"]
    assert by_name["subject"]["selected"] == ["01", "03"]


def test_the_no_filter_fast_path_still_sees_a_location_filter(scalar_table):
    """``factor_summary`` short-circuits when nothing is filtered.

    A fast path that only knew about ``spec.filters`` would report "all 3
    selected" beside a figure drawing 1 — the precise failure the shared-rule
    readout exists to prevent.
    """
    spec = _spec(_loc(("02",)), roles={"session": Role.X})
    assert not spec.filters  # the condition the old fast path tested

    report = capabilities(spec, scalar_table)
    by_name = {f["name"]: f for f in report["factors"]}

    assert by_name["subject"]["selected"] == ["02"]


# --- legend arithmetic ------------------------------------------------------


class TestLevelsAfterLocation:
    def test_a_prefix_that_never_names_the_column_leaves_it_alone(self):
        """The key-named form makes this a membership test, not arithmetic."""
        from scistackplot.codegen import _levels_after_location

        loc = LocationFilter(
            keys=["subject", "timepoint", "speed"],
            include=[[["subject", "01"], ["speed", "SSV"]]],
        )
        spec = PlotSpec(measures=["D"], location_filter=loc)

        assert _levels_after_location(spec, "timepoint", ["T1", "T2"]) == ["T1", "T2"]
        assert _levels_after_location(spec, "speed", ["SSV", "FST"]) == ["SSV"]

    def test_a_selection_at_that_depth_narrows_it(self):
        from scistackplot.codegen import _levels_after_location

        spec = _spec(_loc(("01",), ("03",)))

        assert _levels_after_location(spec, "subject", ["01", "02", "03"]) == [
            "01",
            "03",
        ]

    def test_a_shorter_prefix_leaves_deeper_keys_alone(self):
        """"All of subject 01" did not name a trial, so every trial survives."""
        from scistackplot.codegen import _levels_after_location

        spec = _spec(_loc(("01",), ("02", "pre", "3")))

        assert _levels_after_location(spec, "trial", ["1", "2", "3", "4"]) == [
            "1",
            "2",
            "3",
            "4",
        ]

    def test_every_prefix_reaching_that_depth_does_narrow_it(self):
        from scistackplot.codegen import _levels_after_location

        spec = _spec(_loc(("01", "pre"), ("02", "pre")))

        assert _levels_after_location(spec, "session", ["pre", "post"]) == ["pre"]

    def test_a_column_the_filter_does_not_name_is_untouched(self):
        from scistackplot.codegen import _levels_after_location

        spec = _spec(_loc(("01",)))

        assert _levels_after_location(spec, "gone", ["a", "b"]) == ["a", "b"]
