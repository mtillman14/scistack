"""A run records which DECLARED Parameter fed each constant argument.

Regression for cleanup-audit B1 (docs/claude/cleanup-audit.md §3): a Parameter
declared ``gaitrite_config`` wired into an argument ``gaitRiteConfig`` was
recorded under the argument name only, so the GUI drew a second Parameter node
after the first run. The declared name now rides on the constant's provenance
edge (``_invocation_input.declared_name``) and ``get_aggregated_variants``
keys constants by it.
"""

import textwrap

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, Parameter, configure_database, entities, for_each
from scidb.parameter import declared_parameter_names, parameter_node_name

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_declared_names.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class FilteredSignal(BaseVariable):
    pass


def bandpass_filter(signal, low_hz, high_hz):
    return signal * (low_hz + high_hz)


def _seed_raw(db, subjects=(1,), sessions=("A",)):
    for subj in subjects:
        for sess in sessions:
            RawSignal.save(np.arange(5.0), db=db, subject=subj, session=sess)


def _run(**kwargs):
    inputs = kwargs.pop("inputs", {"signal": RawSignal, "low_hz": 20, "high_hz": 500})
    for_each(
        bandpass_filter,
        inputs=inputs,
        outputs=[FilteredSignal],
        subject=[1],
        session=["A"],
        **kwargs,
    )


def _only_function(result):
    assert len(result["functions"]) == 1, result["functions"].keys()
    return next(iter(result["functions"].values()))


# ---------------------------------------------------------------------------
# The owner of the mapping
# ---------------------------------------------------------------------------


class TestDeclaredParameterNames:
    def test_a_named_parameter_supplies_its_own_name(self):
        p = Parameter(20)
        p.name = "cutoff"
        assert declared_parameter_names({"low_hz": p, "high_hz": 500}) == {
            "low_hz": "cutoff"
        }

    def test_an_unnamed_parameter_or_bare_value_says_nothing(self):
        assert declared_parameter_names({"low_hz": Parameter(20), "x": 3}) == {}

    def test_explicit_wins_over_the_objects_own_name(self):
        """A value recorded in history reaches for_each bare; the caller's
        wiring is the authority when both exist."""
        p = Parameter(20)
        p.name = "old_name"
        assert declared_parameter_names(
            {"low_hz": p}, {"low_hz": "cutoff"}
        ) == {"low_hz": "cutoff"}

    def test_node_name_falls_back_to_the_argument(self):
        assert parameter_node_name("low_hz", {"low_hz": "cutoff"}) == "cutoff"
        assert parameter_node_name("high_hz", {"low_hz": "cutoff"}) == "high_hz"
        assert parameter_node_name("high_hz", None) == "high_hz"

    def test_name_is_not_proxied_to_the_value(self):
        """``Parameter`` proxies unknown attributes to a single value; ``name``
        must be the Parameter's own, never the wrapped value's ``.name``."""

        class Named:
            name = "the value's name"

        assert Parameter(Named()).name is None


# ---------------------------------------------------------------------------
# Declaring sets the name
# ---------------------------------------------------------------------------


def test_the_entities_loader_names_its_parameters(tmp_path):
    path = tmp_path / "scistack_entities.toml"
    path.write_text(
        textwrap.dedent(
            """
            [parameters]
            gaitrite_config = 3
            """
        ).lstrip("\n"),
        encoding="utf-8",
    )
    loaded = entities.load(path)
    assert loaded.parameters["gaitrite_config"].name == "gaitrite_config"


# ---------------------------------------------------------------------------
# Round trip through provenance
# ---------------------------------------------------------------------------


class TestRecordedDeclaredName:
    def test_explicit_names_key_constants_by_the_parameter(self, db):
        _seed_raw(db)
        _run(parameter_names={"low_hz": "cutoff"})

        result = db.get_aggregated_variants()
        fn = _only_function(result)
        # The function still receives (and records the value under) its
        # argument; the NODE is the declared Parameter.
        assert fn["constants"]["low_hz"] == [20]
        assert fn["parameter_names"] == {"low_hz": "cutoff", "high_hz": "high_hz"}
        assert set(result["constants"]) == {"cutoff", "high_hz"}
        assert "low_hz" not in result["constants"]

    def test_a_named_parameter_input_records_its_name(self, db):
        _seed_raw(db)
        p = Parameter(20)
        p.name = "cutoff"
        _run(inputs={"signal": RawSignal, "low_hz": p, "high_hz": 500})

        fn = _only_function(db.get_aggregated_variants())
        assert fn["parameter_names"]["low_hz"] == "cutoff"

    def test_a_multi_valued_named_parameter_names_every_value(self, db):
        """EachOf expansion replaces the Parameter with bare values; the name
        must be resolved before that and ride the recursion."""
        _seed_raw(db)
        p = Parameter(20, 50)
        p.name = "cutoff"
        _run(inputs={"signal": RawSignal, "low_hz": p, "high_hz": 500})

        result = db.get_aggregated_variants()
        assert len(result["functions"]) == 2
        for fn in result["functions"].values():
            assert fn["parameter_names"]["low_hz"] == "cutoff"
        values = {v["value"] for v in result["constants"]["cutoff"]["values"]}
        assert values == {"20", "50"}

    def test_the_name_does_not_change_identity(self, db):
        """Same value, named or not: one call site, one invocation."""
        _seed_raw(db)
        _run()
        _run(parameter_names={"low_hz": "cutoff"})

        result = db.get_aggregated_variants()
        assert len(result["functions"]) == 1
        n_inv = db._duck._fetchall(
            "SELECT COUNT(*) FROM _invocation WHERE function_name = ?",
            ["bandpass_filter"],
        )[0][0]
        assert n_inv == 1

    def test_the_latest_run_names_an_existing_edge(self, db):
        """An edge written by an earlier run is kept (DO NOTHING) — its name
        is refreshed, so the canvas shows the current wiring's Parameter."""
        _seed_raw(db)
        _run(parameter_names={"low_hz": "first"})
        _run(parameter_names={"low_hz": "second"})

        fn = _only_function(db.get_aggregated_variants())
        assert fn["parameter_names"]["low_hz"] == "second"

    def test_no_names_is_the_old_shape(self, db):
        """Legacy rows / bare-value scripts: the node IS the argument."""
        _seed_raw(db)
        _run()

        result = db.get_aggregated_variants()
        fn = _only_function(result)
        assert fn["parameter_names"] == {"low_hz": "low_hz", "high_hz": "high_hz"}
        assert set(result["constants"]) == {"low_hz", "high_hz"}

    def test_pipeline_variants_report_the_recorded_names(self, db):
        _seed_raw(db)
        _run(parameter_names={"low_hz": "cutoff"})

        [variant] = [
            v for v in db.list_pipeline_variants()
            if v["function_name"] == "bandpass_filter"
        ]
        assert variant["parameter_names"] == {"low_hz": "cutoff"}

    def test_the_declared_name_column_exists_on_a_fresh_database(self, db):
        cols = {
            r[0]
            for r in db._duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = '_invocation_input'"
            )
        }
        assert "declared_name" in cols
