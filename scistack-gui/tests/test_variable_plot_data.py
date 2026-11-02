"""
Tests for scistack_gui.services.variable_service.get_variable_plot_data (to-do #4:
default plotting for scalar/1D-numeric variables).

Eligibility is judged from the DuckDB column type of the variable's own
data table (see the function's docstring / plan-default-plotting-by-
schema-level.md) — scalar numeric or 1D numeric array only. These tests
cover a scalar variable, a 1D-array variable (the seeded RawSignal from
conftest already is one), and the ineligible cases (unknown variable,
string data, dict/multi-column data).
"""

from __future__ import annotations

from scistack_gui.runtime.db import get_db
from scistack_gui.services.variable_service import get_variable_plot_data


class TestScalarVariable:
    def test_scalar_points_and_kind(self, client):
        from scidb import BaseVariable

        class StepCount(BaseVariable):
            pass

        StepCount.save(12.5, subject=1, session="pre")
        StepCount.save(14.0, subject=1, session="post")
        StepCount.save(9.25, subject=2, session="pre")

        result = get_variable_plot_data("StepCount", get_db())
        assert result["eligible"] is True
        assert result["kind"] == "scalar"
        assert result["schema_keys"] == ["subject", "session"]
        assert len(result["points"]) == 3
        assert all(isinstance(p["value"], float) for p in result["points"])
        values = {p["value"] for p in result["points"]}
        assert values == {12.5, 14.0, 9.25}
        by_subject_session = {(p["subject"], p["session"]): p["value"] for p in result["points"]}
        assert by_subject_session[("1", "pre")] == 12.5
        assert by_subject_session[("2", "pre")] == 9.25


class TestOneDVariable:
    def test_1d_points_and_kind(self, client):
        # RawSignal is seeded by conftest's populated_db as np.random.randn(10)
        # for (subject, session) in {1,2} x {pre,post} — a real 1D-numeric case.
        result = get_variable_plot_data("RawSignal", get_db())
        assert result["eligible"] is True
        assert result["kind"] == "1d"
        assert len(result["points"]) == 4
        for p in result["points"]:
            assert isinstance(p["value"], list)
            assert len(p["value"]) == 10
            assert all(isinstance(v, float) for v in p["value"])


class TestIneligibleVariables:
    def test_unknown_variable(self, client):
        result = get_variable_plot_data("NoSuchVariable", get_db())
        assert result == {
            "eligible": False,
            "reason": "unknown variable",
            "kind": None,
            "schema_keys": ["subject", "session"],
            "points": [],
        }

    def test_string_variable_not_eligible(self, client):
        from scidb import BaseVariable

        class Condition(BaseVariable):
            pass

        Condition.save("fatigued", subject=1, session="pre")

        result = get_variable_plot_data("Condition", get_db())
        assert result["eligible"] is False
        assert result["kind"] is None
        assert "not scalar/1D numeric" in result["reason"]
        assert result["points"] == []

    def test_2d_array_not_eligible(self, client):
        import numpy as np
        from scidb import BaseVariable

        class Trajectory(BaseVariable):
            pass

        Trajectory.save(np.zeros((3, 4)), subject=1, session="pre")

        result = get_variable_plot_data("Trajectory", get_db())
        assert result["eligible"] is False
        assert "not scalar/1D numeric" in result["reason"]

    def test_dict_variable_not_eligible(self, client):
        from scidb import BaseVariable

        class Summary(BaseVariable):
            pass

        Summary.save({"mean": 1.0, "std": 0.5}, subject=1, session="pre")

        result = get_variable_plot_data("Summary", get_db())
        assert result["eligible"] is False
        assert "multi-column" in result["reason"]

    # Note: the "no records yet" branch (a registered variable whose only
    # record(s) are all schema-excluded — see _numeric_plot_kind's
    # docstring) is exercised by the code but not covered here: it needs
    # exclude_schema's exact matching semantics verified against a live
    # DB, which isn't available in this environment. Worth a regression
    # test once that can be confirmed.


class TestPlotDataApiEndpoint:
    def test_endpoint_matches_service(self, client):
        r = client.get("/api/variables/RawSignal/plot-data")
        assert r.status_code == 200
        body = r.json()
        assert body["eligible"] is True
        assert body["kind"] == "1d"
        assert len(body["points"]) == 4
