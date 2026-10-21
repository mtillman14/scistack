"""Sidebar lists are alphabetical ignoring case, Python and MATLAB together.

Regression: a new Python function ``load_xsens_one_file`` showed up between
``compute_vif_features`` and the MATLAB ``EmgActivationAnalysisTEST_NormData``
because the sidebar rendered the (case-sensitively sorted) Python list
followed by the MATLAB list. scistack_gui.domain.display_order owns the order.
"""

from __future__ import annotations

import pytest

import scistack_gui.registry as _registry
from scistack_gui import matlab_registry
from scistack_gui.domain.display_order import display_sorted


class TestDisplaySorted:
    def test_ignores_case(self):
        assert display_sorted(["beta", "Alpha", "gamma", "Delta"]) == [
            "Alpha", "beta", "Delta", "gamma",
        ]

    def test_case_only_difference_is_stable(self):
        assert display_sorted(["abc", "ABC", "Abc"]) == ["ABC", "Abc", "abc"]

    def test_keyed_items(self):
        items = [("zeta", 1), ("Alpha", 2)]
        assert display_sorted(items, name=lambda kv: kv[0]) == [
            ("Alpha", 2), ("zeta", 1),
        ]


class TestGetRegistrySidebarFunctions:
    @pytest.fixture(autouse=True)
    def _matlab(self, monkeypatch):
        monkeypatch.setattr(
            matlab_registry, "get_all_function_names",
            lambda: ["EmgActivationAnalysisTEST_NormData", "zscore_m"],
        )
        monkeypatch.setattr(matlab_registry, "get_mismatched_function_names", lambda: [])

    def test_python_and_matlab_merged_case_insensitively(self, populated_db, monkeypatch):
        from scistack_gui.services.pipeline_service import get_registry

        monkeypatch.setattr(_registry, "_functions", {
            "load_xsens_one_file": lambda x: x,
            "compute_vif_features": lambda x: x,
            "Average_Trials": lambda x: x,
        })
        result = get_registry()
        assert result["sidebar_functions"] == [
            "Average_Trials",
            "compute_vif_features",
            "EmgActivationAnalysisTEST_NormData",
            "load_xsens_one_file",
            "zscore_m",
        ]
        # Every listed function has a role, so the role filter sees all of them.
        assert set(result["sidebar_functions"]) == set(result["function_roles"])

    def test_variables_ignore_case(self, populated_db):
        from scistack_gui.services.pipeline_service import get_registry

        names = get_registry()["variables"]
        assert names == sorted(names, key=lambda n: (n.casefold(), n))


class TestEntityListsOrdered:
    def test_parameters_ignore_case(self, monkeypatch):
        from scidb import Parameter
        from scistack_gui.services.layout_service import get_parameters

        monkeypatch.setattr(_registry, "get_parameters_registry", lambda: {
            "zeta": Parameter(1), "Alpha": Parameter(2), "beta": Parameter(3),
        })
        assert [p["name"] for p in get_parameters()] == ["Alpha", "beta", "zeta"]

    def test_path_inputs_ignore_case(self, monkeypatch):
        from scistack_gui.services import layout_service

        monkeypatch.setattr(_registry, "get_path_inputs_registry", lambda: {
            "zeta": object(), "Alpha": object(), "beta": object(),
        })
        monkeypatch.setattr(
            "scistack_gui.domain.graph_builder.path_input_display", lambda obj: {},
        )
        assert [p["name"] for p in layout_service.get_path_inputs()] == [
            "Alpha", "beta", "zeta",
        ]
