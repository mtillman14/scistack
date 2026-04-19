"""
Unit tests for scistack_gui.services.pipeline_service.

Covers functions with non-trivial logic: get_function_source, get_function_params,
get_schema, get_variables_list, and get_registry.
"""

from __future__ import annotations

import pytest

import scistack_gui.registry as _registry
from scistack_gui.services.pipeline_service import (
    get_function_params,
    get_function_source,
    get_schema,
    get_variables_list,
    get_registry,
)


# ---------------------------------------------------------------------------
# get_function_source
# ---------------------------------------------------------------------------

class TestGetFunctionSource:
    def test_registered_python_function_returns_file_and_line(self, populated_db):
        # bandpass_filter is registered in the conftest client fixture,
        # but populated_db doesn't register it — use a direct registration.
        def my_fn(x):
            pass
        _registry._functions["my_fn"] = my_fn
        result = get_function_source("my_fn")
        assert result["ok"] is True
        assert "file" in result
        assert isinstance(result["line"], int)
        assert result["line"] >= 1

    def test_unregistered_function_returns_error(self, populated_db):
        result = get_function_source("no_such_function")
        assert result["ok"] is False
        assert "not registered" in result["error"]

    def test_builtin_function_returns_error(self, populated_db):
        # Built-ins have no source file — should return a friendly error.
        _registry._functions["len_builtin"] = len
        result = get_function_source("len_builtin")
        assert result["ok"] is False
        assert "Could not locate source" in result["error"]

    def test_no_functions_registered_returns_error(self, populated_db):
        _registry._functions.clear()
        result = get_function_source("anything")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# get_function_params
# ---------------------------------------------------------------------------

class TestGetFunctionParams:
    def test_returns_param_names_for_registered_function(self, populated_db):
        def my_fn(signal, low_hz):
            pass
        _registry._functions["my_fn"] = my_fn
        params = get_function_params("my_fn")
        assert "signal" in params
        assert "low_hz" in params

    def test_private_params_excluded(self, populated_db):
        def my_fn(signal, _internal):
            pass
        _registry._functions["my_fn"] = my_fn
        params = get_function_params("my_fn")
        assert "_internal" not in params
        assert "signal" in params

    def test_unregistered_function_returns_empty(self, populated_db):
        params = get_function_params("does_not_exist")
        assert params == []


# ---------------------------------------------------------------------------
# get_schema
# ---------------------------------------------------------------------------

class TestGetSchema:
    def test_returns_keys_and_values(self, populated_db):
        result = get_schema(populated_db)
        assert "keys" in result
        assert "values" in result
        assert "subject" in result["keys"]
        assert "session" in result["keys"]

    def test_values_contain_distinct_entries(self, populated_db):
        result = get_schema(populated_db)
        assert set(result["values"]["subject"]) == {"1", "2"}
        assert set(result["values"]["session"]) == {"pre", "post"}


# ---------------------------------------------------------------------------
# get_variables_list
# ---------------------------------------------------------------------------

class TestGetVariablesList:
    def test_returns_registered_variable_names(self, populated_db):
        result = get_variables_list()
        names = [v["variable_name"] for v in result]
        assert "RawSignal" in names
        assert "FilteredSignal" in names

    def test_result_sorted(self, populated_db):
        result = get_variables_list()
        names = [v["variable_name"] for v in result]
        assert names == sorted(names)


# ---------------------------------------------------------------------------
# get_registry
# ---------------------------------------------------------------------------

class TestGetRegistry:
    def test_returns_functions_variables_matlab(self, populated_db):
        _registry._functions["bandpass_filter"] = lambda s, l: s
        result = get_registry()
        assert "functions" in result
        assert "variables" in result
        assert "matlab_functions" in result
        assert "bandpass_filter" in result["functions"]

    def test_variables_include_registered_types(self, populated_db):
        result = get_registry()
        assert "RawSignal" in result["variables"]
        assert "FilteredSignal" in result["variables"]
