"""The GUI registry and an installed SciStack LIBRARY (portability Stage 5a).

A real package on disk (``fakelib``), listed under ``packages`` in the
project's scistack.toml:

* its functions register as ``fakelib.<fn>`` (scidb.names), the project's own
  stay bare, and two libraries with the same function name do not collide;
* its Parameters and PathInputs (in source or in its entities TOML) stay out
  of the project's names;
* the Variables its entities TOML declares are registered, and a name the
  project already defines keeps the project's definition.
"""

from __future__ import annotations

import sys
import textwrap

import pytest

from scistack_gui import registry
from scistack_gui.config import load_config


def _write_library(root, name: str, *, fn: str = "filter_emg"):
    pkg = root / name
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "steps.py").write_text(
        textwrap.dedent(
            f"""
            from scidb import Parameter, PathInput

            LIB_RATE = Parameter(100)
            LIB_DATA = PathInput("{{subject}}.csv", name="LIB_DATA")


            def {fn}(signal):
                return signal
            """
        )
    )
    (pkg / "scistack_entities.toml").write_text(
        'variables = ["LibOnlyVar", "SharedVar"]\n\n'
        "[parameters]\nTOML_RATE = 5\n\n"
        '[path_inputs]\nTOML_DATA = "{subject}.mat"\n'
    )


@pytest.fixture
def project_with_libraries(tmp_path, monkeypatch):
    from scidb import BaseVariable, names, schema_order

    libs = tmp_path / "site"
    _write_library(libs, "fakelib")
    _write_library(libs, "otherlib")
    monkeypatch.syspath_prepend(str(libs))

    project = tmp_path / "proj"
    project.mkdir()
    (project / "analysis.py").write_text(
        textwrap.dedent(
            """
            from scidb import BaseVariable


            class SharedVar(BaseVariable):
                schema_version = 3


            def filter_emg(signal):
                return signal
            """
        )
    )
    (project / "scistack.toml").write_text(
        'modules = ["analysis.py"]\npackages = ["fakelib", "otherlib"]\n'
    )
    from scistack_gui.config import set_project_root_hint

    set_project_root_hint(project)
    names.clear_cache()
    schema_order.clear_cache()
    config = load_config(project, project / "db.duckdb")
    registry.load_from_config(config)
    yield project
    for mod in [m for m in sys.modules if m.split(".")[0] in ("fakelib", "otherlib")]:
        del sys.modules[mod]
    for var in ("LibOnlyVar", "SharedVar"):
        BaseVariable.unregister(var)
    names.clear_cache()


def test_library_functions_are_qualified_and_do_not_collide(project_with_libraries):
    fns = set(registry._functions)
    assert {"fakelib.filter_emg", "otherlib.filter_emg"} <= fns
    # The project's own function keeps its bare name.
    assert "filter_emg" in fns
    assert registry.lookup_function("fakelib.filter_emg") is not None


def test_a_librarys_parameters_and_path_inputs_stay_out(project_with_libraries):
    params = set(registry.get_parameters_registry())
    path_inputs = set(registry.get_path_inputs_registry())
    assert not {"LIB_RATE", "TOML_RATE"} & params
    assert not {"LIB_DATA", "TOML_DATA"} & path_inputs


def test_a_librarys_declared_variables_are_registered(project_with_libraries):
    from scidb import BaseVariable

    assert "LibOnlyVar" in BaseVariable._all_subclasses


def test_the_projects_variable_definition_is_kept(project_with_libraries):
    """The library only NAMES SharedVar; the project DEFINES it (schema 3)."""
    from scidb import BaseVariable

    assert BaseVariable._all_subclasses["SharedVar"].schema_version == 3
    assert "SharedVar" not in BaseVariable.definition_conflicts()
