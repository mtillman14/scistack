"""Names across packages (portability Stage 5a; docs/claude/portability.md,
"Reusing code").

* A function from a LIBRARY the project uses is recorded as ``pkg.fn``; the
  project's own code keeps its bare name; a library is opt-in (listed under
  ``packages`` or advertising an entry point), never "anything installed".
* A Variable is not namespaced: the same name defined the same way is one
  type; a different definition is a recorded conflict, and the project's
  definition is kept over a library's.
"""

from __future__ import annotations

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each, names
from scidb.variable import same_definition


@pytest.fixture
def project(tmp_path):
    """A project whose scistack.toml lists `fakelib` as a library."""
    from scifor.pathinput import clear_project_root, set_project_root

    from scidb import schema_order

    (tmp_path / "scistack.toml").write_text('modules = []\npackages = ["fakelib"]\n')
    set_project_root(tmp_path)
    names.clear_cache()
    schema_order.clear_cache()
    created: list[str] = []
    yield tmp_path, created
    for name in created:
        BaseVariable.unregister(name)
    clear_project_root()
    names.clear_cache()
    schema_order.clear_cache()


def _fn(module: str, name: str = "double"):
    def f(x):
        return x * 2

    f.__name__ = name
    f.__module__ = module
    return f


def _var(name: str, module: str, created: list, **attrs):
    cls = type(BaseVariable)(name, (BaseVariable,), {"__module__": module, **attrs})
    created.append(name)
    return cls


class TestFunctionName:
    def test_a_library_function_is_qualified(self, project):
        assert names.function_name(_fn("fakelib.filters", "filter_emg")) == "fakelib.filter_emg"

    def test_the_projects_own_code_is_bare(self, project):
        assert names.function_name(_fn("my_analysis")) == "double"

    def test_an_unlisted_installed_package_is_not_a_library(self, project):
        """Opt-in: SciStack's own stubs (scidb.state, scimatlab) carry a user
        function's name with SciStack's module and must stay bare."""
        assert names.function_name(_fn("scidb.state", "grSides")) == "grSides"
        assert names.function_name(_fn("numpy", "mean")) == "mean"

    def test_an_already_qualified_name_is_kept(self, project):
        assert names.function_name(_fn("pandas.io", "pandas.read_csv")) == "pandas.read_csv"

    def test_the_own_package_is_never_a_library(self, project):
        root, _ = project
        (root / "pyproject.toml").write_text('[project]\nname = "fakelib"\n')
        (root / "src" / "fakelib").mkdir(parents=True)
        names.clear_cache()
        assert "fakelib" not in names.library_packages()
        assert names.function_name(_fn("fakelib.filters", "filter_emg")) == "filter_emg"

    def test_a_run_records_the_qualified_name(self, project, tmp_path):
        _scifor.set_schema([])
        db = configure_database(tmp_path / "names.duckdb", ["subject"])
        try:
            Raw = _var("NamesRaw", "test_names", project[1])
            Out = _var("NamesOut", "test_names", project[1])
            Raw.save(np.array([1.0, 2.0]), subject="S01")
            for_each(_fn("fakelib.math"), {"x": Raw}, [Out], subject=["S01"])
            recorded = {r[0] for r in db._duck._fetchall("SELECT function_name FROM _invocation")}
            assert recorded == {"fakelib.double"}
        finally:
            db.close()
            _scifor.set_schema([])


class TestVariableMerge:
    def test_identical_definitions_are_one_type(self, project):
        a = _var("MergeA", "mod_one", project[1])
        b = _var("MergeA", "mod_two", project[1])
        assert same_definition(a, b)
        assert "MergeA" not in BaseVariable.definition_conflicts()
        assert BaseVariable._all_subclasses["MergeA"] is b

    def test_a_different_schema_version_is_a_conflict(self, project):
        _var("MergeB", "mod_one", project[1], schema_version=1)
        _var("MergeB", "mod_two", project[1], schema_version=2)
        kept, rejected = BaseVariable.definition_conflicts()["MergeB"]
        assert {kept.__module__, rejected.__module__} == {"mod_one", "mod_two"}

    def test_a_different_codec_is_a_conflict(self, project):
        def to_db(self):
            return None

        _var("MergeC", "mod_one", project[1])
        _var("MergeC", "mod_two", project[1], to_db=to_db)
        assert "MergeC" in BaseVariable.definition_conflicts()

    def test_the_project_keeps_its_definition_over_a_librarys(self, project):
        mine = _var("MergeD", "my_analysis", project[1], schema_version=2)
        _var("MergeD", "fakelib.vars", project[1], schema_version=1)
        assert BaseVariable._all_subclasses["MergeD"] is mine
        kept, rejected = BaseVariable.definition_conflicts()["MergeD"]
        assert kept is mine and rejected.__module__ == "fakelib.vars"

    def test_a_name_only_declaration_never_replaces_a_definition(self, project):
        def to_db(self):
            return None

        defined = _var("MergeE", "my_analysis", project[1], to_db=to_db)
        _var("MergeE", "scidb.entities", project[1], _declared_only=True)
        assert BaseVariable._all_subclasses["MergeE"] is defined
        assert "MergeE" not in BaseVariable.definition_conflicts()

    def test_redefining_in_the_same_module_is_an_edit(self, project):
        _var("MergeF", "my_analysis", project[1], schema_version=1)
        newer = _var("MergeF", "my_analysis", project[1], schema_version=2)
        assert BaseVariable._all_subclasses["MergeF"] is newer
        assert "MergeF" not in BaseVariable.definition_conflicts()
