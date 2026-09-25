"""Unit tests for ``scidb.discover`` — the project / library scanner."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest
from scidb.discover import (
    DiscoveryResult,
    _dist_to_import_names,
    _read_uv_lock_packages,
    scan_project,
)

from scidb import Parameter


# ---------------------------------------------------------------------------
# Fixture factory: build a throwaway scistack project on disk
# ---------------------------------------------------------------------------
@pytest.fixture
def project_factory(tmp_path: Path):
    """
    Factory that scaffolds a throwaway project under tmp_path and cleans
    up sys.modules after the test so repeated imports don't collide.

    Usage::

        def test_x(project_factory):
            root = project_factory(
                package_name="my_fixture",
                files={
                    "variables.py": "from scidb import BaseVariable\\n"
                                    "class Raw(BaseVariable): schema_version = 1\\n",
                },
                uv_lock_packages=["numpy"],
            )
    """
    created_packages: list[str] = []

    def _make(
        package_name: str,
        files: dict[str, str] | None = None,
        uv_lock_packages: list[str] | None = None,
        pyproject_extra: str = "",
    ) -> Path:
        project_root = tmp_path / package_name
        src_pkg = project_root / "src" / package_name
        src_pkg.mkdir(parents=True)

        (project_root / "pyproject.toml").write_text(
            f'[project]\nname = "{package_name}"\nversion = "0.1.0"\n{pyproject_extra}'
        )

        # Ensure the package has an __init__.py even if none is supplied.
        (src_pkg / "__init__.py").write_text("")

        files = files or {}
        for rel, content in files.items():
            full = src_pkg / rel
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(textwrap.dedent(content))

        if uv_lock_packages is not None:
            lock_lines = ["version = 1\n"]
            for name in uv_lock_packages:
                lock_lines.append(
                    f'\n[[package]]\nname = "{name}"\nversion = "0.0.0"\n'
                )
            (project_root / "uv.lock").write_text("".join(lock_lines))

        created_packages.append(package_name)
        return project_root

    yield _make

    # Teardown: drop any imported modules from the fixture projects.
    for name in created_packages:
        prefix = name + "."
        for mod_name in list(sys.modules):
            if mod_name == name or mod_name.startswith(prefix):
                sys.modules.pop(mod_name, None)


# ---------------------------------------------------------------------------
# discover_module: pure, per-module scanner
# ---------------------------------------------------------------------------
class TestSharedPredicates:
    """is_parameter/is_path_input -- shared with registry.py's
    _scan_module_parameters/_scan_module_path_inputs to eliminate
    duplicated isinstance checks between the two scanners."""

    def test_is_parameter(self):
        from scidb import Parameter
        from scidb.discover import is_parameter

        assert is_parameter(Parameter(1))
        assert is_parameter(Parameter(1, 2))
        assert not is_parameter(1)

    def test_is_path_input_direct(self):
        from scidb import PathInput
        from scidb.discover import is_path_input

        assert is_path_input(PathInput("{s}.mat", name="{s}.mat"))
        assert not is_path_input(1)

    def test_is_path_input_each_of_of_path_inputs(self):
        from scidb import EachOf, PathInput
        from scidb.discover import is_path_input

        assert is_path_input(EachOf(PathInput("{s}.mat", name="{s}.mat"), PathInput("{t}.mat", name="{s}.mat")))

    def test_is_path_input_each_of_of_plain_values_is_not(self):
        from scidb import EachOf
        from scidb.discover import is_path_input

        assert not is_path_input(EachOf(1, 2, 3))

    def test_empty_each_of_is_not_a_path_input(self):
        """all([]) is True, so "every alternative is a PathInput" is
        satisfied vacuously by an EachOf with none -- which is exactly what
        a Parameter declared with no value yet is. Without the explicit
        non-empty check it registers as a PathInput."""
        from scidb import EachOf, Parameter
        from scidb.discover import is_parameter, is_path_input

        assert not is_path_input(EachOf())
        assert not is_path_input(Parameter())
        assert is_parameter(Parameter())


class TestDiscoverModule:
    def test_finds_base_variable_subclass(self, project_factory):
        root = project_factory(
            package_name="fix_vars",
            files={
                "variables.py": """
                    from scidb import BaseVariable
                    class RawSignal(BaseVariable):
                        schema_version = 1
                """,
            },
        )
        result = scan_project(root)
        all_vars = [v for m in result.project_code.modules for v in m.variables]
        assert len(all_vars) == 1
        assert all_vars[0].__name__ == "RawSignal"
        # Also check that discover_module attributes it to the right module.
        by_module = {m.module_name: m for m in result.project_code.modules}
        assert "fix_vars.variables" in by_module
        assert len(by_module["fix_vars.variables"].variables) == 1

    def test_finds_pipeline_fcn(self, project_factory):
        root = project_factory(
            package_name="fix_fns",
            files={
                "functions.py": """
                    from scidb import scistack

                    @scistack
                    def preprocess(x):
                        return x + 1
                """,
            },
        )
        result = scan_project(root)
        all_fns = [f for m in result.project_code.modules for f in m.functions]
        assert len(all_fns) == 1
        assert all_fns[0].__name__ == "preprocess"

    def test_finds_constants(self, project_factory):
        root = project_factory(
            package_name="fix_consts",
            files={
                "constants.py": """
                    from scidb import Parameter

                    SAMPLING_RATE_HZ = Parameter(1000, description="Hz")
                    DEFAULT_BANDPASS = Parameter((1.0, 40.0), description="LFP band")
                """,
            },
        )
        result = scan_project(root)
        all_constants = [c for m in result.project_code.modules for c in m.parameters]
        assert len(all_constants) == 2
        names = {name for name, _ in all_constants}
        assert names == {"SAMPLING_RATE_HZ", "DEFAULT_BANDPASS"}

        by_name = dict(all_constants)
        assert isinstance(by_name["SAMPLING_RATE_HZ"], Parameter)
        assert by_name["SAMPLING_RATE_HZ"] == 1000
        assert by_name["DEFAULT_BANDPASS"].description == "LFP band"

    def test_finds_path_inputs(self, project_factory):
        root = project_factory(
            package_name="fix_path_inputs",
            files={
                "paths.py": """
                    from scidb import PathInput

                    RAW_EMG = PathInput("{subject}/{trial}.mat", name="RAW_EMG")
                """,
            },
        )
        result = scan_project(root)
        all_path_inputs = [
            pi for m in result.project_code.modules for pi in m.path_inputs
        ]
        assert len(all_path_inputs) == 1
        name, obj = all_path_inputs[0]
        assert name == "RAW_EMG"
        assert obj.path_template == "{subject}/{trial}.mat"

    def test_finds_each_of_path_inputs_as_alternate_templates(self, project_factory):
        root = project_factory(
            package_name="fix_path_input_alts",
            files={
                "paths.py": """
                    from scidb import EachOf, PathInput

                    GAIT_DATA = EachOf(
                        PathInput("assessment/{subject}.mat", name="GAIT_DATA"),
                        PathInput("training/{subject}.mat", name="GAIT_DATA"),
                    )
                """,
            },
        )
        result = scan_project(root)
        all_path_inputs = [
            pi for m in result.project_code.modules for pi in m.path_inputs
        ]
        assert len(all_path_inputs) == 1
        name, obj = all_path_inputs[0]
        assert name == "GAIT_DATA"
        assert len(obj.alternatives) == 2

    def test_finds_multi_value_parameters(self, project_factory):
        root = project_factory(
            package_name="fix_sweeps",
            files={
                "sweeps.py": """
                    from scidb import Parameter

                    WINDOW_SECONDS = Parameter(10, 20, 30)
                """,
            },
        )
        result = scan_project(root)
        all_sweeps = [sw for m in result.project_code.modules for sw in m.parameters]
        assert len(all_sweeps) == 1
        name, obj = all_sweeps[0]
        assert name == "WINDOW_SECONDS"
        assert obj.alternatives == [10, 20, 30]

    def test_bare_each_of_not_counted_as_parameter_or_path_input(self, project_factory):
        """A bare EachOf of plain numbers is neither a Parameter (wrong type)
        nor a PathInput bundle (alternatives aren't PathInputs) -- it's
        simply not discoverable, same as an unwrapped literal value. Only a
        named, top-level Parameter declaration is GUI-visible."""
        root = project_factory(
            package_name="fix_bare_each_of",
            files={
                "vals.py": """
                    from scidb import EachOf

                    NOT_DISCOVERABLE = EachOf(1, 2, 3)
                """,
            },
        )
        result = scan_project(root)
        all_path_inputs = [
            pi for m in result.project_code.modules for pi in m.path_inputs
        ]
        all_params = [p for m in result.project_code.modules for p in m.parameters]
        assert all_path_inputs == []
        assert all_params == []

    def test_ignores_reexports_of_variables(self, project_factory):
        """A BaseVariable re-imported into another module must not be double-counted."""
        root = project_factory(
            package_name="fix_reexport",
            files={
                "variables.py": """
                    from scidb import BaseVariable
                    class RawSignal(BaseVariable):
                        schema_version = 1
                """,
                "pipeline.py": """
                    # Re-export should NOT count as a second discovery
                    from fix_reexport.variables import RawSignal  # noqa: F401
                """,
            },
        )
        result = scan_project(root)
        all_vars = [v for m in result.project_code.modules for v in m.variables]
        names = [v.__name__ for v in all_vars]
        assert names == ["RawSignal"]  # exactly once, in variables.py

    def test_ignores_reexports_of_pipeline_fcn(self, project_factory):
        root = project_factory(
            package_name="fix_fn_reexport",
            files={
                "functions.py": """
                    from scidb import scistack

                    @scistack
                    def preprocess(x):
                        return x + 1
                """,
                "pipeline.py": """
                    from fix_fn_reexport.functions import preprocess  # noqa: F401
                """,
            },
        )
        result = scan_project(root)
        all_fns = [f for m in result.project_code.modules for f in m.functions]
        assert len(all_fns) == 1

    def test_ignores_private_names(self, project_factory):
        root = project_factory(
            package_name="fix_private",
            files={
                "pipeline.py": """
                    from scidb import Parameter
                    _PRIVATE = Parameter(42)
                    PUBLIC = Parameter(43)
                """,
            },
        )
        result = scan_project(root)
        constants = [
            (name, c) for m in result.project_code.modules for name, c in m.parameters
        ]
        names = {name for name, _ in constants}
        assert "PUBLIC" in names
        assert "_PRIVATE" not in names


# ---------------------------------------------------------------------------
# scan_project: project-level scan end-to-end
# ---------------------------------------------------------------------------
class TestScanProject:
    def test_full_project_scan(self, project_factory):
        root = project_factory(
            package_name="full_study",
            files={
                "variables.py": """
                    from scidb import BaseVariable
                    class RawSignal(BaseVariable):
                        schema_version = 1
                    class FilteredSignal(BaseVariable):
                        schema_version = 1
                """,
                "functions.py": """
                    from scidb import scistack

                    @scistack
                    def preprocess(x):
                        return x

                    @scistack
                    def analyze(x):
                        return x
                """,
                "constants.py": """
                    from scidb import Parameter
                    SAMPLING_RATE_HZ = Parameter(1000)
                """,
            },
        )
        result = scan_project(root)
        assert isinstance(result, DiscoveryResult)
        assert result.project_code.name == "full_study"
        assert result.project_code.variable_count == 2
        assert result.project_code.function_count == 2
        assert result.project_code.parameter_count == 1
        assert result.project_code.is_empty is False
        assert result.libraries == {}  # no uv.lock

    def test_import_error_captured_not_thrown(self, project_factory):
        root = project_factory(
            package_name="fix_broken",
            files={
                "good.py": """
                    from scidb import Parameter
                    GOOD = Parameter(1)
                """,
                "broken.py": """
                    raise ImportError("intentional test failure")
                """,
            },
        )
        # scan_project should NOT raise — it should capture the error.
        result = scan_project(root)
        assert result.project_code.parameter_count == 1

        # The broken module should show up as an error.
        broken_errors = [
            e for e in result.project_code.errors if "broken" in e.module_name
        ]
        assert len(broken_errors) == 1
        assert "intentional test failure" in broken_errors[0].traceback

    def test_syntax_error_captured_not_thrown(self, project_factory):
        root = project_factory(
            package_name="fix_syntax",
            files={
                "good.py": """
                    from scidb import Parameter
                    GOOD = Parameter(1)
                """,
                "bad_syntax.py": """
                    def broken(:
                """,
            },
        )
        result = scan_project(root)
        assert result.project_code.parameter_count == 1
        syntax_errors = [
            e for e in result.project_code.errors if "bad_syntax" in e.module_name
        ]
        assert len(syntax_errors) == 1

    def test_missing_pyproject_toml(self, tmp_path):
        """scan_project with no pyproject.toml returns empty project_code gracefully."""
        (tmp_path / "some_file.txt").write_text("not a project")
        result = scan_project(tmp_path)
        assert result.project_code.is_empty
        assert result.project_code.name == "<unknown>"
        assert result.libraries == {}

    def test_missing_src_directory(self, project_factory):
        """pyproject.toml present but no src/{name}/ — graceful empty result."""
        root = project_factory(package_name="fix_no_src", files={})
        # Remove the scaffolded src/ to simulate a pre-src project.
        import shutil

        shutil.rmtree(root / "src")
        result = scan_project(root)
        assert result.project_code.is_empty
        assert result.project_code.name == "fix_no_src"

    def test_missing_uv_lock(self, project_factory):
        """No uv.lock → no libraries, no error."""
        root = project_factory(
            package_name="fix_no_lock",
            files={
                "constants.py": "from scidb import Parameter\nX = Parameter(1)\n",
            },
        )
        result = scan_project(root)
        assert result.libraries == {}
        assert result.project_code.parameter_count == 1

    def test_unicode_in_source(self, project_factory):
        """Non-ASCII in source should not break the scanner."""
        root = project_factory(
            package_name="fix_unicode",
            files={
                "constants.py": """
                    # coding: utf-8
                    from scidb import Parameter
                    GREEK = Parameter("αβγ", description="Greek letters: αβγ")
                """,
            },
        )
        result = scan_project(root)
        assert result.project_code.parameter_count == 1


# ---------------------------------------------------------------------------
# uv.lock integration: real scan of an installed package
# ---------------------------------------------------------------------------
class TestUvLockIntegration:
    def test_scans_library_from_uv_lock(self, project_factory):
        """
        Put scidb itself in uv.lock. The scanner should import it and
        find non-empty exports (scidb's own test_variables if any, or at
        minimum zero-error scan).
        """
        root = project_factory(
            package_name="fix_lib_scan",
            files={},
            uv_lock_packages=["scilineage"],  # scilineage is importable in test env
        )
        result = scan_project(root)
        assert "scilineage" in result.libraries
        scilineage_result = result.libraries["scilineage"]
        # No hard claim about exports — just that it scanned without fatal error
        # at the top-level package. There may be errors for individual submodules
        # that do optional imports, but the package itself should import.
        assert scilineage_result.name == "scilineage"

    def test_zero_export_library_returned_not_filtered(self, project_factory):
        """
        Scanner returns zero-export libraries unchanged. The panel layer
        (Phase 6) is responsible for filtering them.
        """
        # We'll use ``json`` — it's a stdlib module but we can still fake
        # it as a "library" by listing something installed that has no
        # scistack exports. Use "pytest" — installed in the test env,
        # definitely no BaseVariable subclasses.
        root = project_factory(
            package_name="fix_zero_export",
            files={},
            uv_lock_packages=["pytest"],
        )
        result = scan_project(root)
        assert "pytest" in result.libraries
        # pytest has zero scistack exports — it should be returned but empty.
        assert result.libraries["pytest"].is_empty
        # And the ``non_empty_libraries`` view should hide it.
        assert "pytest" not in result.non_empty_libraries()

    def test_missing_library_captured_as_error(self, project_factory):
        """
        A distribution listed in uv.lock but not installed is reported
        as a single error entry, not a thrown exception.
        """
        root = project_factory(
            package_name="fix_missing_lib",
            files={},
            uv_lock_packages=["nonexistent_package_xyz_12345"],
        )
        result = scan_project(root)
        assert "nonexistent_package_xyz_12345" in result.libraries
        pkg = result.libraries["nonexistent_package_xyz_12345"]
        assert len(pkg.errors) >= 1
        assert pkg.is_empty

    def test_skip_dists_filter(self, project_factory):
        """``skip_dists`` parameter skips named distributions entirely."""
        root = project_factory(
            package_name="fix_skip",
            files={},
            uv_lock_packages=["pytest", "scilineage"],
        )
        result = scan_project(root, skip_dists=["pytest"])
        assert "pytest" not in result.libraries
        assert "scilineage" in result.libraries

    def test_library_filter_callable(self, project_factory):
        root = project_factory(
            package_name="fix_filter",
            files={},
            uv_lock_packages=["pytest", "scilineage"],
        )
        result = scan_project(
            root,
            library_filter=lambda name: name.startswith("sci"),
        )
        assert "scilineage" in result.libraries
        assert "pytest" not in result.libraries

    def test_project_name_skipped_from_libraries(self, project_factory):
        """If the project name shows up in its own uv.lock, it's skipped."""
        root = project_factory(
            package_name="fix_self",
            files={},
            uv_lock_packages=["fix_self", "scilineage"],
        )
        result = scan_project(root)
        assert "fix_self" not in result.libraries
        assert "scilineage" in result.libraries


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
class TestHelpers:
    # read_project_name moved to scifor.discovery -- see
    # scifor/tests/test_discovery.py's TestReadProjectName for its direct
    # unit tests.

    def test_read_uv_lock_packages(self, tmp_path):
        (tmp_path / "uv.lock").write_text(
            textwrap.dedent(
                """
                version = 1

                [[package]]
                name = "numpy"
                version = "2.0.0"

                [[package]]
                name = "pandas"
                version = "2.0.0"
                """
            )
        )
        names = _read_uv_lock_packages(tmp_path)
        assert names == ["numpy", "pandas"]

    def test_read_uv_lock_missing(self, tmp_path):
        assert _read_uv_lock_packages(tmp_path) == []

    def test_dist_to_import_names_known_package(self):
        # pytest is definitely installed
        names = _dist_to_import_names("pytest")
        assert "pytest" in names or "_pytest" in names

    def test_dist_to_import_names_missing(self):
        assert _dist_to_import_names("definitely_not_a_real_package_xyz") == []


# ---------------------------------------------------------------------------
# ModuleExports properties
# ---------------------------------------------------------------------------
class TestModuleExportsProperties:
    def test_is_empty_when_no_exports(self):
        from scidb.discover import ModuleExports

        exports = ModuleExports(module_name="test")
        assert exports.is_empty
        assert exports.total_count == 0

    def test_is_empty_false_with_variable(self):
        from scidb.discover import ModuleExports

        exports = ModuleExports(module_name="test", variables=[object])
        assert not exports.is_empty
        assert exports.total_count == 1

    def test_is_empty_false_with_function(self):
        from scidb.discover import ModuleExports

        exports = ModuleExports(module_name="test", functions=[object])
        assert not exports.is_empty

    def test_is_empty_false_with_parameter(self):
        from scidb.discover import ModuleExports

        from scidb import Parameter

        c = Parameter(42)
        exports = ModuleExports(module_name="test", parameters=[("X", c)])
        assert not exports.is_empty
        assert exports.total_count == 1

    def test_is_empty_false_with_path_input(self):
        from scidb import PathInput
        from scidb.discover import ModuleExports

        pi = PathInput("{subject}.mat", name="{subject}.mat")
        exports = ModuleExports(module_name="test", path_inputs=[("X", pi)])
        assert not exports.is_empty
        assert exports.total_count == 1

    def test_is_empty_false_with_multi_value_parameter(self):
        from scidb import Parameter
        from scidb.discover import ModuleExports

        exports = ModuleExports(
            module_name="test", parameters=[("X", Parameter(1, 2))]
        )
        assert not exports.is_empty
        assert exports.total_count == 1

    def test_total_count_mixed(self):
        from scidb.discover import ModuleExports

        from scidb import Parameter, PathInput

        c = Parameter(42)
        exports = ModuleExports(
            module_name="test",
            variables=[object, object],
            functions=[object],
            parameters=[("X", c), ("Y", c), ("S", Parameter(1, 2))],
            path_inputs=[("P", PathInput("{s}.mat", name="{s}.mat"))],
        )
        assert exports.total_count == 7


# ---------------------------------------------------------------------------
# PackageResult properties
# ---------------------------------------------------------------------------
class TestPackageResultProperties:
    def test_counts_across_modules(self):
        from scidb.discover import ModuleExports, PackageResult

        from scidb import Parameter, PathInput

        m1 = ModuleExports(module_name="a", variables=[object, object])
        m2 = ModuleExports(
            module_name="b",
            functions=[object],
            parameters=[("X", Parameter(1)), ("S", Parameter(1, 2))],
            path_inputs=[("P", PathInput("{s}.mat", name="{s}.mat"))],
        )
        result = PackageResult(name="test", modules=[m1, m2])
        assert result.variable_count == 2
        assert result.function_count == 1
        assert result.parameter_count == 2
        assert result.path_input_count == 1

    def test_is_empty_all_empty_modules(self):
        from scidb.discover import ModuleExports, PackageResult

        m1 = ModuleExports(module_name="a")
        m2 = ModuleExports(module_name="b")
        result = PackageResult(name="test", modules=[m1, m2])
        assert result.is_empty

    def test_is_empty_no_modules(self):
        from scidb.discover import PackageResult

        result = PackageResult(name="test")
        assert result.is_empty

    def test_is_empty_false_if_any_module_has_exports(self):
        from scidb.discover import ModuleExports, PackageResult

        m1 = ModuleExports(module_name="a")
        m2 = ModuleExports(module_name="b", variables=[object])
        result = PackageResult(name="test", modules=[m1, m2])
        assert not result.is_empty


# ---------------------------------------------------------------------------
# DiscoveryResult.non_empty_libraries
# ---------------------------------------------------------------------------
class TestDiscoveryResultNonEmptyLibraries:
    def test_filters_empty_libraries(self):
        from scidb.discover import ModuleExports, PackageResult

        empty_pkg = PackageResult(
            name="empty_lib", modules=[ModuleExports(module_name="e")]
        )
        nonempty_pkg = PackageResult(
            name="good_lib",
            modules=[ModuleExports(module_name="g", variables=[object])],
        )
        result = DiscoveryResult(
            project_code=PackageResult(name="proj"),
            libraries={"empty_lib": empty_pkg, "good_lib": nonempty_pkg},
        )
        non_empty = result.non_empty_libraries()
        assert "good_lib" in non_empty
        assert "empty_lib" not in non_empty

    def test_all_empty_returns_empty_dict(self):
        from scidb.discover import ModuleExports, PackageResult

        empty1 = PackageResult(name="a", modules=[ModuleExports(module_name="a")])
        empty2 = PackageResult(name="b", modules=[ModuleExports(module_name="b")])
        result = DiscoveryResult(
            project_code=PackageResult(name="proj"),
            libraries={"a": empty1, "b": empty2},
        )
        assert result.non_empty_libraries() == {}


# ---------------------------------------------------------------------------
# PathInsert/purge_module moved to scifor.discovery -- see
# scifor/tests/test_discovery.py for their direct unit tests.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# scan_project: deep submodule hierarchy
# ---------------------------------------------------------------------------
class TestDeepSubmodules:
    def test_deep_nested_modules_scanned(self, project_factory):
        root = project_factory(
            package_name="fix_deep",
            files={
                "level1/__init__.py": "",
                "level1/level2/__init__.py": "",
                "level1/level2/constants.py": """
                    from scidb import Parameter
                    DEEP_CONST = Parameter(42, description="deeply nested")
                """,
            },
        )
        result = scan_project(root)
        all_constants = [
            (name, c) for m in result.project_code.modules for name, c in m.parameters
        ]
        names = {name for name, _ in all_constants}
        assert "DEEP_CONST" in names

    def test_init_defines_exports(self, project_factory):
        """Exports defined in __init__.py should be discovered."""
        root = project_factory(
            package_name="fix_init_exports",
            files={
                "__init__.py": """
                    from scidb import Parameter
                    INIT_CONST = Parameter(99, description="from init")
                """,
            },
        )
        result = scan_project(root)
        all_constants = [
            (name, c) for m in result.project_code.modules for name, c in m.parameters
        ]
        names = {name for name, _ in all_constants}
        assert "INIT_CONST" in names

    def test_empty_package_no_error(self, project_factory):
        """A package with only __init__.py and no modules should scan cleanly."""
        root = project_factory(
            package_name="fix_empty_pkg",
            files={},
        )
        result = scan_project(root)
        assert result.project_code.is_empty
        assert result.project_code.errors == []


# ---------------------------------------------------------------------------
# scan_project: multiple types in one module
# ---------------------------------------------------------------------------
class TestMixedModuleExports:
    def test_all_types_in_one_module(self, project_factory):
        root = project_factory(
            package_name="fix_mixed",
            files={
                "everything.py": """
                    from scidb import BaseVariable, Parameter
                    from scidb import scistack

                    class MixedVar(BaseVariable):
                        schema_version = 1

                    @scistack
                    def mixed_fn(x):
                        return x

                    MIXED_CONST = Parameter(42)
                """,
            },
        )
        result = scan_project(root)
        by_module = {m.module_name: m for m in result.project_code.modules}
        assert "fix_mixed.everything" in by_module

        mod = by_module["fix_mixed.everything"]
        assert len(mod.variables) == 1
        assert mod.variables[0].__name__ == "MixedVar"
        assert len(mod.functions) == 1
        assert mod.functions[0].__name__ == "mixed_fn"
        assert len(mod.parameters) == 1
        assert mod.parameters[0][0] == "MIXED_CONST"


# ---------------------------------------------------------------------------
# scan_project: BaseVariable itself not collected
# ---------------------------------------------------------------------------
class TestBaseVariableNotCollected:
    def test_base_variable_not_in_results(self, project_factory):
        """BaseVariable itself must be excluded even if imported."""
        root = project_factory(
            package_name="fix_base",
            files={
                "variables.py": """
                    from scidb import BaseVariable
                    # Only re-imports BaseVariable, no subclass
                """,
            },
        )
        result = scan_project(root)
        all_vars = [v for m in result.project_code.modules for v in m.variables]
        assert len(all_vars) == 0


# ---------------------------------------------------------------------------
# scan_package / scan_project: test-only content excluded from discovery
#
# is_test_path/is_test_modname themselves moved to scifor.discovery (the
# generic, scidb-unaware walking harness) -- see scifor/tests/test_discovery.py
# for their direct unit tests. These tests exercise the end-to-end behavior
# through scan_project, which now delegates its walking to scifor.discovery.
# ---------------------------------------------------------------------------
class TestExcludesTestOnlyContent:
    def test_tests_subpackage_excluded_from_scan_project(self, project_factory):
        root = project_factory(
            package_name="fix_test_exclusion",
            files={
                "prod.py": """
                    from scidb import Parameter
                    PROD_CONST = Parameter(1, description="production")
                """,
                "tests/__init__.py": "",
                "tests/helpers.py": """
                    from scidb import Parameter
                    TEST_ONLY_CONST = Parameter(2, description="test-only")
                """,
            },
        )
        result = scan_project(root)
        all_constants = [
            (name, c) for m in result.project_code.modules for name, c in m.parameters
        ]
        names = {name for name, _ in all_constants}
        assert "PROD_CONST" in names
        assert "TEST_ONLY_CONST" not in names

    def test_test_prefixed_module_excluded(self, project_factory):
        root = project_factory(
            package_name="fix_test_module_exclusion",
            files={
                "prod.py": """
                    from scidb import Parameter
                    PROD_CONST = Parameter(1)
                """,
                "test_helpers.py": """
                    from scidb import Parameter
                    TEST_ONLY_CONST = Parameter(2)
                """,
            },
        )
        result = scan_project(root)
        all_constants = [
            (name, c) for m in result.project_code.modules for name, c in m.parameters
        ]
        names = {name for name, _ in all_constants}
        assert "PROD_CONST" in names
        assert "TEST_ONLY_CONST" not in names
