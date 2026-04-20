"""
Tests for MATLAB support: parser, registry, and command generation.
"""

import textwrap
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# matlab_parser tests
# ---------------------------------------------------------------------------


class TestParseMatlabFunction:
    def test_basic_function(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "bandpass_filter.m"
        f.write_text(textwrap.dedent("""\
            function [filtered] = bandpass_filter(signal, low_hz, high_hz)
            % BANDPASS_FILTER  Apply a bandpass filter.
                filtered = signal * low_hz;
            end
        """))

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "bandpass_filter"
        assert info.params == ["signal", "low_hz", "high_hz"]
        assert info.language == "matlab"
        assert len(info.source_hash) == 64  # SHA-256 hex
        assert info.n_outputs == 1  # [filtered]

    def test_single_output(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "compute_vo2.m"
        f.write_text("function result = compute_vo2(breath_data)\n  result = 0;\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "compute_vo2"
        assert info.params == ["breath_data"]
        assert info.n_outputs == 1

    def test_no_output(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "plot_results.m"
        f.write_text("function plot_results(data, title_str)\n  plot(data);\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "plot_results"
        assert info.params == ["data", "title_str"]
        assert info.n_outputs == 0

    def test_no_params(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "setup.m"
        f.write_text("function setup()\n  disp('hi');\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "setup"
        assert info.params == []

    def test_not_a_function(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "script.m"
        f.write_text("% Just a script\nx = 5;\n")

        info = parse_matlab_function(f)
        assert info is None

    def test_missing_file(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        info = parse_matlab_function(tmp_path / "nonexistent.m")
        assert info is None

    def test_multiple_outputs(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "decompose.m"
        f.write_text("function [amp, phase, freq] = decompose(signal, fs)\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "decompose"
        assert info.params == ["signal", "fs"]
        assert info.n_outputs == 3  # [amp, phase, freq]

    def test_source_hash_changes(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "foo.m"
        f.write_text("function y = foo(x)\n  y = x;\nend\n")
        info1 = parse_matlab_function(f)

        f.write_text("function y = foo(x)\n  y = x * 2;\nend\n")
        info2 = parse_matlab_function(f)

        assert info1.source_hash != info2.source_hash


class TestParseMatlabVariable:
    def test_basic_classdef(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "RawSignal.m"
        f.write_text(textwrap.dedent("""\
            classdef RawSignal < scidb.BaseVariable
                % Raw EMG signal data
            end
        """))

        name = parse_matlab_variable(f)
        assert name == "RawSignal"

    def test_not_base_variable(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "MyClass.m"
        f.write_text("classdef MyClass < handle\nend\n")

        name = parse_matlab_variable(f)
        assert name is None

    def test_custom_base(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "Foo.m"
        f.write_text("classdef Foo < mylib.BaseVariable\nend\n")

        name = parse_matlab_variable(f)
        assert name == "Foo"

    def test_missing_file(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        name = parse_matlab_variable(tmp_path / "nonexistent.m")
        assert name is None

    def test_no_classdef(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "script.m"
        f.write_text("% Just a script\n")

        name = parse_matlab_variable(f)
        assert name is None


# ---------------------------------------------------------------------------
# matlab_command tests
# ---------------------------------------------------------------------------


class TestGenerateMatlabCommand:
    def test_template_no_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
        )

        assert "bandpass_filter" in cmd
        assert "/data/experiment.duckdb" in cmd
        assert "scihist.configure_database" in cmd
        assert "scihist.for_each" in cmd

    def test_with_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {"signal": "RawSignal"},
                "output_type": "FilteredSignal",
                "constants": {"low_hz": 20},
                "record_count": 4,
            }
        ]

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
            variants=variants,
        )

        assert "scidb.register_variable('FilteredSignal')" in cmd
        assert "scidb.register_variable('RawSignal')" in cmd
        assert "@bandpass_filter" in cmd
        assert "RawSignal()" in cmd
        assert "{FilteredSignal()}" in cmd
        assert "20" in cmd

    def test_addpath(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="foo",
            db_path="/data/db.duckdb",
            schema_keys=["subject"],
            addpath_dirs=["/home/user/matlab/lib", "/home/user/shared"],
        )

        assert "addpath('/home/user/matlab/lib')" in cmd
        assert "addpath('/home/user/shared')" in cmd

    def test_schema_filter(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {"x": "X"},
                "output_type": "Y",
                "constants": {},
                "record_count": 2,
            }
        ]

        cmd = generate_matlab_command(
            function_name="process",
            db_path="/data/db.duckdb",
            schema_keys=["subject", "session"],
            variants=variants,
            schema_filter={"subject": [1, 2, 3]},
        )

        assert "'subject'" in cmd
        assert "[1 2 3]" in cmd

    def test_string_schema_filter(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {},
                "output_type": "Y",
                "constants": {},
                "record_count": 1,
            }
        ]

        cmd = generate_matlab_command(
            function_name="process",
            db_path="/db.duckdb",
            schema_keys=["session"],
            variants=variants,
            schema_filter={"session": ["pre", "post"]},
        )

        assert '"pre"' in cmd
        assert '"post"' in cmd

    def test_deduplicates_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        # Same constants, different output types → should deduplicate.
        variants = [
            {
                "input_types": {"x": "X"},
                "output_type": "Y1",
                "constants": {"k": 5},
                "record_count": 1,
            },
            {
                "input_types": {"x": "X"},
                "output_type": "Y2",
                "constants": {"k": 5},
                "record_count": 1,
            },
        ]

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            variants=variants,
        )

        # Should only have one for_each call.
        assert cmd.count("scihist.for_each") == 1

    def test_escape_single_quotes(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/path/with'quote/db.duckdb",
            schema_keys=["s"],
        )

        assert "/path/with''quote/db.duckdb" in cmd

    def test_pyenv_preamble_present(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable="/usr/bin/python3",
        )

        # Stage 1: bind
        assert "pyenv('Version', scistack_pyenv_target__)" in cmd
        assert "scistack_pyenv_target__ = '/usr/bin/python3';" in cmd
        assert 'if scistack_pyenv__.Status == "NotLoaded"' in cmd
        assert "SciStack:PyenvMismatch" in cmd
        # Stage 2: force-load (smoke test)
        assert "py.sys.version" in cmd
        # Stage 3: diagnostic dump on smoke-test failure
        assert "OutOfProcess" in cmd
        # Stage 4: pre-import scidb so py.scidb.* is warm
        assert "py.importlib.import_module('scidb')" in cmd
        # Teardown: clear all temporaries
        assert "clear scistack_pyenv__ scistack_pyenv_target__" in cmd
        # clear functions is NOT emitted — it breaks py.list inside package
        # functions (MATLAB resolves py.X as a module lookup post-cache-clear,
        # which fails for builtins like list).
        assert "clear functions" not in cmd

    def test_pyenv_preamble_omitted_when_none(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=None,
        )

        assert "pyenv" not in cmd

    def test_pyenv_preamble_escapes_single_quotes(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable="/tmp/O'Neil/python",
        )

        # Single quote in path must be doubled inside the MATLAB literal.
        assert "scistack_pyenv_target__ = '/tmp/O''Neil/python';" in cmd

    def test_pyenv_preamble_windows_path(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=r"C:\Users\mtillman\venvs\stim-device-comparison\Scripts\python.exe",
        )

        # Backslashes converted to forward slashes for the MATLAB literal.
        assert (
            "scistack_pyenv_target__ = "
            "'C:/Users/mtillman/venvs/stim-device-comparison/Scripts/python.exe';"
        ) in cmd
        assert "\\" not in cmd.split("scistack_pyenv_target__ =")[1].splitlines()[0]

    def test_pyenv_preamble_mismatch_uses_normalized_compare(self):
        """The mismatch check must tolerate backslash/forward-slash differences
        between what MATLAB's pyenv returns and our target literal.
        Regression: previously ``string(Executable) ~= string(target)`` fired
        erroneously when Status=Loaded and the paths differed only in separators.
        """
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=r"C:\Users\mtillman\venvs\scistack-gui\.venv\Scripts\python.exe",
        )

        # The comparison MUST use a path normalizer (strrep + strcmpi), not a
        # raw string equality.
        assert "scistack_norm_path__" in cmd
        # MATLAB literal: strrep(char(p), '\', '/')  (single backslash in MATLAB).
        assert "strrep(char(p), '\\', '/')" in cmd
        assert "strcmpi(" in cmd
        # And the raw mismatching pattern must NOT be present.
        assert (
            "string(scistack_pyenv__.Executable) ~= string(scistack_pyenv_target__)"
            not in cmd
        )

    def test_pyenv_preamble_ordered_before_addpath(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            addpath_dirs=["/home/user/matlab/lib"],
            python_executable="/usr/bin/python3",
        )

        pyenv_idx = cmd.index("scistack_pyenv_target__")
        addpath_idx = cmd.index("addpath(")
        assert pyenv_idx < addpath_idx


# ---------------------------------------------------------------------------
# _format_path_input tests
# ---------------------------------------------------------------------------


class TestFormatPathInput:
    def test_explicit_root_folder_used_as_is(self):
        from scistack_gui.api.matlab_command import _format_path_input
        pi = {"template": "{subject}/data.mat", "root_folder": "/my/data"}
        result = _format_path_input(pi)
        assert result == 'scifor.PathInput("{subject}/data.mat", root_folder="/my/data")'

    def test_no_root_folder_no_project_root(self):
        from scistack_gui.api.matlab_command import _format_path_input
        pi = {"template": "{subject}/data.mat", "root_folder": None}
        result = _format_path_input(pi)
        assert result == 'scifor.PathInput("{subject}/data.mat")'

    def test_relative_template_uses_project_root_when_no_root_folder(self):
        from scistack_gui.api.matlab_command import _format_path_input
        pi = {"template": "{subject}/data.mat", "root_folder": None}
        result = _format_path_input(pi, project_root="/projects/myexp")
        assert result == 'scifor.PathInput("{subject}/data.mat", root_folder="/projects/myexp")'

    def test_explicit_root_folder_takes_priority_over_project_root(self):
        from scistack_gui.api.matlab_command import _format_path_input
        pi = {"template": "{subject}/data.mat", "root_folder": "/explicit/root"}
        result = _format_path_input(pi, project_root="/projects/myexp")
        assert result == 'scifor.PathInput("{subject}/data.mat", root_folder="/explicit/root")'

    def test_absolute_template_ignores_project_root(self):
        from scistack_gui.api.matlab_command import _format_path_input
        pi = {"template": "/absolute/path/{subject}.mat", "root_folder": None}
        result = _format_path_input(pi, project_root="/projects/myexp")
        assert result == 'scifor.PathInput("/absolute/path/{subject}.mat")'

    def test_generate_matlab_command_injects_project_root_for_path_inputs(self):
        from scistack_gui.api.matlab_command import generate_matlab_command
        cmd = generate_matlab_command(
            function_name="load_file",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            path_inputs={"filepath": {"template": "{subject}/data.mat", "root_folder": None}},
            project_root="/projects/myexp",
        )
        assert 'root_folder="/projects/myexp"' in cmd


# ---------------------------------------------------------------------------
# config MATLAB parsing tests
# ---------------------------------------------------------------------------


class TestConfigMatlabParsing:
    def test_pyproject_with_matlab(self, tmp_path):
        from scistack_gui.config import load_config

        # Create a pyproject.toml with MATLAB section.
        (tmp_path / "pyproject.toml").write_text(textwrap.dedent("""\
            [tool.scistack]
            modules = []

            [tool.scistack.matlab]
            functions = ["matlab/bandpass_filter.m"]
            variables = ["matlab/types/*.m"]
            variable_dir = "matlab/types"
        """))

        # Create the referenced files.
        (tmp_path / "matlab").mkdir()
        (tmp_path / "matlab" / "types").mkdir()

        (tmp_path / "matlab" / "bandpass_filter.m").write_text(
            "function y = bandpass_filter(x)\ny = x;\nend\n"
        )
        (tmp_path / "matlab" / "types" / "RawSignal.m").write_text(
            "classdef RawSignal < scidb.BaseVariable\nend\n"
        )

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert len(config.matlab_functions) == 1
        assert config.matlab_functions[0].name == "bandpass_filter.m"
        assert len(config.matlab_variables) == 1
        assert config.matlab_variables[0].name == "RawSignal.m"
        # addpath is auto-derived from parent dirs of functions, variables, and variable_dir
        assert len(config.matlab_addpath) == 2
        # Paths are stored in absolute-but-not-UNC-canonicalized form (see
        # config._normalize); compare against that form, not .resolve().
        addpath_set = set(config.matlab_addpath)
        assert (tmp_path / "matlab") in addpath_set
        assert (tmp_path / "matlab" / "types") in addpath_set
        assert config.matlab_variable_dir == (tmp_path / "matlab" / "types")

    def test_scistack_toml(self, tmp_path):
        from scistack_gui.config import load_config

        # Create a scistack.toml (standalone, no pyproject.toml).
        (tmp_path / "scistack.toml").write_text(textwrap.dedent("""\
            modules = []

            [matlab]
            functions = ["process.m"]
        """))

        (tmp_path / "process.m").write_text(
            "function y = process(x)\ny = x;\nend\n"
        )

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert len(config.matlab_functions) == 1

    def test_no_matlab_section(self, tmp_path):
        from scistack_gui.config import load_config

        (tmp_path / "pyproject.toml").write_text(textwrap.dedent("""\
            [tool.scistack]
            modules = []
        """))

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert config.matlab_functions == []
        assert config.matlab_variables == []
        assert config.matlab_addpath == []
        assert config.matlab_variable_dir is None


# ---------------------------------------------------------------------------
# sci-matlab MATLAB directory discovery
# ---------------------------------------------------------------------------


class TestGenerateMatlabCommandOutputTypes:
    """Regression: MATLAB function output param names must not leak into the
    generated MATLAB command as BaseVariable class names.

    A function declared as ``function [time, force_left, force_right] = load_csv(f)``
    has output *parameter names* ``time`` / ``force_left`` / ``force_right``.  The
    actual BaseVariable class names are ``Time`` / ``Force_Left`` / ``Force_Right``
    (whatever is wired to the function node's output handles in the GUI).
    The generated MATLAB command must use the class names, not the param names.
    """

    def test_output_types_from_variants_use_class_names(self):
        """When DB variants exist, output_type (class name) must appear in
        the outputs cell array, not the function's output parameter names."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {},
                "output_type": "Time",
                "constants": {},
                "record_count": 1,
            },
            {
                "input_types": {},
                "output_type": "Force_Left",
                "constants": {},
                "record_count": 1,
            },
            {
                "input_types": {},
                "output_type": "Force_Right",
                "constants": {},
                "record_count": 1,
            },
        ]

        cmd = generate_matlab_command(
            function_name="load_csv",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            variants=variants,
        )

        assert "Time()" in cmd
        assert "Force_Left()" in cmd
        assert "Force_Right()" in cmd
        # Lowercase param names must NOT appear as class instantiations
        assert "time()" not in cmd
        assert "force_left()" not in cmd
        assert "force_right()" not in cmd

    def test_output_types_with_no_variants_uses_provided_output_types(self):
        """When no DB variants exist and output_types are provided, the class
        names should appear (not lowercase function param names)."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_csv",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            variants=None,
            output_types=["Time", "Force_Left", "Force_Right"],
        )

        assert "Time()" in cmd
        assert "Force_Left()" in cmd
        assert "Force_Right()" in cmd
        assert "time()" not in cmd
        assert "force_left()" not in cmd
        assert "force_right()" not in cmd


class TestSortInferredByParamsOrder:
    def test_reorders_to_match_params(self):
        from scistack_gui.services.matlab_command_service import _sort_inferred_by_params_order

        inferred = ["Force_Right", "Force_Left", "Time"]
        params = ["time", "force_left", "force_right"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Force_Right"]

    def test_passthrough_when_already_ordered(self):
        from scistack_gui.services.matlab_command_service import _sort_inferred_by_params_order

        inferred = ["Time", "Force_Left", "Force_Right"]
        params = ["time", "force_left", "force_right"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Force_Right"]

    def test_unmatched_appended_at_end(self):
        from scistack_gui.services.matlab_command_service import _sort_inferred_by_params_order

        inferred = ["Extra", "Time", "Force_Left"]
        params = ["time", "force_left"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Extra"]

    def test_empty_params_preserves_inferred_order(self):
        from scistack_gui.services.matlab_command_service import _sort_inferred_by_params_order

        inferred = ["Force_Right", "Time"]
        result = _sort_inferred_by_params_order(inferred, [])
        assert result == ["Force_Right", "Time"]


class TestMatlabFnProxyHash:
    """Fix A — the proxy hash must match what MATLAB's scidb.LineageFcn(fn)
    (unpack_output=false default) produces, so scihist.check_node_state does
    not report every combo as "stale: function hash changed"."""

    def test_proxy_uses_unpack_false(self, monkeypatch):
        from hashlib import sha256

        from scistack_gui.api.pipeline import _build_matlab_fn_proxy
        from scistack_gui import matlab_registry as _mr

        class FakeInfo:
            source_hash = "a" * 64
            n_outputs = 3
            params = ("x",)
            output_names = ("a", "b", "c")

        monkeypatch.setattr(
            _mr, "get_matlab_function", lambda _name: FakeInfo()
        )

        proxy = _build_matlab_fn_proxy("load_csv")
        expected = sha256(f"{FakeInfo.source_hash}-False".encode()).hexdigest()
        assert proxy.hash == expected
        assert proxy.unpack_output is False

    def test_single_output_hash_also_unpack_false(self, monkeypatch):
        from hashlib import sha256

        from scistack_gui.api.pipeline import _build_matlab_fn_proxy
        from scistack_gui import matlab_registry as _mr

        class FakeInfo:
            source_hash = "b" * 64
            n_outputs = 1
            params = ()
            output_names = ("only",)

        monkeypatch.setattr(
            _mr, "get_matlab_function", lambda _name: FakeInfo()
        )
        proxy = _build_matlab_fn_proxy("fn")
        expected = sha256(f"{FakeInfo.source_hash}-False".encode()).hexdigest()
        assert proxy.hash == expected


class TestFindSciMatlabMatlabDir:
    def test_finds_matlab_dir(self):
        """sci-matlab is installed in this environment; its matlab/ dir must be found."""
        from scistack_gui.server import _find_sci_matlab_matlab_dir
        from pathlib import Path

        result = _find_sci_matlab_matlab_dir()
        assert result is not None, (
            "sci-matlab is installed but _find_sci_matlab_matlab_dir returned None"
        )
        d = Path(result)
        assert d.is_dir(), f"Expected a directory at {result}"
        # The directory must contain the +scihist, +scidb, +scifor MATLAB packages.
        assert (d / "+scihist").is_dir(), f"+scihist not found under {result}"
        assert (d / "+scidb").is_dir(), f"+scidb not found under {result}"
        assert (d / "+scifor").is_dir(), f"+scifor not found under {result}"

    def test_close_database_helper_present(self):
        """Regression: +scidb/close_database.m must exist so matlab_command.py
        can call scidb.close_database(db) for post-close lock-release logging.
        """
        from scistack_gui.server import _find_sci_matlab_matlab_dir
        from pathlib import Path

        result = _find_sci_matlab_matlab_dir()
        assert result is not None
        close_db = Path(result) / "+scidb" / "close_database.m"
        assert close_db.exists(), (
            f"scidb.close_database not found at {close_db}"
        )
        contents = close_db.read_text()
        # The RELEASED log MUST fire after close returns, not before.
        release_idx = contents.find("DuckDB lock RELEASED")
        close_idx = contents.find("db.close()")
        assert 0 < close_idx < release_idx, (
            "RELEASED log must appear after db.close() in close_database.m"
        )
        # A close error must be logged and rethrown (not silently swallowed).
        assert "db.close FAILED" in contents
        assert "rethrow(close_err__)" in contents

    def test_scihist_configure_database_present(self):
        """Regression: +scihist/configure_database.m must exist so MATLAB can call it."""
        from scistack_gui.server import _find_sci_matlab_matlab_dir
        from pathlib import Path

        result = _find_sci_matlab_matlab_dir()
        assert result is not None
        cfg_db = Path(result) / "+scihist" / "configure_database.m"
        assert cfg_db.exists(), (
            f"scihist.configure_database not found at {cfg_db}"
        )
