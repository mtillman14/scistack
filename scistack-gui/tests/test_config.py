"""Tests for scistack_gui.config — config loading edge cases."""

import os
import sys
from pathlib import Path

import pytest

# Ensure the local package is importable.
sys.path.insert(0, str(Path(__file__).parent.parent))

from scistack_gui.config import (
    SciStackConfig,
    _extract_scistack_section,
    _normalize,
    add_path,
    clear_entities_file,
    load_config,
    locate_config_at,
    remove_path,
    resolve_project_root,
    set_entities_file,
    set_project_root_hint,
    tomllib,
)


# The project-root hint is pinned to tmp_path for every test by conftest's
# autouse `_pin_project_root` fixture -- without it, a project with no config
# file resolves its root to the server's working directory (the repo, under
# pytest). Tests that need a *different* root call set_project_root_hint
# themselves; see test_set_entities_file_outside_project_root_written_absolute.

# ---------------------------------------------------------------------------
# _extract_scistack_section
# ---------------------------------------------------------------------------


def test_empty_scistack_toml_returns_empty_dict():
    """An empty scistack.toml should return {} (valid all-defaults config)."""
    result = _extract_scistack_section({}, "scistack.toml")
    assert result == {}


def test_scistack_toml_with_content():
    """A scistack.toml with actual content returns that content."""
    data = {"modules": ["foo.py"], "auto_discover": False}
    result = _extract_scistack_section(data, "scistack.toml")
    assert result == data


def test_pyproject_with_scistack_section():
    """pyproject.toml with [tool.scistack] returns the section."""
    data = {"tool": {"scistack": {"modules": ["bar.py"]}}}
    result = _extract_scistack_section(data, "pyproject.toml")
    assert result == {"modules": ["bar.py"]}


def test_pyproject_without_scistack_section():
    """pyproject.toml without [tool.scistack] returns None."""
    data = {"tool": {"black": {"line-length": 88}}}
    result = _extract_scistack_section(data, "pyproject.toml")
    assert result is None


# ---------------------------------------------------------------------------
# load_config — integration tests using tmp_path
# ---------------------------------------------------------------------------


def test_empty_scistack_toml_loads_defaults(tmp_path):
    """An empty scistack.toml should produce a SciStackConfig with defaults."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text("")  # empty file → parsed as {}

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert isinstance(config, SciStackConfig)
    assert config.project_root == tmp_path
    assert config.modules == []
    assert config.packages == []
    assert config.auto_discover is True


def test_pyproject_without_scistack_section_loads_defaults(tmp_path):
    """A pyproject.toml lacking [tool.scistack] should use all defaults."""
    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text("[tool.black]\nline-length = 88\n")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert isinstance(config, SciStackConfig)
    assert config.project_root == tmp_path
    assert config.modules == []
    assert config.packages == []
    assert config.auto_discover is True


def test_packaged_project_auto_folds_own_name_into_packages(tmp_path):
    """A packaged project ([project].name + src/{name}/, no explicit
    [tool.scistack] packages entry) should have its own code auto-folded
    into config.packages -- otherwise registry.load_from_config never
    loads it, and a function shown in the "Discovered Code" panel would
    raise KeyError at actual run time (the packaged-mode execution gap)."""
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "my_pkg"\nversion = "0.1.0"\n[tool.scistack]\n'
    )
    (tmp_path / "src" / "my_pkg").mkdir(parents=True)
    (tmp_path / "src" / "my_pkg" / "__init__.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.packages == ["my_pkg"]


def test_packaged_project_already_listed_not_duplicated(tmp_path):
    """If the project's own name is already in [tool.scistack] packages,
    auto-fold must not add a second entry."""
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "my_pkg"\nversion = "0.1.0"\n'
        '[tool.scistack]\npackages = ["my_pkg"]\n'
    )
    (tmp_path / "src" / "my_pkg").mkdir(parents=True)
    (tmp_path / "src" / "my_pkg" / "__init__.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.packages == ["my_pkg"]


def test_packaged_project_without_src_layout_not_auto_folded(tmp_path):
    """No src/{name}/ directory -- matches scan_project's own precondition,
    so auto-fold must not add a name that isn't actually importable this way."""
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "my_pkg"\nversion = "0.1.0"\n[tool.scistack]\n'
    )

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.packages == []


def test_directory_without_any_toml_falls_back_to_folder_scan(tmp_path):
    """A directory with no toml files no longer raises — it falls back to
    scanning the directory directly for .py/.m files (zero-config mode for
    loose-script projects with no pyproject.toml/scistack.toml at all)."""
    (tmp_path / "pipeline.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert isinstance(config, SciStackConfig)
    assert [p.name for p in config.modules] == ["pipeline.py"]


def test_explicit_config_found_does_not_fall_back_to_folder_scan(tmp_path):
    """When a scistack.toml IS found, unlisted stray .py files must not
    leak in via folder-scan — only explicit config fields apply."""
    (tmp_path / "scistack.toml").write_text("")  # empty = all defaults
    (tmp_path / "stray.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.modules == []


def test_pyproject_with_scistack_section_loads_normally(tmp_path):
    """Happy path: pyproject.toml with [tool.scistack] works as before."""
    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text(
        '[tool.scistack]\nmodules = ["pipeline.py"]\nauto_discover = false\n'
    )
    # Create the module file so we don't get a warning
    (tmp_path / "pipeline.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert isinstance(config, SciStackConfig)
    assert len(config.modules) == 1
    assert config.auto_discover is False


# ---------------------------------------------------------------------------
# modules — directory and glob support
# ---------------------------------------------------------------------------


def test_modules_directory_recursively_discovers_py_files(tmp_path):
    """A directory entry in modules should recursively find all .py files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["lib"]')

    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "alpha.py").write_text("")
    (lib / "beta.py").write_text("")
    sub = lib / "sub"
    sub.mkdir()
    (sub / "gamma.py").write_text("")
    # Non-.py files should be ignored
    (lib / "readme.txt").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["alpha", "beta", "gamma"]


def test_modules_glob_pattern(tmp_path):
    """A glob pattern in modules should match only .py files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["src/**/*.py"]')

    src = tmp_path / "src"
    src.mkdir()
    (src / "one.py").write_text("")
    (src / "two.py").write_text("")
    (src / "data.csv").write_text("")
    nested = src / "nested"
    nested.mkdir()
    (nested / "three.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["one", "three", "two"]


def test_modules_mixed_files_dirs_and_globs(tmp_path):
    """modules list can mix individual files, directories, and globs."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["single.py", "lib_dir", "extra/*.py"]')

    (tmp_path / "single.py").write_text("")

    lib_dir = tmp_path / "lib_dir"
    lib_dir.mkdir()
    (lib_dir / "a.py").write_text("")
    (lib_dir / "b.py").write_text("")

    extra = tmp_path / "extra"
    extra.mkdir()
    (extra / "c.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["a", "b", "c", "single"]


def test_modules_empty_directory_warns(tmp_path, caplog):
    """A directory with no .py files should log a warning."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["empty_dir"]')

    (tmp_path / "empty_dir").mkdir()

    import logging

    with caplog.at_level(logging.WARNING):
        config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    assert config.modules == []
    assert "no .py files" in caplog.text


def test_modules_directory_excludes_noise_directories(tmp_path):
    """A directory entry in modules should prune .venv/node_modules/etc.,
    the same way folder-scan mode already does -- otherwise transitioning
    a loose project from folder-scan to an explicit scistack.toml with
    modules = ["."] would newly sweep in noise dirs it never used to."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["lib"]')

    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "real.py").write_text("")
    venv = lib / ".venv" / "site-packages"
    venv.mkdir(parents=True)
    (venv / "noise.py").write_text("")
    node_modules = lib / "node_modules" / "pkg"
    node_modules.mkdir(parents=True)
    (node_modules / "noise2.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["real"]


def test_modules_directory_excludes_test_files_and_dirs(tmp_path):
    """Anything under a tests/ dir, or named test_*.py/*_test.py, should be
    excluded from discovery -- these are found exclusively in a test and
    must not leak into functions/variables/constants/PathInputs/Sweeps."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["lib"]')

    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "prod.py").write_text("")
    (lib / "test_helper.py").write_text("")
    (lib / "helper_test.py").write_text("")
    tests_dir = lib / "tests"
    tests_dir.mkdir()
    (tests_dir / "fixtures.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["prod"]


def test_explicit_single_test_file_module_entry_still_excluded(tmp_path):
    """Even an explicitly-listed single file is excluded if it's a test file
    -- there is no override for explicit config entries."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["tests/test_x.py", "prod.py"]')

    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("")
    (tmp_path / "prod.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["prod"]


def test_modules_glob_excludes_test_files(tmp_path):
    """A glob pattern in modules should not match test-named files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = ["src/**/*.py"]')

    src = tmp_path / "src"
    src.mkdir()
    (src / "one.py").write_text("")
    (src / "test_one.py").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.modules)
    assert stems == ["one"]


# ---------------------------------------------------------------------------
# matlab.functions / matlab.variables — directory and glob support
# ---------------------------------------------------------------------------


def test_matlab_functions_directory_recursively_discovers_m_files(tmp_path):
    """A directory entry in matlab.functions should recursively find .m files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nfunctions = ["matlab"]')

    matlab = tmp_path / "matlab"
    matlab.mkdir()
    (matlab / "foo.m").write_text("")
    (matlab / "bar.m").write_text("")
    sub = matlab / "sub"
    sub.mkdir()
    (sub / "baz.m").write_text("")
    # Non-.m files should not appear
    (matlab / "notes.txt").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_functions)
    assert stems == ["bar", "baz", "foo"]


def test_matlab_variables_directory(tmp_path):
    """A directory entry in matlab.variables should recursively find .m files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nvariables = ["types"]')

    types_dir = tmp_path / "types"
    types_dir.mkdir()
    (types_dir / "MyVar.m").write_text("")
    (types_dir / "OtherVar.m").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_variables)
    assert stems == ["MyVar", "OtherVar"]


def test_matlab_glob_filters_to_m_files_only(tmp_path):
    """A glob like 'matlab/*' should only return .m files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nfunctions = ["matlab/*"]')

    matlab = tmp_path / "matlab"
    matlab.mkdir()
    (matlab / "good.m").write_text("")
    (matlab / "readme.md").write_text("")
    (matlab / "data.csv").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = [p.stem for p in config.matlab_functions]
    assert stems == ["good"]


def test_matlab_empty_directory_warns(tmp_path, caplog):
    """A directory with no .m files should log a warning."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nfunctions = ["empty"]')

    (tmp_path / "empty").mkdir()

    import logging

    with caplog.at_level(logging.WARNING):
        config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    assert config.matlab_functions == []
    assert "no .m files" in caplog.text


def test_matlab_sources_key_populates_matlab_sources_unified(tmp_path):
    """[matlab] sources = [...] should populate matlab_sources unsplit --
    NOT matlab_functions/matlab_variables -- since each file is classified
    per-content at registry-load time (see matlab_registry.load_from_config),
    not pre-sorted by the config author. This is the key that lets a single
    GUI-added path (Paths popup) work for MATLAB without declaring
    function-vs-variable up front."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nsources = ["mixed"]')

    mixed = tmp_path / "mixed"
    mixed.mkdir()
    (mixed / "some_function.m").write_text("")
    (mixed / "SomeClass.m").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_sources)
    assert stems == ["SomeClass", "some_function"]
    assert config.matlab_functions == []
    assert config.matlab_variables == []


def test_matlab_sources_directory_excludes_noise_and_skip_dirs(tmp_path):
    """[matlab] sources directory entries should prune noise dirs and
    MATLAB private/@class/+package dirs, same as folder-scan mode."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nsources = ["mixed"]')

    mixed = tmp_path / "mixed"
    mixed.mkdir()
    (mixed / "keep.m").write_text("")
    private_dir = mixed / "private"
    private_dir.mkdir()
    (private_dir / "helper.m").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_sources)
    assert stems == ["keep"]


def test_matlab_sources_excludes_test_dir_and_pascal_test_files(tmp_path):
    """[matlab] sources should exclude anything under a tests/ dir, plus
    PascalCase Test*.m/*Test.m files even outside a tests/ dir -- these are
    found exclusively in a MATLAB test and must not leak into discovery."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nsources = ["mixed"]')

    mixed = tmp_path / "mixed"
    mixed.mkdir()
    (mixed / "good.m").write_text("")
    (mixed / "TestBar.m").write_text("")
    (mixed / "BazTest.m").write_text("")
    tests_dir = mixed / "tests"
    tests_dir.mkdir()
    (tests_dir / "Foo.m").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_sources)
    assert stems == ["good"]


def test_matlab_explicit_single_test_file_entry_still_excluded(tmp_path):
    """Even an explicitly-listed single .m file is excluded if it matches
    the test naming convention -- no override for explicit config entries."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nfunctions = ["tests/TestForEach.m", "good.m"]')

    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "TestForEach.m").write_text("")
    (tmp_path / "good.m").write_text("")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    stems = sorted(p.stem for p in config.matlab_functions)
    assert stems == ["good"]


# ---------------------------------------------------------------------------
# matlab_addpath auto-derivation
# ---------------------------------------------------------------------------


def test_matlab_addpath_auto_derived_from_functions_and_variables(tmp_path):
    """matlab_addpath should be auto-derived from parent dirs of functions, variables, and variable_dir."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        "[matlab]\n"
        'functions = ["matlab/funcs/foo.m"]\n'
        'variables = ["matlab/types/MyVar.m"]\n'
        'variable_dir = "matlab/types"\n'
    )

    # Create directories and files.
    (tmp_path / "matlab" / "funcs").mkdir(parents=True)
    (tmp_path / "matlab" / "types").mkdir(parents=True)
    (tmp_path / "matlab" / "funcs" / "foo.m").write_text("function y = foo(x)\nend\n")
    (tmp_path / "matlab" / "types" / "MyVar.m").write_text(
        "classdef MyVar < scidb.BaseVariable\nend\n"
    )

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    # Compare against the non-canonicalized form since config no longer
    # calls .resolve() on stored paths (that would convert Windows mapped
    # drives to UNC, breaking VS Code's reveal_in_editor).
    addpath_set = set(config.matlab_addpath)
    assert (tmp_path / "matlab" / "funcs") in addpath_set
    assert (tmp_path / "matlab" / "types") in addpath_set
    assert len(config.matlab_addpath) == 2


def test_matlab_addpath_empty_when_no_matlab_files(tmp_path):
    """matlab_addpath should be empty when there are no MATLAB files."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text("modules = []")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.matlab_addpath == []


def test_matlab_addpath_includes_the_default_variable_stub_dir(tmp_path):
    """With MATLAB code and an entities file but no [matlab] variable_dir,
    classdef stubs land in scimatlab's default directory — which must be on
    the generated script's path, or the type it materializes still fails to
    resolve."""
    from scimatlab.stubs import DEFAULT_STUB_DIRNAME

    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        'entities_file = "src/scistack_entities.toml"\n'
        "[matlab]\n"
        'functions = ["matlab/foo.m"]\n'
    )
    (tmp_path / "matlab").mkdir()
    (tmp_path / "matlab" / "foo.m").write_text("function y = foo(x)\ny = x;\nend\n")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scistack_entities.toml").write_text('variables = ["RawEMG"]\n')

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    assert config.matlab_variable_dir is None  # still configured-only
    assert (tmp_path / "src" / DEFAULT_STUB_DIRNAME) in set(config.matlab_addpath)


def test_matlab_addpath_omits_the_stub_dir_for_a_python_only_project(tmp_path):
    """A Python-only project never needs a MATLAB classdef directory, and
    must not start loading the MATLAB registry because one appeared."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('entities_file = "src/scistack_entities.toml"\n')
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scistack_entities.toml").write_text('variables = ["RawEMG"]\n')

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    assert config.matlab_addpath == []
    assert config.has_matlab is False


def test_matlab_variables_excluded_from_functions(tmp_path):
    """Files in matlab.variables must not also be parsed as matlab.functions.

    Regression test for the case where ``matlab.functions`` points at a
    parent directory that contains the ``matlab.variables`` subtree — the
    recursive walk would otherwise include variable .m files in the
    functions list, producing spurious parse warnings.
    """
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('[matlab]\nfunctions = ["src/"]\nvariables = ["src/vars/"]\n')

    src = tmp_path / "src"
    src.mkdir()
    (src / "func.m").write_text("function y = func(x)\ny = x;\nend\n")
    vars_dir = src / "vars"
    vars_dir.mkdir()
    (vars_dir / "var.m").write_text("classdef var < scidb.BaseVariable\nend\n")

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.matlab_functions] == ["func.m"]
    assert [p.name for p in config.matlab_variables] == ["var.m"]


# ---------------------------------------------------------------------------
# _normalize — preserves user's drive-letter form
# ---------------------------------------------------------------------------


def test_normalize_is_absolute_and_normpath(tmp_path):
    """_normalize should make the path absolute and collapse ``.``/``..``."""
    # Relative with .. segment
    rel = Path("a") / ".." / "b"
    result = _normalize(rel)
    assert result.is_absolute()
    # Normalized form has no .. segment.
    assert ".." not in result.parts

    # Already-absolute input passes through as-is (no following of symlinks).
    result2 = _normalize(tmp_path / "sub")
    assert result2 == tmp_path / "sub"


def test_normalize_does_not_follow_symlinks(tmp_path):
    """_normalize must NOT resolve symlinks (or Windows mapped drives) —
    that's what Path.resolve() does and what causes the UNC issue."""
    import os

    real = tmp_path / "real_dir"
    real.mkdir()
    (real / "file.m").write_text("")

    link = tmp_path / "link_dir"
    try:
        os.symlink(real, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported on this platform")

    normalized = _normalize(link / "file.m")
    # The symlinked path is preserved; only .resolve() would rewrite it.
    assert normalized == link / "file.m"
    # Sanity: .resolve() WOULD follow the symlink (this is the behavior we
    # deliberately avoid on Windows mapped drives).
    assert normalized.resolve() == real / "file.m"


def test_matlab_functions_not_canonicalized_through_symlink(tmp_path):
    """Regression test for the mapped-drive → UNC issue on Windows.

    If the MATLAB functions directory is referenced through a symlink
    (which simulates y:\\ → \\\\server\\share\\ on Windows), the stored
    paths in ``config.matlab_functions`` must retain the symlink form,
    not the resolved form. This ensures VS Code can open them using the
    same path the workspace was rooted at.
    """
    import os

    real_root = tmp_path / "real"
    real_root.mkdir()
    (real_root / "func.m").write_text("function y = func(x)\ny=x;\nend\n")
    (real_root / "scistack.toml").write_text('[matlab]\nfunctions = ["func.m"]\n')

    link_root = tmp_path / "link"
    try:
        os.symlink(real_root, link_root, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported on this platform")

    config = load_config(link_root, link_root / "dummy.duckdb")
    assert len(config.matlab_functions) == 1
    # Crucial: stored path uses the link prefix, not the real prefix.
    assert config.matlab_functions[0] == link_root / "func.m"
    assert str(config.matlab_functions[0]).startswith(str(link_root))


def test_matlab_addpath_deduplicates(tmp_path):
    """matlab_addpath should deduplicate when functions and variables are in the same directory."""
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        '[matlab]\nfunctions = ["matlab/foo.m"]\nvariables = ["matlab/MyVar.m"]\n'
    )

    (tmp_path / "matlab").mkdir()
    (tmp_path / "matlab" / "foo.m").write_text("function y = foo(x)\nend\n")
    (tmp_path / "matlab" / "MyVar.m").write_text(
        "classdef MyVar < scidb.BaseVariable\nend\n"
    )

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    # Both files are in the same directory — should produce exactly one entry.
    assert len(config.matlab_addpath) == 1
    assert config.matlab_addpath[0] == (tmp_path / "matlab")


# ---------------------------------------------------------------------------
# Folder-scan fallback (no pyproject.toml/scistack.toml anywhere)
# ---------------------------------------------------------------------------


def test_folder_scan_discovers_py_and_m_files_mixed(tmp_path):
    """Loose Python + MATLAB files with no config file at all: .py files
    become modules, .m files become the unified matlab_sources list."""
    (tmp_path / "loader.py").write_text("")
    (tmp_path / "analysis.py").write_text("")
    (tmp_path / "bandpass_filter.m").write_text("function y = bandpass_filter(x)\ny=x;\nend\n")
    (tmp_path / "RawSignal.m").write_text("classdef RawSignal < scidb.BaseVariable\nend\n")
    sub = tmp_path / "helpers"
    sub.mkdir()
    (sub / "util.py").write_text("")
    (sub / "util.m").write_text("function y = util(x)\ny=x;\nend\n")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert sorted(p.name for p in config.modules) == [
        "analysis.py",
        "loader.py",
        "util.py",
    ]
    assert sorted(p.name for p in config.matlab_sources) == [
        "RawSignal.m",
        "bandpass_filter.m",
        "util.m",
    ]
    # matlab_functions/matlab_variables stay empty — classification happens
    # per-file later, in matlab_registry.load_from_sources.
    assert config.matlab_functions == []
    assert config.matlab_variables == []


def test_folder_scan_project_path_directory_with_no_config(tmp_path):
    """An explicit --project pointing at a directory with no config file
    inside it also falls back to folder-scan, rooted at that directory."""
    (tmp_path / "pipeline.py").write_text("")

    config = load_config(tmp_path, tmp_path / "elsewhere" / "dummy.duckdb")
    assert config.project_root == tmp_path
    assert [p.name for p in config.modules] == ["pipeline.py"]


def test_folder_scan_no_project_path_roots_at_the_project_not_the_db(tmp_path):
    """With no --project, folder-scan roots at the PROJECT root.

    It used to root at the .duckdb's own directory, which is how a project
    whose database sits in a datasets folder on another drive scanned the
    wrong tree and discovered nothing. The autouse ``_pin_project_root``
    fixture pins the hint to tmp_path, standing in for the VS Code
    workspace folder.
    """
    db_dir = tmp_path / "datasets"
    db_dir.mkdir()
    (db_dir / "not_code.txt").write_text("")
    (tmp_path / "pipeline.py").write_text("")

    config = load_config(None, db_dir / "my_study.duckdb")
    assert config.project_root == _normalize(tmp_path)
    assert [p.name for p in config.modules] == ["pipeline.py"]


def test_folder_scan_excludes_noise_directories(tmp_path):
    """Folder-scan must not walk into VCS/venv/build noise directories."""
    (tmp_path / "real.py").write_text("")
    for noise in (".git", "__pycache__", ".venv", "venv", "node_modules", "build", "dist"):
        d = tmp_path / noise
        d.mkdir()
        (d / "ignored.py").write_text("")
        (d / "ignored.m").write_text("function y = ignored(x)\ny=x;\nend\n")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.modules] == ["real.py"]
    assert config.matlab_sources == []


def test_folder_scan_matlab_excludes_private_class_and_package_dirs(tmp_path):
    """private/, @ClassName/, and +package/ folders are skipped during
    folder-scan — sweeping them in would mis-register class methods or
    namespaced functions as standalone top-level functions."""
    (tmp_path / "public.m").write_text("function y = public(x)\ny=x;\nend\n")
    for skip_dir in ("private", "@MyClass", "+mypkg"):
        d = tmp_path / skip_dir
        d.mkdir()
        (d / "hidden.m").write_text("function y = hidden(x)\ny=x;\nend\n")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.matlab_sources] == ["public.m"]


def test_folder_scan_excludes_test_dirs_and_test_named_files(tmp_path):
    """Folder-scan must not sweep in tests/ dirs or test-named files for
    either language -- these are found exclusively in a test."""
    (tmp_path / "real.py").write_text("")
    (tmp_path / "test_real.py").write_text("")
    (tmp_path / "real.m").write_text("function y = real_fn(x)\ny=x;\nend\n")
    (tmp_path / "TestReal.m").write_text("function y = t(x)\ny=x;\nend\n")
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "helper.py").write_text("")
    (tests_dir / "helper.m").write_text("function y = helper(x)\ny=x;\nend\n")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.modules] == ["real.py"]
    assert [p.name for p in config.matlab_sources] == ["real.m"]


def test_folder_scan_matlab_helpers_fixture_excludes_test_only_content(tmp_path):
    """Real regression test: scimatlab/tests/matlab/helpers/ contains plain
    functions (sum_all.m, col_max.m, ...) and BaseVariable classdefs
    (RawSignal.m, BaselineSignal.m, ...) that exist exclusively to support
    the MATLAB test suite. Copying that fixture under a tests/ dir and
    folder-scanning it must discover zero of its files."""
    import shutil

    real_helpers = (
        Path(__file__).parent.parent.parent
        / "scimatlab"
        / "tests"
        / "matlab"
        / "helpers"
    )
    assert real_helpers.is_dir(), "expected scimatlab/tests/matlab/helpers to exist"

    project_tests_dir = tmp_path / "tests" / "matlab" / "helpers"
    project_tests_dir.parent.mkdir(parents=True)
    shutil.copytree(real_helpers, project_tests_dir)
    (tmp_path / "real.py").write_text("")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.modules] == ["real.py"]
    assert config.matlab_sources == []


def test_folder_scan_matlab_addpath_derived_from_sources(tmp_path):
    """matlab_addpath should be derived from matlab_sources' parent dirs
    too, not just the explicit matlab_functions/matlab_variables lists."""
    sub = tmp_path / "matlab"
    sub.mkdir()
    (sub / "foo.m").write_text("function y = foo(x)\ny=x;\nend\n")

    config = load_config(None, tmp_path / "dummy.duckdb")
    assert config.matlab_addpath == [sub]


# ---------------------------------------------------------------------------
# add_path / remove_path — GUI Paths popup write-back (loose-script only)
# ---------------------------------------------------------------------------


def _read_raw_section(toml_path: Path) -> dict:
    with open(toml_path, "rb") as f:
        return tomllib.load(f)


def test_add_path_creates_scistack_toml_when_none_exists(tmp_path):
    """First-ever '+' click on a pure folder-scan project: creates
    scistack.toml, seeding it with the project root (so code implicitly
    discovered under folder-scan mode isn't silently dropped) plus the
    newly added path -- in both modules and [matlab] sources."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()

    written = add_path(db_path, shared_repo)

    assert written == tmp_path / "scistack.toml"
    data = _read_raw_section(written)
    root_str = str(_normalize(tmp_path))
    repo_str = str(_normalize(shared_repo))
    assert set(data["modules"]) == {root_str, repo_str}
    assert set(data["matlab"]["sources"]) == {root_str, repo_str}


def test_add_path_appends_to_existing_scistack_toml_preserving_other_keys(tmp_path):
    """Adding a path to an already-configured project must not disturb
    unrelated hand-authored keys (packages, auto_discover, existing
    modules entries)."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "existing").mkdir()
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        'modules = ["existing"]\n'
        'packages = ["foo"]\n'
        "auto_discover = false\n"
    )

    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()
    add_path(db_path, shared_repo)

    data = _read_raw_section(toml_file)
    assert "existing" in data["modules"]
    assert str(_normalize(shared_repo)) in data["modules"]
    assert data["packages"] == ["foo"]
    assert data["auto_discover"] is False


def test_add_path_preserves_matlab_entities_file(tmp_path):
    """Every _render_scistack_toml call site must pass entities_file through,
    or the Paths popup silently drops it on save — the exact class of bug
    this function has had before (see its docstring)."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        "modules = []\n"
        "\n"
        "[matlab]\n"
        'variable_dir = "matlab/types"\n'
        'entities_file = "matlab/scistack_entities.m"\n'
    )

    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()
    add_path(db_path, shared_repo)

    data = _read_raw_section(toml_file)
    assert data["matlab"]["entities_file"] == "matlab/scistack_entities.m"
    assert data["matlab"]["variable_dir"] == "matlab/types"


def test_matlab_entities_file_is_resolved_against_project_root(tmp_path):
    from scistack_gui.config import load_config

    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "matlab").mkdir()
    (tmp_path / "scistack.toml").write_text(
        "modules = []\n\n[matlab]\nentities_file = \"matlab/scistack_entities.m\"\n"
    )

    config = load_config(None, db_path)
    assert config.matlab_entities_file == _normalize(
        tmp_path / "matlab" / "scistack_entities.m"
    )


def test_matlab_entities_file_defaults_to_none(tmp_path):
    from scistack_gui.config import load_config

    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "scistack.toml").write_text("modules = []\n")

    assert load_config(None, db_path).matlab_entities_file is None


def test_add_path_is_idempotent_on_duplicate(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()

    add_path(db_path, shared_repo)
    written = add_path(db_path, shared_repo)

    data = _read_raw_section(written)
    repo_str = str(_normalize(shared_repo))
    assert data["modules"].count(repo_str) == 1
    assert data["matlab"]["sources"].count(repo_str) == 1


def test_add_path_rejects_relative_path(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    with pytest.raises(ValueError):
        add_path(db_path, Path("relative/dir"))


def test_add_path_rejects_nonexistent_path(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    with pytest.raises(FileNotFoundError):
        add_path(db_path, tmp_path / "does_not_exist")


def test_add_path_rejects_file_not_directory(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    a_file = tmp_path / "not_a_dir.py"
    a_file.write_text("")
    with pytest.raises(NotADirectoryError):
        add_path(db_path, a_file)


def test_add_path_rejects_packaged_project(tmp_path):
    """pyproject.toml projects are out of scope for this write path --
    the Paths popup keeps its old read-only view for those."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "pyproject.toml").write_text("[tool.scistack]\n")
    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()

    with pytest.raises(ValueError):
        add_path(db_path, shared_repo)


def test_remove_path_deletes_entry_from_both_lists(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    keep_dir = tmp_path / "keep"
    keep_dir.mkdir()
    drop_dir = tmp_path / "drop"
    drop_dir.mkdir()

    add_path(db_path, keep_dir)
    add_path(db_path, drop_dir)
    written = remove_path(db_path, drop_dir)

    data = _read_raw_section(written)
    keep_str = str(_normalize(keep_dir))
    drop_str = str(_normalize(drop_dir))
    assert keep_str in data["modules"]
    assert drop_str not in data["modules"]
    assert keep_str in data["matlab"]["sources"]
    assert drop_str not in data["matlab"]["sources"]


def test_remove_path_noop_when_no_config_file(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    with pytest.raises(FileNotFoundError):
        remove_path(db_path, tmp_path / "whatever")
    assert not (tmp_path / "scistack.toml").exists()


def test_remove_path_rejects_packaged_project(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "pyproject.toml").write_text("[tool.scistack]\nmodules = [\"x\"]\n")

    with pytest.raises(ValueError):
        remove_path(db_path, tmp_path / "x")


# ---------------------------------------------------------------------------
# load_config's entities_file -- one answer, scidb's
#
# This used to read the `entities_file` key here and stop, missing
# scidb.entities' conventional-path fallback. A project holding a
# src/scistack_entities.toml with no key had its entities read by scidb and
# by MATLAB and NEVER loaded into the GUI registry, so nothing it declared
# showed up in the GUI -- the reported "entities aren't populated when I
# create a database in a folder that already has one". See
# .claude/plan-preexisting-entities-on-db-create-26-09-10.md.
# ---------------------------------------------------------------------------


def test_entities_file_falls_back_to_the_conventional_path(tmp_path):
    """No entities_file key, but src/scistack_entities.toml is right there."""
    (tmp_path / "scistack.toml").write_text("modules = []\n")
    (tmp_path / "src").mkdir()
    conventional = tmp_path / "src" / "scistack_entities.toml"
    conventional.write_text('variables = ["RawEMG"]\n')

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert config.entities_file == _normalize(conventional)


def test_entities_file_is_none_when_the_conventional_file_is_absent(tmp_path):
    """No key and no file: nothing is guessed into existence."""
    (tmp_path / "scistack.toml").write_text("modules = []\n")

    assert load_config(tmp_path, tmp_path / "proj.duckdb").entities_file is None


def test_entities_file_empty_key_opts_out_of_the_fallback(tmp_path):
    """What clear_entities_file writes must actually stop the file being
    read, even though it sits at the conventional path."""
    (tmp_path / "scistack.toml").write_text('modules = []\nentities_file = ""\n')
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scistack_entities.toml").write_text('variables = ["RawEMG"]\n')

    assert load_config(tmp_path, tmp_path / "proj.duckdb").entities_file is None


def test_entities_file_key_still_wins_over_the_conventional_path(tmp_path):
    (tmp_path / "scistack.toml").write_text('entities_file = "declared.toml"\n')
    (tmp_path / "declared.toml").write_text('variables = ["A"]\n')
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scistack_entities.toml").write_text('variables = ["B"]\n')

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert config.entities_file == _normalize(tmp_path / "declared.toml")


def test_packaged_project_also_gets_the_conventional_fallback(tmp_path):
    """The GUI refuses to WRITE a pyproject.toml, but reading is not writing:
    a packaged project with the conventional file must still discover it."""
    (tmp_path / "pyproject.toml").write_text("[tool.scistack]\nmodules = []\n")
    (tmp_path / "src").mkdir()
    conventional = tmp_path / "src" / "scistack_entities.toml"
    conventional.write_text('variables = ["RawEMG"]\n')

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert config.entities_file == _normalize(conventional)


# ---------------------------------------------------------------------------
# Cross-platform config paths
#
# A scistack.toml is committed and shared: the same project gets opened on
# Windows and on macOS. The GUI wrote its paths with the HOST separator, so a
# Windows session recorded `entities_file = "src\scistack_entities.toml"`. On
# POSIX that is not an error -- `\` is a legal filename character -- it names
# a file called `src\scistack_entities.toml` in the project ROOT. The entities
# file was created and read there, and the MATLAB classdef stub directory
# (which sits beside the entities file) went to `<root>/scistack_matlab_variables`
# instead of `<root>/src/scistack_matlab_variables`. Observed 2026-09-09.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
def test_windows_written_entities_file_resolves_into_src(tmp_path):
    (tmp_path / "scistack.toml").write_text(
        'modules = []\nentities_file = "src\\\\scistack_entities.toml"\n'
    )
    (tmp_path / "src").mkdir()
    real = tmp_path / "src" / "scistack_entities.toml"
    real.write_text('variables = ["RawEMG"]\n')

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert config.entities_file == _normalize(real)
    assert config.entities_file.parent == _normalize(tmp_path / "src")


@pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
def test_windows_written_module_entry_resolves(tmp_path):
    (tmp_path / "scistack.toml").write_text('modules = ["src\\\\steps.py"]\n')
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "steps.py").write_text("def a(x):\n    return x\n")

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert _normalize(tmp_path / "src" / "steps.py") in config.modules


@pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
def test_windows_absolute_module_entry_is_not_glued_onto_the_root(tmp_path):
    """`Y:\\LabMembers\\...` names a mapped drive that does not exist here.
    Joining it onto the project root produced the nonsense path
    `/Users/me/project/y:\\LabMembers\\...` in every discovery error."""
    (tmp_path / "scistack.toml").write_text(
        'modules = ["Y:\\\\LabMembers\\\\MTillman\\\\repo"]\n'
    )

    config = load_config(tmp_path, tmp_path / "proj.duckdb")

    assert all(not str(p).startswith(str(tmp_path)) for p in config.modules)


def test_entities_file_is_recorded_with_forward_slashes(tmp_path):
    """The write half: relative keys point INSIDE the project, so they must
    round-trip on another platform. Windows reads `/` natively."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    set_entities_file(db_path, None)

    data = _read_raw_section(tmp_path / "scistack.toml")
    assert data["entities_file"] == "src/scistack_entities.toml"
    assert "\\" not in data["entities_file"]


# ---------------------------------------------------------------------------
# set_entities_file / clear_entities_file
#
# Regression coverage for the "creating a PathInput/Parameter/Variable from
# the GUI silently failed with 'No module file was loaded at startup'" bug --
# see .claude/pathinput-sweep-variable-creation-fixes.md. Before this, there
# was no way to configure the entities file for a loose-script project at
# all. Now also covers where that file LANDS (todos item 3b) -- see
# .claude/plan-entities-toml-26-08-31.md.
# ---------------------------------------------------------------------------


def test_set_entities_file_auto_creates_default_when_none_exists(tmp_path):
    """First-ever auto-create on a pure folder-scan project (no scistack.toml/
    pyproject.toml yet): creates scistack.toml seeded with the project root
    (same reasoning as add_path), creates src/scistack_entities.toml, and
    points entities_file at it."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    result = set_entities_file(db_path, None)

    expected = _normalize(tmp_path / "src" / "scistack_entities.toml")
    assert result == expected
    assert expected.exists()
    content = expected.read_text()
    # The `variables` key is scaffolded up front and above the first section
    # header: TOML would bind it to the preceding table if added later.
    assert "variables = []" in content
    assert content.index("variables") < content.index("[parameters]")

    toml_path = tmp_path / "scistack.toml"
    data = _read_raw_section(toml_path)
    # Written relative to project_root for portability -- an entities file
    # always lives inside the project, unlike modules/sources which may
    # point at shared directories outside it.
    assert data["entities_file"] == "src/scistack_entities.toml"
    assert str(_normalize(tmp_path)) in data["modules"]


def test_set_entities_file_is_not_added_to_modules(tmp_path):
    """It is TOML, not Python: executing it as a module would fail. The .py
    entities file it replaces WAS added to modules."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    entities_file = set_entities_file(db_path, None)

    data = _read_raw_section(tmp_path / "scistack.toml")
    assert str(entities_file) not in data["modules"]
    assert not any(str(m).endswith(".toml") for m in data["modules"])


def test_set_entities_file_accepts_relative_path(tmp_path):
    """A relative file_path (as the project-creation wizard sends, e.g.
    'src/scistack_entities.toml') resolves against whatever project_root
    this function determines internally, instead of raising."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    result = set_entities_file(db_path, "custom/relative_entities.toml")

    expected = _normalize(tmp_path / "custom" / "relative_entities.toml")
    assert result == expected
    assert expected.exists()

    data = _read_raw_section(tmp_path / "scistack.toml")
    assert data["entities_file"] == "custom/relative_entities.toml"


def test_set_entities_file_relative_path_resolves_against_existing_root(tmp_path):
    """When a scistack.toml already exists (not the first-write case), a
    relative file_path resolves against the PROJECT root, not db_path's
    parent -- here the database sits in a subdirectory of the project.

    The hint is pinned explicitly: the project root is now strictly the
    folder the user opened, so an existing config only counts when it is AT
    that folder. It is no longer found by walking up from the database (see
    .claude/plan-unify-project-root.md).
    """
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "scistack.toml").write_text("")
    db_path = project_root / "sub" / "proj.duckdb"
    db_path.parent.mkdir()
    db_path.write_text("")
    set_project_root_hint(project_root)

    result = set_entities_file(db_path, "src/entities.toml")

    assert result == _normalize(project_root / "src" / "entities.toml")


def test_set_entities_file_lands_in_project_root_not_datasets_folder(tmp_path):
    """The (b) fix: a database in a datasets folder must not drag the
    project's config and entities file in with it."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    db_path = datasets / "proj.duckdb"
    db_path.write_text("")
    set_project_root_hint(project_root)

    result = set_entities_file(db_path, None)

    assert result == _normalize(project_root / "src" / "scistack_entities.toml")
    assert (project_root / "scistack.toml").exists()
    assert not (datasets / "scistack.toml").exists()


def test_first_write_seeds_both_the_db_folder_and_the_project_root(tmp_path):
    """Switching from folder-scan to config-driven discovery must not drop
    the code the folder scan was implicitly finding next to the database,
    even though the project root is now a different directory."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    db_path = datasets / "proj.duckdb"
    db_path.write_text("")
    set_project_root_hint(project_root)

    set_entities_file(db_path, None)

    data = _read_raw_section(project_root / "scistack.toml")
    assert str(_normalize(datasets)) in data["modules"]
    assert str(_normalize(project_root)) in data["modules"]


def test_set_entities_file_does_not_overwrite_existing_file(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    existing = tmp_path / "my_entities.toml"
    existing.write_text("[parameters]\nRATE = 1000\n")

    set_entities_file(db_path, existing)

    assert existing.read_text() == "[parameters]\nRATE = 1000\n"


def test_set_entities_file_absolute_path_used_as_is(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    absolute = tmp_path / "elsewhere" / "entities.toml"

    result = set_entities_file(db_path, absolute)

    assert result == _normalize(absolute)


def test_set_entities_file_outside_project_root_written_absolute(tmp_path):
    """An entities_file that isn't under project_root at all can't be made
    relative -- falls back to the absolute form rather than raising.

    The hint has to be pinned to ``project_root`` explicitly here: the
    autouse fixture points it at ``tmp_path``, which would make ``outside``
    *inside* the root and quietly test the relative path instead.
    """
    project_root = tmp_path / "project"
    project_root.mkdir()
    db_path = project_root / "proj.duckdb"
    db_path.write_text("")
    set_project_root_hint(project_root)
    outside = tmp_path / "elsewhere" / "entities.toml"
    outside.parent.mkdir()
    outside.write_text("[parameters]\n")

    set_entities_file(db_path, outside)

    data = _read_raw_section(project_root / "scistack.toml")
    assert data["entities_file"] == str(_normalize(outside))


def test_set_entities_file_preserves_legacy_variable_file_key(tmp_path):
    """variable_file is read-only now, but a user who still has one must not
    lose it when the GUI rewrites scistack.toml."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('variable_file = "src/legacy.py"\n')

    set_entities_file(db_path, None)

    data = _read_raw_section(toml_file)
    assert data["variable_file"] == "src/legacy.py"
    assert data["entities_file"] == "src/scistack_entities.toml"


def test_set_entities_file_rejects_packaged_project(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "pyproject.toml").write_text("[tool.scistack]\n")

    with pytest.raises(ValueError):
        set_entities_file(db_path, None)


def test_clear_entities_file_writes_an_explicit_opt_out_and_keeps_file(tmp_path):
    """Cleared as ``entities_file = ""``, not by deleting the key.

    load_config now honours scidb's conventional-path fallback, so a deleted
    key would leave a file at the default src/scistack_entities.toml still
    fully live -- clearing would be a silent no-op for the overwhelmingly
    common case. The empty string says "no entities file" to every layer."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    entities_file = set_entities_file(db_path, None)

    toml_path = tmp_path / "scistack.toml"
    written = clear_entities_file(db_path)

    assert written == toml_path
    data = _read_raw_section(toml_path)
    assert data["entities_file"] == ""
    assert entities_file.exists()  # never deletes the file itself
    assert load_config(tmp_path, db_path).entities_file is None


def test_clear_entities_file_noop_when_no_config_file(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    with pytest.raises(FileNotFoundError):
        clear_entities_file(db_path)


def test_clear_entities_file_rejects_packaged_project(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.scistack]\nentities_file = "x.toml"\n'
    )

    with pytest.raises(ValueError):
        clear_entities_file(db_path)


# ---------------------------------------------------------------------------
# resolve_project_root -- THE answer to "which directory is this project?"
#
# One resolver for readers and writers alike. These used to be two: writing
# consulted --project-root, reading walked upward from the DATABASE. With a
# database on one drive and the project on another, the GUI wrote a
# scistack.toml it could never read back and discovery silently stayed
# empty. See .claude/plan-unify-project-root.md.
# ---------------------------------------------------------------------------


def test_resolve_project_root_prefers_an_explicit_path(tmp_path):
    """--project/--module names the project outright and outranks all else."""
    explicit = tmp_path / "named"
    explicit.mkdir()
    set_project_root_hint(tmp_path / "workspace")
    db_path = tmp_path / "datasets" / "proj.duckdb"
    db_path.parent.mkdir()
    db_path.write_text("")

    assert resolve_project_root(explicit, db_path) == _normalize(explicit)


def test_resolve_project_root_explicit_file_uses_its_directory(tmp_path):
    """--module points at a .py file; the project is the folder holding it."""
    module = tmp_path / "proj" / "pipeline.py"
    module.parent.mkdir()
    module.write_text("")

    assert resolve_project_root(module, tmp_path / "x.duckdb") == _normalize(
        module.parent
    )


def test_resolve_project_root_uses_the_hint(tmp_path):
    """--project-root is the VS Code workspace folder: what the user opened."""
    hint = tmp_path / "workspace"
    hint.mkdir()
    db_path = tmp_path / "datasets" / "proj.duckdb"
    db_path.parent.mkdir()
    db_path.write_text("")
    set_project_root_hint(hint)

    assert resolve_project_root(None, db_path) == _normalize(hint)


def test_resolve_project_root_ignores_a_config_above_the_database(tmp_path):
    """The rule is "the folder you opened", full stop.

    A config file sitting above the DATABASE used to win outright, which is
    precisely the anchor that made the reader and the writer disagree.
    """
    beside_db = tmp_path / "datasets"
    beside_db.mkdir()
    (beside_db / "scistack.toml").write_text("modules = []\n")
    db_path = beside_db / "proj.duckdb"
    db_path.write_text("")
    hint = tmp_path / "workspace"
    hint.mkdir()
    set_project_root_hint(hint)

    assert resolve_project_root(None, db_path) == _normalize(hint)


def test_resolve_project_root_falls_back_to_cwd(tmp_path, monkeypatch):
    db_path = tmp_path / "datasets" / "proj.duckdb"
    db_path.parent.mkdir()
    db_path.write_text("")
    set_project_root_hint(None)
    workdir = tmp_path / "cwd"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    assert resolve_project_root(None, db_path) == _normalize(workdir)


def test_locate_config_at_does_not_walk_upward(tmp_path):
    """The project root is decided once; the config lives there or nowhere.

    An upward walk is what let a stray config in a parent directory — very
    plausible on a shared network drive — capture every project beneath it.
    """
    (tmp_path / "scistack.toml").write_text("modules = []\n")
    child = tmp_path / "child"
    child.mkdir()

    assert locate_config_at(tmp_path) == _normalize(tmp_path / "scistack.toml")
    assert locate_config_at(child) is None


def test_locate_config_at_prefers_pyproject(tmp_path):
    (tmp_path / "scistack.toml").write_text("modules = []\n")
    (tmp_path / "pyproject.toml").write_text("[tool.scistack]\n")

    assert locate_config_at(tmp_path) == _normalize(tmp_path / "pyproject.toml")


# ---------------------------------------------------------------------------
# The regression itself: database and project on different drives.
# ---------------------------------------------------------------------------


def test_add_path_is_readable_back_when_db_is_outside_the_project(tmp_path):
    """Write it, then read it. This is the bug from test_logs.md.

    The database lived under C:\\Users\\...\\Datasets while the project was
    on Y:\\LabMembers\\..., so the reader's upward walk from the database
    could never reach the config the writer had just created. Discovery
    reported "0 .py, 0 .m" forever.
    """
    from scistack_gui.config import add_path, load_config

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "pipeline.py").write_text("")
    # Nowhere near the project — a different tree entirely, as C:\ is from Y:\.
    db_path = tmp_path / "datasets" / "study" / "proj.duckdb"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("")
    library = tmp_path / "shared_libs"
    library.mkdir()
    (library / "helper.py").write_text("")

    set_project_root_hint(project_root)

    written = add_path(db_path, library)
    assert written == _normalize(project_root / "scistack.toml")

    # The reader must find that exact file, and see the added library.
    config = load_config(None, db_path)
    assert config.project_root == _normalize(project_root)
    assert _normalize(library / "helper.py") in [_normalize(m) for m in config.modules]


def test_two_add_paths_both_survive(tmp_path):
    """Each add used to believe it was the first — because it looked for the
    config next to the DATABASE and never found the one it had written to the
    project root — so it re-seeded from an empty section and silently
    discarded everything added before."""
    from scistack_gui.config import add_path, load_config

    project_root = tmp_path / "project"
    project_root.mkdir()
    db_path = tmp_path / "datasets" / "proj.duckdb"
    db_path.parent.mkdir()
    db_path.write_text("")
    first = tmp_path / "lib_one"
    first.mkdir()
    (first / "one.py").write_text("")
    second = tmp_path / "lib_two"
    second.mkdir()
    (second / "two.py").write_text("")

    set_project_root_hint(project_root)

    add_path(db_path, first)
    add_path(db_path, second)

    names = {p.name for p in load_config(None, db_path).modules}
    assert "one.py" in names
    assert "two.py" in names


# ---------------------------------------------------------------------------
# Same directory, two spellings — mapped drive vs UNC (2026-09-01)
#
# The database is routinely opened by one spelling and the project root
# arrives as another for the SAME directory:
#
#   [config] add_path: seeding new scistack.toml with \\fs2...\aging-well-abilitylab
#   [config] add_path: seeding new scistack.toml with y:\...\aging-well-abilitylab
#   [config] Resolved 34 module files total          <- 17 files, counted twice
#
# ...which made discovery parse and register every file in the project twice
# (35 "shadows previous definition" warnings; discovery load errors doubling
# 17 -> 36 mid-session). _normalize deliberately preserves the caller's
# spelling, so comparison has to go through file identity instead.
#
# Windows mapped drives don't exist here, so a symlinked directory stands in:
# os.path.samefile / (st_dev, st_ino) treat both aliases identically.
# See .claude/plan-layout-write-race-and-duplicate-seed-roots.md.
# ---------------------------------------------------------------------------


def _aliased_dir(tmp_path):
    """``(real, alias)`` — two spellings of one directory, or skip."""
    real = tmp_path / "project"
    real.mkdir()
    alias = tmp_path / "project_alias"
    try:
        alias.symlink_to(real, target_is_directory=True)
    except (OSError, NotImplementedError) as exc:  # pragma: no cover - platform
        pytest.skip(f"cannot create a directory symlink here: {exc}")
    return real, alias


def test_first_write_seeds_aliased_root_once(tmp_path):
    """DB opened via one spelling, project root given as another."""
    real, alias = _aliased_dir(tmp_path)
    db_path = alias / "proj.duckdb"
    db_path.write_text("")
    library = tmp_path / "lib"
    library.mkdir()
    set_project_root_hint(real)

    written = add_path(db_path, library)

    data = _read_raw_section(written)
    root_entries = [
        e for e in data["modules"] if str(_normalize(library)) != e
    ]
    assert len(root_entries) == 1, (
        f"the project directory was seeded more than once: {root_entries}"
    )
    assert data["matlab"]["sources"].count(root_entries[0]) == 1


def test_first_write_keeps_the_project_root_spelling(tmp_path):
    """Of the two spellings, the project root's wins — it is the non-UNC
    one, and the form the rest of the session already uses."""
    real, alias = _aliased_dir(tmp_path)
    db_path = alias / "proj.duckdb"
    db_path.write_text("")
    library = tmp_path / "lib"
    library.mkdir()
    set_project_root_hint(real)

    written = add_path(db_path, library)

    assert str(_normalize(real)) in _read_raw_section(written)["modules"]


def test_add_path_skips_an_aliased_existing_entry(tmp_path):
    """Adding \\\\server\\share\\x when y:\\x is already listed must not
    append a second entry for the same directory."""
    real, alias = _aliased_dir(tmp_path)
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    library = real / "lib"
    library.mkdir()

    add_path(db_path, library)
    written = add_path(db_path, alias / "lib")

    data = _read_raw_section(written)
    assert len(data["modules"]) == len(set(data["modules"]))
    lib_entries = [e for e in data["modules"] if e.endswith("lib")]
    assert len(lib_entries) == 1, f"library added twice: {lib_entries}"


def test_remove_path_matches_the_other_spelling(tmp_path):
    """Removing one spelling must not leave the other behind."""
    real, alias = _aliased_dir(tmp_path)
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    library = real / "lib"
    library.mkdir()

    add_path(db_path, library)
    written = remove_path(db_path, alias / "lib")

    data = _read_raw_section(written)
    assert not [e for e in data["modules"] if e.endswith("lib")]
    assert not [e for e in data["matlab"]["sources"] if e.endswith("lib")]


def test_load_config_dedups_aliased_module_files(tmp_path):
    """Repairs a config that ALREADY lists both spellings — the state the
    broken add_path left on disk — with no migration step."""
    real, alias = _aliased_dir(tmp_path)
    (real / "analysis.py").write_text("")
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "scistack.toml").write_text(
        f'modules = ["{_normalize(real).as_posix()}", '
        f'"{_normalize(alias).as_posix()}"]\n'
    )

    modules = load_config(None, db_path).modules

    assert [p.name for p in modules] == ["analysis.py"]


def test_load_config_dedups_aliased_matlab_sources(tmp_path):
    """Same repair for the MATLAB lists (matlab_registry shadow warnings)."""
    real, alias = _aliased_dir(tmp_path)
    (real / "computeThing.m").write_text("function y = computeThing(x)\ny = x;\nend\n")
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    (tmp_path / "scistack.toml").write_text(
        "[matlab]\n"
        f'sources = ["{_normalize(real).as_posix()}", '
        f'"{_normalize(alias).as_posix()}"]\n'
    )

    sources = load_config(None, db_path).matlab_sources

    assert [p.name for p in sources] == ["computeThing.m"]


def test_normalize_still_preserves_the_alias_spelling(tmp_path):
    """Guard the constraint the fix had to work around: _normalize must NOT
    canonicalize, or reveal_in_editor starts handing VS Code UNC paths it
    refuses to open (config.py:38)."""
    real, alias = _aliased_dir(tmp_path)

    assert _normalize(alias / "x.py") == alias / "x.py"
    assert _normalize(alias / "x.py") != _normalize(real / "x.py")


# ---------------------------------------------------------------------------
# glue_dir — the second writable surface
# ---------------------------------------------------------------------------
def test_glue_dir_round_trips_through_add_path(tmp_path):
    """The documented trap: a new key silently dropped on the next Paths save.

    Every _render_scistack_toml call site must thread glue_dir through, exactly
    as they must for entities_file."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = []\nglue_dir = "src/scistack_glue"\n')

    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()
    add_path(db_path, shared_repo)

    assert _read_raw_section(toml_file)["glue_dir"] == "src/scistack_glue"


def test_glue_dir_survives_remove_path(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    gone = tmp_path / "gone"
    gone.mkdir()
    toml_file.write_text(
        f'modules = ["{_normalize(gone).as_posix()}"]\nglue_dir = "src/scistack_glue"\n'
    )

    remove_path(db_path, gone)

    assert _read_raw_section(toml_file)["glue_dir"] == "src/scistack_glue"


def test_glue_dir_survives_set_and_clear_entities_file(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text('modules = []\nglue_dir = "src/scistack_glue"\n')

    set_entities_file(db_path)
    assert _read_raw_section(toml_file)["glue_dir"] == "src/scistack_glue"

    clear_entities_file(db_path)
    assert _read_raw_section(toml_file)["glue_dir"] == "src/scistack_glue"


def test_set_glue_dir_creates_the_directory_and_writes_the_key(tmp_path):
    from scistack_gui.config import DEFAULT_GLUE_RELPATH, set_glue_dir

    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    glue_dir = set_glue_dir(db_path)

    assert glue_dir == _normalize(tmp_path / DEFAULT_GLUE_RELPATH)
    assert glue_dir.is_dir()
    data = _read_raw_section(tmp_path / "scistack.toml")
    assert Path(data["glue_dir"]).as_posix() == DEFAULT_GLUE_RELPATH


def test_set_glue_dir_leaves_an_existing_directory_alone(tmp_path):
    from scistack_gui.config import set_glue_dir

    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    existing = tmp_path / "glue"
    existing.mkdir()
    (existing / "glue_keep.py").write_text("def glue_keep(x):\n    return x\n")

    set_glue_dir(db_path, "glue")

    assert (existing / "glue_keep.py").exists()


def test_glue_dir_files_are_discovered_as_modules(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    glue = tmp_path / "src" / "scistack_glue"
    glue.mkdir(parents=True)
    (glue / "glue_drop_baseline.py").write_text(
        "def glue_drop_baseline(emg):\n    return emg\n"
    )
    (tmp_path / "scistack.toml").write_text('modules = []\nglue_dir = "src/scistack_glue"\n')

    config = load_config(None, db_path)

    assert config.glue_dir == _normalize(glue)
    assert [p.name for p in config.modules] == ["glue_drop_baseline.py"]


def test_glue_dir_files_are_not_duplicated_when_also_in_modules(tmp_path):
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    glue = tmp_path / "src" / "scistack_glue"
    glue.mkdir(parents=True)
    (glue / "glue_x.py").write_text("def glue_x(v):\n    return v\n")
    (tmp_path / "scistack.toml").write_text(
        'modules = ["src/scistack_glue"]\nglue_dir = "src/scistack_glue"\n'
    )

    config = load_config(None, db_path)

    assert [p.name for p in config.modules] == ["glue_x.py"]


def test_add_path_preserves_a_hand_authored_schema_keys_table(tmp_path):
    """``[schema_keys]`` is read by scidb.schema_order and written by nobody.

    The Paths popup rewrites the whole file from the fields it knows, so a
    level order the user typed would be DELETED by an unrelated '+' click —
    the exact failure `_render_scistack_toml`'s docstring says must not
    happen, one table later.
    """
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        'modules = ["existing"]\n'
        "\n"
        "[schema_keys]\n"
        'session = ["BL", "POST", "FU"]\n'
        'speed = ["SSV", "FAST"]\n'
    )
    (tmp_path / "existing").mkdir()
    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()

    add_path(db_path, shared_repo)

    data = _read_raw_section(toml_file)
    assert data["schema_keys"] == {
        "session": ["BL", "POST", "FU"],
        "speed": ["SSV", "FAST"],
    }
    # And the write it was actually asked to do still happened.
    assert str(_normalize(shared_repo)) in data["modules"]


def test_a_preserved_schema_keys_table_is_rendered_last(tmp_path):
    """A TOML table swallows every key after it, so a top-level key emitted
    below `[schema_keys]` would silently become `schema_keys.modules`."""
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")
    toml_file = tmp_path / "scistack.toml"
    toml_file.write_text(
        'modules = ["existing"]\n'
        'packages = ["foo"]\n'
        "\n"
        "[schema_keys]\n"
        'session = ["BL"]\n'
    )
    (tmp_path / "existing").mkdir()
    shared_repo = tmp_path / "shared_repo"
    shared_repo.mkdir()

    add_path(db_path, shared_repo)

    data = _read_raw_section(toml_file)
    assert data["packages"] == ["foo"]
    assert set(data["schema_keys"]) == {"session"}
