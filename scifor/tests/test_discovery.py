"""Unit tests for scifor.discovery -- the generic package-walking harness."""

from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

import pytest
from scifor.discovery import (
    PathInsert,
    PathInsertAll,
    find_top_level_side_effects,
    headless_matplotlib,
    is_test_modname,
    is_test_path,
    is_windows_absolute,
    purge_module,
    read_project_name,
    resolve_config_path,
    sibling_import_dirs,
    walk_package,
)


@pytest.fixture
def package_factory(tmp_path: Path):
    """Scaffold a throwaway importable package under tmp_path, add it to
    sys.path, and clean up sys.modules/sys.path after the test."""
    created: list[str] = []

    def _make(package_name: str, files: dict[str, str]) -> None:
        pkg_dir = tmp_path / package_name
        pkg_dir.mkdir(parents=True, exist_ok=True)
        (pkg_dir / "__init__.py").write_text("")
        for rel, content in files.items():
            full = pkg_dir / rel
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(textwrap.dedent(content))
        created.append(package_name)

    sys.path.insert(0, str(tmp_path))
    yield _make

    sys.path.remove(str(tmp_path))
    for name in created:
        prefix = name + "."
        for mod_name in list(sys.modules):
            if mod_name == name or mod_name.startswith(prefix):
                sys.modules.pop(mod_name, None)


class TestWalkPackage:
    def test_top_level_import_failure_captured(self):
        wr = walk_package("definitely_not_a_real_package_xyz", lambda m: None)
        assert wr.per_module == []
        assert len(wr.errors) == 1
        assert wr.errors[0].module_name == "definitely_not_a_real_package_xyz"

    def test_calls_on_module_for_top_level_and_submodules(self, package_factory):
        package_factory(
            "fix_walk_basic",
            files={"a.py": "X = 1\n", "b.py": "Y = 2\n"},
        )
        seen = []
        wr = walk_package("fix_walk_basic", lambda m: seen.append(m.__name__))
        names = {n for n, _ in wr.per_module}
        assert names == {"fix_walk_basic", "fix_walk_basic.a", "fix_walk_basic.b"}
        assert set(seen) == names
        assert wr.errors == []

    def test_submodule_import_failure_does_not_abort_walk(self, package_factory):
        package_factory(
            "fix_walk_broken",
            files={
                "good.py": "X = 1\n",
                "broken.py": "raise RuntimeError('boom')\n",
            },
        )
        wr = walk_package("fix_walk_broken", lambda m: m.__name__)
        names = {n for n, _ in wr.per_module}
        assert "fix_walk_broken.good" in names
        assert "fix_walk_broken.broken" not in names
        broken_errors = [e for e in wr.errors if "broken" in e.module_name]
        assert len(broken_errors) == 1
        assert "boom" in broken_errors[0].traceback

    def test_on_module_exception_captured_not_raised(self, package_factory):
        package_factory("fix_walk_callback_error", files={"a.py": "X = 1\n"})

        def on_module(mod):
            if mod.__name__.endswith(".a"):
                raise ValueError("callback exploded")
            return None

        wr = walk_package("fix_walk_callback_error", on_module)
        errors = [e for e in wr.errors if e.module_name.endswith(".a")]
        assert len(errors) == 1
        assert "callback exploded" in errors[0].traceback

    def test_test_modules_skipped(self, package_factory):
        package_factory(
            "fix_walk_test_skip",
            files={
                "prod.py": "X = 1\n",
                "tests/__init__.py": "",
                "tests/helpers.py": "Y = 2\n",
                "test_helper.py": "Z = 3\n",
            },
        )
        wr = walk_package("fix_walk_test_skip", lambda m: m.__name__)
        names = {n for n, _ in wr.per_module}
        assert "fix_walk_test_skip.prod" in names
        assert not any("tests" in n for n in names)
        assert "fix_walk_test_skip.test_helper" not in names

    def test_non_package_module_no_submodule_walk(self, package_factory):
        """A module (not a package) has no __path__ -- walk_package should
        just call on_module once and return, not raise."""
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            Path(d, "fix_bare_module.py").write_text("X = 1\n")
            sys.path.insert(0, d)
            try:
                wr = walk_package("fix_bare_module", lambda m: m.__name__)
                assert [n for n, _ in wr.per_module] == ["fix_bare_module"]
                assert wr.errors == []
            finally:
                sys.path.remove(d)
                sys.modules.pop("fix_bare_module", None)


class TestIsTestPathAndModname:
    def test_file_inside_tests_dir(self):
        assert is_test_path("pkg/tests/helper.py")

    def test_test_prefixed_filename(self):
        assert is_test_path("pkg/test_foo.py")

    def test_test_suffixed_filename(self):
        assert is_test_path("pkg/foo_test.py")

    def test_normal_file_not_excluded(self):
        assert not is_test_path("pkg/helper.py")

    def test_false_positive_guard_latest(self):
        assert not is_test_path("pkg/latest.py")

    def test_modname_with_tests_component(self):
        assert is_test_modname("mypackage.tests.helpers")

    def test_modname_bare_tests_leaf(self):
        assert is_test_modname("mypackage.tests")

    def test_modname_normal_not_excluded(self):
        assert not is_test_modname("mypackage.helpers")

    def test_modname_false_positive_guard(self):
        assert not is_test_modname("mypackage.latest")


class TestReadProjectName:
    def test_reads_project_name(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "my_study"\nversion = "0.1.0"\n'
        )
        assert read_project_name(tmp_path) == "my_study"

    def test_missing_file(self, tmp_path):
        assert read_project_name(tmp_path) is None

    def test_missing_section(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text("[build-system]\nrequires = []\n")
        assert read_project_name(tmp_path) is None


class TestPathInsert:
    def test_inserts_and_removes_path(self, tmp_path):
        target = str(tmp_path / "my_src")
        assert target not in sys.path
        with PathInsert(target):
            assert target in sys.path
        assert target not in sys.path

    def test_does_not_double_insert(self, tmp_path):
        target = str(tmp_path / "already")
        sys.path.insert(0, target)
        try:
            initial_count = sys.path.count(target)
            with PathInsert(target):
                assert sys.path.count(target) == initial_count
            assert target in sys.path
        finally:
            sys.path.remove(target)


class TestPathInsertAll:
    def test_inserts_and_removes_every_directory(self, tmp_path):
        targets = [str(tmp_path / "a"), str(tmp_path / "b"), str(tmp_path / "c")]
        for t in targets:
            assert t not in sys.path

        with PathInsertAll(targets):
            for t in targets:
                assert t in sys.path

        for t in targets:
            assert t not in sys.path

    def test_restores_path_when_body_raises(self, tmp_path):
        targets = [str(tmp_path / "a"), str(tmp_path / "b")]
        with pytest.raises(RuntimeError):
            with PathInsertAll(targets):
                raise RuntimeError("import blew up")
        for t in targets:
            assert t not in sys.path

    def test_empty_list_is_a_noop(self):
        before = list(sys.path)
        with PathInsertAll([]):
            assert sys.path == before
        assert sys.path == before

    def test_leaves_preexisting_entries_alone(self, tmp_path):
        pre = str(tmp_path / "already")
        sys.path.insert(0, pre)
        try:
            with PathInsertAll([pre, str(tmp_path / "fresh")]):
                assert sys.path.count(pre) == 1
            assert pre in sys.path
        finally:
            sys.path.remove(pre)


class TestSiblingImportDirs:
    def test_returns_distinct_parent_dirs(self, tmp_path):
        (tmp_path / "sub").mkdir()
        paths = [
            tmp_path / "a.py",
            tmp_path / "b.py",
            tmp_path / "sub" / "c.py",
        ]
        assert sibling_import_dirs(paths) == sorted(
            {str(tmp_path.resolve()), str((tmp_path / "sub").resolve())}
        )

    def test_accepts_strings(self, tmp_path):
        assert sibling_import_dirs([str(tmp_path / "a.py")]) == [
            str(tmp_path.resolve())
        ]

    def test_empty_input(self):
        assert sibling_import_dirs([]) == []


class TestFindTopLevelSideEffects:
    def test_flags_bare_call_with_lineno_and_callee(self):
        src = textwrap.dedent(
            """\
            def plot_gait(a, b, c):
                return a

            plot_gait(1, 2, 3)
            """
        )
        effects = find_top_level_side_effects(src)
        assert len(effects) == 1
        assert effects[0].lineno == 4
        assert effects[0].call == "plot_gait"
        assert "plot_gait() at line 4" in effects[0].describe()

    def test_flags_dotted_callee(self):
        effects = find_top_level_side_effects("import plt\nplt.savefig('x.png')\n")
        assert [e.call for e in effects] == ["plt.savefig"]

    def test_reports_multiple_in_line_order(self):
        src = "f()\ng()\nh()\n"
        effects = find_top_level_side_effects(src)
        assert [(e.lineno, e.call) for e in effects] == [
            (1, "f"),
            (2, "g"),
            (3, "h"),
        ]

    # -- Form 2: assignment calling a function defined in this file --------

    def test_flags_assignment_calling_local_function(self):
        """The hole a form-only rule leaves: an Assign does the same work as
        a bare call, and ``Parameter(...)`` looks identical by shape."""
        src = textwrap.dedent(
            """\
            def plot_gait(a, b, c):
                return a

            data = plot_gait(1, 2, 3)
            """
        )
        effects = find_top_level_side_effects(src)
        assert len(effects) == 1
        assert effects[0].lineno == 4
        assert effects[0].call == "plot_gait"
        assert "defined in this file" in effects[0].reason

    def test_flags_annotated_assignment_calling_local_function(self):
        src = "def work():\n    return 1\n\nx: int = work()\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["work"]

    def test_flags_local_call_nested_in_expression(self):
        src = "def work():\n    return 1\n\nx = 2 * work() + 1\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["work"]

    def test_flags_local_call_in_async_def(self):
        src = "async def work():\n    return 1\n\nx = work()\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["work"]

    def test_deduplicates_repeated_call_on_one_line(self):
        src = "def f(x):\n    return x\n\ny = f(f(1))\n"
        effects = find_top_level_side_effects(src)
        assert len(effects) == 1

    def test_does_not_flag_assignment_calling_imported_name(self):
        """The discriminator: imported callee, so it must run."""
        src = textwrap.dedent(
            """\
            import logging
            from pathlib import Path

            logger = logging.getLogger(__name__)
            HERE = Path(__file__).parent
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_does_not_flag_locally_defined_class_instantiation(self):
        src = textwrap.dedent(
            """\
            class Config:
                pass

            CONFIG = Config()
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_local_function_call_inside_another_def_is_not_flagged(self):
        src = "def a():\n    return 1\n\ndef b():\n    return a()\n"
        assert find_top_level_side_effects(src) == []

    def test_reports_both_forms_together_in_line_order(self):
        src = textwrap.dedent(
            """\
            def work():
                return 1

            x = work()
            work()
            """
        )
        effects = find_top_level_side_effects(src)
        assert [(e.lineno, e.call) for e in effects] == [(4, "work"), (5, "work")]

    # -- Must NOT flag: these are the whole point of discovery --------------

    def test_does_not_flag_parameter_assignment(self):
        assert find_top_level_side_effects("RATE = Parameter(1, 2, 3)\n") == []

    def test_does_not_flag_path_input_assignment(self):
        assert find_top_level_side_effects("RAW = PathInput('{s}.mat')\n") == []

    def test_does_not_flag_annotated_assignment(self):
        assert find_top_level_side_effects("RATE: object = Parameter(1)\n") == []

    def test_does_not_flag_module_docstring(self):
        assert find_top_level_side_effects('"""A module docstring."""\n') == []

    def test_does_not_flag_imports_defs_or_classes(self):
        src = textwrap.dedent(
            """\
            import os
            from pathlib import Path

            def fn(x):
                return x

            class Thing:
                def method(self):
                    helper()
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_does_not_flag_main_guard(self):
        """The If is the module-body child; we never descend into it."""
        src = textwrap.dedent(
            """\
            def main():
                return 1

            if __name__ == "__main__":
                main()
                render_everything()
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_does_not_flag_calls_nested_inside_functions(self):
        src = "def fn():\n    plot_gait(1, 2, 3)\n"
        assert find_top_level_side_effects(src) == []

    # -- Compound statements: their bodies run on import too ---------------
    # Regression: stats scripts that loop over conditions writing CSVs passed
    # the screen, ran their whole analysis during discovery, and blew the
    # GUI's 60 s startup window (Stroke-R01-Aim1, 2026-09-25).

    def test_flags_bare_call_inside_top_level_for_loop(self):
        src = textwrap.dedent(
            """\
            import pandas as pd

            for cond in ["SSV", "FV"]:
                df = pd.DataFrame()
                df.to_csv(f"{cond}.csv")
            """
        )
        effects = find_top_level_side_effects(src)
        assert [(e.lineno, e.call) for e in effects] == [(5, "df.to_csv")]
        assert "inside a top-level for loop at line 3" in effects[0].describe()

    def test_flags_local_function_assignment_inside_for_loop(self):
        src = textwrap.dedent(
            """\
            def make_df(c):
                return c

            for cond in ["SSV", "FV"]:
                out = make_df(cond)
            """
        )
        effects = find_top_level_side_effects(src)
        assert [(e.lineno, e.call) for e in effects] == [(5, "make_df")]
        assert "calls a function defined in this file" in effects[0].reason

    def test_flags_local_function_in_loop_iterable(self):
        src = "def rows():\n    return []\n\nfor r in rows():\n    pass\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["rows"]

    def test_flags_nested_loops(self):
        src = textwrap.dedent(
            """\
            for a in [1, 2]:
                for b in [3, 4]:
                    save(a, b)
            """
        )
        effects = find_top_level_side_effects(src)
        assert [(e.lineno, e.call) for e in effects] == [(3, "save")]

    def test_flags_while_loop_body(self):
        src = "i = 0\nwhile i < 3:\n    step(i)\n    i += 1\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["step"]

    def test_flags_with_block_body(self):
        src = 'with open("out.txt", "w") as f:\n    f.write("x")\n'
        effects = find_top_level_side_effects(src)
        assert [e.call for e in effects] == ["f.write"]
        assert "inside a top-level with block at line 1" in effects[0].describe()

    def test_flags_non_main_if_block(self):
        src = "DEBUG = True\nif DEBUG:\n    render_everything()\n"
        assert [e.call for e in find_top_level_side_effects(src)] == [
            "render_everything"
        ]

    def test_flags_else_branch_of_if(self):
        src = "if False:\n    pass\nelse:\n    render_everything()\n"
        assert [e.call for e in find_top_level_side_effects(src)] == [
            "render_everything"
        ]

    def test_does_not_flag_reversed_main_guard(self):
        src = 'if "__main__" == __name__:\n    render_everything()\n'
        assert find_top_level_side_effects(src) == []

    def test_flags_try_block_body(self):
        src = "try:\n    run_all()\nexcept Exception:\n    report()\n"
        assert [e.call for e in find_top_level_side_effects(src)] == [
            "run_all",
            "report",
        ]

    def test_does_not_flag_try_import_fallback(self):
        src = textwrap.dedent(
            """\
            try:
                import fastlib as lib
            except ImportError:
                lib = None
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_local_function_defined_in_try_fallback_is_known(self):
        src = textwrap.dedent(
            """\
            try:
                from fastlib import work
            except ImportError:
                def work():
                    return 1

            data = work()
            """
        )
        assert [e.call for e in find_top_level_side_effects(src)] == ["work"]

    def test_does_not_flag_print_only_loop(self):
        src = 'for name in ["a", "b"]:\n    print(name)\n    logger.info(name)\n'
        assert find_top_level_side_effects(src) == []

    def test_does_not_flag_loop_building_entities(self):
        src = textwrap.dedent(
            """\
            from scidb import Parameter

            for n in [1, 2]:
                RATE = Parameter(n)
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_does_not_descend_into_class_bodies(self):
        src = "class Cfg:\n    for x in []:\n        render(x)\n"
        assert find_top_level_side_effects(src) == []

    # -- Allowlist ---------------------------------------------------------

    @pytest.mark.parametrize(
        "line",
        [
            "matplotlib.use('Agg')",
            "mpl.use('Agg')",
            "logging.basicConfig()",
            "warnings.filterwarnings('ignore')",
            "pd.set_option('display.width', 200)",
            "sns.set_theme()",
            "plt.style.use('ggplot')",
        ],
    )
    def test_allowlisted_config_calls_are_not_flagged(self, line):
        assert find_top_level_side_effects(line + "\n") == []

    @pytest.mark.parametrize(
        "line",
        [
            "print('hello')",
            "pprint(thing)",
            "pprint.pprint(thing)",
            "sys.stdout.write('x')",
            "sys.stderr.write('x')",
            "logger.info('module loaded')",
            "log.debug('x')",
            "LOGGER.warning('x')",
            "_log.error('x')",
            "warnings.warn('deprecated')",
        ],
    )
    def test_output_only_calls_are_not_flagged(self, line):
        """A stray print says nothing about whether a file does work, and
        refusing over one would cost every function the file defines."""
        assert find_top_level_side_effects(line + "\n") == []

    def test_print_alongside_definitions_does_not_refuse_the_file(self):
        src = textwrap.dedent(
            """\
            print('hello from noisy module')

            def noisy_fn(x):
                return x
            """
        )
        assert find_top_level_side_effects(src) == []

    def test_local_function_named_like_a_logging_method_is_still_flagged(self):
        """The logging carve-out requires a dotted callee, so a local
        ``def error(...)`` invoked bare is not smuggled through."""
        src = "def error(msg):\n    return msg\n\nerror('boom')\n"
        assert [e.call for e in find_top_level_side_effects(src)] == ["error"]

    def test_allowlist_can_be_overridden(self):
        src = "setup_project()\n"
        assert len(find_top_level_side_effects(src)) == 1
        assert find_top_level_side_effects(src, allow=frozenset({"setup_project"})) == []

    def test_non_name_callee_is_flagged_and_rendered_safely(self):
        effects = find_top_level_side_effects("funcs[0]()\n")
        assert [e.call for e in effects] == ["<expr>"]

    def test_raises_on_unparseable_source(self):
        with pytest.raises(SyntaxError):
            find_top_level_side_effects("def broken(\n")


class TestHeadlessMatplotlib:
    def test_sets_and_restores_env_var(self, monkeypatch):
        monkeypatch.delenv("MPLBACKEND", raising=False)
        with headless_matplotlib():
            assert os.environ["MPLBACKEND"] == "Agg"
        assert "MPLBACKEND" not in os.environ

    def test_restores_preexisting_env_var(self, monkeypatch):
        monkeypatch.setenv("MPLBACKEND", "TkAgg")
        with headless_matplotlib():
            assert os.environ["MPLBACKEND"] == "Agg"
        assert os.environ["MPLBACKEND"] == "TkAgg"

    def test_restores_env_var_when_body_raises(self, monkeypatch):
        monkeypatch.setenv("MPLBACKEND", "TkAgg")
        with pytest.raises(RuntimeError):
            with headless_matplotlib():
                raise RuntimeError("import blew up")
        assert os.environ["MPLBACKEND"] == "TkAgg"

    def test_no_op_when_matplotlib_not_imported(self, monkeypatch):
        """Must not import matplotlib itself -- it stays an optional dep."""
        monkeypatch.delitem(sys.modules, "matplotlib", raising=False)
        with headless_matplotlib():
            assert "matplotlib" not in sys.modules

    def test_switches_and_restores_already_imported_matplotlib(self, monkeypatch):
        calls: list[tuple[str, bool]] = []

        fake = type(sys)("matplotlib")
        fake.get_backend = lambda: "TkAgg"
        fake.use = lambda backend, force=False: calls.append((backend, force))
        monkeypatch.setitem(sys.modules, "matplotlib", fake)

        with headless_matplotlib():
            assert calls == [("Agg", True)]
        assert calls == [("Agg", True), ("TkAgg", True)]

    def test_does_not_switch_when_already_agg(self, monkeypatch):
        calls: list[str] = []

        fake = type(sys)("matplotlib")
        fake.get_backend = lambda: "agg"
        fake.use = lambda backend, force=False: calls.append(backend)
        monkeypatch.setitem(sys.modules, "matplotlib", fake)

        with headless_matplotlib():
            pass
        assert calls == []

    def test_survives_matplotlib_raising(self, monkeypatch):
        """A broken/partial matplotlib must not abort discovery."""
        fake = type(sys)("matplotlib")

        def boom():
            raise RuntimeError("no backend")

        fake.get_backend = boom
        monkeypatch.setitem(sys.modules, "matplotlib", fake)

        with headless_matplotlib():
            pass  # no exception


class TestPurgeModule:
    def test_purge_removes_main_and_sub_modules(self):
        sys.modules["_fake_purge_test"] = type(sys)("_fake_purge_test")
        sys.modules["_fake_purge_test.sub1"] = type(sys)("_fake_purge_test.sub1")

        purge_module("_fake_purge_test")

        assert "_fake_purge_test" not in sys.modules
        assert "_fake_purge_test.sub1" not in sys.modules

    def test_purge_nonexistent_is_noop(self):
        purge_module("_definitely_not_in_sys_modules_xyz")


class TestResolveConfigPath:
    """A scistack.toml is a shared, committed file: the same project is
    opened on Windows and on macOS, and the GUI wrote its paths with the
    host's separator.

    Reading ``entities_file = "src\\scistack_entities.toml"`` naively on
    POSIX does not error -- ``\\`` is a legal filename character -- it names
    a FILE called ``src\\scistack_entities.toml`` in the project root. The
    entities file was then created and read there, and everything derived
    from its location moved with it: MATLAB classdef stubs landed in
    ``<root>/scistack_matlab_variables`` instead of
    ``<root>/src/scistack_matlab_variables``.
    """

    def test_forward_slashes_resolve_against_the_root(self, tmp_path):
        assert resolve_config_path(tmp_path, "src/e.toml") == tmp_path / "src" / "e.toml"

    @pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
    def test_windows_relative_path_is_read_as_a_path(self, tmp_path):
        resolved = resolve_config_path(tmp_path, "src\\scistack_entities.toml")

        assert resolved == tmp_path / "src" / "scistack_entities.toml"
        assert resolved.name == "scistack_entities.toml"
        assert resolved.parent == tmp_path / "src"

    @pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
    def test_the_stub_directory_follows_the_corrected_parent(self, tmp_path):
        """The symptom, stated directly: stubs go beside the entities file."""
        resolved = resolve_config_path(tmp_path, "src\\scistack_entities.toml")

        assert resolved.parent / "scistack_matlab_variables" == (
            tmp_path / "src" / "scistack_matlab_variables"
        )

    def test_posix_absolute_path_ignores_the_root(self, tmp_path):
        assert resolve_config_path(tmp_path, "/opt/shared/e.toml") == Path(
            "/opt/shared/e.toml"
        )

    @pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
    def test_windows_absolute_path_is_not_glued_onto_the_root(self, tmp_path):
        """``Y:\\LabMembers\\...`` cannot resolve here, and joining it onto
        the project root produced the nonsense
        ``/Users/me/project/y:\\LabMembers\\...`` in every error message."""
        resolved = resolve_config_path(tmp_path, "Y:\\LabMembers\\MTillman\\repo")

        assert not str(resolved).startswith(str(tmp_path))

    def test_is_windows_absolute(self):
        assert is_windows_absolute("Y:\\LabMembers")
        assert is_windows_absolute("y:/LabMembers")
        assert is_windows_absolute("\\\\server\\share")
        assert not is_windows_absolute("src\\e.toml")
        assert not is_windows_absolute("/opt/shared")
