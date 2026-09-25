"""
Round-trip tests for source-declared entity editing (plan Stage 10).

The unit tests elsewhere check each link in isolation: the span finder, the
renderers, the write guards, the registry scan. These check the LOOP —
edit → file on disk → re-scan → what the GUI shows — because that is what
the feature actually promises, and a break anywhere in the chain shows up
here even when every individual piece still passes its own test.

Four properties, from docs/claude/entity-editability-model.md:

1. A GUI edit lands in source, and a re-scan reads back what was written.
2. A SOURCE edit surfaces in the GUI on Refresh Code (the other direction —
   the property the original source-of-truth migration bought, which the
   write-back must not regress).
3. An exported script carries edited values, so "eject to a script" stays
   truthful after editing.
4. A concurrent edit is refused, and the file is left exactly as the other
   writer left it.

MATLAB entities-script round-trips are covered too — parse → splice →
re-parse needs no MATLAB, only the file.
"""

from __future__ import annotations

import scistack_gui.registry as _registry
from scistack_gui import config as config_mod


def _project(tmp_path, body):
    """A loose project whose Python entities file contains *body*."""
    from scistack_gui.db import get_db_path

    entities = tmp_path / "entities.py"
    entities.write_text(body)
    config_mod.set_entities_file(get_db_path(), entities)
    _registry._module_path = None
    _registry.load_from_config(config_mod.load_config(None, get_db_path()))
    return entities


def _rescan():
    from scistack_gui.db import get_db_path

    _registry.load_from_config(config_mod.load_config(None, get_db_path()))


class TestGuiEditRoundTrip:
    """Edit in the GUI -> the file on disk -> a fresh scan agrees."""

    def test_parameter_value(self, populated_db, tmp_path):
        from scistack_gui.services.parameter_service import update_parameter

        entities = _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='w')\n"
        )

        assert update_parameter("WINDOW", [45], "w")["ok"]

        # The FILE is the source of truth — read it back from disk, not from
        # the in-memory registry the edit just updated.
        assert "scidb.Parameter(45, description='w')" in entities.read_text()
        _rescan()
        assert _registry.get_parameters_registry()["WINDOW"].value == 45

    def test_adding_a_value_is_additive(self, populated_db, tmp_path):
        """The D6 property: more values is more arguments, same declaration,
        same name — nothing about the entity's identity changes."""
        from scistack_gui.services.parameter_service import update_parameter

        entities = _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='')\n"
        )

        assert update_parameter("WINDOW", [30, 45])["ok"]

        assert "WINDOW = scidb.Parameter(30, 45, description='')" in entities.read_text()
        _rescan()
        param = _registry.get_parameters_registry()["WINDOW"]
        assert param.values == [30, 45]

    def test_path_input_template(self, populated_db, tmp_path):
        from scistack_gui.services.path_input_service import update_path_input

        entities = _project(
            tmp_path, "import scidb\n\nRAW = scidb.PathInput('a.csv', name='RAW')\n"
        )

        assert update_path_input("RAW", "{subject}/b.csv", "/data")["ok"]

        assert (
            "scidb.PathInput('{subject}/b.csv', root_folder='/data', name='RAW')"
            in entities.read_text()
        )
        _rescan()
        pi = _registry.get_path_inputs_registry()["RAW"]
        assert pi.path_template == "{subject}/b.csv"
        assert str(pi.root_folder) == "/data"

    def test_repeated_edits_each_land(self, populated_db, tmp_path):
        """Three same-length edits in a row. Guards the stale-bytecode trap:
        an edit that changes neither the file's size nor its whole-second
        mtime is served from cached .pyc unless the write invalidates it,
        and the GUI silently keeps showing the old value."""
        from scistack_gui.services.parameter_service import update_parameter

        entities = _project(
            tmp_path, "import scidb\n\nW = scidb.Parameter(11, description='')\n"
        )

        for value in (22, 33, 44):
            assert update_parameter("W", [value])["ok"]
            _rescan()
            assert _registry.get_parameters_registry()["W"].value == value
            assert f"scidb.Parameter({value}, description='')" in entities.read_text()


class TestSourceEditRoundTrip:
    """The other direction: hand-edit the file, Refresh Code, GUI agrees.

    This is what the original source-of-truth migration bought; the
    write-back machinery must not regress it.
    """

    def test_hand_edited_value_surfaces(self, populated_db, tmp_path):
        entities = _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='')\n"
        )

        entities.write_text(
            "import scidb\n\nWINDOW = scidb.Parameter(99, description='edited')\n"
        )
        _rescan()

        param = _registry.get_parameters_registry()["WINDOW"]
        assert param.value == 99
        assert param.description == "edited"

    def test_hand_added_entity_appears(self, populated_db, tmp_path):
        entities = _project(tmp_path, "import scidb\n")

        entities.write_text(
            "import scidb\n\nNEW_ONE = scidb.Parameter(1, 2, 3, description='')\n"
        )
        _rescan()

        assert _registry.get_parameters_registry()["NEW_ONE"].values == [1, 2, 3]

    def test_hand_removed_entity_disappears(self, populated_db, tmp_path):
        entities = _project(
            tmp_path, "import scidb\n\nGONE = scidb.Parameter(1, description='')\n"
        )
        assert "GONE" in _registry.get_parameters_registry()

        entities.write_text("import scidb\n")
        _rescan()

        assert "GONE" not in _registry.get_parameters_registry()


class TestStaleGuardRoundTrip:
    def test_concurrent_edit_leaves_the_other_writers_file_intact(
        self, populated_db, tmp_path
    ):
        """The GUI read one version, someone else wrote another. The write
        must be refused and the file left byte-for-byte as the other writer
        left it — not merged, not clobbered."""
        from scistack_gui.services.parameter_service import update_parameter

        entities = _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='')\n"
        )

        other_writer = (
            "import scidb\n\n"
            "WINDOW = scidb.Parameter(99, description='by someone else')\n"
        )
        entities.write_text(other_writer)

        result = update_parameter("WINDOW", [45])

        assert result["ok"] is False
        assert result["reason"] == "stale"
        assert entities.read_text() == other_writer

    def test_refresh_then_edit_succeeds(self, populated_db, tmp_path):
        """The documented recovery: Refresh Code re-baselines the guard."""
        from scistack_gui.services.parameter_service import update_parameter

        entities = _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='')\n"
        )
        entities.write_text("import scidb\n\nWINDOW = scidb.Parameter(99, description='')\n")

        assert update_parameter("WINDOW", [45])["ok"] is False
        _rescan()
        assert update_parameter("WINDOW", [45])["ok"] is True
        assert "scidb.Parameter(45" in entities.read_text()


class TestReadOnlyRoundTrip:
    def test_declaration_outside_the_entities_file_is_untouched(
        self, populated_db, tmp_path
    ):
        """Confinement is the contract, not a soft preference: the foreign
        file must be byte-identical afterwards."""
        from scistack_gui.db import get_db_path
        from scistack_gui.services.parameter_service import update_parameter

        other = tmp_path / "params.py"
        original = "import scidb\n\nOUTSIDE = scidb.Parameter(7, description='')\n"
        other.write_text(original)

        entities = tmp_path / "entities.py"
        entities.write_text("import scidb\n")
        config_mod.set_entities_file(get_db_path(), entities)
        config_mod.add_path(get_db_path(), tmp_path)
        _registry._module_path = None
        _rescan()

        result = update_parameter("OUTSIDE", [9])

        assert result["ok"] is False
        assert result["reason"] == "read_only"
        assert result["file"] == str(other)
        assert result["line"] == 3
        assert other.read_text() == original


class TestExportRoundTrip:
    def test_edited_value_reaches_the_exported_script(self, populated_db, tmp_path):
        """"Eject to a standalone script" must stay truthful after an edit —
        the export recomputes from the same declarations the GUI edits."""
        from scidb.source_edit import render_parameter

        # The export path serialises resolved values via repr(); asserting on
        # the renderer + a re-scan proves the edited object is what an export
        # would serialise, without standing up a whole runnable pipeline.
        _project(
            tmp_path, "import scidb\n\nWINDOW = scidb.Parameter(30, description='')\n"
        )
        from scistack_gui.services.parameter_service import update_parameter

        assert update_parameter("WINDOW", [45, 60])["ok"]
        _rescan()

        param = _registry.get_parameters_registry()["WINDOW"]
        assert repr(param) == "Parameter(45, 60, description='')"
        # And the same values re-render to a declaration that parses back.
        assert render_parameter(param.values, param.description) == (
            "scidb.Parameter(45, 60, description='')"
        )

    def test_exported_header_imports_what_repr_emits(self):
        """repr(Parameter) prints `Parameter(...)`, so the generated script's
        header MUST import it — otherwise every exported script containing a
        Parameter dies with NameError at the top."""
        from scistack_gui.services.code_export_service import _py_literal

        from scidb import Parameter

        rendered = _py_literal(Parameter(1, 2))
        assert rendered.startswith("Parameter(")

        import inspect

        from scistack_gui.services import code_export_service

        # The header is a multi-line import; every name a literal can emit
        # must be in it (AcrossVariants joined 2026-09-20). Read from the
        # SOURCE, not from `co_consts` — CPython folds an all-constant list
        # literal into a single tuple constant, so the individual strings
        # are not there to find.
        src = inspect.getsource(code_export_service._py_header)
        for name in ("AcrossVariants", "EachOf", "Parameter", "PathInput"):
            assert f"{name}," in src, f"{name} is not imported by the exported header"


class TestMatlabEntitiesRoundTrip:
    """parse -> splice -> re-parse. Needs the file, not MATLAB."""

    def _entities(self, tmp_path, body):
        f = tmp_path / "scistack_entities.m"
        f.write_text(body)
        return f

    def test_parameter_value_rewrite(self, tmp_path):
        from scidb.source_edit import splice

        from scistack_gui.matlab_parser import (
            binding_parameter_literal,
            find_entities_binding,
            read_source_text,
            render_matlab_parameter,
        )

        f = self._entities(
            tmp_path, "window = scidb.Parameter(10, 20);\nother = 1;\n"
        )

        binding = find_entities_binding(f, "window")
        text = read_source_text(f)
        updated = splice(
            text, binding.expr_span, render_matlab_parameter([30, 40, 50], "secs")
        )
        f.write_text(updated)

        # Re-parse the file we just wrote — the loop closes only if the
        # renderer emits something the parser accepts.
        reparsed = find_entities_binding(f, "window")
        assert binding_parameter_literal(reparsed, read_source_text(f)) == (
            [30, 40, 50],
            "secs",
        )
        # Everything outside the span survived.
        assert "other = 1;" in f.read_text()

    def test_path_input_rewrite(self, tmp_path):
        from scidb.source_edit import splice

        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            find_entities_binding,
            read_source_text,
            render_matlab_path_input,
        )

        f = self._entities(tmp_path, "raw = scidb.PathInput('a.mat');\n")

        binding = find_entities_binding(f, "raw")
        updated = splice(
            read_source_text(f),
            binding.expr_span,
            render_matlab_path_input("{subject}/b.mat", "/data", name="raw"),
        )
        f.write_text(updated)

        reparsed = find_entities_binding(f, "raw")
        assert binding_path_input_literal(reparsed, read_source_text(f)) == (
            "{subject}/b.mat",
            "/data",
        )

    def test_registry_reads_back_the_rewritten_file(self, tmp_path):
        """The full MATLAB loop: rewrite, then load through matlab_registry
        and confirm the GUI-visible object matches."""
        from scidb.source_edit import splice

        from scistack_gui import matlab_registry, registry
        from scistack_gui.matlab_parser import (
            find_entities_binding,
            read_source_text,
            render_matlab_parameter,
        )

        f = self._entities(tmp_path, "window = scidb.Parameter(10);\n")

        binding = find_entities_binding(f, "window")
        f.write_text(
            splice(
                read_source_text(f),
                binding.expr_span,
                render_matlab_parameter([7, 8]),
            )
        )

        registry._parameters.pop("window", None)
        matlab_registry.load_entities_script(f)

        assert list(registry.get_parameters_registry()["window"].values) == [7, 8]
