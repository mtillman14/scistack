"""
Variables must leave ``BaseVariable._all_subclasses`` when their declaration
does.

Regression cover for the 2026-09-01 report: creating a variable failed with
"A variable named 'RawEMG' already exists" while nothing on disk declared
one. ``registry.load_from_config`` cleared every registry it maintained --
functions, parameters, path inputs, module paths, load errors -- except
variables, which ``__init_subclass__`` only ever appended to. A name was
therefore registered for the life of the server process: a declaration
deleted from disk stayed live, a previously-opened project's variables
stayed live, and the sidebar listed types nothing declared (which is how one
got dragged onto the canvas and failed inside MATLAB as ``Unrecognized
function or variable 'Raw_EMG'``).

See ``.claude/plan-entity-surfaces-and-reload-cost.md`` Stage 2.
"""

from __future__ import annotations

import pytest
from scidb import BaseVariable

import scistack_gui.project.registry as _registry


@pytest.fixture(autouse=True)
def _clean_registry():
    """Leave no tracked variable behind — these tests deliberately register
    into a process-global dict."""
    yield
    _registry._unregister_tracked_variables()
    _registry._config = None
    _registry._module_path = None


def _project(tmp_path, variables: list[str]):
    """A loose project whose entities file declares *variables*."""
    (tmp_path / "scistack.toml").write_text(
        'entities_file = "src/scistack_entities.toml"\n', encoding="utf-8"
    )
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    rendered = ", ".join(f'"{v}"' for v in variables)
    (src / "scistack_entities.toml").write_text(
        f"variables = [{rendered}]\n", encoding="utf-8"
    )
    from scidb import entities

    entities.clear_cache()
    from scistack_gui.project.config import load_config

    return load_config(tmp_path, tmp_path / "test.duckdb")


class TestUnregister:
    def test_unregister_removes_the_name(self):
        class TempVar(BaseVariable):
            pass

        assert "TempVar" in BaseVariable._all_subclasses
        assert BaseVariable.unregister("TempVar") is True
        assert "TempVar" not in BaseVariable._all_subclasses

    def test_unregister_is_idempotent_and_reports_absence(self):
        assert BaseVariable.unregister("NeverExisted") is False

    def test_the_class_object_still_works_after_unregistering(self):
        """Unregistering affects lookup by name, not the type itself: code
        already holding a reference keeps working."""

        class TempVar2(BaseVariable):
            pass

        BaseVariable.unregister("TempVar2")

        assert BaseVariable.get_subclass_by_name("TempVar2") is None
        assert TempVar2.__name__ == "TempVar2"
        assert issubclass(TempVar2, BaseVariable)


class TestReloadPrunesDeletedDeclarations:
    def test_a_variable_deleted_from_the_entities_file_is_unregistered(self, tmp_path):
        config = _project(tmp_path, ["RawEMG"])
        _registry.load_from_config(config)
        assert "RawEMG" in BaseVariable._all_subclasses

        config = _project(tmp_path, [])
        _registry.load_from_config(config)

        assert "RawEMG" not in BaseVariable._all_subclasses
        assert "RawEMG" not in _registry._variable_sources

    def test_the_name_becomes_creatable_again(self, tmp_path):
        """The actual reported symptom: create refused a name with no source."""
        from scistack_gui.services.variable_service import create_variable

        config = _project(tmp_path, ["RawEMG"])
        _registry.load_from_config(config)

        refused = create_variable("RawEMG")
        assert refused["ok"] is False
        assert "already exists" in refused["error"]
        # ...and it now says WHERE, which the bare message never did.
        assert "scistack_entities.toml" in refused["error"]

        config = _project(tmp_path, [])
        _registry.load_from_config(config)

        assert create_variable("RawEMG")["ok"] is True

    def test_a_surviving_declaration_is_not_pruned(self, tmp_path):
        config = _project(tmp_path, ["Keep", "Drop"])
        _registry.load_from_config(config)

        config = _project(tmp_path, ["Keep"])
        _registry.load_from_config(config)

        assert "Keep" in BaseVariable._all_subclasses
        assert "Drop" not in BaseVariable._all_subclasses

    def test_untracked_variables_are_never_collateral(self, tmp_path):
        """A class this registry did not register -- scidb's own types, a
        test fixture, another importer's -- must survive a reload."""

        class NotOurs(BaseVariable):
            pass

        config = _project(tmp_path, ["RawEMG"])
        _registry.load_from_config(config)
        _registry.load_from_config(config)

        assert "NotOurs" in BaseVariable._all_subclasses
        BaseVariable.unregister("NotOurs")


class TestSourceAttribution:
    def test_entities_file_variables_are_attributed_to_it(self, tmp_path):
        config = _project(tmp_path, ["RawEMG"])
        _registry.load_from_config(config)

        assert _registry._variable_sources["RawEMG"] == str(config.entities_file)

    def test_a_module_only_claims_classes_it_defines(self, tmp_path):
        """``_scan_module_variables`` filters on ``__module__`` so a file
        that merely imports a sibling's variable does not claim it -- a
        reload of the importer would otherwise unregister someone else's
        type."""
        src = tmp_path / "src"
        src.mkdir(parents=True, exist_ok=True)
        (src / "owner.py").write_text(
            "import scidb\n\n\nclass OwnedVar(scidb.BaseVariable):\n    pass\n",
            encoding="utf-8",
        )
        (src / "importer.py").write_text(
            "from owner import OwnedVar  # noqa: F401\n", encoding="utf-8"
        )
        (tmp_path / "scistack.toml").write_text(
            'modules = ["src/owner.py", "src/importer.py"]\n', encoding="utf-8"
        )
        from scistack_gui.project.config import load_config

        config = load_config(tmp_path, tmp_path / "test.duckdb")
        _registry.load_from_config(config)

        assert "OwnedVar" in BaseVariable._all_subclasses
        assert _registry._variable_sources["OwnedVar"].endswith("owner.py")
