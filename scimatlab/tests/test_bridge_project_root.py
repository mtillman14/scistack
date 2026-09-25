"""Pinning scifor's PathInput resolution base from MATLAB.

A ``PathInput`` declared with no ``root_folder`` resolves its relative
template against ``scifor.project_root()``, which is the **cwd** unless
pinned. Under MATLAB the cwd is wherever MATLAB is sitting — for a
GUI-generated command, a temp script directory — so it names the wrong folder
and every relative template misses.

``+scidb/entities.m`` already receives the project root; these tests cover the
Python half that turns it into scifor's resolution base, and the invariant
that makes it safe: it never touches ``root_folder``, so a run records the
same identity wherever it was launched from. Writing the root into
``root_folder`` instead is what grew ``__unresolved__`` ghost nodes on the GUI
canvas — see ``.claude/plan-pathinput-unresolved-after-run.md``.
"""

import pytest
from scifor import clear_project_root, get_project_root
from scifor.pathinput import PathInput

from scimatlab.bridge import load_entities, set_pathinput_project_root


@pytest.fixture(autouse=True)
def _clean_globals():
    """Both the override and the entities cache are process-global."""
    from scidb import entities

    entities.clear_cache()
    clear_project_root()
    yield
    clear_project_root()
    entities.clear_cache()


@pytest.fixture
def project(tmp_path):
    """A project with an entities file declaring one rootless PathInput."""
    (tmp_path / "scistack.toml").write_text(
        'entities_file = "src/scistack_entities.toml"\n', encoding="utf-8"
    )
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scistack_entities.toml").write_text(
        "[path_inputs]\nEMG = { template = \"data/{subject}.mat\" }\n",
        encoding="utf-8",
    )
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "s01.mat").touch()
    return tmp_path


class TestSetPathinputProjectRoot:
    def test_sets_scifors_override(self, tmp_path):
        set_pathinput_project_root(tmp_path)
        assert get_project_root() == tmp_path.resolve()

    def test_returns_the_resolved_root(self, tmp_path):
        assert set_pathinput_project_root(str(tmp_path)) == str(tmp_path.resolve())

    def test_none_clears(self, tmp_path):
        set_pathinput_project_root(tmp_path)
        assert set_pathinput_project_root(None) == ""
        assert get_project_root() is None


class TestLoadEntitiesPins:
    def test_load_entities_pins_the_project(self, project):
        """scidb.entities(PROJECT_ROOT) is the one call every generated script
        and most hand-written ones already make, so pinning here means a
        terminal MATLAB run gets the same resolution the GUI does."""
        load_entities(str(project))
        assert get_project_root() == project.resolve()

    def test_rootless_declaration_resolves_after_load(
        self, project, tmp_path, monkeypatch
    ):
        outside = tmp_path.parent / "outside"
        outside.mkdir(exist_ok=True)
        monkeypatch.chdir(outside)

        load_entities(str(project))

        assert PathInput("data/{subject}.mat", name="data/{subject}.mat").load(subject="s01") == (
            project / "data" / "s01.mat"
        ).resolve()

    def test_declared_root_folder_stays_none_in_the_payload(self, project):
        """The pin must not leak into what MATLAB rebuilds: entities.m builds
        scidb.PathInput(template) with no root for these, which is what keeps
        the recorded key identical to the declaration."""
        payload = load_entities(str(project))
        assert payload["path_inputs"]["EMG"] == [
            {"template": "data/{subject}.mat", "root_folder": None}
        ]

    def test_a_named_folder_without_a_config_is_still_the_root(self, tmp_path):
        """The folder MATLAB names IS the project root, config or not
        (scifor.project_root never infers a root from config files)."""
        empty = tmp_path / "no_config"
        empty.mkdir()
        load_entities(str(empty))
        assert get_project_root() == empty.resolve()

    def test_no_folder_named_pins_nothing(self, tmp_path, monkeypatch):
        """With no folder named, the cwd is already what an unpinned root
        means -- pinning it would freeze MATLAB's arbitrary cwd."""
        monkeypatch.chdir(tmp_path)
        load_entities(None)
        assert get_project_root() is None
