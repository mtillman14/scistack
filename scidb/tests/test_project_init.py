"""scidb.project.init_project: the one owner of creating a SciStack project.

The properties that matter: it builds a buildable package whose entities file
lives inside it, it only ever CREATES (an existing file is never rewritten),
running it twice changes nothing, and it writes no pyproject [tool.scistack].
"""

from __future__ import annotations

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from scidb.project import (
    init_project,
    package_name_for,
    validate_project_name,
)


def _tree(root):
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    )


def _snapshot(root):
    return {rel: (root / rel).read_bytes() for rel in _tree(root)}


class TestNames:
    @pytest.mark.parametrize("name", ["my_study", "x", "eeg2024", "trailing_"])
    def test_valid(self, name):
        validate_project_name(name)

    @pytest.mark.parametrize("name", ["", "My_Study", "1study", "my-study", "my study", "a.b"])
    def test_invalid(self, name):
        with pytest.raises(ValueError):
            validate_project_name(name)

    def test_folder_name_is_made_valid(self, tmp_path):
        assert package_name_for(tmp_path / "Gait Study 2") == "gait_study_2"
        assert package_name_for(tmp_path / "2024 data") == "project_2024_data"

    def test_pyproject_name_wins(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "my-study"\n')
        assert package_name_for(tmp_path) == "my_study"


class TestFreshProject:
    def test_layout(self, tmp_path):
        root = tmp_path / "gait"
        report = init_project(root)

        assert report.package == "gait"
        assert _tree(root) == [
            ".gitignore",
            "pyproject.toml",
            "scistack.toml",
            "src/gait/__init__.py",
            "src/gait/scistack_entities.toml",
        ]
        assert report.entities_file == root / "src" / "gait" / "scistack_entities.toml"
        assert not report.warnings
        assert not report.kept

    def test_pyproject_is_packaging_only_and_buildable(self, tmp_path):
        init_project(tmp_path / "gait")
        data = tomllib.loads((tmp_path / "gait" / "pyproject.toml").read_text())
        assert data["project"]["name"] == "gait"
        assert "scidb" in data["project"]["dependencies"]
        assert data["build-system"]["build-backend"] == "hatchling.build"
        assert data["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["src/gait"]
        assert "scistack" not in data.get("tool", {})

    def test_config_names_the_package_entities_file_and_is_portable(self, tmp_path):
        init_project(tmp_path / "gait")
        config = tomllib.loads((tmp_path / "gait" / "scistack.toml").read_text())
        assert config["entities_file"] == "src/gait/scistack_entities.toml"
        # The project root is seeded relative, never as this machine's path.
        assert config["modules"] == ["."]
        assert config["matlab"]["sources"] == ["."]

    def test_entities_file_is_the_scidb_initial_text(self, tmp_path):
        from scidb.entities import initial_text

        report = init_project(tmp_path / "gait")
        assert report.entities_file.read_text() == initial_text()

    def test_an_explicit_entities_file_is_used(self, tmp_path):
        report = init_project(tmp_path / "gait", entities_file="decl/ents.toml")
        assert report.entities_file == tmp_path / "gait" / "decl" / "ents.toml"
        assert report.entities_file.exists()
        assert not (tmp_path / "gait" / "src" / "gait" / "scistack_entities.toml").exists()

    def test_an_explicit_name(self, tmp_path):
        report = init_project(tmp_path / "Folder Name", name="study")
        assert report.package == "study"
        assert (tmp_path / "Folder Name" / "src" / "study" / "__init__.py").exists()

    def test_an_invalid_name_is_refused_before_anything_is_written(self, tmp_path):
        with pytest.raises(ValueError):
            init_project(tmp_path / "x", name="Bad Name")
        assert not (tmp_path / "x").exists()


class TestOnlyCreates:
    def test_running_twice_changes_nothing(self, tmp_path):
        root = tmp_path / "gait"
        init_project(root)
        before = _snapshot(root)

        report = init_project(root)

        assert _snapshot(root) == before
        assert report.created == []
        assert len(report.kept) == 5

    def test_existing_files_are_kept_byte_for_byte(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        pyproject = '[project]\nname = "gait"\n# mine\n[tool.ruff]\nline-length = 100\n'
        (root / "pyproject.toml").write_text(pyproject)
        (root / ".gitignore").write_text("my-ignores\n")
        (root / "src" / "gait").mkdir(parents=True)
        (root / "src" / "gait" / "__init__.py").write_text("X = 1\n")

        init_project(root)

        assert (root / "pyproject.toml").read_text() == pyproject
        assert (root / ".gitignore").read_text() == "my-ignores\n"
        assert (root / "src" / "gait" / "__init__.py").read_text() == "X = 1\n"

    def test_an_existing_config_keeps_its_entities_file(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        (root / "scistack.toml").write_text(
            'modules = ["code"]\nentities_file = "decl/mine.toml"\ndb = "x.duckdb"\n'
        )
        before = (root / "scistack.toml").read_text()

        report = init_project(root)

        assert (root / "scistack.toml").read_text() == before
        assert report.entities_file == root / "decl" / "mine.toml"
        assert report.entities_file.exists()  # named but missing -> created

    def test_a_config_with_no_entities_key_gets_one_and_keeps_every_other_key(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        (root / "scistack.toml").write_text('modules = ["code"]\ndb = "x.duckdb"\ncustom = 3\n')

        report = init_project(root)

        config = tomllib.loads((root / "scistack.toml").read_text())
        assert config["entities_file"] == "src/gait/scistack_entities.toml"
        assert config["modules"] == ["code"]
        assert config["db"] == "x.duckdb"
        assert config["custom"] == 3
        assert report.entities_file.exists()

    def test_the_conventional_entities_file_is_adopted_not_duplicated(self, tmp_path):
        root = tmp_path / "gait"
        (root / "src").mkdir(parents=True)
        (root / "src" / "scistack_entities.toml").write_text('variables = ["A"]\n')
        (root / "scistack.toml").write_text("modules = []\n")

        report = init_project(root)

        assert report.entities_file == root / "src" / "scistack_entities.toml"
        assert not (root / "src" / "gait" / "scistack_entities.toml").exists()

    def test_an_entities_opt_out_is_respected(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        (root / "scistack.toml").write_text('modules = []\nentities_file = ""\n')

        report = init_project(root)

        assert report.entities_file is None
        assert not (root / "src" / "gait" / "scistack_entities.toml").exists()

    def test_an_unparseable_config_is_left_alone_and_reported(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        (root / "scistack.toml").write_text("this = = broken")

        report = init_project(root)

        assert (root / "scistack.toml").read_text() == "this = = broken"
        assert report.entities_file is None
        assert any("could not be parsed" in w for w in report.warnings)

    def test_a_pyproject_naming_another_project_is_reported(self, tmp_path):
        root = tmp_path / "gait"
        root.mkdir()
        (root / "pyproject.toml").write_text('[project]\nname = "other"\n')

        report = init_project(root, name="gait")

        assert any("'other'" in w for w in report.warnings)


def test_the_package_is_discovered_once_not_as_loose_files_too(tmp_path):
    """The seeded modules = ["."] covers src/<pkg>/; the package itself is
    the owner of those files (scifor.discovery.own_package_dir)."""
    from scifor.discovery import own_package_dir

    root = tmp_path / "gait"
    init_project(root)
    assert own_package_dir(root) == ("gait", root / "src" / "gait")
