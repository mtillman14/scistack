"""The seam between the entities grammar (scifor) and materialization (scidb).

``scifor.project.entities_file`` parses TOML into plain data;
``scidb.project.entities.materialize`` builds the objects. Guards the hand-off: a
declaration that parses must come out as the real scidb object, and a
declaration only ``PathInput`` can refuse must be rejected on its own,
with its line, without taking the rest of the file down (plan P3 stage 3).
"""

from __future__ import annotations

from scidb import entities
from scidb.parameter import Parameter
from scidb.variable import BaseVariable
from scifor import EachOf, PathInput
from scifor.project.entities_file import parse_entities

TEXT = """\
variables = ["SeamVariable"]

[parameters]
SEAM_WINDOW = [10, 20]

[path_inputs]
SEAM_RAW = "{subject}/raw.csv"
SEAM_ALT = ["{subject}/a.csv", "{subject}/b.csv"]
SEAM_BAD = { template = "{subject}/x.csv", aliases = { session = { BL = ["Baseline"] } } }
"""


def test_toml_text_becomes_scidb_objects(tmp_path):
    declared = parse_entities(TEXT, tmp_path / "scistack_entities.toml")
    result = entities.materialize(declared)

    cls = result.variables["SeamVariable"]
    assert isinstance(cls, type) and issubclass(cls, BaseVariable)
    assert cls.__name__ == "SeamVariable"

    param = result.parameters["SEAM_WINDOW"]
    assert isinstance(param, Parameter)
    assert param.values == [10, 20]
    assert param.name == "SEAM_WINDOW"
    assert param.source_line == 4

    raw = result.path_inputs["SEAM_RAW"]
    assert isinstance(raw, PathInput)
    assert raw.path_template == "{subject}/raw.csv"
    assert isinstance(result.path_inputs["SEAM_ALT"], EachOf)


def test_a_declaration_pathinput_refuses_is_rejected_alone(tmp_path):
    """``aliases`` on a key the template does not have parses (the grammar
    cannot know the placeholders' meaning) and is refused by PathInput."""
    result = entities.materialize(parse_entities(TEXT, tmp_path / "e.toml"))

    assert "SEAM_BAD" not in result.path_inputs
    assert "SEAM_BAD" not in result.lines
    [error] = result.errors
    assert error.name == "SEAM_BAD"
    assert error.line == 9
    assert result.names() == ["SeamVariable", "SEAM_WINDOW", "SEAM_RAW", "SEAM_ALT"]


def test_load_logs_the_materialized_summary(tmp_path, caplog):
    path = tmp_path / "scistack_entities.toml"
    path.write_text('variables = ["SeamLogged"]\n', encoding="utf-8")
    with caplog.at_level("INFO", logger="scidb"):
        entities.load(path)
    assert "[entities] Loaded" in caplog.text
