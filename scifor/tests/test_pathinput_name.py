"""``PathInput`` requires ``name=``, and its identity key is the name alone
(2026-09-25, docs/claude/identity-layers-pathinput.md)."""

import json

import pytest

from scifor import PathInput
from scifor.pathinput import path_input_key


def test_name_is_required():
    with pytest.raises(TypeError):
        PathInput("{subject}/a.csv")  # type: ignore[call-arg]


@pytest.mark.parametrize("bad", ["", "  ", "x ", None, 3])
def test_name_must_be_a_clean_non_empty_string(bad):
    with pytest.raises(ValueError):
        PathInput("{subject}/a.csv", name=bad)


def test_key_is_the_name_and_nothing_else():
    pi = PathInput("{subject}/a.csv", root_folder="/data", regex=True, name="Raw")
    assert json.loads(pi.to_key()) == {"__type": "PathInput", "name": "Raw"}
    assert pi.to_key() == path_input_key("Raw")


def test_location_never_reaches_the_key():
    a = PathInput("{subject}/a.csv", root_folder="/one", name="Raw")
    b = PathInput("elsewhere/{subject}.txt", root_folder="/two", name="Raw")
    assert a.to_key() == b.to_key()
    assert a.to_spec() != b.to_spec()


def test_spec_carries_everything():
    spec = json.loads(PathInput("{s}/a.csv", root_folder="/d", name="Raw").to_spec())
    assert spec == {
        "__type": "PathInput",
        "name": "Raw",
        "template": "{s}/a.csv",
        "root_folder": "/d",
    }
