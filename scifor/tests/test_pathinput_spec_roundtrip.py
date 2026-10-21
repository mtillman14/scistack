"""PathInput.from_spec is the exact inverse of PathInput.to_spec.

The MATLAB bridge ships a PathInput as to_spec() in both directions and
rebuilds it with from_spec(). Before 2026-10-21 it copied fields by hand and
dropped ``aliases`` and ``key_regex``, so a ``Baseline`` folder holding
``*_BL_*`` files discovered 0 combos on a MATLAB run.
"""

import inspect
import json

import pytest

from scifor.pathinput import _SPEC_FIELDS, PathInput

TEMPLATE = "{subject}/{session}/Xsens/{subject}_{session}_xsens_{speed}{trial}.xlsx"
ALIASES = {"session": {"BL": ["Baseline"], "PR": ["Pre-Test"]}}
KEY_REGEX = {"subject": r"R2_\d{3}", "speed": "[A-Za-z]+", "trial": r"\d+"}


def _state(pi: PathInput) -> dict:
    return {
        "name": pi.name,
        "template": pi.path_template,
        "root_folder": pi.root_folder,
        "regex": pi.regex,
        "aliases": pi.aliases,
        "key_regex": pi.key_regex,
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"root_folder": "/data"},
        {"regex": True},
        {"aliases": ALIASES},
        {"key_regex": KEY_REGEX},
        {"root_folder": "/data", "aliases": ALIASES, "key_regex": KEY_REGEX},
    ],
)
def test_round_trip_keeps_every_option(kwargs):
    pi = PathInput(TEMPLATE, name="xsens_path", **kwargs)
    back = PathInput.from_spec(pi.to_spec())
    assert _state(back) == _state(pi)
    assert back.to_spec() == pi.to_spec()


def test_accepts_the_decoded_dict():
    pi = PathInput(TEMPLATE, name="xsens_path", aliases=ALIASES)
    assert _state(PathInput.from_spec(json.loads(pi.to_spec()))) == _state(pi)


def test_unknown_field_raises():
    spec = json.loads(PathInput(TEMPLATE, name="x").to_spec())
    spec["new_option"] = 1
    with pytest.raises(ValueError, match="new_option"):
        PathInput.from_spec(spec)


def test_not_a_pathinput_spec_raises():
    with pytest.raises(ValueError, match="not a PathInput spec"):
        PathInput.from_spec({"name": "x", "template": "a.csv"})


def test_every_constructor_option_is_a_spec_field():
    """A new __init__ option must be written by to_spec and read by from_spec;
    this fails until _SPEC_FIELDS (and both methods) learn it."""
    params = set(inspect.signature(PathInput.__init__).parameters) - {"self"}
    as_spec = {"path_template": "template"}
    assert {as_spec.get(p, p) for p in params} <= set(_SPEC_FIELDS)


def test_round_tripped_pathinput_still_discovers_aliased_files(tmp_path):
    """The user's layout: folder spelled ``Baseline``, file spelled ``BL``."""
    d = tmp_path / "R2_001" / "Baseline" / "Xsens"
    d.mkdir(parents=True)
    for name in ("R2_001_BL_xsens_FV1.xlsx", "R2_001_BL_xsens_SSV2.xlsx"):
        (d / name).write_text("")
    pi = PathInput(
        TEMPLATE, root_folder=tmp_path, name="xsens_path",
        aliases=ALIASES, key_regex=KEY_REGEX,
    )
    combos = PathInput.from_spec(pi.to_spec()).discover()
    assert sorted((c["session"], c["speed"], c["trial"]) for c in combos) == [
        ("BL", "FV", "1"),
        ("BL", "SSV", "2"),
    ]
