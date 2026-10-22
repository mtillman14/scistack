"""Schema-key types in scifor: the ONE conversion rule and the standalone
``for_each(schema_key_types=...)`` declaration.

2026-10-21: the rule (``canonical_numeric_value``) moved here from scidb, so
standalone scifor and scidb read ``"001"`` / ``"1.0"`` the same way (scifor's
``condense_numeric`` used a bare ``int()``). Without a database the call owns
the declaration; see docs/claude/schema-key-types.md.
"""

from __future__ import annotations

import pytest

from scifor import (
    PathInput,
    SchemaKeyTypeError,
    canonical_numeric_value,
    for_each,
    key_regex_is_digit_only,
    set_schema,
)
from scifor.schema_types import (
    canonicalize_combos,
    canonicalize_values,
    numeric_match_keys,
    validate_key_types,
)


@pytest.fixture(autouse=True)
def _no_schema():
    set_schema([])
    yield
    set_schema([])


class TestRule:
    def test_spellings_collapse(self):
        assert canonical_numeric_value("trial", "001") == 1
        assert canonical_numeric_value("trial", 1.0) == 1
        assert canonical_numeric_value("trial", "1.50") == 1.5
        assert canonical_numeric_value("trial", " 7 ") == 7

    def test_numpy_scalars_are_numbers(self):
        np = pytest.importorskip("numpy")
        assert canonical_numeric_value("trial", np.int64(3)) == 3
        assert type(canonical_numeric_value("trial", np.int64(3))) is int

    def test_non_numbers_raise_fatal(self):
        with pytest.raises(SchemaKeyTypeError, match="non-numeric"):
            canonical_numeric_value("trial", "r")
        with pytest.raises(SchemaKeyTypeError, match="bool"):
            canonical_numeric_value("trial", True)
        assert SchemaKeyTypeError.scifor_fatal is True

    def test_scidb_raises_its_own_subclass(self):
        scidb_exceptions = pytest.importorskip("scidb.exceptions")
        assert issubclass(scidb_exceptions.SchemaKeyTypeError, SchemaKeyTypeError)

    def test_validate(self):
        assert validate_key_types(None) == {}
        with pytest.raises(ValueError, match="must be one of"):
            validate_key_types({"trial": "int"})


class TestHelpers:
    def test_values_dedupe_keeping_first(self):
        assert canonicalize_values("trial", ["001", 1, "2", 2.0], "numeric") == [1, 2]
        assert canonicalize_values("trial", ["001", "1"], "string") == ["001", "1"]
        assert canonicalize_values("trial", ["001"], None) == ["001"]

    def test_combos_by_type(self):
        combos = [{"s": "01", "trial": "001"}, {"s": "01", "trial": "1"}]
        assert canonicalize_combos(combos, {"trial": "numeric"}) == [{"s": "01", "trial": 1}]
        # string keys are never condensed, even with condense_undeclared.
        out = canonicalize_combos(
            combos, {"trial": "string"}, condense_undeclared=True
        )
        assert out == [{"s": 1, "trial": "001"}, {"s": 1, "trial": "1"}]
        # input not mutated
        assert combos[0] == {"s": "01", "trial": "001"}

    def test_numeric_match_keys(self):
        assert numeric_match_keys(["s", "trial"], {}) is None
        assert numeric_match_keys(["s", "trial"], {"trial": "numeric"}) is None
        assert numeric_match_keys(["s", "trial"], {"trial": "string"}) == {"s"}


class TestDigitOnlyRegex:
    @pytest.mark.parametrize(
        "pattern",
        [r"\d+", r"[0-9]{3}", r"\d+|\d{2}", r"(?:\d)+", r"[0-35-9]+", r"0*\d+", r"^\d+$"],
    )
    def test_digit_only(self, pattern):
        assert key_regex_is_digit_only(pattern) is True

    @pytest.mark.parametrize(
        "pattern",
        [r"[A-Za-z]+", r".+", r"\w+", r"R2_\d{3}", r"[^a-z]+", r"\d+|x", r"\D+", "", "(\\d+)", "[", 7],
    )
    def test_not_digit_only(self, pattern):
        assert key_regex_is_digit_only(pattern) is False


@pytest.fixture
def padded_dir(tmp_path):
    """6MWT-001.txt (1.5) and 6MWT-002.txt (2.5)."""
    (tmp_path / "6MWT-001.txt").write_text("1.5")
    (tmp_path / "6MWT-002.txt").write_text("2.5")
    return tmp_path


def _read(f):
    from pathlib import Path

    return float(Path(str(f)).read_text())


def _pi(root):
    return PathInput("6MWT-{trial}.txt", root_folder=str(root), name="six_mwt")


class TestStandaloneForEach:
    def test_numeric_explicit_spellings_are_one_trial(self, padded_dir):
        calls = []

        def fn(f):
            calls.append(f)
            return _read(f)

        result = for_each(
            fn, {"f": _pi(padded_dir)}, trial=["001", 1, 2],
            schema_key_types={"trial": "numeric"},
        )
        assert len(calls) == 2
        assert sorted(result["trial"].tolist()) == [1, 2]
        assert sorted(result["output"].tolist()) == [1.5, 2.5]

    def test_string_key_turns_off_zero_padding(self, padded_dir):
        exact = for_each(
            _read, {"f": _pi(padded_dir)}, trial=["001"],
            schema_key_types={"trial": "string"},
        )
        assert exact["output"].tolist() == [1.5]
        bridged = for_each(
            _read, {"f": _pi(padded_dir)}, trial=[1],
            schema_key_types={"trial": "string"},
        )
        assert bridged is None or len(bridged) == 0

    def test_undeclared_keeps_automatic_condensing(self, padded_dir):
        result = for_each(_read, {"f": _pi(padded_dir)}, trial=[])
        assert sorted(result["trial"].tolist()) == [1, 2]

    def test_string_key_keeps_discovered_spelling(self, padded_dir):
        result = for_each(
            _read, {"f": _pi(padded_dir)}, trial=[],
            schema_key_types={"trial": "string"},
        )
        assert sorted(result["trial"].tolist()) == ["001", "002"]

    def test_bad_type_rejected(self, padded_dir):
        with pytest.raises(ValueError, match="must be one of"):
            for_each(_read, {"f": _pi(padded_dir)}, trial=[1], schema_key_types={"trial": "int"})
