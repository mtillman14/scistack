"""Tests for to_key() on Fixed/Merge/ColumnSelection.

Ported from scidb's (pre-unification) versions of these classes so
scidb's version-key/call_id hashing gets byte-identical output once it
imports these from scifor instead of its own copies -- see Landmine A in
docs/claude / the unification plan.
"""

import json

import pandas as pd

from scifor import ColumnSelection, Fixed, Merge


class Dummy:
    pass


def test_fixed_to_key_with_type():
    f = Fixed(Dummy, session="BL")
    assert f.to_key() == "Fixed(Dummy, session='BL')"


def test_fixed_to_key_no_metadata():
    f = Fixed(Dummy)
    assert f.to_key() == "Fixed(Dummy)"


def test_fixed_to_key_sorts_metadata_keys():
    f = Fixed(Dummy, b=2, a=1)
    assert f.to_key() == "Fixed(Dummy, a=1, b=2)"


def test_fixed_to_key_with_column_selection():
    cs = ColumnSelection(Dummy, ["col"])
    f = Fixed(cs, session="BL")
    assert f.to_key() == f"Fixed({cs.to_key()}, session='BL')"


def test_merge_to_key_with_types():
    m = Merge(Dummy, Dummy, name="merged")
    assert json.loads(m.to_key()) == {
        "__type": "Merge", "name": "merged", "how": "exact", "tables": ["Dummy", "Dummy"],
    }


def test_merge_to_key_with_fixed():
    m = Merge(Dummy, Fixed(Dummy, session="BL"), name="merged")
    assert json.loads(m.to_key())["tables"] == ["Dummy", "Fixed(Dummy, session='BL')"]


def test_merge_to_key_changes_with_name_and_how():
    keys = {
        Merge(Dummy, Dummy, name="a").to_key(),
        Merge(Dummy, Dummy, name="b").to_key(),
        Merge(Dummy, Dummy, name="a", how="left").to_key(),
    }
    assert len(keys) == 3


def test_column_selection_to_key_basic():
    cs = ColumnSelection(Dummy, ["a", "b"])
    assert cs.to_key() == "Dummy[['a', 'b'], iterate=False]"


def test_column_selection_to_key_iterate():
    cs = ColumnSelection(Dummy, ["a"], iterate=True)
    assert cs.to_key() == "Dummy[['a'], iterate=True]"


def test_column_selection_to_key_omits_empty_excl_columns():
    """excl_columns is only appended when non-empty, so existing (scidb)
    keys without it are byte-identical."""
    cs = ColumnSelection(Dummy, ["a"])
    assert "excl" not in cs.to_key()


def test_column_selection_to_key_includes_excl_columns_when_present():
    cs = ColumnSelection(Dummy, ["a", "b"], excl_columns=["b"])
    assert cs.to_key() == "Dummy[['a', 'b'], iterate=False, excl=['b']]"


def test_column_selection_to_key_with_dataframe_data():
    df = pd.DataFrame({"subject": [1], "a": [1.0]})
    cs = ColumnSelection(df, ["a"])
    # No __name__ on a DataFrame -> falls back to repr().
    assert cs.to_key() == f"{df!r}[['a'], iterate=False]"
