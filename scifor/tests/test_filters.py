"""Tests for scifor.filters — Col, ColFilter, CompoundFilter, NotFilter."""

import pytest
import pandas as pd
from scifor.filters import Col, ColFilter, CompoundFilter, NotFilter


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "side": ["L", "R", "L", "R"],
        "speed": [1.0, 1.5, 2.0, 2.5],
        "value": [10, 20, 30, 40],
    })


# --- Col factory ---

def test_col_eq_creates_colfilter():
    f = Col("side") == "R"
    assert isinstance(f, ColFilter)
    assert f.column == "side"
    assert f.op == "=="
    assert f.value == "R"


def test_col_ne(sample_df):
    f = Col("side") != "R"
    mask = f.apply(sample_df)
    assert list(mask) == [True, False, True, False]


def test_col_lt(sample_df):
    f = Col("speed") < 2.0
    mask = f.apply(sample_df)
    assert list(mask) == [True, True, False, False]


def test_col_le(sample_df):
    f = Col("speed") <= 2.0
    mask = f.apply(sample_df)
    assert list(mask) == [True, True, True, False]


def test_col_gt(sample_df):
    f = Col("speed") > 1.5
    mask = f.apply(sample_df)
    assert list(mask) == [False, False, True, True]


def test_col_ge(sample_df):
    f = Col("speed") >= 1.5
    mask = f.apply(sample_df)
    assert list(mask) == [False, True, True, True]


# --- ColFilter.apply ---

def test_colfilter_apply_eq(sample_df):
    f = Col("side") == "R"
    mask = f.apply(sample_df)
    assert list(mask) == [False, True, False, True]


# --- CompoundFilter ---

def test_compound_and(sample_df):
    f = (Col("side") == "R") & (Col("speed") > 1.5)
    mask = f.apply(sample_df)
    assert list(mask) == [False, False, False, True]


def test_compound_or(sample_df):
    f = (Col("side") == "L") | (Col("speed") > 2.0)
    mask = f.apply(sample_df)
    assert list(mask) == [True, False, True, True]


# --- NotFilter ---

def test_not_filter(sample_df):
    f = ~(Col("side") == "R")
    assert isinstance(f, NotFilter)
    mask = f.apply(sample_df)
    assert list(mask) == [True, False, True, False]


def test_double_invert(sample_df):
    f = Col("side") == "R"
    double_inv = ~~f
    # double invert should give back something equivalent to original
    mask = double_inv.apply(sample_df)
    assert list(mask) == [False, True, False, True]


# --- to_key ---

def test_colfilter_to_key():
    f = Col("side") == "R"
    assert f.to_key() == "Col('side') == 'R'"


def test_compound_to_key():
    f = (Col("side") == "R") & (Col("speed") > 1.5)
    key = f.to_key()
    assert "Col('side') == 'R'" in key
    assert "Col('speed') > 1.5" in key


def test_not_filter_to_key():
    f = ~(Col("side") == "R")
    assert "Col('side') == 'R'" in f.to_key()
