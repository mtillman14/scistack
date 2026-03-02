"""Tests for scifor.schema — set_schema / get_schema."""

import pytest
from scifor.schema import set_schema, get_schema


def setup_function():
    set_schema([])  # reset before each test


def test_get_schema_default_empty():
    assert get_schema() == []


def test_set_and_get_schema():
    set_schema(["subject", "session"])
    assert get_schema() == ["subject", "session"]


def test_get_schema_returns_copy():
    set_schema(["subject"])
    copy = get_schema()
    copy.append("mutated")
    assert get_schema() == ["subject"]  # original not mutated


def test_set_schema_overwrites():
    set_schema(["subject", "session"])
    set_schema(["trial"])
    assert get_schema() == ["trial"]


def test_set_schema_empty_list():
    set_schema(["subject"])
    set_schema([])
    assert get_schema() == []
