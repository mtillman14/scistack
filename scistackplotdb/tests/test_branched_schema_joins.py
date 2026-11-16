"""Characterization of branched schema keys in Plot Studio joins.

NO production code changed. Flat key list
``[brand, category, item, location, person]``, real shape two branches under
``brand``. ``scistackplotdb.hierarchy.join_kind`` decides "broadcast" today by
LIST PREFIX; the plan (stage subset_joins) decides it by key-SET subset.

Failures are recorded under "Findings" in .claude/plan-branched-schema-keys.md
and marked ``xfail(strict=True)`` naming the fixing stage.
"""

from __future__ import annotations

import pytest
from scidb import BaseVariable, configure_database
from scidb.store.database import _local
from scistackplot import FactorVariable

from scistackplotdb import ScidbSource
from scistackplotdb.hierarchy import join_kind

KEYS = ["brand", "category", "item", "location", "person"]
BRANCHES = [["brand", "category", "item"], ["brand", "location", "person"]]


class ItemPrice(BaseVariable):
    """brand/category/item."""


class PersonAge(BaseVariable):
    """brand/location/person."""


class BrandRating(BaseVariable):
    """brand."""


class Purchase(BaseVariable):
    """All five keys."""


@pytest.fixture
def seeded(tmp_path):
    db = configure_database(tmp_path / "branched_plots.duckdb", BRANCHES)
    for b in ("B1", "B2"):
        BrandRating.save(1.0, brand=b)
        for c in ("c1", "c2"):
            for i in ("i1", "i2"):
                ItemPrice.save(10.0, brand=b, category=c, item=i)
        for loc in ("l1", "l2"):
            for p in ("p1", "p2"):
                PersonAge.save(30.0, brand=b, location=loc, person=p)
    for b in ("B1", "B2"):
        for c, i in (("c1", "i1"), ("c2", "i2")):
            for loc, p in (("l1", "p1"), ("l2", "p2")):
                Purchase.save(
                    2.0, brand=b, category=c, item=i, location=loc, person=p
                )
    yield db
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


# --- the pure rule, on key lists -------------------------------------------


def test_brand_onto_item_broadcasts():
    assert join_kind(["brand"], ["brand", "category", "item"]) == "broadcast"


def test_person_branch_onto_purchase_broadcasts_by_key_set():
    """{brand, location, person} is a SUBSET of Purchase's five keys but not a
    list prefix of them."""
    assert (
        join_kind(["brand", "location", "person"], KEYS) == "broadcast"
    )


def test_item_against_person_is_incompatible():
    assert (
        join_kind(["brand", "category", "item"], ["brand", "location", "person"])
        == "incompatible"
    )


# --- against a real database ------------------------------------------------


def test_brand_factor_reaches_item_measure(seeded):
    table = ScidbSource(seeded).get_table(
        ["ItemPrice"], factor_variables=[FactorVariable("BrandRating")]
    )
    assert len(table.frame) == 8, "the join must not multiply rows"
    assert table.has_factor("BrandRating")


def test_person_level_factor_reaches_purchase_measure(seeded):
    table = ScidbSource(seeded).get_table(
        ["Purchase"], factor_variables=[FactorVariable("PersonAge")]
    )
    assert len(table.frame) == 8
    assert table.has_factor("PersonAge")


def test_item_measure_with_person_factor_is_refused_with_a_reason(seeded):
    """No record names both an item and a person except Purchase, which is not
    the measure here. Characterize the refusal's wording."""
    with pytest.raises(Exception) as excinfo:
        ScidbSource(seeded).get_table(
            ["ItemPrice"], factor_variables=[FactorVariable("PersonAge")]
        )
    assert "item" in str(excinfo.value) or "prefix" in str(excinfo.value)


# --- stage subset_joins: no firewall between branches -----------------------


def test_a_person_factor_groups_a_cross_branch_measure_and_keeps_its_depth_after_person(seeded):
    table = ScidbSource(seeded).get_table(
        ["Purchase"], factor_variables=[FactorVariable("PersonAge")]
    )
    assert len(table.frame) == 8, "the join must not multiply rows"
    # Pinned by brand/location/person; in the flat key order that is after
    # `person` (position 5), not "3 keys deep".
    assert table.factor_depths["PersonAge"] == 5


def test_a_brand_factor_sits_at_depth_one(seeded):
    table = ScidbSource(seeded).get_table(
        ["Purchase"], factor_variables=[FactorVariable("BrandRating")]
    )
    assert table.factor_depths["BrandRating"] == 1


def test_category_and_person_can_both_group_the_purchase_table(seeded):
    """The user's case: group items by category and person together. The
    Purchase table carries both keys, so both are columns to group by."""
    from scistackplot import PlotKind, PlotSpec, Role, resolve

    table = ScidbSource(seeded).get_table(["Purchase"])
    assert {"category", "person"} <= set(table.frame.columns)
    spec = PlotSpec(
        measures=["Purchase"],
        roles={
            "category": Role.GROUP,
            "person": Role.GROUP,
            "brand": Role.COLLAPSE,
            "item": Role.COLLAPSE,
            "location": Role.COLLAPSE,
        },
        kind=PlotKind.BOX,
        groups=["category", "person"],
    )
    figures = resolve(spec, table)
    assert len(figures) == 1


def test_a_two_branch_join_is_refused_naming_both_sides(seeded):
    with pytest.raises(ValueError) as excinfo:
        ScidbSource(seeded).get_table(
            ["ItemPrice"], factor_variables=[FactorVariable("PersonAge")]
        )
    message = str(excinfo.value)
    assert "location" in message and "person" in message
    assert "link" in message
