"""Characterization of branched schema keys (plan-branched-schema-keys.md).

NO production code changed for this file. It records what a FLAT key list
``[brand, category, item, location, person]`` does today with a dataset whose
real shape is two branches under ``brand``::

    brand > category > item
    brand > location > person

Each test is one row of the plan's behaviour table. Some will pass (the flat
list already does the right thing), some will fail. A failing test is recorded
in the plan's "Findings" and marked ``xfail(strict=True)`` naming the stage that
fixes it, so the stage that fixes it must remove the mark.

The shape of the data (deliberately uneven, so a wrong join cannot pass by
symmetric counts):

* ``ItemPrice``  at brand/category/item      (2 brands x 2 categories x 2 items)
* ``PersonAge``  at brand/location/person    (2 brands x 2 locations x 2 people)
* ``BrandRating`` at brand
* ``Purchase``   at all five keys (a cross-branch record)
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance import expectation as ex

KEYS = ["brand", "category", "item", "location", "person"]
BRANDS = ["B1", "B2"]
CATEGORIES = ["c1", "c2"]
ITEMS = ["i1", "i2"]
LOCATIONS = ["l1", "l2"]
PEOPLE = ["p1", "p2"]
BRANCHES = [["brand", "category", "item"], ["brand", "location", "person"]]

ALL_KEYS = dict(
    brand=BRANDS, category=CATEGORIES, item=ITEMS, location=LOCATIONS, person=PEOPLE
)


class ItemPrice(BaseVariable):
    """Item-branch scalar."""


class PersonAge(BaseVariable):
    """Person-branch scalar."""


class BrandRating(BaseVariable):
    """Brand-level scalar, shared by both branches."""


class Purchase(BaseVariable):
    """Cross-branch: names an item AND a person."""


class EmptyPersonVar(BaseVariable):
    """Never saved; registered at ``person`` by hand."""


class BrandTotal(BaseVariable):
    pass


class PurchaseScaled(BaseVariable):
    pass


class Spread(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "branched.duckdb", BRANCHES)
    yield database
    _scifor.set_schema([])
    database.close()


@pytest.fixture
def seeded(db):
    for b in BRANDS:
        BrandRating.save(float(BRANDS.index(b) + 1), brand=b)
        for c in CATEGORIES:
            for i in ITEMS:
                price = 10.0 * (BRANDS.index(b) + 1) + CATEGORIES.index(c) + ITEMS.index(i) / 10
                ItemPrice.save(price, brand=b, category=c, item=i)
        for loc in LOCATIONS:
            for p in PEOPLE:
                age = 20.0 + LOCATIONS.index(loc) * 10 + PEOPLE.index(p)
                PersonAge.save(age, brand=b, location=loc, person=p)
    # One brand has a purchase at one (item, person) pair: the cross-branch fact.
    Purchase.save(
        3.0,
        brand="B1",
        category="c1",
        item="i1",
        location="l1",
        person="p1",
    )
    return db


# ---------------------------------------------------------------------------
# Save / load on each branch
# ---------------------------------------------------------------------------


class TestSaveLoad:
    def test_item_branch_round_trips(self, seeded):
        rec = ItemPrice.load(brand="B1", category="c1", item="i2")
        assert float(rec.data) == pytest.approx(10.1)

    def test_person_branch_round_trips(self, seeded):
        rec = PersonAge.load(brand="B2", location="l2", person="p2")
        assert float(rec.data) == pytest.approx(31.0)

    def test_brand_level_round_trips(self, seeded):
        assert float(BrandRating.load(brand="B2").data) == 2.0

    def test_cross_branch_record_round_trips(self, seeded):
        rec = Purchase.load(
            brand="B1", category="c1", item="i1", location="l1", person="p1"
        )
        assert float(rec.data) == 3.0


# ---------------------------------------------------------------------------
# for_each on each branch
# ---------------------------------------------------------------------------


def _double(x):
    return x * 2


class TestForEachPerBranch:
    def test_item_branch_iterates_its_own_keys(self, seeded):
        result = for_each(
            _double, {"x": ItemPrice}, [BrandTotal],
            brand=BRANDS, category=CATEGORIES, item=ITEMS, save=False,
        )
        assert result is not None and len(result) == 8

    def test_person_branch_iterates_its_own_keys(self, seeded):
        result = for_each(
            _double, {"x": PersonAge}, [BrandTotal],
            brand=BRANDS, location=LOCATIONS, person=PEOPLE, save=False,
        )
        assert result is not None and len(result) == 8


# ---------------------------------------------------------------------------
# Aggregate both branches to brand
# ---------------------------------------------------------------------------


def _total(x):
    return float(np.asarray(x).sum())


class TestAggregateToBrand:
    @pytest.mark.parametrize(
        "var,expected",
        [
            # B1 item prices: 10, 10.1, 11, 11.1 per category pair -> 42.2 total
            (ItemPrice, {"B1": 42.2, "B2": 82.2}),
            # B1 ages: l1 -> 20, 21 ; l2 -> 30, 31 -> 102 total ; same for B2
            (PersonAge, {"B1": 102.0, "B2": 102.0}),
        ],
    )
    def test_each_branch_sums_to_brand(self, seeded, var, expected):
        result = for_each(
            _total,
            {"x": var},
            [BrandTotal],
            brand=BRANDS,
            save=False,
        )
        assert result is not None and len(result) == 2
        got = dict(zip(result["brand"], result["BrandTotal"], strict=True))
        for b, v in expected.items():
            assert got[b] == pytest.approx(v)


# ---------------------------------------------------------------------------
# A run over Purchase with item- and person-level inputs broadcast onto it
# ---------------------------------------------------------------------------


def _price_times_age_plus_qty(price, age, qty):
    return float(price) * float(age) + float(qty)


class TestBroadcastOntoCrossBranch:
    """``ItemPrice`` has keys {brand, category, item}; ``PersonAge`` has
    {brand, location, person}; both are SUBSETS of Purchase's key set. This already works today: a
    coarse input's all-NULL key columns are dropped (``[coarse-input]`` in
    foreach_load), so it filters only on the keys it names. The iterables must
    list every key: with none, ``for_each`` aggregates (existing rule), and the
    default level choosing all five is stage level_default."""

    def test_item_input_broadcasts_onto_purchase(self, seeded):
        result = for_each(
            lambda price, qty: float(price) + float(qty),
            {"price": ItemPrice, "qty": Purchase},
            [PurchaseScaled],
            save=False,
            **ALL_KEYS,
        )
        assert result is not None and len(result) == 1
        assert result["PurchaseScaled"].iloc[0] == pytest.approx(13.0)

    def test_person_input_broadcasts_onto_purchase(self, seeded):
        result = for_each(
            lambda age, qty: float(age) + float(qty),
            {"age": PersonAge, "qty": Purchase},
            [PurchaseScaled],
            save=False,
            **ALL_KEYS,
        )
        assert result is not None and len(result) == 1
        assert result["PurchaseScaled"].iloc[0] == pytest.approx(23.0)

    def test_item_and_person_inputs_together_on_purchase(self, seeded):
        result = for_each(
            _price_times_age_plus_qty,
            {"price": ItemPrice, "age": PersonAge, "qty": Purchase},
            [PurchaseScaled],
            save=False,
            **ALL_KEYS,
        )
        assert result is not None and len(result) == 1
        assert result["PurchaseScaled"].iloc[0] == pytest.approx(10.0 * 20.0 + 3.0)


# ---------------------------------------------------------------------------
# distribute
# ---------------------------------------------------------------------------


def _spread(x):
    return np.asarray([float(x) + k for k in range(3)])


class TestDistribute:
    def test_distribute_from_brand_is_ambiguous(self, seeded):
        """Brand has two children (category, location). Today the target is
        'the next key in the list' = category, silently. The plan raises and
        asks for the target by name."""
        with pytest.raises(Exception, match="category|location|target"):
            for_each(
                _spread,
                {"x": BrandRating},
                [Spread],
                brand=["B1"],
                distribute=True,
                save=False,
            )

    def test_distribute_from_item_has_no_child_today(self, seeded):
        """item is a leaf of its branch; in the flat list its 'next key' is
        location, which is the wrong branch."""
        with pytest.raises(Exception):
            for_each(
                _spread,
                {"x": ItemPrice},
                [Spread],
                brand=["B1"],
                category=["c1"],
                item=["i1"],
                distribute=True,
                save=False,
            )


# ---------------------------------------------------------------------------
# A where= filter from the other branch
# ---------------------------------------------------------------------------


class TestFilterFromOtherBranch:
    def test_item_variable_filtered_by_person_variable_never_passes_everything(self, seeded):
        """No record names both an item and a person except Purchase, so an
        ItemPrice row cannot be matched to a PersonAge value. Whatever the
        outcome (error, empty), it must not silently return all 8 rows."""
        try:
            result = for_each(
                lambda x: float(x),
                {"x": ItemPrice},
                [PurchaseScaled],
                where=(PersonAge == 20.0),
                save=False,
            )
        except Exception:
            return
        assert result is None or len(result) < 8

    def test_purchase_filtered_by_person_attribute(self, seeded):
        result = for_each(
            lambda qty: float(qty),
            {"qty": Purchase},
            [PurchaseScaled],
            where=(PersonAge == 21.0),
            save=False,
        )
        # Purchase is at p1/l1 whose age is 20.0, so == 21.0 removes it.
        assert result is None or len(result) == 0


# ---------------------------------------------------------------------------
# Expected locations of an empty variable
# ---------------------------------------------------------------------------


class TestExpectedLocationsOfEmptyVariable:
    def test_an_empty_person_variable_expects_its_branch_not_a_list_prefix(self, seeded):
        """Registered at ``person``. Today: keys = list prefix up to person =
        all five, including category and item. Correct: brand, location,
        person (the chain)."""
        seeded._duck.execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?)",
            ["EmptyPersonVar", "person", "{}", ""],
        )
        result = ex.expectation(seeded, "EmptyPersonVar")
        assert result.keys == ["brand", "location", "person"]
