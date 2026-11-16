"""The settings panel's level checkboxes on a BRANCHED schema.

``execution_service.default_schema_level`` is the one resolver every run route
and the panel share; with a branched dataset its inputs imply the keys they
share, and ``level_choices`` says which boxes to show
(plan-branched-schema-keys.md, stage level_default).
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database
from scidb.run.schema_level import level_choices
from scidb.store.database import clear_current_database

from scistack_gui.domain.edge_resolver import variable_binding
from scistack_gui.services.execution_service import default_schema_level

SHOP = [["brand", "category", "item"], ["brand", "location", "person"]]


class LvItem(BaseVariable):
    pass


class LvPerson(BaseVariable):
    pass


class LvPurchase(BaseVariable):
    pass


@pytest.fixture
def shop(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "shop.duckdb", SHOP)
    LvItem.save(np.array([1.0]), brand="B1", category="c1", item="i1")
    LvPerson.save(np.array([2.0]), brand="B1", location="l1", person="p1")
    LvPurchase.save(
        np.array([3.0]), brand="B1", category="c1", item="i1", location="l1", person="p1"
    )
    yield db
    db.close()
    clear_current_database()
    _scifor.set_schema([])


def _target(**bindings):
    return {"constants": {}, "output_type": "X", "bindings": bindings}


def _level(db, **bindings):
    levels: list = []
    level, rule = default_schema_level(
        db, "never_ran", [_target(**bindings)], levels_out=levels
    )
    return level, rule, levels


def test_item_and_person_inputs_iterate_brand(shop):
    level, rule, _ = _level(
        shop, a=variable_binding(["LvItem"]), b=variable_binding(["LvPerson"])
    )
    assert level.keys == ("brand",)
    assert "every input carries" in rule


def test_adding_a_cross_branch_input_iterates_all_five(shop):
    level, _, _ = _level(
        shop,
        a=variable_binding(["LvItem"]),
        b=variable_binding(["LvPerson"]),
        c=variable_binding(["LvPurchase"]),
    )
    assert level.keys == tuple(shop.dataset_schema_keys)


def test_the_panel_offers_only_the_shared_key_for_item_and_person(shop):
    level, _, levels = _level(
        shop, a=variable_binding(["LvItem"]), b=variable_binding(["LvPerson"])
    )
    choices = level_choices(shop.schema_tree, levels, level.keys)
    assert choices.keys == ("brand",)
    assert choices.disabled == {}


def test_a_brand_only_input_offers_both_branches_and_disables_the_other_after_a_tick(shop):
    class LvBrand(BaseVariable):
        pass

    LvBrand.save(np.array([4.0]), brand="B1")
    levels: list = []
    default_schema_level(shop, "never_ran", [_target(a=variable_binding(["LvBrand"]))], levels_out=levels)
    choices = level_choices(shop.schema_tree, levels, ["category"])
    assert choices.keys == tuple(shop.dataset_schema_keys)
    assert sorted(choices.disabled) == ["location", "person"]
