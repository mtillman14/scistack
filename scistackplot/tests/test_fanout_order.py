"""
Which factors a spec fans out over, and in what order.

* **Order is the schema's, not the roles dict's.** The fan-out list used to come
  from ``roles.items()``, i.e. whichever role the user clicked first, so
  assigning ``trial`` before ``subject`` produced trial-major figures — and the
  exported ``PathOutput`` template inherited the same order.
* **Nothing is promoted.** The old model silently promoted FREE ancestors of an
  iterated key to ITERATE, because FREE would otherwise pool them. Every role
  is explicit now — an unmentioned factor defaults to ITERATE in the open —
  so "one figure per trial" with a grouped subject is exactly that.
"""

from __future__ import annotations

import pytest

from scistackplot import PlotSpec, Role, fanout_keys, resolve
from scistackplot.roles import complete_roles


def test_fanout_runs_in_schema_order_not_dict_order(scalar_table):
    """The roles dict is built in click order; the fan-out must not be."""
    reversed_order = PlotSpec(
        measures=["StepLength"],
        # trial named FIRST, subject second — the order a user produces by
        # setting the deepest key's dropdown before the shallowest.
        roles={"trial": Role.ITERATE, "subject": Role.ITERATE, "session": Role.GROUP},
    )
    assert fanout_keys(reversed_order, scalar_table) == ["subject", "trial"]


def test_figures_roll_over_from_one_subject_to_the_next(scalar_table):
    """Subject-major, each key in its declared level order.

    This is the navigator's whole contract: stepping past the last trial of
    subject 01 lands on subject 02's FIRST trial.
    """
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.ITERATE, "session": Role.GROUP},
    )
    keys = [
        (figure.figure_key["subject"], figure.figure_key["trial"])
        for figure in resolve(spec, scalar_table)
    ]

    assert keys[0] == ("01", "1")
    assert keys == sorted(keys)  # zero-padded IDs, so this is the real order
    boundary = keys.index(("02", "1"))
    assert keys[boundary - 1] == ("01", "4")


def test_an_iterated_key_does_not_drag_its_ancestors_along(scalar_table):
    """`trial=Separate figures` with subject grouped: one figure per trial,
    every subject's trial 1 side by side — which is what was asked for."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.GROUP, "session": Role.COLLAPSE},
    )
    assert fanout_keys(spec, scalar_table) == ["trial"]
    roles = complete_roles(spec, scalar_table)
    assert roles["subject"] is Role.GROUP
    assert roles["session"] is Role.COLLAPSE


def test_unmentioned_factors_iterate_in_the_open(scalar_table):
    """The replacement for promotion: a factor the spec never named fans out
    visibly instead of pooling silently, and it shows up in `fanout_keys`."""
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.GROUP})
    assert fanout_keys(spec, scalar_table) == ["subject", "session"]


def test_a_non_schema_factor_iterates_alone(struct_table):
    """`ColName` is a struct's fields; iterating it involves nobody else."""
    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.ITERATE, "subject": Role.GROUP, "trial": Role.COLLAPSE},
    )
    assert fanout_keys(spec, struct_table) == ["ColName"]


def test_a_csv_table_has_no_hierarchy(scalar_frame):
    """Sources that cannot know the nesting say so; the fan-out is still the
    declared order."""
    from scistackplot import LongTable

    table = LongTable.from_frame(
        scalar_frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
    )
    assert table.schema_levels == []
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.GROUP, "session": Role.COLLAPSE},
    )
    assert fanout_keys(spec, table) == ["trial"]


@pytest.mark.parametrize("missing", ["session", "trial"])
def test_only_keys_the_table_carries_fan_out(scalar_frame, missing):
    """A variable saved at subject level has no `trial` column to iterate."""
    from scistackplot import LongTable

    kept = [k for k in ["subject", "session", "trial"] if k != missing]
    table = LongTable.from_frame(
        scalar_frame.drop(columns=[missing]),
        factors=kept,
        measures=["StepLength"],
        schema_levels=["subject", "session", "trial"],
    )
    assert missing not in table.schema_levels

    spec = PlotSpec(measures=["StepLength"], roles={kept[-1]: Role.GROUP})
    assert missing not in fanout_keys(spec, table)
