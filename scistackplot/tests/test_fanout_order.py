"""
Which factors a spec fans out over, and in what order.

Two rules, both of which used to be wrong in ways that only showed up as a
figure set nobody could navigate:

* **Order is the schema's, not the roles dict's.** The fan-out list used to come
  from ``roles.items()``, i.e. whichever role the user clicked first, so
  assigning ``trial`` before ``subject`` produced trial-major figures — and the
  exported ``PathOutput`` template inherited the same order.
* **An iterated key iterates its ancestors.** "One figure per trial" with a
  ``[subject, trial]`` schema otherwise means one figure holding every subject's
  trial 1, which pools unrelated observations.
"""

from __future__ import annotations

import pytest

from scistackplot import PlotSpec, Role, fanout_keys, resolve
from scistackplot.roles import complete_roles, iterate_ancestors


def test_fanout_runs_in_schema_order_not_dict_order(scalar_table):
    """The roles dict is built in click order; the fan-out must not be."""
    reversed_order = PlotSpec(
        measures=["StepLength"],
        # trial named FIRST, subject second — the order a user produces by
        # setting the deepest key's dropdown before the shallowest.
        roles={"trial": Role.ITERATE, "subject": Role.ITERATE, "session": Role.X},
    )
    assert fanout_keys(reversed_order, scalar_table) == ["subject", "trial"]


def test_figures_roll_over_from_one_subject_to_the_next(scalar_table):
    """Subject-major, each key in its declared level order.

    This is the navigator's whole contract: stepping past the last trial of
    subject 01 lands on subject 02's FIRST trial.
    """
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.ITERATE, "session": Role.X},
    )
    keys = [
        (figure.figure_key["subject"], figure.figure_key["trial"])
        for figure in resolve(spec, scalar_table)
    ]

    assert keys[0] == ("01", "1")
    assert keys == sorted(keys)  # zero-padded IDs, so this is the real order
    boundary = keys.index(("02", "1"))
    assert keys[boundary - 1] == ("01", "4")


def test_iterating_a_nested_key_iterates_its_ancestors(scalar_table):
    """One figure per trial is really one figure per (subject, session, trial)."""
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})

    assert fanout_keys(spec, scalar_table) == ["subject", "session", "trial"]

    figures = resolve(spec, scalar_table)
    assert len(figures) == 3 * 2 * 4
    # Each figure is ONE location, not one trial number pooled across subjects.
    assert all(
        set(f.figure_key) == {"subject", "session", "trial"} for f in figures
    )
    assert len({tuple(f.figure_key.values()) for f in figures}) == len(figures)


def test_promotion_is_reported_not_silent(scalar_table):
    """A user who asked for one figure per trial and got 24 must be told why."""
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})
    notes = resolve(spec, scalar_table)[0].fanout_notes

    assert len(notes) == 1
    assert "subject" in notes[0] and "trial" in notes[0]


def test_the_note_is_computed_from_declared_roles_not_promoted_ones(scalar_table):
    """Regression: the promotion is invisible in its own output.

    `complete_roles` applies the promotion, so asking the roles it returns what
    was promoted answers "nothing" — and the note explaining a fan-out four
    times the expected size silently disappeared.
    """
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})

    assert iterate_ancestors(complete_roles(spec, scalar_table), scalar_table) == []
    assert iterate_ancestors(
        complete_roles(spec, scalar_table, promote=False), scalar_table
    ) == ["subject", "session"]
    assert resolve(spec, scalar_table)[0].fanout_notes


def test_an_assigned_ancestor_keeps_its_channel(scalar_table):
    """`subject=colour, trial=separate figures` is a legitimate figure.

    Promotion is only ever FROM FREE — the role that would silently pool. A
    user who put an ancestor on a channel meant it.
    """
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.COLOR, "session": Role.X},
    )

    assert fanout_keys(spec, scalar_table) == ["trial"]
    assert iterate_ancestors(complete_roles(spec, scalar_table), scalar_table) == []
    assert len(resolve(spec, scalar_table)) == 4


def test_promotion_changes_the_roles_every_consumer_sees(scalar_table):
    """Promotion happens in complete_roles, so capability agrees with reduce.

    If the fan-out promoted `subject` but the roles dict still called it FREE,
    `has_replicates` would claim a distribution the figure cannot draw — one
    location has a single value.
    """
    from scistackplot.capability import has_replicates

    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})
    roles = complete_roles(spec, scalar_table)

    assert roles["subject"] is Role.ITERATE
    assert roles["session"] is Role.ITERATE
    assert not has_replicates(roles)


def test_no_schema_levels_means_no_promotion(struct_table):
    """A factor that is not a schema key has no ancestors.

    `ColName` is a struct's fields; iterating it must not drag subject and
    trial along.
    """
    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.ITERATE, "subject": Role.COLOR, "trial": Role.FREE},
    )
    assert fanout_keys(spec, struct_table) == ["ColName"]


def test_a_csv_table_has_no_hierarchy(scalar_frame):
    """Sources that cannot know the nesting say so, and nothing is promoted."""
    from scistackplot import LongTable

    table = LongTable.from_frame(
        scalar_frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
    )
    assert table.schema_levels == []
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})
    assert fanout_keys(spec, table) == ["trial"]
    assert resolve(spec, table)[0].fanout_notes == []


@pytest.mark.parametrize("missing", ["session", "trial"])
def test_only_keys_the_table_carries_are_promoted(scalar_frame, missing):
    """A variable saved at subject level has no `trial` column to group by."""
    from scistackplot import LongTable

    kept = [k for k in ["subject", "session", "trial"] if k != missing]
    table = LongTable.from_frame(
        scalar_frame.drop(columns=[missing]),
        factors=kept,
        measures=["StepLength"],
        schema_levels=["subject", "session", "trial"],
    )
    assert missing not in table.schema_levels

    spec = PlotSpec(measures=["StepLength"], roles={kept[-1]: Role.ITERATE})
    assert missing not in fanout_keys(spec, table)
