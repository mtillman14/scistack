"""
Schema-depth joins.

Two variables can share a plot only if their schema levels can be joined.
A variable lives at a SET of schema keys (the keys its records name). The tree
of keys is only a default (docs/claude/branched-schema-keys.md); what can join
what is decided by comparing those sets, so a person-level variable can attach
to a cross-branch Purchase even though its keys are not a prefix of the key
list. On a linear schema the sets are prefixes, as before:

* identical key sets → a straight merge on all of them;
* one a subset of the other → **broadcast**: the smaller variable's value is
  repeated across every row of the larger that shares its keys (subject-level
  Mass against trial-level Speed; a person-level age against a Purchase);
* otherwise → refuse, and say why (no record names both).

Answering this question is also what lets the GUI populate its measure list
honestly instead of offering combinations that cannot be built.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd
from scistacklog import Log

from .load import LATEST_COLUMN, VariableFrame

LAYER = "scistackplotdb"

JoinKind = Literal["identical", "broadcast", "incompatible"]

#: Private carriers for each side's latest-flag across a merge. Never leave
#: :func:`join_frames` — they are combined into one :data:`LATEST_COLUMN` and
#: dropped, so nothing downstream has to know a join happened.
_LEFT_LATEST = "__latest_left"
_RIGHT_LATEST = "__latest_right"


def join_kind(levels_a: list[str], levels_b: list[str]) -> JoinKind:
    keys_a, keys_b = set(levels_a), set(levels_b)
    if keys_a == keys_b:
        return "identical"
    if keys_a < keys_b or keys_b < keys_a:
        return "broadcast"
    return "incompatible"


def joinable(levels_a: list[str], levels_b: list[str]) -> bool:
    return join_kind(levels_a, levels_b) != "incompatible"


def join_frames(
    left: VariableFrame,
    right: VariableFrame,
    *,
    left_value: str,
    right_value: str,
) -> pd.DataFrame:
    """
    Join two variables into one long frame carrying both measures.

    The merge is on the *shallower* variable's levels, which is exactly what
    broadcasting means: one subject-level row is reused for each of that
    subject's trials.
    """
    kind = join_kind(left.levels, right.levels)
    if kind == "incompatible":
        only_left = [k for k in left.levels if k not in right.levels]
        only_right = [k for k in right.levels if k not in left.levels]
        raise ValueError(
            f"{left.name} (keys {left.levels}) and {right.name} (keys {right.levels}) "
            f"cannot share a plot: no record names both {only_left} and {only_right}, "
            f"so there is no row to line their values up on. A variable saved at "
            f"both is what would link them."
        )

    shallow_levels = left.levels if len(left.levels) <= len(right.levels) else right.levels
    on = list(shallow_levels)

    left_frame = left.frame[[*left.levels, left_value, *left.variant_columns]].copy()
    right_frame = right.frame[[*right.levels, right_value, *right.variant_columns]].copy()

    # Carry each side's "my chain is current" flag through the join under a
    # private name, so the two cannot collide and neither is lost. Before this
    # the flag was simply dropped and a two-measure plot silently fell back to
    # showing every code version — the one case where the pin-latest default
    # quietly stopped applying.
    if left.latest_column:
        left_frame[_LEFT_LATEST] = left.frame[left.latest_column].values
    if right.latest_column:
        right_frame[_RIGHT_LATEST] = right.frame[right.latest_column].values

    # Variant columns can collide by name when both variables carry the same
    # branch param; suffix the right one so neither is silently dropped.
    overlap = set(left.variant_columns) & set(right.variant_columns)
    if overlap:
        right_frame = right_frame.rename(
            columns={name: f"{name}::{right.name}" for name in overlap}
        )

    merged = left_frame.merge(right_frame, on=on, how="inner", suffixes=("", "_right"))

    # A joined row is current only if BOTH of its measures are. Either side
    # being stale makes the pair a comparison across code versions, which is
    # exactly what the flag exists to keep out of the default figure.
    if _LEFT_LATEST in merged.columns or _RIGHT_LATEST in merged.columns:
        combined = None
        for column in (_LEFT_LATEST, _RIGHT_LATEST):
            if column in merged.columns:
                flag = merged[column].astype(bool)
                combined = flag if combined is None else (combined & flag)
        merged[LATEST_COLUMN] = combined
        merged = merged.drop(
            columns=[c for c in (_LEFT_LATEST, _RIGHT_LATEST) if c in merged.columns]
        )

    Log.info(
        "%s join: %s(%d) x %s(%d) on %s -> %d row(s)",
        kind,
        left.name,
        len(left_frame),
        right.name,
        len(right_frame),
        on,
        len(merged),
        layer=LAYER,
    )
    if kind == "broadcast":
        Log.debug(
            "broadcast: %s is shallower (%s); its values repeat across deeper rows",
            left.name if len(left.levels) <= len(right.levels) else right.name,
            shallow_levels,
            layer=LAYER,
        )
    return merged


def joined_levels(left: VariableFrame, right: VariableFrame) -> list[str]:
    """The level set of the joined frame — always the deeper of the two."""
    return left.levels if len(left.levels) >= len(right.levels) else right.levels
