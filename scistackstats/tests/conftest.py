"""Fixtures: the Better Popcorn ratings, with optional between and nested factors."""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role

SUBJECTS = [f"S{i:02d}" for i in range(1, 7)]
BRANDS = ["Acme", "Butter", "Corn"]
BRAND_EFFECT = {"Acme": 0.0, "Butter": 1.5, "Corn": 3.0}


def popcorn_frame(*, sessions: bool = False, group: bool = False, trials: int = 2) -> pd.DataFrame:
    """Ratings per subject × brand (× session) × trial; deterministic values.

    ``group`` adds a subject-level between factor (ctl: S01–S03, trt: S04–S06).
    """
    rows = []
    for si, subject in enumerate(SUBJECTS):
        for brand in BRANDS:
            for session in (["pre", "post"] if sessions else [None]):
                for trial in range(1, trials + 1):
                    value = 5.0 + 0.3 * si + BRAND_EFFECT[brand] + 0.1 * trial
                    value += 0.07 * ((si * 7 + trial * 3 + len(brand)) % 5)
                    if session == "post":
                        value += 0.8
                    row = {"subject": subject, "brand": brand, "trial": str(trial), "Ratings": value}
                    if sessions:
                        row["session"] = session
                    if group:
                        row["group"] = "ctl" if si < 3 else "trt"
                    rows.append(row)
    return pd.DataFrame(rows)


def popcorn_table(**kwargs) -> LongTable:
    frame = popcorn_frame(**kwargs)
    factors = [c for c in ("subject", "brand", "session", "group", "trial") if c in frame.columns]
    return LongTable.from_frame(
        frame, factors=factors, measures=["Ratings"], name="Ratings",
        schema_levels=["subject", "trial"],
        factor_depths={"group": 1} if "group" in frame.columns else None,
        level_order={"brand": BRANDS},
    )


def bar_spec(**changes) -> PlotSpec:
    base = dict(
        measures=["Ratings"],
        roles={"brand": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["brand"],
        kind=PlotKind.BAR,
    )
    base.update(changes)
    return PlotSpec(**base)


@pytest.fixture
def table() -> LongTable:
    return popcorn_table()
