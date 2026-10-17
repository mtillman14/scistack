"""
scistackstatsdb — scidb-backed statistics.

Loads StatStudio tables from a scidb database through scistackplotdb's
``ScidbSource`` (the same loading the figures use), and, in later stages,
saves stats specs, links them to saved plots, and writes ``stat_`` endpoints.
The statistics themselves are :mod:`scistackstats`.
"""

from __future__ import annotations

from typing import Any

from scistackstats import StatsSpec, load_tables


def load_stats_tables(db: Any, stats_spec: StatsSpec) -> dict:
    """``{variable: LongTable}`` for every DV of ``stats_spec``, from ``db``."""
    from scistackplotdb import ScidbSource

    return load_tables(ScidbSource(db), stats_spec)


__all__ = ["load_stats_tables"]
