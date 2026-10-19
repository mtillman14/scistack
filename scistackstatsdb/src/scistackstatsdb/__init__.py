"""
scistackstatsdb — scidb-backed statistics.

Loads StatStudio tables from a scidb database through scistackplotdb's
``ScidbSource`` (the same loading the figures use), saves stats tables and
stats presets (``saved``), finds the stats tables linked to a saved plot, and
carries them in project bundles (``bundle_section``). The statistics
themselves are :mod:`scistackstats`.
"""

from __future__ import annotations

from typing import Any

from scistackstats import StatsSpec, load_tables

from .endpoint import StatsEndpointCode, generate_stats_endpoint, required_declarations
from .saved import (
    SavedStats,
    SavedStatsError,
    SavedStatsExists,
    SavedStatsInfo,
    StatsPresetInfo,
    apply_stats_preset,
    find_saved_stats,
    hide_saved_stats,
    hide_stats_preset,
    linked_stats_for,
    list_saved_stats,
    list_stats_presets,
    load_saved_stats,
    rename_saved_stats,
    rename_stats_preset,
    save_stats,
    save_stats_preset,
    saved_stats_history,
)


def load_stats_tables(db: Any, stats_spec: StatsSpec) -> dict:
    """``{variable: LongTable}`` for every DV of ``stats_spec``, from ``db``."""
    from scistackplotdb import ScidbSource

    return load_tables(ScidbSource(db), stats_spec)


__all__ = [
    "StatsEndpointCode", "generate_stats_endpoint", "required_declarations",
    "SavedStats", "SavedStatsError", "SavedStatsExists", "SavedStatsInfo", "StatsPresetInfo",
    "apply_stats_preset", "find_saved_stats", "hide_saved_stats", "hide_stats_preset", "linked_stats_for",
    "list_saved_stats", "list_stats_presets", "load_saved_stats", "load_stats_tables", "rename_saved_stats",
    "rename_stats_preset", "save_stats", "save_stats_preset", "saved_stats_history",
]
