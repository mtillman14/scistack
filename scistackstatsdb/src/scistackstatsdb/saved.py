"""Saved StatStudio tables and stats presets, kept in the project database.

Both use scistackplotdb's :class:`~scistackplotdb._versioned.VersionedStore`,
the owner of the saved-plot rules (S20). So a saved stats table behaves like
a saved plot:

- the id is the identity, the name a label, unique across the project;
- re-saving a visible name appends a version, and the newest version opens;
- "remove" hides every version, and nothing is ever deleted;
- save is strict (``StatsSpec.from_dict``), open salvages
  (``scistackstats.restore``), and opening never rewrites the stored copy.

A saved stats table may name the saved plot it syncs with
(``StatsSpec.linked_plot``, S11). The link is stored only here.
:func:`linked_stats_for` finds a plot's stats tables by query, so the plot
store never holds a back-reference.

A **stats preset** stores only the stats settings
(``scistackstats.restore.STATS_SETTINGS``): what to compute and how it reads.
Applying one to a stats table keeps that table's design, DVs and link.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log
from scistackplotdb._versioned import VersionedStore

from scistackstats import StatsSpec, StatsSpecError
from scistackstats.restore import restore_stats_spec, stats_settings, with_stats_settings

LAYER = "scistackstatsdb"
ENVELOPE_FORMAT = 1


class SavedStatsError(ValueError):
    """A save, rename or load that cannot be done as asked."""


class SavedStatsExists(SavedStatsError):
    """``save_stats(overwrite=False)`` found another visible item with that name."""

    def __init__(self, existing: "SavedStatsInfo"):
        super().__init__(f"There is already a saved stats table named {existing.name!r} (version {existing.version}).")
        self.existing = existing


STORE = VersionedStore(
    table="_stats_spec", id_column="stats_id", scope_column=None,
    noun="saved stats table", tag="saved-stats", error=SavedStatsError,
)
PRESET_STORE = VersionedStore(
    table="_stats_preset", id_column="preset_id", scope_column=None,
    noun="stats preset", tag="stats-preset", error=SavedStatsError,
)


@dataclass(frozen=True)
class SavedStatsInfo:
    stats_id: str
    name: str
    version: int
    saved_at: str
    hidden: bool = False
    linked_plot: str | None = None
    dvs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"stats_id": self.stats_id, "name": self.name, "version": self.version, "saved_at": self.saved_at,
                "hidden": self.hidden, "linked_plot": self.linked_plot, "dvs": list(self.dvs)}


@dataclass
class SavedStats:
    info: SavedStatsInfo
    spec: StatsSpec
    #: Settings that did not come back as saved (``RestoreNote`` dicts).
    notes: list[dict[str, Any]] = field(default_factory=list)
    stored_format: Any = None


def _versions() -> dict[str, str]:
    import scistackstats
    from scistackstats import get_engine

    out = {"scistackstats": getattr(scistackstats, "__version__", "dev")}
    try:
        out.update(get_engine().versions())
    except Exception:  # pragma: no cover - versions are display only
        pass
    return out


def _strict(spec: Any) -> StatsSpec:
    if isinstance(spec, StatsSpec):
        return spec
    try:
        return StatsSpec.from_dict(spec)
    except (StatsSpecError, KeyError, TypeError, ValueError) as exc:
        raise SavedStatsError(f"The stats settings could not be read: {exc}") from exc


def _info(row, envelope: dict | None = None) -> SavedStatsInfo:
    stats_id, _scope, name, version, saved_at, hidden = row
    envelope = envelope or {}
    linked = envelope.get("linked_plot")
    dvs = envelope.get("dvs")
    return SavedStatsInfo(
        stats_id=str(stats_id), name=str(name), version=int(version), saved_at=str(saved_at), hidden=bool(hidden),
        linked_plot=linked if isinstance(linked, str) else None,
        dvs=tuple(str(d) for d in dvs) if isinstance(dvs, list) else (),
    )


def _newest(db, stats_id: str) -> SavedStatsInfo:
    row, text = STORE.load(db, stats_id)
    return _info(row, STORE.parse_envelope(text, f"{row[2]!r} version {row[3]}"))


def save_stats(db, name: str, spec: Any, *, overwrite: bool = True, current_stats_id: str | None = None) -> SavedStatsInfo:
    """Save a stats spec (or its dict, read strictly) as *name*."""
    name = STORE.check_name(name)
    spec = _strict(spec)
    envelope = {
        "format": ENVELOPE_FORMAT,
        "spec": spec.to_dict(),
        "linked_plot": spec.linked_plot,
        "dvs": [d.key for d in spec.targets],
        "saved_with": _versions(),
    }
    row, _new = STORE.save(db, None, name, envelope, overwrite=overwrite, current_id=current_stats_id,
                           on_clash=lambda clash: SavedStatsExists(_info(clash)))
    return _info(row, envelope)


def load_saved_stats(db, stats_id: str, *, version: int | None = None) -> SavedStats:
    """One version (the newest by default), salvaged. Never rewrites it."""
    row, text = STORE.load(db, stats_id, version)
    envelope = STORE.parse_envelope(text, f"{row[2]!r} version {row[3]}")
    info = _info(row, envelope)
    restored = restore_stats_spec(envelope.get("spec"), fallback_measure=(info.dvs[0].split("[")[0] if info.dvs else None))
    Log.info("[saved-stats] opened %r version %d (envelope format %r): %d note(s)",
             info.name, info.version, envelope.get("format"), len(restored.notes), layer=LAYER)
    return SavedStats(info=info, spec=restored.spec, notes=[n.to_dict() for n in restored.notes],
                      stored_format=envelope.get("format"))


def list_saved_stats(db, *, include_hidden: bool = False) -> list[SavedStatsInfo]:
    return [_info(row[:6], STORE.parse_envelope(row[6], repr(row[2])))
            for row in STORE.list_rows(db, include_hidden=include_hidden, with_envelope=True)]


def saved_stats_history(db, stats_id: str) -> list[SavedStatsInfo]:
    return [_info(row[:6], STORE.parse_envelope(row[6], repr(row[2])))
            for row in STORE.history(db, stats_id, with_envelope=True)]


def rename_saved_stats(db, stats_id: str, new_name: str) -> SavedStatsInfo:
    STORE.rename(db, stats_id, new_name)
    return _newest(db, stats_id)


def hide_saved_stats(db, stats_id: str, hidden: bool = True) -> SavedStatsInfo:
    STORE.set_hidden(db, stats_id, hidden)
    return _newest(db, stats_id)


def linked_stats_for(db, saved_plot_id: str) -> list[SavedStatsInfo]:
    """The visible saved stats tables linked to one saved plot (S11)."""
    found = [info for info in list_saved_stats(db) if info.linked_plot == saved_plot_id]
    Log.debug("[saved-stats] plot %s: %d linked stats table(s)", saved_plot_id, len(found), layer=LAYER)
    return found


def find_saved_stats(db, name: str) -> SavedStatsInfo | None:
    row = STORE.visible_by_name(db, None, STORE.check_name(name))
    return _newest(db, row[0]) if row else None


# ---------------------------------------------------------------------------
# Stats presets
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StatsPresetInfo:
    preset_id: str
    name: str
    version: int
    saved_at: str
    hidden: bool = False

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _preset_info(row) -> StatsPresetInfo:
    preset_id, _scope, name, version, saved_at, hidden = row[:6]
    return StatsPresetInfo(str(preset_id), str(name), int(version), str(saved_at), bool(hidden))


def save_stats_preset(db, name: str, spec: Any, *, overwrite: bool = True,
                      current_preset_id: str | None = None) -> StatsPresetInfo:
    """Save *spec*'s stats settings (no design, DVs or link) as a preset."""
    name = PRESET_STORE.check_name(name)
    spec = _strict(spec)
    envelope = {"format": ENVELOPE_FORMAT, "settings": stats_settings(spec), "saved_with": _versions()}
    row, _ = PRESET_STORE.save(db, None, name, envelope, overwrite=overwrite, current_id=current_preset_id,
                               on_clash=lambda clash: SavedStatsError(PRESET_STORE.clash_message(None, name)))
    return _preset_info(row)


def list_stats_presets(db, *, include_hidden: bool = False) -> list[StatsPresetInfo]:
    return [_preset_info(row) for row in PRESET_STORE.list_rows(db, include_hidden=include_hidden)]


def hide_stats_preset(db, preset_id: str, hidden: bool = True) -> StatsPresetInfo:
    PRESET_STORE.set_hidden(db, preset_id, hidden)
    return _preset_info(PRESET_STORE.latest(db, preset_id))


def rename_stats_preset(db, preset_id: str, new_name: str) -> StatsPresetInfo:
    PRESET_STORE.rename(db, preset_id, new_name)
    return _preset_info(PRESET_STORE.latest(db, preset_id))


def apply_stats_preset(db, preset_id: str, target: StatsSpec, *, version: int | None = None) -> tuple[StatsSpec, list[dict]]:
    """``target`` with the preset's stats settings; its design, DVs and link kept."""
    row, text = PRESET_STORE.load(db, preset_id, version)
    envelope = PRESET_STORE.parse_envelope(text, f"{row[2]!r} version {row[3]}")
    spec, notes = with_stats_settings(target, envelope.get("settings") or {})
    Log.info("[stats-preset] %r version %d applied to %s: %d note(s)", row[2], row[3],
             ", ".join(d.key for d in target.targets), len(notes), layer=LAYER)
    return spec, [n.to_dict() for n in notes]
