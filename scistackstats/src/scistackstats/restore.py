"""Open a stored stats spec, keeping everything that still parses (Stage 9).

Save is strict (``StatsSpec.from_dict``); open salvages, as saved plots do
(``scistackplot.restore``): every setting that cannot be read becomes a
:class:`~scistackplot.restore.RestoreNote` and its default, and the stored
copy is never rewritten. The design is salvaged by the plot layer's own
``restore_spec``, so a stats spec and a saved plot recover a design the same
way.

Also here: the one definition of a **stats preset**, a spec's stats-only
settings (:data:`STATS_SETTINGS`) without its design, DVs or link.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any

from scistacklog import Log

from scistackplot import PlotSpec
from scistackplot.restore import NoteKind, RestoreNote, restore_spec

from .catalog import TESTS
from .describe import STATISTICS
from .spec import (
    DV,
    FORMAT,
    VARIANT_MODES,
    Analysis,
    DescriptiveSpec,
    FormatSpec,
    OverlaySpec,
    StatsSpec,
    StatsSpecError,
)

LAYER = "scistackstats"

#: The settings a stats preset carries: what to compute and how it reads,
#: never what data it is computed on.
STATS_SETTINGS = ("descriptives", "comparisons", "formatting")


def stats_settings(spec: StatsSpec) -> dict[str, Any]:
    """The preset part of ``spec``, as JSON."""
    raw = spec.to_dict()
    return {name: raw[name] for name in STATS_SETTINGS}


def with_stats_settings(spec: StatsSpec, settings: dict[str, Any]) -> tuple[StatsSpec, list[RestoreNote]]:
    """``spec`` with the preset's stats settings; design, DVs and link kept."""
    notes: list[RestoreNote] = []
    raw = spec.to_dict()
    raw.update({k: v for k, v in settings.items() if k in STATS_SETTINGS})
    restored = restore_stats_spec(raw, fallback_measure=spec.design.y_measure)
    notes.extend(restored.notes)
    result = replace(restored.spec, design=spec.design, dvs=spec.dvs, linked_plot=spec.linked_plot)
    return result, notes


@dataclass
class RestoredStats:
    spec: StatsSpec
    notes: list[RestoreNote] = field(default_factory=list)


def _note(notes: list[RestoreNote], path: str, kind: NoteKind, message: str, value: Any = None) -> None:
    notes.append(RestoreNote(path, kind, message, value))


def _salvage_block(cls, raw: Any, path: str, notes: list[RestoreNote]):
    """A small frozen spec block: drop unknown keys and invalid values one
    at a time, each with a note."""
    if not isinstance(raw, dict):
        if raw is not None:
            _note(notes, path, NoteKind.INVALID_VALUE, f"{path} is not a table; defaults used", raw)
        return cls()
    allowed = {f.name for f in fields(cls)}
    kept = {}
    for key, value in raw.items():
        if key not in allowed:
            _note(notes, f"{path}.{key}", NoteKind.UNKNOWN_SETTING, f"{path}.{key} is no longer a setting", value)
        else:
            kept[key] = value
    while True:
        try:
            return cls.from_dict(kept)
        except (StatsSpecError, TypeError, ValueError) as exc:
            bad = next((k for k in kept if k in str(exc)), None) or next(iter(kept), None)
            if bad is None:
                return cls()
            _note(notes, f"{path}.{bad}", NoteKind.INVALID_VALUE, f"{path}.{bad}: {exc}; default used", kept.pop(bad))


def restore_stats_spec(raw: Any, *, fallback_measure: str | None = None) -> RestoredStats:
    """A usable StatsSpec from a stored dict, plus notes (module docstring)."""
    notes: list[RestoreNote] = []
    if not isinstance(raw, dict):
        _note(notes, "", NoteKind.INVALID_VALUE, "the stored stats settings are not a table", raw)
        raw = {}
    known = {"format", "design", "dvs", "descriptives", "comparisons", "variant_mode", "linked_plot", "formatting", "overlay"}
    for key in sorted(set(raw) - known):
        _note(notes, key, NoteKind.UNKNOWN_SETTING, f"{key} is no longer a setting", raw[key])
    if raw.get("format") not in (None, FORMAT):
        Log.info("[stats restore] stored format %r (current %d): salvaging", raw.get("format"), FORMAT, layer=LAYER)

    dvs: list[DV] = []
    for i, entry in enumerate(raw.get("dvs") or []):
        try:
            dv = DV.from_dict(entry)
            if dv.key in {d.key for d in dvs}:
                raise StatsSpecError(f"duplicate DV {dv.key}")
            dvs.append(dv)
        except (StatsSpecError, TypeError, ValueError, AttributeError) as exc:
            _note(notes, f"dvs[{i}]", NoteKind.DROPPED_ENTRY, f"DV {i + 1} could not be read ({exc})", entry)

    fallback = dvs[0].variable if dvs else fallback_measure
    design_raw = raw.get("design")
    restored = restore_spec(design_raw if isinstance(design_raw, dict) else {}, fallback_measure=fallback or "unknown")
    for n in restored.notes:
        notes.append(RestoreNote(f"design.{n.path}" if n.path else "design", n.kind, n.message, n.value))
    design: PlotSpec = restored.spec

    analyses: list[Analysis] = []
    for i, entry in enumerate(raw.get("comparisons") or []):
        path = f"comparisons[{i}]"
        analysis = _salvage_block(Analysis, entry, path, notes)
        for name in ("omnibus_test", "pairwise_test"):
            test = getattr(analysis, name)
            if test is not None and test not in TESTS:
                _note(notes, f"{path}.{name}", NoteKind.INVALID_VALUE,
                      f"unknown test {test!r}; the design chooses the test instead", test)
                analysis = replace(analysis, **{name: None})
        if analysis.id in {a.id for a in analyses}:
            new_id = f"{analysis.id}_{i + 1}"
            _note(notes, f"{path}.id", NoteKind.INVALID_VALUE, f"duplicate id {analysis.id!r}; renamed {new_id!r}")
            analysis = replace(analysis, id=new_id)
        analyses.append(analysis)

    linked = raw.get("linked_plot")
    if linked is not None and not isinstance(linked, str):
        _note(notes, "linked_plot", NoteKind.INVALID_VALUE, "linked_plot is not a saved-plot id; unlinked", linked)
        linked = None
    descriptives = _salvage_block(DescriptiveSpec, raw.get("descriptives"), "descriptives", notes)
    unknown = [s for s in descriptives.statistics if s not in STATISTICS]
    if unknown:
        _note(notes, "descriptives.statistics", NoteKind.DROPPED_ENTRY,
              f"unknown statistic(s) {unknown} dropped", unknown)
        descriptives = replace(descriptives, statistics=tuple(s for s in descriptives.statistics if s in STATISTICS))
    variant_mode = raw.get("variant_mode", "pinned")
    if variant_mode not in VARIANT_MODES:
        _note(notes, "variant_mode", NoteKind.INVALID_VALUE, f"variant_mode {variant_mode!r} unknown; pinned", variant_mode)
        variant_mode = "pinned"
    spec = StatsSpec(
        design=design,
        dvs=tuple(dvs),
        linked_plot=linked,
        variant_mode=variant_mode,
        descriptives=descriptives,
        comparisons=tuple(analyses),
        formatting=_salvage_block(FormatSpec, raw.get("formatting"), "formatting", notes),
        overlay=_salvage_block(OverlaySpec, raw.get("overlay"), "overlay", notes),
    )
    if notes:
        for n in notes:
            Log.info("[stats restore] %s (%s): %s", n.path or "(spec)", n.kind, n.message, layer=LAYER)
    Log.info("[stats restore] %d DV(s), %d analysis(es), %d note(s)", len(dvs), len(analyses), len(notes), layer=LAYER)
    return RestoredStats(spec=spec, notes=notes)
