"""StatStudio's backend: thin wrappers over scistackstats / scistackstatsdb.

Every number, caption and table string comes from ``scistackstats`` (and
through it csv-stats). This module only loads tables through Plot Studio's
cached source, under the GUI's connection policy, and shapes results for the
webview. The webview displays the ``Grid`` strings it is given, and never
formats a number itself.

Connection policy (as for plots, the 2026-09-11 rule): the database is held
only while tables load (``_tables``). Describing, testing and formatting run
on the in-memory tables after release.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from scistack_gui import history
from scistack_gui.services import plot_service

logger = logging.getLogger(__name__)


def _spec(payload: dict):
    from scistackstats import StatsSpec

    if not isinstance(payload, dict):
        raise ValueError("A stats spec must be a table of settings.")
    return StatsSpec.from_dict(payload)


def _tables(db, spec, *, csv_path: str | None, label: str) -> dict:
    from scistackstats import load_tables

    from scistack_gui.db import db_connection

    t0 = time.perf_counter()
    with db_connection(label, needed=not csv_path):
        source = plot_service.get_source(db, csv_path=csv_path)
        tables = load_tables(source, spec)
    logger.info("[stats] %s: %d table(s) loaded in %.3fs", label, len(tables), time.perf_counter() - t0)
    return tables


def _load(db, spec, *, csv_path: str | None, label: str) -> tuple[dict, dict]:
    """The DV tables and every analysis' covariate values, under one hold of
    the connection (``scistackstats.load_covariates``: the grouping loader's
    variant pin and one-value-per-location rules)."""
    from scistackstats import load_covariates, load_tables

    from scistack_gui.db import db_connection

    t0 = time.perf_counter()
    with db_connection(label, needed=not csv_path):
        source = plot_service.get_source(db, csv_path=csv_path)
        tables = load_tables(source, spec)
        covariates = load_covariates(source, spec)
    logger.info("[stats] %s: %d table(s), %d covariate(s) loaded in %.3fs", label, len(tables), len(covariates),
                time.perf_counter() - t0)
    return tables, covariates


# ---------------------------------------------------------------------------
# Open
# ---------------------------------------------------------------------------


def open_stats(
    db,
    *,
    stats_id: str | None = None,
    version: int | None = None,
    seed: dict | None = None,
    plot_id: str | None = None,
    variable: str | None = None,
    csv_path: str | None = None,
) -> dict:
    """A stats spec to edit: a saved one, one seeded from a figure, or a
    fresh one for a variable. Never runs the statistics (``compute`` does)."""
    from scistackplot import PlotSpec, default_spec
    from scistackstats import SeedError, seed_from_plot

    if stats_id:
        from scistackstatsdb import load_saved_stats

        from scistack_gui.db import db_connection

        with db_connection("stats_open"):
            saved = load_saved_stats(db, stats_id, version=version)
        logger.info("[stats] opened saved %r v%d: %d note(s)", saved.info.name, saved.info.version, len(saved.notes))
        return {"ok": True, "spec": saved.spec.to_dict(), "notes": saved.notes, "info": saved.info.to_dict(),
                "seed_notes": []}
    if seed is not None:
        try:
            seeded = seed_from_plot(PlotSpec.from_dict(seed), linked_plot=plot_id)
        except SeedError as exc:
            return {"ok": False, "error": str(exc)}
        logger.info("[stats] seeded from a figure of %s (linked plot %s): %s",
                    seeded.spec.design.y_measure, plot_id, "; ".join(seeded.notes))
        return {"ok": True, "spec": seeded.spec.to_dict(), "notes": [], "info": None, "seed_notes": seeded.notes}
    if variable:
        from scistack_gui.db import db_connection

        with db_connection("stats_open", needed=not csv_path):
            table = plot_service.get_source(db, csv_path=csv_path).get_table([variable])
        seeded = seed_from_plot(default_spec(table, variable))
        return {"ok": True, "spec": seeded.spec.to_dict(), "notes": [], "info": None, "seed_notes": seeded.notes}
    raise ValueError("stats_open needs stats_id, seed or variable")


# ---------------------------------------------------------------------------
# Compute (the two tabs)
# ---------------------------------------------------------------------------


def _readout(spec, tables) -> list[dict]:
    """Per DV, the first stratum's design, every test with its reason (for
    the design chips and the "+ test" menu) and the first analysis' model
    formula as it stands (the rail's one-line formula)."""
    from scistackstats import AnalysisFrameError, analysis_frame, available_tests, derive_design, for_frame

    first = spec.comparisons[0] if spec.comparisons else None
    overrides = first.overrides if first else {}
    out = []
    for dv in spec.targets:
        try:
            frame = analysis_frame(spec, dv, tables[dv.variable])
        except (AnalysisFrameError, KeyError) as exc:
            out.append({"dv": dv.key, "error": str(exc)})
            continue
        stratum, rows = frame.stratum_groups()[0]
        design = derive_design(frame, stratum, rows, overrides)
        model = for_frame(first.formula if first else None, first.covariates if first else (), frame,
                          available=list(tables[dv.variable].factor_names))
        out.append({"dv": dv.key, "rows": frame.summary(), "design": design.to_dict(),
                    "tests": available_tests(design, mixed_formula=model.ok and model.needs_mixed_model),
                    "model": model.to_dict()})
    return out


# ---------------------------------------------------------------------------
# Model formula (the popup; plan-statstudio-model-formula)
# ---------------------------------------------------------------------------


def _formula_analysis(spec, analysis_id: str | None, text: str | None, covariates: list | None):
    """The analysis the popup edits, with its draft text and covariates."""
    from dataclasses import replace

    from scistackplot import FactorVariable
    from scistackstats import Analysis

    analyses = list(spec.comparisons) or [Analysis()]
    index = next((i for i, a in enumerate(analyses) if a.id == analysis_id), 0)
    draft = replace(analyses[index], formula=text if text and text.strip() else None,
                    covariates=tuple(FactorVariable.from_dict(c) for c in (covariates or [])))
    return analyses, index, draft


def _formula_frames(db, spec, draft, *, csv_path, label):
    """Per DV: (frame with the draft's covariates joined, its table's factors, join problems)."""
    from dataclasses import replace

    from scistackstats import AnalysisFrameError, analysis_frame
    from scistackstats.tables import join_covariates

    probe = replace(spec, comparisons=(draft,))
    tables, covariates = _load(db, probe, csv_path=csv_path, label=label)
    out = []
    for dv in spec.targets:
        try:
            frame = analysis_frame(spec, dv, tables[dv.variable])
        except (AnalysisFrameError, KeyError) as exc:
            out.append((dv.key, None, [], [str(exc)]))
            continue
        problems: list[str] = []
        if draft.covariates:
            rows, problems = join_covariates(frame.rows, [c.factor_name for c in draft.covariates], covariates)
            frame = replace(frame, rows=rows)
        out.append((dv.key, frame, list(tables[dv.variable].factor_names), problems))
    return out


def formula_check(db, spec_payload: dict, *, analysis_id: str | None = None, text: str | None = None,
                  covariates: list | None = None, csv_path: str | None = None) -> dict:
    """The popup's live check of a draft formula: per DV, what each name
    means here, the factors it would add to or drop from the design, and the
    problems with their spans (``scistackstats.model.check``, rebase off).
    Also what the helpers may offer: groups and slopes."""
    from scistackstats import default_formula, for_frame

    spec = _spec(spec_payload)
    _, _, draft = _formula_analysis(spec, analysis_id, text, covariates)
    results = []
    for dv_key, frame, available, problems in _formula_frames(db, spec, draft, csv_path=csv_path,
                                                             label="stats_formula_check"):
        if frame is None:
            results.append({"dv": dv_key, "errors": [{"message": problems[0]}]})
            continue
        model = for_frame(draft.formula, draft.covariates, frame, available=available, rebase=False)
        model.errors.extend({"message": p} for p in problems)
        entry = model.to_dict()
        entry.update(
            dv=dv_key, default=default_formula(frame.outer_first, frame.unit),
            offers={
                "groups": [g for g in [frame.unit, *[k for k in frame.unit_keys if k != frame.unit]] if g],
                "factors": list(frame.outer_first),
                "addable": [f for f in available if f not in frame.factors and f not in frame.unit_keys
                            and f not in frame.strata],
                "covariates": [c.factor_name for c in draft.covariates],
            },
        )
        results.append(entry)
    logger.info("[stats] formula_check %r: %s", text, [(r["dv"], len(r.get("errors", []))) for r in results])
    return {"ok": True, "checks": results}


def formula_apply(db, spec_payload: dict, *, analysis_id: str | None = None, text: str | None = None,
                  covariates: list | None = None, csv_path: str | None = None) -> dict:
    """Write the popup's formula into the spec: the analysis keeps the text
    (or None when it says nothing beyond the design: no covariates, the
    default random part) and only the covariates the text names; factor
    changes are written to the DESIGN (``apply_to_design``), which raises the
    plot-divergence banner. Refused, unchanged, while the text has problems."""
    from dataclasses import replace

    from scistackstats import apply_to_design, for_frame
    from scistackstats.model import bindings_in

    spec = _spec(spec_payload)
    analyses, index, draft = _formula_analysis(spec, analysis_id, text, covariates)
    checks = []
    for dv_key, frame, available, problems in _formula_frames(db, spec, draft, csv_path=csv_path,
                                                             label="stats_formula_apply"):
        if frame is None:
            return {"ok": False, "errors": [{"message": f"{dv_key}: {problems[0]}"}]}
        model = for_frame(draft.formula, draft.covariates, frame, available=available, rebase=False)
        model.errors.extend({"message": p} for p in problems)
        if model.errors:
            return {"ok": False, "errors": model.errors}
        checks.append(model)
    if not checks:
        return {"ok": False, "errors": [{"message": "no measure to apply the formula to"}]}
    model = checks[0]
    design = apply_to_design(spec.design, model.add, model.drop)
    keep_text = model.needs_mixed_model
    analyses[index] = replace(draft, formula=model.text if keep_text else None,
                              covariates=bindings_in(model.covariates, draft.covariates))
    new = replace(spec.with_design(design), comparisons=tuple(analyses))
    logger.info("[stats] formula_apply %r -> stored %r, covariates %s, design add %s drop %s", text,
                analyses[index].formula, [c.label for c in analyses[index].covariates], model.add, model.drop)
    return {"ok": True, "spec": new.to_dict(), "design_changed": bool(model.add or model.drop)}


def catalog() -> dict:
    """Every choice StatStudio offers, from its owners: the webview holds no
    statistics vocabulary of its own."""
    from scistackstats import CELL_LAYOUTS, EFFECT_SIZES, STATISTICS, TESTS, get_engine
    from scistackstats.run import CORRECTION_LABELS, SCOPE_LABELS
    from scistackstats.spec import (
        BAR_LABELS, BAR_PAIRS, DIFFERENCE_SPREADS, FAMILIES, FAMILY_LABELS, FAMILY_SCOPES, MISSING_LABELS,
        MISSING_POLICIES, TAILS, VARIANT_MODES,
    )

    return {
        "statistics": [{"id": s.id, "label": s.label} for s in STATISTICS.values()],
        "cells": [{"id": k, "header": v[0] or "(none: statistic rows only)"} for k, v in CELL_LAYOUTS.items()],
        "tests": [{"id": t.id, "label": t.label, "role": t.role, "family": t.family} for t in TESTS.values()],
        "effect_sizes": [{"id": e.id, "name": e.name, "formula": e.formula, "tests": list(e.tests)}
                         for e in EFFECT_SIZES.values()],
        "corrections": [{"id": c, "label": CORRECTION_LABELS.get(c, c)} for c in get_engine().corrections()],
        "scopes": [{"id": s, "label": SCOPE_LABELS.get(s, "no correction")} for s in FAMILY_SCOPES],
        "families": list(FAMILIES),
        "family_labels": dict(FAMILY_LABELS),
        "missing_policies": [{"id": m, "label": MISSING_LABELS[m]} for m in MISSING_POLICIES],
        "tails": list(TAILS),
        "spreads": list(DIFFERENCE_SPREADS),
        "bar_pairs": list(BAR_PAIRS),
        "bar_labels": list(BAR_LABELS),
        "variant_modes": list(VARIANT_MODES),
    }


def compute(db, spec_payload: dict, *, csv_path: str | None = None) -> dict:
    from scistackstats import (
        StatsSpecError,
        apa_sentence,
        comparisons_grid,
        describe_table,
        descriptives_grids,
        labels_for,
        omnibus_grid,
        run,
        sweep_grid,
        why_not_exportable,
    )

    try:
        spec = _spec(spec_payload)
    except (StatsSpecError, ValueError, KeyError, TypeError) as exc:
        return {"ok": False, "error": f"The stats settings could not be read: {exc}"}
    tables, covariates = _load(db, spec, csv_path=csv_path, label="stats_compute")
    t0 = time.perf_counter()
    labels = labels_for(spec, tables)
    try:
        descriptives = describe_table(spec, tables)
        report = run(spec, tables, covariates) if spec.comparisons else None
    except (StatsSpecError, ValueError) as exc:
        return {"ok": False, "error": str(exc)}
    comparisons = []
    if report is not None:
        for analysis in report.analyses:
            omnibus = omnibus_grid(report, analysis, spec, labels)
            swept = sweep_grid(report, analysis, spec, labels)
            comparisons.append({
                "id": analysis.analysis.get("id"),
                "grid": comparisons_grid(report, analysis, spec, labels).to_dict(),
                "omnibus": omnibus.to_dict() if omnibus else None,
                "sweep": swept.to_dict() if swept else None,
                "sentences": [apa_sentence(c, analysis, spec.formatting, labels) for c in analysis.comparisons()],
                "family_statement": analysis.family_statement,
                "strata": [{"dv": s.dv, "stratum": s.stratum, "selection": s.selection, "refused": s.refused,
                            "action": s.action, "excluded": s.excluded,
                            "design": s.design, "model": s.model} for s in analysis.strata],
            })
    result = {
        "ok": True,
        "descriptives": [g.to_dict() for g in descriptives_grids(descriptives, spec, labels, report)],
        "comparisons": comparisons,
        "readout": _readout(spec, tables),
        "refusals": report.refusals if report else [],
        "design_digest": report.design_digest if report else None,
        "engine": report.engine if report else descriptives.engine,
        "not_exportable": why_not_exportable(spec, tables),
        "catalog": catalog(),
    }
    logger.info("[stats] compute: %d DV(s), %d analysis(es), %d comparison table(s) in %.3fs",
                len(spec.targets), len(spec.comparisons), len(comparisons), time.perf_counter() - t0)
    return result


def export(db, spec_payload: dict, fmt: str, which: str, *, csv_path: str | None = None,
           path: str | None = None) -> dict:
    """One tab as text (``txt md csv latex html json``), optionally written to *path*."""
    from scistackstats import describe_table, export_comparisons, export_descriptives, run

    spec = _spec(spec_payload)
    tables, covariates = _load(db, spec, csv_path=csv_path, label="stats_export")
    report = run(spec, tables, covariates) if spec.comparisons else None
    if which == "descriptives":
        text = export_descriptives(describe_table(spec, tables), spec, fmt, tables=tables, report=report)
    elif which == "comparisons":
        if report is None:
            return {"ok": False, "error": "There are no comparisons to export."}
        text = export_comparisons(report, spec, fmt, tables=tables)
    else:
        raise ValueError(f"which must be descriptives or comparisons, got {which!r}")
    if path:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
        logger.info("[stats] exported %s as %s to %s (%d chars)", which, fmt, path, len(text))
    return {"ok": True, "text": text, "path": path}


# ---------------------------------------------------------------------------
# Plot link: divergence, sync, brackets
# ---------------------------------------------------------------------------


def divergence(stats_payload: dict, plot_payload: dict) -> dict:
    from scistackplot import PlotSpec
    from scistackplot.design import design_diff, design_fingerprint

    stats = _spec(stats_payload)
    plot = PlotSpec.from_dict(plot_payload)
    differences = design_diff(plot, stats.design)
    logger.info("[stats] divergence: %d design field(s) differ%s", len(differences),
                f" ({', '.join(d.path for d in differences)})" if differences else "")
    return {"diverged": bool(differences), "differences": [d.to_dict() for d in differences],
            "plot_digest": design_fingerprint(plot), "stats_digest": design_fingerprint(stats.design)}


def sync(stats_payload: dict, plot_payload: dict, direction: str) -> dict:
    """``to_stats``: the stats spec takes the figure's design. ``to_plot``: the
    figure takes the stats design. One way, wholesale (P10)."""
    from scistackplot import PlotSpec
    from scistackplot.design import copy_design

    stats = _spec(stats_payload)
    plot = PlotSpec.from_dict(plot_payload)
    if direction == "to_stats":
        design, notes = copy_design(plot, stats.design)
        return {"ok": True, "stats_spec": stats.with_design(design).to_dict(), "notes": [n.to_dict() for n in notes]}
    if direction == "to_plot":
        merged, notes = copy_design(stats.design, plot)
        return {"ok": True, "plot_spec": merged.to_dict(), "notes": [n.to_dict() for n in notes]}
    raise ValueError(f"direction must be to_stats or to_plot, got {direction!r}")


def overlay_bars(db, stats_payload: dict, plot_payload: dict, *, csv_path: str | None = None) -> dict:
    from scistackplot import PlotSpec
    from scistackstats import run
    from scistackstats.overlays import bars_for

    stats = _spec(stats_payload)
    plot = PlotSpec.from_dict(plot_payload)
    if not stats.overlay.show or not stats.comparisons:
        return {"bars": [], "ids": [], "reason": "test brackets are off", "notes": []}
    tables = _tables(db, stats, csv_path=csv_path, label="stats_overlay")
    return bars_for(run(stats, tables), stats, plot).to_dict()


def overlay_for_plot(db, plot_id: str, plot_payload: dict) -> dict:
    """The brackets of the first linked stats table that draws them."""
    from scistackstatsdb import linked_stats_for, load_saved_stats

    from scistack_gui.db import db_connection

    with db_connection("stats_overlay_for_plot"):
        linked = linked_stats_for(db, plot_id)
        specs = [(info, load_saved_stats(db, info.stats_id).spec) for info in linked]
    for info, spec in specs:
        if spec.overlay.show:
            out = overlay_bars(db, spec.to_dict(), plot_payload)
            return {**out, "stats_id": info.stats_id, "stats_name": info.name}
    return {"bars": [], "ids": [], "reason": "no linked stats table draws brackets", "notes": []}


# ---------------------------------------------------------------------------
# Saved stats tables and presets
# ---------------------------------------------------------------------------


def saved_list(db) -> dict:
    from scistackstatsdb import list_saved_stats

    return {"items": [i.to_dict() for i in list_saved_stats(db)]}


def saved_save(db, name: str, spec_payload: dict, *, overwrite: bool, current_stats_id: str | None) -> dict:
    from scistackstatsdb import SavedStatsExists, save_stats

    try:
        info = save_stats(db, name, spec_payload, overwrite=overwrite, current_stats_id=current_stats_id)
    except SavedStatsExists as exc:
        return {"ok": False, "exists": exc.existing.to_dict(), "error": str(exc)}
    return {"ok": True, "info": info.to_dict()}


def saved_rename(db, stats_id: str, name: str) -> dict:
    from scistackstatsdb import rename_saved_stats

    return {"ok": True, "info": rename_saved_stats(db, stats_id, name).to_dict()}


def saved_hide(db, stats_id: str, hidden: bool) -> dict:
    from scistackstatsdb import hide_saved_stats

    return {"ok": True, "info": hide_saved_stats(db, stats_id, hidden).to_dict()}


def saved_history(db, stats_id: str) -> dict:
    from scistackstatsdb import saved_stats_history

    return {"items": [i.to_dict() for i in saved_stats_history(db, stats_id)]}


def linked(db, plot_id: str) -> dict:
    from scistackstatsdb import linked_stats_for

    return {"items": [i.to_dict() for i in linked_stats_for(db, plot_id)]}


def presets_list(db) -> dict:
    from scistackstatsdb import list_stats_presets

    return {"items": [p.to_dict() for p in list_stats_presets(db)]}


def presets_save(db, name: str, spec_payload: dict) -> dict:
    from scistackstatsdb import save_stats_preset

    return {"ok": True, "info": save_stats_preset(db, name, spec_payload).to_dict()}


def presets_apply(db, preset_id: str, spec_payload: dict) -> dict:
    from scistackstatsdb import apply_stats_preset

    spec, notes = apply_stats_preset(db, preset_id, _spec(spec_payload))
    return {"ok": True, "spec": spec.to_dict(), "notes": notes}


def presets_hide(db, preset_id: str, hidden: bool) -> dict:
    from scistackstatsdb import hide_stats_preset

    return {"ok": True, "info": hide_stats_preset(db, preset_id, hidden).to_dict()}


# ---------------------------------------------------------------------------
# Add to pipeline
# ---------------------------------------------------------------------------


def add_to_pipeline(db, spec_payload: dict, *, function_name: str | None = None,
                    output_variable: str | None = None, finalized: bool = True) -> dict:
    """Write the ``stat_`` endpoint into ``scistack_stats.py`` beside the
    entities file and declare its output (the plot path's mechanism)."""
    from pathlib import Path

    from scistackstats import NotExportable
    from scistackstatsdb import generate_stats_endpoint

    from scistack_gui.services.target_file_service import (
        _reload_after_write,
        get_or_create_target_file,
        validate_entity_name,
    )
    from scistack_gui.services.variable_service import create_variable

    spec = _spec(spec_payload)
    tables = _tables(db, spec, csv_path=None, label="stats_add_to_pipeline")
    try:
        code = generate_stats_endpoint(spec, tables, function_name=function_name,
                                       output_variable=output_variable, finalized=finalized)
    except NotExportable as exc:
        return {"ok": False, "error": f"Not exportable yet: {exc}"}
    name_error = validate_entity_name(code.output_variable)
    if name_error:
        return {"ok": False, "error": name_error}
    declared = create_variable(code.output_variable)
    if not declared.get("ok") and "already exists" not in str(declared.get("error", "")):
        return {"ok": False, "error": declared.get("error")}
    target_file, target_err = get_or_create_target_file()
    if target_file is None:
        return {"ok": False, "error": target_err}
    stats_file = Path(target_file).parent / "scistack_stats.py"
    existing = stats_file.read_text(encoding="utf-8") if stats_file.exists() else ""
    if f"def {code.function_name}(" in existing:
        return {"ok": False, "error": f"{stats_file.name} already defines {code.function_name}. "
                                      f"Choose a different function name, or edit the existing one."}
    header = '"""Statistics endpoints generated by scistackstats."""\n' if not existing else ""
    history.note_write(stats_file)
    with open(stats_file, "a", encoding="utf-8") as handle:
        handle.write(header)
        handle.write("\n\n" if existing else "\n")
        handle.write(code.function_source)
    logger.info("[stats] wrote %s to %s (output=%s)", code.function_name, stats_file, code.output_variable)
    reload_error = _reload_after_write(stats_file)
    result = {"ok": reload_error is None, "function_name": code.function_name, "function_source": code.function_source,
              "foreach_source": code.foreach_source, "output_variable": code.output_variable, "file": str(stats_file)}
    if reload_error:
        result["error"] = reload_error
    return result
