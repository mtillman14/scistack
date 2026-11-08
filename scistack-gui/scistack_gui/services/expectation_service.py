"""Expected locations in the GUI: thin wrappers over ``scidb.inspect``.

Every status, sentence, count and rule comes from scidb
(``scidb.provenance.expectation`` / ``coverage`` / ``outcomes``, through
``Inspector`` and ``Mutator``), the same calls the ``scidb`` CLI makes
(docs/claude/expected-locations.md). This module only shapes replies, caps
rows, and caches the canvas badges.

**Badge cache.** A badge needs ``expectation()`` per variable on the canvas,
which can take a while on a large database. Results are cached per variable
against a fingerprint of everything that can change them (records saved or
deleted, marks, key settings, exclusions, run outcomes): one cheap query, so
a stale badge is impossible and an unchanged database costs nothing.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

#: Rows the popup is sent at most (the CSV export path is the CLI's).
ROW_LIMIT = 5000

#: ``{(db path, variable): (fingerprint, badge)}``.
_BADGES: dict[tuple[str, str], tuple[tuple, dict]] = {}

_FINGERPRINT_TABLES = (
    "_record_save",
    "_record",
    "_expectation_mark",
    "_schema_key_expectation",
    "__scidb_schema_overrides",
    "_run_outcome",
)


def _inspector(db):
    from scidb.inspect import Inspector

    return Inspector(db)


def _mutator(db):
    from scidb.inspect.mutate import Mutator

    return Mutator(db)


def variable_expectation(db, variable: str, problems_only: bool = False, limit: int = ROW_LIMIT) -> dict:
    """The popup's whole reply: the expectation (rows capped) and the
    variable's own current marks."""
    insp = _inspector(db)
    result = insp.expectation(variable)
    out = result.to_dict(problems_only=problems_only, limit=max(0, limit))
    out["marks"] = [m.to_dict() for m in insp.expectation_marks(variable)]
    logger.info(
        "[expectation] %s problems_only=%s -> %d/%d row(s) sent, counts=%s",
        variable, problems_only, len(out["rows"]), out["total_rows"], out["counts"],
    )
    return out


def set_key_expectation(db, key: str, sharing: str | None, values_text: str | None) -> dict:
    """One key's setting; *values_text* is parsed by scidb's one parser."""
    from scidb.provenance.expectation import parse_values_text

    values = parse_values_text(values_text) if values_text and values_text.strip() else None
    result = _mutator(db).set_key_expectation(key, sharing, values)
    return {"key": key, "detail": result.detail, "setting": result.extra.get("setting")}


def mark(db, variable: str, keys: dict, expected: bool, reason: str) -> dict:
    result = _mutator(db).mark_expectation(variable, expected, reason, **keys)
    return {"target": result.target, "detail": result.detail}


def unmark(db, variable: str, keys: dict, reason: str = "") -> dict:
    result = _mutator(db).unmark_expectation(variable, reason, **keys)
    return {"target": result.target, "detail": result.detail}


def coverage(db, by: list[str], variables: list[str] | None = None) -> dict:
    return _inspector(db).coverage(by, variables).to_dict()


def follow(db, location: dict, variables: list[str] | None = None) -> dict:
    return _inspector(db).follow(variables, **location).to_dict()


def _fingerprint(db) -> tuple | None:
    duck = db._duck
    parts = []
    try:
        for table in _FINGERPRINT_TABLES:
            if not duck.table_exists(table):
                parts.append(None)
                continue
            parts.append(duck.fetchone(f'SELECT count(*) FROM "{table}"')[0])
        parts.append(duck.fetchone("SELECT max(timestamp) FROM _record_save")[0])
    except Exception as e:  # a closed or odd database: no caching, never a crash
        logger.debug("[expectation] badge fingerprint unavailable: %s", e)
        return None
    return tuple(parts)


def badges(db, variables: list[str]) -> dict:
    """``{variable: {problems, marks, error?}}`` for the canvas badges.

    ``problems`` = locations whose fact does not match the expectation
    (tone ``problem``); ``marks`` = the variable's own current marks. A
    variable that cannot be computed gets ``error`` and zero counts, never
    an exception for the whole canvas.
    """
    t0 = time.perf_counter()
    path = str(getattr(db, "dataset_db_path", ""))
    fp = _fingerprint(db)
    insp = _inspector(db)
    out, computed = {}, 0
    for name in variables:
        hit = _BADGES.get((path, name))
        if fp is not None and hit is not None and hit[0] == fp:
            out[name] = hit[1]
            continue
        try:
            e = insp.expectation(name)
            badge = {
                "problems": e.tone_counts()["problem"],
                "marks": len(insp.expectation_marks(name)),
            }
        except Exception as exc:
            badge = {"problems": 0, "marks": 0, "error": str(exc)}
            logger.debug("[expectation] badge for %s unavailable: %s", name, exc)
        computed += 1
        out[name] = badge
        if fp is not None:
            _BADGES[(path, name)] = (fp, badge)
    logger.info(
        "[expectation] badges for %d variable(s): %d computed, %d cached in %.0f ms",
        len(variables), computed, len(variables) - computed, (time.perf_counter() - t0) * 1000,
    )
    return out
