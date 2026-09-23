"""Which schema locations a run is about: ragged prefixes plus a level rule.

One meaning, stated once in ``docs/claude/location-filter-semantics.md`` and
transcribed here for combos, in ``scistackplot.reduce`` for DataFrame rows, and
in the GUI's ``locationSelection.ts`` for checkbox state. ``scistackplot``
cannot import ``scifor`` (pandas/numpy/scistacklog only), so the parity test
table at the bottom of that document is what keeps the copies honest.

A selection is a PAIR, and each half exists because the other cannot say it:

``include``
    A minimal covering set of PREFIXES -- ``[("subject", "02"), ("trial", "3")]``
    is one trial. The only form that can express something ragged (all of
    subject 01, plus trials 1-3 of subject 02), which is what a tree of
    checkboxes means and what real datasets look like.

``exclude_levels``
    A standing per-key RULE -- ``{"session": ["BL"]}`` means *BL is out,
    everywhere, including in data that does not exist yet*. Exploding that into
    prefixes would enumerate today's subjects and freeze them, so a subject
    added next month would vanish entirely rather than merely lose its BL.

An empty pair is INERT: it constrains nothing, which is also what an untouched
picker means, so "everything is selected" and "never opened the dialog" cannot
produce two different specs for one run.

Combos are coarser than locations
---------------------------------
This module filters ``for_each`` combos, which need not name every key the
selection does: a run iterating ``subject`` only, against a selection naming
``trial``, produces combos that are ANCESTORS of the selected locations. Such a
combo is KEPT -- some of what it covers was selected, and dropping it would
silently run nothing. So a step whose key is absent from the combo is ignored,
in both halves of the rule (the exclusion half matches
``schema-hierarchy-contiguity``'s cross-cutting records, which genuinely have
no step for a key they were not saved at).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from scistacklog import Log

LAYER = "scifor"

#: One ``(key, value)`` step of a location path.
Step = tuple[str, str]
#: The outermost steps of a location. ``()`` would mean "everything".
Prefix = tuple[Step, ...]


#: A number with no leading zeros whose value is integral: ``1``, ``-3``,
#: ``1.0``, ``1.00``. NOT ``01`` (leading zero) and NOT ``1.5``.
_INTEGRAL_RE = re.compile(r"-?(?:0|[1-9]\d*)(?:\.0+)?")


def _as_text(value: Any) -> str:
    """Every selection here crosses JSON (and TOML, for a saved spec), so
    ``1`` and ``"1"`` are one value. Schema key canonicalization
    (``docs/claude/schema-key-types.md``) has already decided identity by the
    time a combo reaches this module -- this is only how the comparison is
    spelled, not where identity is decided."""
    return str(value)


def _is_null(value: Any) -> bool:
    """NULL at a key is not a level: it matches nothing and is excluded by
    nothing.

    Stated explicitly because the two sides spell it differently -- ``None``
    in a combo, ``NaN`` in a DataFrame column, whose text forms ("None" and
    "nan") are not even equal. Without this, a level legitimately named
    ``"None"`` would match a null combo on this side and not match a null row
    on the other. Cross-cutting records (a result saved at subject+speed with
    ``timepoint`` NULL) make this a routine case, not an exotic one.
    """
    if value is None:
        return True
    return isinstance(value, float) and value != value  # NaN


def value_spellings(value: Any) -> frozenset[str]:
    """Every text form *value* may legitimately arrive as.

    One value crosses three layers and can pick up a different spelling in
    each: a schema key that round-tripped through DuckDB as a float reaches
    the plotting frame as ``1.0``, while the picker -- whose tree comes from
    scidb -- sends ``1``. Compared as raw text those select nothing, in
    silence, which is the worst available failure.

    So an integral number matches both spellings. A zero-padded string does
    NOT: ``"01"`` and ``"1"`` can be two genuinely distinct trials, and which
    one is identity is scidb's decision (``schema-key-types.md``), never a
    shortcut taken in a comparison.
    """
    text = _as_text(value)
    if _INTEGRAL_RE.fullmatch(text):
        whole = int(float(text))
        return frozenset({text, str(whole), f"{whole}.0"})
    return frozenset({text})


def _cell_matches(cell: Any, wanted: str) -> bool:
    """Whether a location's value at some key is *wanted*."""
    if _is_null(cell):
        return False
    return _as_text(cell) in value_spellings(wanted)


def _as_prefix(raw: Iterable) -> Prefix:
    steps: list[Step] = []
    for step in raw:
        if isinstance(step, Mapping):
            raise TypeError(
                "A location prefix step must be a [key, value] pair, not a "
                f"mapping: {step!r}"
            )
        if isinstance(step, (str, bytes)) or not isinstance(step, Sequence):
            raise TypeError(
                f"A location prefix step must be a [key, value] pair: {step!r}"
            )
        if len(step) != 2:
            raise ValueError(
                f"A location prefix step must have exactly 2 elements: {step!r}"
            )
        key, value = step
        steps.append((str(key), _as_text(value)))
    return tuple(steps)


@dataclass(frozen=True)
class LocationFilter:
    """Which schema locations are in play. See the module docstring."""

    #: Prefixes to keep, outermost step first. Empty means "every location".
    include: tuple[Prefix, ...] = ()
    #: ``{key: [levels]}`` to drop wherever they appear, now and in future data.
    exclude_levels: tuple[tuple[str, tuple[str, ...]], ...] = ()

    # ---- construction ----------------------------------------------------

    @classmethod
    def of(cls, value: Any) -> "LocationFilter":
        """Coerce *value* -- ``None``, a mapping (what crosses the RPC), or an
        existing filter -- into one of these.

        Accepting the mapping form here rather than at each call site is what
        lets ``for_each(locations={...})`` work straight from JSON without the
        GUI and the Python API disagreeing about the shape.
        """
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            unknown = set(value) - {"include", "exclude_levels"}
            if unknown:
                raise ValueError(
                    "Unknown key(s) in a locations= mapping: "
                    f"{sorted(unknown)}. Expected 'include' and/or "
                    "'exclude_levels'."
                )
            return cls.build(
                include=value.get("include") or (),
                exclude_levels=value.get("exclude_levels") or {},
            )
        raise TypeError(
            "locations= must be a LocationFilter, a mapping with 'include' / "
            f"'exclude_levels', or None -- got {type(value).__name__}."
        )

    @classmethod
    def build(
        cls,
        include: Iterable[Iterable] = (),
        exclude_levels: Mapping[str, Iterable] | None = None,
    ) -> "LocationFilter":
        """Normalize loose input (lists, ints, tuples) into the frozen form."""
        # An EMPTY prefix constrains nothing, so it is dropped rather than
        # kept as a step-less entry: `include=[[]]` then normalises to the
        # inert filter, and `is_empty()` agrees with scistackplot's
        # `LocationFilter.prefixes()`, which drops them too. Both already
        # SELECTED everything in that case; this makes them say so identically.
        prefixes = tuple(p for p in (_as_prefix(prefix) for prefix in include) if p)
        levels: list[tuple[str, tuple[str, ...]]] = []
        for key, values in (exclude_levels or {}).items():
            # Materialised before the emptiness test: `if list(values)` inside a
            # comprehension would consume a generator and store an empty rule.
            texts = tuple(dict.fromkeys(_as_text(v) for v in values))
            if texts:
                levels.append((str(key), texts))
        return cls(include=prefixes, exclude_levels=tuple(levels))

    def to_dict(self) -> dict:
        """The JSON/TOML form, as the GUI and a saved spec hold it."""
        return {
            "include": [[list(step) for step in prefix] for prefix in self.include],
            "exclude_levels": {
                key: list(values) for key, values in self.exclude_levels
            },
        }

    def renamed(self, key_map: Mapping[str, str] | None) -> "LocationFilter":
        """The same selection with schema keys renamed through *key_map*
        (old -> new; keys not in it unchanged) — what a pipeline ``key_map``
        binding applies to every schema-keyed option of a step it reuses."""
        if not key_map:
            return self
        return LocationFilter(
            include=tuple(
                tuple((key_map.get(k, k), v) for k, v in prefix)
                for prefix in self.include
            ),
            exclude_levels=tuple(
                (key_map.get(k, k), values) for k, values in self.exclude_levels
            ),
        )

    # ---- the rule --------------------------------------------------------

    def is_empty(self) -> bool:
        """Inert: constrains nothing, so callers can skip the walk entirely."""
        return not self.include and not self.exclude_levels

    def keys(self) -> set[str]:
        """Every schema key this filter mentions, either half."""
        named = {key for prefix in self.include for key, _ in prefix}
        named.update(key for key, _ in self.exclude_levels)
        return named

    def excluded_level(self, combo: Mapping[str, Any]) -> Step | None:
        """The ``(key, value)`` that excludes *combo*, or None.

        Returns the offending step rather than a bool so the caller can say
        WHICH rule dropped a combo -- "0 of 240 combos kept" is not a
        diagnosis, "0 kept, all by session=BL" is.
        """
        for key, values in self.exclude_levels:
            if key not in combo:
                continue
            if any(_cell_matches(combo[key], value) for value in values):
                return (key, _as_text(combo[key]))
        return None

    def covered(self, combo: Mapping[str, Any]) -> bool:
        """Whether any prefix covers *combo* (True when ``include`` is empty).

        A step naming a key the combo does not have is ignored: the combo is
        then an ancestor of the selected location, and some of what it covers
        was selected. See the module docstring.
        """
        if not self.include:
            return True
        return any(
            all(
                _cell_matches(combo[key], value)
                for key, value in prefix
                if key in combo
            )
            for prefix in self.include
        )

    def matches(self, combo: Mapping[str, Any]) -> bool:
        """The whole rule: covered by a prefix, and not level-excluded.

        Exclusion is applied AFTER coverage and always wins -- "all of subject
        01" plus "BL is out" draws subject 01 minus its BL sessions.
        """
        return self.covered(combo) and self.excluded_level(combo) is None


def filter_combos(
    combos: Sequence[Mapping[str, Any]],
    locations: Any,
    *,
    context: str = "",
) -> list[dict]:
    """*combos* narrowed by *locations*, reporting what it dropped.

    A filter that quietly removes everything is the failure mode this feature
    introduces -- a run that does nothing looks like a broken pipeline rather
    than a selection the user made three days ago -- so the counts are INFO and
    the empty result is a WARNING (CLAUDE.md NOTE 2).
    """
    location_filter = LocationFilter.of(locations)
    if location_filter.is_empty():
        return [dict(combo) for combo in combos]

    where = f"{context}: " if context else ""

    # A selection naming keys this run does not iterate constrains nothing it
    # can see. Silently keeping every combo would read as "the filter did not
    # work"; say so instead.
    iterated = {key for combo in combos for key in combo}
    unseen = sorted(location_filter.keys() - iterated)
    if unseen:
        Log.warn(
            f"{where}locations= names {unseen}, which this run does not "
            f"iterate — those steps constrain nothing here",
            layer=LAYER,
        )

    kept: list[dict] = []
    dropped_by_prefix = 0
    dropped_by_level: dict[str, int] = {}
    for combo in combos:
        if not location_filter.covered(combo):
            dropped_by_prefix += 1
            continue
        excluded = location_filter.excluded_level(combo)
        if excluded is not None:
            label = f"{excluded[0]}={excluded[1]}"
            dropped_by_level[label] = dropped_by_level.get(label, 0) + 1
            continue
        kept.append(dict(combo))

    by_level = ", ".join(
        f"{label} ({count})" for label, count in sorted(dropped_by_level.items())
    )
    Log.info(
        f"{where}locations: {len(kept)} of {len(combos)} combo(s) kept "
        f"({dropped_by_prefix} dropped by prefix, "
        f"{sum(dropped_by_level.values())} by level rule"
        f"{f': {by_level}' if by_level else ''})",
        layer=LAYER,
    )
    if combos and not kept:
        Log.warn(
            f"{where}locations= removed every one of the {len(combos)} "
            f"combination(s): include={location_filter.to_dict()['include']}, "
            f"exclude_levels={location_filter.to_dict()['exclude_levels']}. "
            "Nothing will run.",
            layer=LAYER,
        )
    return kept
