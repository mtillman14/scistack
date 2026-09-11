"""
Named variants: turning a list of selections into a factor you can plot by.

A :class:`~scistackplot.spec.VariantSet` names a region of variant space —
"baseline" is ``Code:bandpass == v1``, "new filter" is ``low_hz in (20, 50)``.
:func:`apply_variant_sets` collapses however many of those a spec carries into
**one ordinary factor**, ``Variant``, whose levels are the names. From that
point on nothing downstream needs to know variants exist: the factor takes a
role, gets a colour or a facet row, and appears in a legend, exactly like
``session``.

Two decisions here are load-bearing.

**Answered columns leave the factor list.** Once "baseline" *means*
``Code:bandpass == v1``, keeping ``Code:bandpass`` as its own factor states the
same thing twice — and worse, with two sets it is a two-level variant factor
nobody assigned, so ``roles.validate`` would refuse the figure the user just
asked for. The information is not lost; it moved into the name. Which columns
count as answered is :func:`_answered`, and both of its rules matter.

**"latest" resolves against the data, never to a hard-coded ordinal.** See
:func:`resolve_selection` — this is the difference between a figure that keeps
every subject and one that silently drops the subjects nobody re-ran.

See ``docs/claude/variant-selection.md`` and ``.claude/plan-plot-variant-rows.md``.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

import pandas as pd
from scistacklog import Log

from .spec import PlotSpec, VariantSet
from .table import CODE_FACTOR_PREFIX, FactorInfo, LongTable

LAYER = "scistackplot"

#: Column (and factor) the named variants collapse into. Named for what it is
#: to a reader of the figure — the legend says "Variant: baseline", not
#: "Code:bandpass: v1", which is the whole point of letting the user name it.
VARIANT_FACTOR = "Variant"

#: Bookkeeping column naming which variable each stacked row came from.
#:
#: Present only when a spec's rows span more than one variable, and never a
#: user-facing factor: it is consumed by :func:`apply_variant_sets` exactly like
#: an answered code axis, because "which variable" is already what the row's
#: name says. Its whole job is to let a row's mask claim only its own
#: variable's rows.
VARIABLE_COLUMN = "Variable"

#: A selection value asking for "whatever is current", rather than a named
#: ordinal. Matches ``scidb.variant.LATEST_VERSION``; duplicated as a literal
#: rather than imported because this package must work with no scidb installed.
LATEST = "latest"

#: Name given to the variant a table opens on — the source's own "these rows are
#: the current ones" recommendation. Named rather than auto-labelled because
#: ``CodeIsLatest=True`` is a flag, not a coordinate, and "CodeIsLatest=True" in
#: a legend tells a reader nothing.
CURRENT_VARIANT_NAME = "current"

_ORDINAL = re.compile(r"^v(\d+)$")


def is_code_axis(column: str) -> bool:
    return column.startswith(CODE_FACTOR_PREFIX)


def resolve_selection(
    frame: pd.DataFrame,
    selection: dict[str, Any],
    *,
    latest_column: str | None = None,
) -> dict[str, Any]:
    """
    Replace every ``"latest"`` in ``selection`` with something the frame can test.

    Two resolutions, and which one applies depends on the rest of the set:

    * **Nothing else pinned** — every code axis in the selection says ``latest``.
      The set filters on ``latest_column``, the per-schema-location flag, and the
      code axes drop out of the selection entirely. This is the important case
      and the reason ``latest`` is not simply "the highest ordinal": a subject
      never re-run under the newest code keeps contributing its own newest
      record instead of vanishing from the figure.
    * **Something else is pinned** to a named ordinal. The location-wise flag is
      no longer usable — a record pinned to ``v1`` is by definition not the
      latest — so the remaining ``latest`` axes resolve to the **highest ordinal
      present in the data**. That does drop locations which never ran it, but
      the user already asked for that by naming a version; the honest thing is
      to say which ordinal it became (the GUI shows ``latest (v3)``), not to
      quietly switch semantics.

    Keys naming a column the frame does not have are dropped, not treated as
    matching nothing: a spec outlives the table it was written against, and a
    stale key must never silently empty a figure.
    """
    present = {k: v for k, v in selection.items() if k in frame.columns}
    dropped = [k for k in selection if k not in frame.columns]
    if dropped:
        Log.debug(
            "variant selection ignores %s — not column(s) of this table",
            dropped,
            layer=LAYER,
        )

    latest_axes = [k for k, v in present.items() if _is_latest(v)]
    if not latest_axes:
        return present

    pinned_elsewhere = any(k not in latest_axes for k in present)
    resolved = {k: v for k, v in present.items() if k not in latest_axes}

    if not pinned_elsewhere and latest_column and latest_column in frame.columns:
        resolved[latest_column] = True
        return resolved

    for axis in latest_axes:
        highest = _highest_ordinal(frame[axis])
        if highest is not None:
            resolved[axis] = highest
    return resolved


def _is_latest(value: Any) -> bool:
    return isinstance(value, str) and value == LATEST


def _highest_ordinal(column: pd.Series) -> Any:
    """The largest ``vN`` in a code column, or None when it holds none.

    Sorted numerically on the ordinal rather than lexically, so ``v10`` beats
    ``v9``. Non-ordinal levels (scidb's ``(n/a)`` for a record whose chain never
    ran this function) are ignored rather than compared against.
    """
    ordinals = []
    for value in column.dropna().astype(str).unique():
        match = _ORDINAL.match(value)
        if match:
            ordinals.append((int(match.group(1)), value))
    if not ordinals:
        return None
    return max(ordinals)[1]


def default_selection(table: LongTable) -> dict[str, Any]:
    """The selection a figure opens on: **exactly one variant**.

    Plotting one thing is the common case; comparing is what you opt into by
    adding a row. Opening on several series is how a figure that nobody asked
    for gets drawn — and worse, how two pipeline variants get read as replicates
    of one condition.

    The rule is deterministic and per axis, so the same table always opens the
    same way:

    * **code axes** -> the latest function body;
    * **branch-param axes** -> the first level, in the table's own declared level
      order (``source._ordered``: natural sort, so ``Parameter1`` opens on ``1``
      and zero-padded IDs go ``01, 02, ... 10`` rather than ``1, 10, 2``).

    Which value a parameter opens on genuinely does not matter — the user
    changes it if they want another — but *stability* does, so it is the level
    order rather than whatever order the rows arrived in.

    **The pin is applied blindly.** Real data is ragged, so (latest code) x
    (first value) may be a combination nobody ever ran, and this will happily
    select nothing. That is deliberate: a rule that quietly picks a different
    value to avoid an empty figure is no longer a rule anyone can predict. The
    empty figure explains itself instead (``capability.variant_summary`` reports
    ``row_count == 0`` and what combinations do exist).

    **Why the latest FLAG and not** ``Code:<fn> = "latest"``. Both spell "latest"
    but they resolve differently, and only one of them is right here.
    :func:`resolve_selection` treats a selection as "nothing else pinned" only
    when every key in it is a ``"latest"`` axis (``pinned_elsewhere``, line ~116)
    — so the moment a branch param is pinned alongside, a ``"latest"`` code axis
    stops resolving through the per-location flag and becomes the **global
    highest ordinal**. That would silently drop every schema location never
    re-run under the newest code. Pinning the boolean flag keeps the
    per-location meaning no matter what else is in the selection, which is the
    behaviour ``test_pin_keeps_locations_never_rerun_under_the_newest_code``
    exists to protect.

    The ordinal fallback below therefore only runs for a source that has code
    axes but no flag to go with them — impossible from scidb, which writes the
    two together, but a CSV or a hand-built table may declare variant factors
    without one.
    """
    # The source's own recommendation comes first and is never overwritten:
    # ``default_pin`` is where a source says which rows are CURRENT, and only
    # scidb can know that. This function's job is to finish the job — pin the
    # axes the source had no opinion about — not to second-guess it.
    #
    # One caveat for a source that pins a code axis to the string ``"latest"``
    # rather than to the flag: adding a branch-param pin beside it flips it to
    # the global highest ordinal (see above). Left alone rather than rewritten,
    # because silently changing what a source asked for is worse than honouring
    # a request it can express and `resolve_selection` documents.
    selection: dict[str, Any] = dict(table.default_pin or {})
    frame = table.frame
    latest = table.latest_column if table.latest_column in frame.columns else None
    if latest:
        selection.setdefault(latest, True)

    for factor in table.variant_factors:
        if factor.name in selection or factor.name not in frame.columns:
            continue
        if is_code_axis(factor.name):
            if latest:
                continue  # already answered, per-location, by the flag
            highest = _highest_ordinal(frame[factor.name])
            if highest is not None:
                selection[factor.name] = highest
            elif factor.levels:
                selection[factor.name] = _level_text(factor.levels[-1])
            continue
        if factor.levels:
            selection[factor.name] = _level_text(factor.levels[0])

    Log.debug(
        "default selection for %r: %s", table.name, selection, layer=LAYER
    )
    return selection


def variant_set_mask(
    frame: pd.DataFrame,
    selection: dict[str, Any],
    *,
    latest_column: str | None = None,
) -> pd.Series:
    """Rows this selection keeps — the single definition of what a variant selects.

    Public because the GUI reports *"4 of 24 combinations"* and a per-row count
    per variant, and both have to be measured with exactly the rule the renderer
    will apply. A second implementation that counted differently would put a
    number on screen the figure disagrees with, which is worse than no number.

    A list/tuple/set value means "any of these" — the subcube rule the popup's
    checkboxes produce, and the same membership semantics
    ``scidb.database._match_branch_param`` already gives a list-valued
    ``Variant(...)`` kwarg.
    """
    mask = pd.Series(True, index=frame.index)
    resolved = resolve_selection(frame, selection, latest_column=latest_column)
    for key, value in resolved.items():
        column = frame[key]
        if isinstance(value, bool):
            # The latest flag is a real bool column; comparing it as text would
            # depend on how pandas spells True.
            mask &= column.fillna(False).astype(bool) == value
            continue
        as_text = column.astype(str)
        if isinstance(value, (list, tuple, set, frozenset)):
            mask &= as_text.isin({str(v) for v in value})
        else:
            mask &= as_text == str(value)
    return mask


def row_mask(
    frame: pd.DataFrame,
    spec: PlotSpec,
    variant: VariantSet,
    *,
    latest_column: str | None = None,
) -> pd.Series:
    """Rows one variant ROW claims: its selection, within its own variable.

    The single definition, called by :func:`apply_variant_sets` (which builds
    the figure) and by ``capability.variant_summary`` (which reports how many
    rows each variant matched). Those two disagreeing would put a number on
    screen the figure contradicts — the same reason
    :func:`variant_set_mask` is public.
    """
    mask = variant_set_mask(frame, variant.selection, latest_column=latest_column)
    if VARIABLE_COLUMN in frame.columns:
        # A row claims only its own variable's rows. Without this, a row
        # pinning `Code:filterEMG=v1` names nothing about a second variable —
        # `resolve_selection` drops keys the frame lacks, deliberately — so it
        # would match every row of that variable too and draw it once per
        # variant, identically.
        mask &= frame[VARIABLE_COLUMN].astype(str) == (
            variant.variable or spec.y_measure
        )
    return mask


def auto_label(selection: dict[str, Any], *, index: int = 0) -> str:
    """The name a variant gets until the user types over it.

    Built from the selection so it stays true while the row is being edited:
    ``bandpass v1 · low_hz=20``. The label has to survive being read in a legend
    a week later, where "Variant 1" would say nothing at all.

    An empty selection is ``(not set)``, not "all variants": an unfilled row is
    inert (:func:`defined_sets`), so a label promising every variant would
    describe a row that contributes nothing.
    """
    parts: list[str] = []
    for column, value in selection.items():
        text = _level_text(value)
        if is_code_axis(column):
            parts.append(f"{column[len(CODE_FACTOR_PREFIX):]} {text}")
        else:
            # Branch params arrive namespaced (``bandpass.low_hz``); the
            # function is usually obvious from context in a legend, the
            # parameter never is.
            parts.append(f"{column.rsplit('.', 1)[-1]}={text}")
    if not parts:
        return "(not set)" if index == 0 else f"(not set {index + 1})"
    return " · ".join(parts)


def _level_text(value: Any) -> str:
    if isinstance(value, (list, tuple, set, frozenset)):
        return "+".join(str(v) for v in value)
    return str(value)


def set_name(variant_set: VariantSet, index: int) -> str:
    if variant_set.name:
        return variant_set.name
    if not variant_set.selection and variant_set.variable:
        # A row that only names a variable IS that variable — "RawEMG" beats
        # "Variant 1" in a legend, and beats restating the same thing twice
        # when the selection also has something to say.
        return variant_set.variable
    return auto_label(variant_set.selection, index=index)


def defined_sets(sets: list[VariantSet]) -> list[VariantSet]:
    """The variants that actually select something.

    A row with an empty selection is a row the user has added but not filled in
    yet, and it is **inert**: it claims no data, contributes no level, and
    decides nothing about which columns the Variants section answers. Treating
    it as "all variants" instead — which is what an empty selection means once
    applied — made adding a row change the figure before the user had said
    anything about it, and un-answered the code axis for every *other* row,
    dropping `Code:<fn>` back into Factors with a pooling error attached.

    Clicking "+" is not a statement about the data. Nothing should happen until
    the row says something.

    Naming a **variable** is saying something, even with no selection: "also
    plot FilteredEMG, all of it" is a complete instruction, and the row it
    describes is a series the figure has to draw.
    """
    return [
        variant for variant in sets if variant.selection or variant.variable
    ]


def _answered(table: LongTable, sets: list[VariantSet]) -> set[str]:
    """Columns the named variants have already accounted for.

    These leave the factor list: once "baseline" *means* ``Code:bandpass == v1``,
    keeping the column as its own factor states the same thing twice, and with
    two variants it is an unassigned two-level variant factor that
    ``roles.validate`` refuses — rejecting the very comparison the user asked
    for.

    Two rules, and they differ by axis kind on purpose.

    **Code axes belong to the Variants section, entirely.** Once any variant is
    defined, every ``Code:<fn>`` column is answered — whether that variant named
    a version, asked for ``latest``, or selected on the chain-wide
    ``CodeIsLatest`` flag. "Which version of the code" is the question the
    variant rows exist to answer, so offering the same question again as a
    factor is asking the user to decide the same thing twice, in two places,
    with no way to tell which one wins.

    That deliberately allows a variant to hold rows built by different ordinals.
    Usually that is exactly right — under ``latest``, subject A on v2 and
    subject B on v1 are each the newest AT THEIR OWN LOCATION, which is what
    "current" means. Where it is *not* obviously right, the row says so rather
    than the column coming back: see :func:`spanned_code_axes`.

    **Branch-param axes are answered only when EVERY variant answers them** — an
    intersection, not a union. Nothing about "current code" decides which filter
    cutoff to plot, so if one variant pins ``low_hz == 20`` while another leaves
    it open, the second still holds both cutoffs and the user must still say
    what to do with them.
    """
    sets = defined_sets(sets)
    if not sets:
        return set()
    frame = table.frame
    answered = {column for column in frame.columns if is_code_axis(column)}
    if VARIABLE_COLUMN in frame.columns:
        # Which variable a row came from is what the row's NAME says; leaving
        # the column as a factor would offer the same question twice, and as an
        # unassigned multi-level factor `roles.validate` would refuse it.
        answered.add(VARIABLE_COLUMN)
    per_variant: list[set[str]] = []
    for variant in sets:
        per_variant.append(
            set(
                resolve_selection(
                    frame, variant.selection, latest_column=table.latest_column
                )
            )
        )
    return answered | set.intersection(*per_variant)


#: Schema locations named per version before a span report starts saying
#: "and N more". Enough to recognise a pattern ("ah, only subject 01"), few
#: enough to fit in a banner.
SPAN_LOCATION_LIMIT = 6


def spanned_code_axes(
    rows: pd.DataFrame, selection: dict[str, Any], table: LongTable
) -> dict[str, dict]:
    """Code axes this variant left open and that its rows disagree on.

    The honesty mechanism that lets code axes leave the factor list
    unconditionally. A variant whose rows were built by two different versions
    of a function is pooling code versions — and the figure looks exactly like
    one that is not. Rather than resurrecting the column as a factor (asking the
    user to answer in Factors a question they are already answering in
    Variants), the *row* reports it.

    Returns ``{column: {function, versions, locations, schema_levels,
    truncated}}`` — ``versions`` counts rows per version, ``locations`` names
    the schema locations holding each, capped at
    :data:`SPAN_LOCATION_LIMIT`. Empty when nothing is spanned.

    **The latest flag is no longer exempt, and that is a deliberate reversal.**
    This function used to return ``{}`` outright whenever the selection resolved
    through the per-location ``CodeIsLatest`` flag, on the reasoning that
    spanning ordinals is what per-location "latest" *means*, so reporting it
    would cry wolf on the most ordinary state there is. That argument is sound
    about *frequency* and wrong about *consequence*: the state it stays silent
    about is a figure whose points were computed by different versions of the
    same function, which is precisely the thing a reader cannot see and cannot
    afford to assume away. The user's call, 2026-09-11 — if the body actually
    used differs between schema locations, say so prominently.

    So "latest" is now reported like any other unpinned code axis. What makes
    that tolerable rather than noisy is *which locations hold which version*:
    "v1 everywhere except subject 01" is a sentence someone can act on, where a
    bare "pools 2 versions" on the most common state in the system is not.

    Do not restore the exemption without re-reading
    ``docs/claude/plot-variant-rows.md`` §3, which argued for it.
    """
    from .table import natural_sort_key

    resolved = resolve_selection(
        rows, selection, latest_column=table.latest_column
    )
    levels_of = [key for key in table.schema_levels if key in rows.columns]
    spans: dict[str, dict] = {}

    for column in rows.columns:
        if not is_code_axis(column) or column in resolved:
            continue
        present = rows[column].dropna().astype(str)
        if present.nunique() <= 1:
            continue

        versions: dict[str, int] = {}
        locations: dict[str, list[str]] = {}
        truncated = False
        for version in sorted(present.unique(), key=natural_sort_key):
            at_version = rows.loc[present.index[present == version]]
            versions[version] = int(len(at_version))
            if not levels_of:
                # A CSV, or a table with no schema — the versions are still
                # worth reporting, there is just nowhere to place them.
                locations[version] = []
                continue
            labels = sorted(
                {
                    "/".join(str(value) for value in label)
                    for label in at_version[levels_of].itertuples(index=False)
                },
                key=natural_sort_key,
            )
            locations[version] = labels[:SPAN_LOCATION_LIMIT]
            truncated = truncated or len(labels) > SPAN_LOCATION_LIMIT

        spans[column] = {
            "function": column[len(CODE_FACTOR_PREFIX):]
            if column.startswith(CODE_FACTOR_PREFIX)
            else column,
            "versions": versions,
            "locations": locations,
            # What a location string means, outermost first, so a reader knows
            # whether "01/pre/2" is subject/session/trial or something else.
            "schema_levels": levels_of,
            "truncated": truncated,
        }
    return spans


def describe_span(span: dict) -> str:
    """One sentence naming the versions and where each one is.

    Shared so the log warning, the row tag and the figure banner cannot describe
    the same span three different ways — the reason every other count in this
    module goes through one function.
    """
    parts = []
    for version, count in span["versions"].items():
        where = span["locations"].get(version) or []
        if where:
            listed = ", ".join(where)
            if span["truncated"] and len(where) == SPAN_LOCATION_LIMIT:
                listed += ", …"
            parts.append(f"{version} ({listed})")
        else:
            parts.append(f"{version} ({count} row(s))")
    return f"{span['function']}: {'; '.join(parts)}"


def strip_answered_roles(
    spec: PlotSpec, table: LongTable, derived: LongTable
) -> PlotSpec:
    """Drop roles naming factors the variants answered.

    A role assigned before a variant claimed its column is not a mistake, it is
    stale — and ``validate`` would call it an unknown factor and refuse to draw
    anything. Two ways in, both ordinary:

    * ``default_roles`` puts a multi-level ``Code:<fn>`` on COLOUR, and the
      table then opens on the "current" variant, which answers it;
    * the user assigns a code axis to a facet, then adds a variant that pins it.

    Only names that WERE factors of the undecided table are dropped. A role
    naming something that was never a factor at all is still a typo, and
    ``validate`` should still say so.
    """
    stale = [
        name
        for name in spec.roles
        if table.has_factor(name) and not derived.has_factor(name)
    ]
    if not stale:
        return spec
    Log.debug(
        "dropping role(s) %s — the named variants now account for those columns",
        stale,
        layer=LAYER,
    )
    return replace(
        spec, roles={k: v for k, v in spec.roles.items() if k not in stale}
    )


def apply_variant_sets(spec: PlotSpec, table: LongTable) -> LongTable:
    """
    Fold ``spec.variant_sets`` into a ``Variant`` factor on a derived table.

    Returns ``table`` unchanged when the spec names no variants — or none that
    say anything yet (:func:`defined_sets`) — so a project that never edited a
    function or swept a parameter pays nothing and behaves exactly as before,
    and a half-added row changes nothing until it is filled in.

    Rows matching no set are dropped: the sets are the figure's subject, and a
    row belonging to none of them was not asked for. A row matching several
    goes to the **first** — overlapping selections are legal (``all where
    low_hz=20`` and ``all where code=v1`` genuinely intersect) and duplicating
    the row into both would double-count it in every mean.
    """
    sets = defined_sets(spec.variant_sets)
    if not sets:
        return table

    frame = table.frame
    names = [set_name(s, i) for i, s in enumerate(sets)]
    assigned = pd.Series(pd.NA, index=frame.index, dtype="object")
    counts: list[int] = []

    for name, variant in zip(names, sets, strict=True):
        mask = row_mask(frame, spec, variant, latest_column=table.latest_column)
        fresh = mask & assigned.isna()
        counts.append(int(fresh.sum()))
        assigned[fresh] = name

        # Code axes leave the factor list unconditionally, so a variant that
        # quietly straddles two versions has to say so here — this is the log
        # half of what the GUI shows on the row.
        spans = spanned_code_axes(frame[fresh], variant.selection, table)
        if spans:
            Log.warn(
                "variant %r was built by MORE THAN ONE version of the code — "
                "%s. The figure cannot show this; pin a version on the row, or "
                "split it into one variant per version.",
                name,
                " | ".join(describe_span(span) for span in spans.values()),
                layer=LAYER,
            )

    kept = frame[assigned.notna()].copy()
    kept[VARIANT_FACTOR] = assigned[assigned.notna()]

    factors = [f for f in table.factors if f.name not in _answered(table, sets)]
    factors.insert(
        0,
        FactorInfo(
            name=VARIANT_FACTOR,
            # Declared order, not the order the data happens to be in: the rows
            # are a list the user arranged, and a legend that reorders itself
            # when a variant loses its last record is disorienting.
            levels=[name for name, count in zip(names, counts, strict=True) if count],
            is_variant=True,
        ),
    )

    empty = [name for name, count in zip(names, counts, strict=True) if not count]
    if empty:
        # Never silent: a variant that matched nothing is either a typo or a
        # pipeline that was never run, and both look identical to "it worked"
        # if the only symptom is a missing series.
        Log.warn(
            "variant(s) %s matched no rows — they contribute nothing to the "
            "figure. Check the selection against what has actually run.",
            empty,
            layer=LAYER,
        )
    Log.info(
        "variant sets kept %d of %d row(s): %s",
        len(kept),
        len(frame),
        dict(zip(names, counts, strict=True)),
        layer=LAYER,
    )

    return replace(table, frame=kept, factors=factors)
