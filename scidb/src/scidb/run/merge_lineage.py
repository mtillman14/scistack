"""Which constituent records a Merge input read at a location (lineage only).

A bulk-loaded Merge strips ``__record_id`` from each constituent before scifor
joins them (the column would clash in the join). Until 2026-11-17 nothing
kept it, so a Merge-fed output was saved with NO input edges to the records
it was built from (``tests/test_merge_provenance.py``; docs/claude/merge-node.md).

This module keeps that information beside the loaded Merge:

* :class:`LoadedMerge` is the ``scifor.Merge`` the loader hands scifor. It
  joins exactly as before, and also carries a :class:`MergeLineage`.
* :meth:`MergeLineage.rids_at` answers "which records did the join read at
  this combo?" with the same row choice scifor's per-combo filter makes for
  a constituent. A key the constituent populates must equal the combo's
  value, a ``Fixed`` pin overrides the combo, and a key it does not populate
  (a coarser variable) matches everything, i.e. broadcasts.

The records ride in each combination's ``Selection`` under the Merge's
param, as a ``LINEAGE_ONLY`` binding (the ColumnSelection precedent): they
become edges and never an iteration axis. Branch params are NOT inherited
from constituents. That matches the behaviour before this change, and the
pinned test is ``test_merge_provenance.py``.

Not covered: a ``PerComboLoaderMerge`` (a constituent with no bulk load)
loads per combo, after the Selections exist, so it still records no edges.
It logs that once per run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import scifor as _scifor

from scidb.model.log import Log
from scidb.run.bindings import RECORD_ID_COLUMN
from scidb.store.schema_values import schema_str


@dataclass
class _Constituent:
    label: str
    frame: Any  # pandas DataFrame: populated schema columns + __record_id
    keys: tuple[str, ...]  # the schema keys this constituent populates
    pinned: dict  # Fixed metadata, schema_str'd
    _index: dict = field(default_factory=dict)  # frozenset(keys) -> {values: rids}

    def rids_at(self, combo: Mapping[str, Any]) -> list[str]:
        effective = {k: schema_str(v) for k, v in combo.items() if k in self.keys}
        effective.update({k: v for k, v in self.pinned.items() if k in self.keys})
        used = tuple(k for k in self.keys if k in effective)
        index = self._index.get(used)
        if index is None:
            index = {}
            cols = [self.frame[k].map(schema_str) for k in used]
            for i, rid in enumerate(self.frame[RECORD_ID_COLUMN]):
                if rid is None:
                    continue
                vals = tuple(c.iat[i] for c in cols)
                index.setdefault(vals, []).append(str(rid))
            self._index[used] = {k: list(dict.fromkeys(v)) for k, v in index.items()}
            index = self._index[used]
        return index.get(tuple(effective[k] for k in used), [])


class MergeLineage:
    """The record-bearing rows of each constituent of one loaded Merge."""

    def __init__(self) -> None:
        self._parts: list[_Constituent] = []

    def add(self, label: str, loaded: Any, schema_keys) -> None:
        """Record *loaded* (a constituent as ``_load_input`` returned it,
        BEFORE ``__record_id`` is stripped). A constituent with no record ids
        (a raw DataFrame handed in by the caller) contributes nothing."""
        import pandas as pd

        df = loaded if isinstance(loaded, pd.DataFrame) else getattr(loaded, "data", None)
        if not isinstance(df, pd.DataFrame) or RECORD_ID_COLUMN not in df.columns:
            Log.debug(f"[merge] lineage: constituent {label} has no {RECORD_ID_COLUMN}")
            return
        keys = tuple(k for k in schema_keys if k in df.columns and not df[k].isna().all())
        pinned = {
            k: schema_str(v)
            for k, v in (getattr(loaded, "fixed_metadata", None) or {}).items()
        }
        frame = df[list(keys) + [RECORD_ID_COLUMN]].dropna(subset=[RECORD_ID_COLUMN])
        self._parts.append(_Constituent(label, frame, keys, pinned))

    def rids_at(self, combo: Mapping[str, Any]) -> tuple[str, ...]:
        """Every constituent record the join reads at *combo*, in constituent
        order, de-duplicated."""
        out: list[str] = []
        for part in self._parts:
            out.extend(part.rids_at(combo))
        return tuple(dict.fromkeys(out))

    def describe(self) -> str:
        return ", ".join(f"{p.label}{list(p.keys)}" for p in self._parts)


class LoadedMerge(_scifor.Merge):
    """A ``scifor.Merge`` of loaded constituents plus their :class:`MergeLineage`."""

    def __init__(self, *tables: Any, name: str, how: str, lineage: MergeLineage):
        super().__init__(*tables, name=name, how=how)
        self.lineage = lineage


def merge_lineages(loaded_inputs: Mapping[str, Any]) -> dict[str, MergeLineage]:
    """``{param: lineage}`` for every bulk-loaded Merge input. The ONE place
    the selection builders find them."""
    return {
        p: v.lineage for p, v in loaded_inputs.items() if isinstance(v, LoadedMerge)
    }


def merge_rids_at(lineages: Mapping[str, MergeLineage], combo: Mapping[str, Any]) -> dict:
    """``{param: rids}`` for the Selection of *combo*; params with none omitted."""
    out = {}
    for param, lineage in lineages.items():
        rids = lineage.rids_at(combo)
        if rids:
            out[param] = rids
        else:
            Log.debug(f"[merge] lineage: {param} read no record at {dict(combo)}")
    return out
