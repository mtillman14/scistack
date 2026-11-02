"""Which facts of a ``for_each`` call reach which id (design review 04b W4).

:data:`IDENTITY_TABLE` is the OWNER of the table; ``docs/claude/identity-table.md``
is its human-readable mirror and says so. Each row is one fact of a call;
each column one id. A test per row runs a tiny call twice — a baseline and a
variant that differs in exactly that fact, each in its own fresh database —
and asserts that exactly the ids marked True differ:

* ``record_id``     — the output records (``_record``);
* ``invocation_id`` — the activity that produced them (``_invocation_output``);
* ``call_id``       — the call site (``ForEachConfig.to_call_id``);
* ``wiring_id``     — the canvas node (``provenance.graph.compute_wiring_id``).

The control row (nothing varied) pins that every id is reproducible across two
databases, which is what makes the other rows meaningful. Change an id recipe
and this test names the row it moved; then update the dict and the doc
together.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.model.across_variants import AcrossVariants
from scidb.provenance.graph import compute_wiring_id
from scidb.run.foreach_config import ForEachConfig
from scifor import PathInput
from scifor.project.names import function_name

R, I, C, W = "record_id", "invocation_id", "call_id", "wiring_id"

#: fact -> {id: does varying the fact change it?}   (THE table)
IDENTITY_TABLE: "dict[str, dict[str, bool]]" = {
    "nothing (control)": {R: False, I: False, C: False, W: False},
    "function hash": {R: True, I: True, C: False, W: False},
    "function name": {R: True, I: True, C: True, W: True},
    "input types": {R: True, I: True, C: True, W: True},
    "input record ids": {R: True, I: True, C: False, W: False},
    "selectors": {R: True, I: True, C: False, W: False},
    "constants": {R: True, I: True, C: True, W: False},
    "as_table": {R: True, I: True, C: True, W: False},
    "distribute": {R: True, I: True, C: True, W: False},
    "across_variants": {R: True, I: True, C: True, W: False},
    "iteration level": {R: True, I: True, C: False, W: False},
    "output types": {R: True, I: False, C: False, W: True},
    "PathInput names": {R: True, I: True, C: True, W: True},
}


class IdIn(BaseVariable):
    pass


class IdInB(BaseVariable):
    pass


class IdTrial(BaseVariable):
    pass


class IdDict(BaseVariable):
    pass


class IdOutA(BaseVariable):
    pass


class IdOutB(BaseVariable):
    pass


def _flat(x: Any) -> np.ndarray:
    """Every number in an input, whatever shape the call handed over."""
    if isinstance(x, pd.DataFrame):
        x = x.drop(columns=[c for c in ("subject", "trial") if c in x.columns])
        x = x.select_dtypes("number").to_numpy()
    if isinstance(x, dict):
        x = list(x.values())
    parts = [np.ravel(np.asarray(v, dtype=float)) for v in np.asarray(x, dtype=object).ravel()]
    return np.concatenate(parts) if parts else np.zeros(0)


def total(x, k):
    return float(np.sum(_flat(x))) * k


def total_edited(x, k):
    return float(np.sum(_flat(x))) * k + 0.0


total_edited.__name__ = total_edited.__qualname__ = "total"


def total_renamed(x, k):
    return float(np.sum(_flat(x))) * k


def pieces(x, k):
    s = float(np.sum(_flat(x)))
    return np.array([s, s * k])


def path_len(path, k):
    return float(len(Path(path).name)) * k


@dataclass(frozen=True)
class Call:
    fn: Any = total
    inputs: dict = field(default_factory=lambda: {"x": IdIn, "k": 2})
    outputs: tuple = (IdOutA,)
    options: dict = field(default_factory=lambda: {"subject": []})
    seed_offset: float = 0.0


BASE = Call()

#: fact -> (baseline, variant)
CALLS: "dict[str, tuple[Call, Call]]" = {
    "nothing (control)": (BASE, BASE),
    "function hash": (BASE, replace(BASE, fn=total_edited)),
    "function name": (BASE, replace(BASE, fn=total_renamed)),
    "input types": (BASE, replace(BASE, inputs={"x": IdInB, "k": 2})),
    "input record ids": (BASE, replace(BASE, seed_offset=100.0)),
    "selectors": (
        replace(BASE, inputs={"x": IdDict["a"], "k": 2}),
        replace(BASE, inputs={"x": IdDict["b"], "k": 2}),
    ),
    "constants": (BASE, replace(BASE, inputs={"x": IdIn, "k": 3})),
    "as_table": (BASE, replace(BASE, options={"subject": [], "as_table": ["x"]})),
    "distribute": (
        replace(BASE, fn=pieces),
        replace(BASE, fn=pieces, options={"subject": [], "distribute": True}),
    ),
    "across_variants": (
        replace(BASE, inputs={"x": IdTrial, "k": 2}),
        replace(BASE, inputs={"x": AcrossVariants(IdTrial), "k": 2}),
    ),
    "iteration level": (BASE, replace(BASE, options={"schema_keys": None})),
    "output types": (BASE, replace(BASE, outputs=(IdOutB,))),
    "PathInput names": (
        replace(BASE, fn=path_len, inputs={"path": "raw", "k": 2}),
        replace(BASE, fn=path_len, inputs={"path": "raw2", "k": 2}),
    ),
}


def _seed(db, offset: float, files: Path) -> None:
    for s in (1, 2):
        IdIn.save(np.arange(3.0) + s + offset, db=db, subject=s)
        IdInB.save(np.arange(3.0) + s + offset, db=db, subject=s)
        IdDict.save({"a": np.arange(3.0) + s, "b": np.arange(3.0) - s}, db=db, subject=s)
        for t in (1, 2):
            IdTrial.save(np.arange(3.0) * t + s, db=db, subject=s, trial=t)
        (files / f"s{s}.txt").write_text("x")


def _materialize(inputs: dict, files: Path) -> dict:
    """PathInput names in the table above become PathInputs over ``files``."""
    out = {}
    for param, spec in inputs.items():
        if param == "path":
            out[param] = PathInput("s{subject}.txt", root_folder=files, name=spec)
        else:
            out[param] = spec
    return out


def _ids(tmp_path: Path, tag: str, call: Call) -> dict:
    _scifor.set_schema([])
    files = tmp_path / f"{tag}_files"
    files.mkdir()
    db = configure_database(tmp_path / f"{tag}.duckdb", ["subject", "trial"])
    try:
        _seed(db, call.seed_offset, files)
        inputs = _materialize(call.inputs, files)
        options = dict(call.options)
        for_each(call.fn, inputs, list(call.outputs), db=db, **options)
        types = [o.__name__ for o in call.outputs]
        ph = ", ".join(["?"] * len(types))
        records = {
            r[0]
            for r in db._duck.fetchall(f"SELECT record_id FROM _record WHERE type IN ({ph})", types)
        }
        invocations = {
            r[0]
            for r in db._duck.fetchall(
                "SELECT DISTINCT io.invocation_id FROM _invocation_output io "
                "JOIN _record r ON r.record_id = io.output_record_id "
                f"WHERE r.type IN ({ph})",
                types,
            )
        }
        config = ForEachConfig(
            call.fn,
            inputs,
            distribute=bool(options.get("distribute", False)),
            as_table=options.get("as_table"),
        )
        path_inputs = {p: v.name for p, v in inputs.items() if isinstance(v, PathInput)}
        input_types = {
            p: t for p, t in config.call_site_inputs().items() if p not in path_inputs
        }
        return {
            R: records,
            I: invocations,
            C: config.to_call_id(),
            W: compute_wiring_id(function_name(call.fn), input_types, types, path_inputs),
        }
    finally:
        _scifor.set_schema([])
        db.close()


def test_table_and_calls_name_the_same_facts():
    assert set(IDENTITY_TABLE) == set(CALLS)
    assert all(set(row) == {R, I, C, W} for row in IDENTITY_TABLE.values())


@pytest.mark.parametrize("fact", list(IDENTITY_TABLE), ids=list(IDENTITY_TABLE))
def test_fact_changes_exactly_its_ids(fact, tmp_path):
    baseline, variant = CALLS[fact]
    before = _ids(tmp_path, "base", baseline)
    after = _ids(tmp_path, "variant", variant)
    assert before[R] and before[I], "the baseline run saved nothing"
    changed = {key: before[key] != after[key] for key in (R, I, C, W)}
    assert changed == IDENTITY_TABLE[fact], (
        f"{fact}: ids that changed {changed} differ from the identity table "
        f"(docs/claude/identity-table.md)"
    )
