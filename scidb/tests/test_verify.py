"""scidb.verify: your re-run vs the exporter's history (portability Stage 9).

Each test: an exporter project runs a tiny pipeline (raw saves -> _triple),
exports, the bundle is imported into a fresh project, the recipient re-runs
with one deliberate difference, and verify classifies every record.
"""

from __future__ import annotations

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.bundle import ExportOptions, export_project, import_project
from scidb.database import clear_current_database, get_database
from scidb.project import init_project
from scidb.verify import VerifyError, verify

SCHEMA = ["subject", "session"]
RAW = {"S01": [1.0, 2.0], "S02": [3.0]}


class VerRaw(BaseVariable):
    pass


class VerOut(BaseVariable):
    pass


def _triple(x):
    return x * 3


def _run(values: dict, fn=_triple, key="subject"):
    for subject, v in values.items():
        VerRaw.save(np.array(v), **{key: subject, "session": "1"})
    for_each(fn, {"x": VerRaw}, [VerOut], **{key: list(values), "session": ["1"]})


@pytest.fixture
def bundles(tmp_path):
    """``(history-only bundle, bundle with data)`` of the exporter's run."""
    _scifor.set_schema([])
    root = tmp_path / "exp"
    init_project(root)
    db = configure_database(root / "exp.duckdb", SCHEMA)
    _run(RAW)
    hist = export_project(root, db, tmp_path / "hist")
    data = export_project(root, db, tmp_path / "data", options=ExportOptions(include_data=True))
    db.close()
    clear_current_database()
    yield hist, data
    try:
        get_database().close()
    except Exception:
        pass
    clear_current_database()
    _scifor.set_schema([])


def _recipient(bundle, tmp_path, **kw):
    target = tmp_path / "copy"
    import_project(bundle, target, **kw)
    return target, get_database()


def test_an_identical_rerun_reproduces_everything(bundles, tmp_path):
    root, db = _recipient(bundles[0], tmp_path)
    _run(RAW)
    r = verify(db, root=root)
    assert r.ok
    assert r.counts["reproduced"] == 4 and r.new == 0
    assert r.data is False  # an archive: exact comparison


def test_a_different_raw_value_is_found_at_its_source(bundles, tmp_path):
    root, db = _recipient(bundles[0], tmp_path)
    _run({"S01": RAW["S01"], "S02": [3.5]})
    r = verify(db, root=root)
    assert not r.ok
    assert r.counts["reproduced"] == 2
    assert r.counts["differs_at_source"] == 1 and r.counts["differs_downstream"] == 1
    (first,) = r.first_divergences
    assert first["location"] == {"subject": "S02", "session": "1"} and first["type"] == "VerRaw"


def test_a_location_not_rerun_is_reported(bundles, tmp_path):
    root, db = _recipient(bundles[0], tmp_path)
    _run({"S01": RAW["S01"]})
    r = verify(db, root=root)
    assert r.counts["not_run"] == 2 and r.counts["reproduced"] == 2
    assert {e["location"]["subject"] for e in r.not_run} == {"S02"}


def test_an_edited_function_is_code_changed(bundles, tmp_path):
    def _triple_edited(x):
        return x * 3 + 0  # same result, different code

    _triple_edited.__name__ = "_triple"
    root, db = _recipient(bundles[0], tmp_path)
    _run(RAW, fn=_triple_edited)
    r = verify(db, root=root)
    assert r.counts["code_changed"] == 2 and r.counts["reproduced"] == 2
    assert r.code_changed == ["_triple"]


def test_rounding_noise_is_within_tolerance_when_the_data_travels(bundles, tmp_path):
    root, db = _recipient(bundles[1], tmp_path, import_history=False)  # a fresh project
    _run({s: [x * (1 + 1e-10) for x in v] for s, v in RAW.items()})
    r = verify(db, root=root, against=bundles[1])
    assert r.data is True
    assert r.ok and r.counts["within_tolerance"] == 4
    strict = verify(db, root=root, against=bundles[1], rtol=1e-14, atol=0.0)
    assert not strict.ok and strict.counts["differs_at_source"] == 2
    assert all(e["compared_values"] for e in strict.first_divergences)


def test_another_schema_matches_through_the_key_map(bundles, tmp_path):
    root, db = _recipient(
        bundles[0], tmp_path, schema_keys=["participant", "session"],
        key_map={"subject": "participant"},
    )
    _run(RAW, key="participant")
    r = verify(db, root=root)
    assert r.ok and r.counts["reproduced"] == 4


def test_a_dropped_key_is_not_comparable(bundles, tmp_path):
    root, db = _recipient(bundles[0], tmp_path, schema_keys=["session"])
    r = verify(db, root=root)
    assert r.counts["not_comparable"] == 4 and not r.ok


def test_nothing_to_verify_against_says_how(bundles, tmp_path):
    root, db = _recipient(bundles[1], tmp_path, import_history=False)
    with pytest.raises(VerifyError, match="--against"):
        verify(db, root=root)


def test_the_exporters_history_never_enters_the_live_database(bundles, tmp_path):
    root, db = _recipient(bundles[0], tmp_path)
    _run({"S01": RAW["S01"]})
    before = db._duck._fetchone("SELECT count(*) FROM _record")[0]
    verify(db, root=root)
    assert db._duck._fetchone("SELECT count(*) FROM _record")[0] == before


def test_the_cli_exits_2_on_differences_and_prints_json(bundles, tmp_path, capsys):
    import json

    from scidb.inspect.cli import main

    root, db = _recipient(bundles[0], tmp_path)
    _run({"S01": RAW["S01"], "S02": [9.0]})
    db_path = str(db.dataset_db_path)
    db.close()
    clear_current_database()
    capsys.readouterr()
    code = main(["verify", "--db", db_path, "--project", str(root), "--json"])
    captured = capsys.readouterr()
    assert captured.out.strip(), f"no JSON on stdout; stderr: {captured.err}"
    out = json.loads(captured.out)
    assert code == 2 and out["ok"] is False
    assert out["counts"]["differs_at_source"] == 1
