"""The canvas and ``scidb graph`` group call sites into the SAME steps
(cleanup-audit F38).

Both compute ``compute_wiring_id`` over the same facts; the PathInput term is
the declared name the run recorded. The registry below declares two
PathInputs with ONE template — the case content-matching cannot tell apart,
and the reason the recorded name wins over it.
"""

from pathlib import Path

import pytest
import scifor as _scifor
from scifor import PathInput
from scistack_gui.domain.graph_builder import (
    aggregate_from_scidb,
    path_input_bindings_by_fkey,
    wiring_id,
)

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq
from scidb.database import aggregate_pipeline_variants, call_site_wiring_ids
from scidb.parameter import stamp_path_input_name


class ParityLoaded(BaseVariable):
    pass


def parity_load(filepath, k):
    return float(Path(filepath).read_text().strip()) * k


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "parity.duckdb", ["subject"])
    for folder in ("a", "b"):
        for s in ("S01", "S02"):
            d = tmp_path / folder / s
            d.mkdir(parents=True)
            (d / "value.txt").write_text("2")
    yield database
    _scifor.set_schema([])
    database.close()


def _pi(tmp_path, folder, name):
    pi = PathInput("{subject}/value.txt", root_folder=str(tmp_path / folder))
    stamp_path_input_name(pi, name)
    return pi


def test_gui_and_scidb_agree_on_every_call_site(db, tmp_path):
    for pi, k in (
        (_pi(tmp_path, "a", "gait_files"), 1.0),
        (_pi(tmp_path, "b", "gait_files"), 2.0),  # template edited, same name
        (_pi(tmp_path, "b", "other_files"), 3.0),  # same template, other name
    ):
        for_each(
            parity_load,
            {"filepath": pi, "k": k},
            [ParityLoaded],
            subject=["S01", "S02"],
        )

    scidb_agg = aggregate_pipeline_variants(pq.pipeline_variants(db._duck))
    registry = {
        "gait_files": _pi(tmp_path, "b", "gait_files"),
        "other_files": _pi(tmp_path, "b", "other_files"),
    }
    agg = aggregate_from_scidb(scidb_agg, registry, None, None)
    pi_by_fkey = path_input_bindings_by_fkey(agg.path_inputs)
    gui = {
        fkey: wiring_id(
            fkey[0],
            agg.fn_input_params[fkey],
            agg.fn_outputs.get(fkey, set()),
            pi_by_fkey.get(fkey, {}),
        )
        for fkey in agg.fn_input_params
    }
    scidb_ids = call_site_wiring_ids(scidb_agg)
    assert gui == scidb_ids
    assert len(set(scidb_ids.values())) == 2, "gait_files (2 runs) + other_files"
    assert {
        name: sorted(p for _f, p in entry["functions"])
        for name, entry in agg.path_inputs.items()
    } == {"gait_files": ["filepath", "filepath"], "other_files": ["filepath"]}
