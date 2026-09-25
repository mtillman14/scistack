"""Phase 3 tests: trace / runs / audit / state (incl. PathInput discovery).

Fixture pipeline (schema keys subject/session, 2 subjects):
    P3Raw ─ bandpass3(low_hz=20) → P3Filt ─ feature3(win=3) → P3Feat
    P3Raw ─ gain3(k=1|k=2)       → P3Gain   (leaf w/ 2 variants — trace ambiguity)
    files ─ import3(PathInput)   → P3Loaded (loader, no variable inputs)

P3Gain is a *leaf* on purpose: expected-invocation prediction cross-products
all current variants of an input, so a mid-chain two-variant variable would
correctly turn its consumer red (the new variant's records were never fed
through it) — ambiguity coverage therefore lives on a leaf.

The "red" variant additionally saves a P3Raw for S03 and drops a new S03
file on disk without re-running: bandpass3 red (1 missing), gain3 red
(2 configs × S03 = 2 missing), feature3 stays green (its own input data is
unchanged), and the loader is red only under the discovery check.
"""

import dataclasses
import json
from pathlib import Path

import numpy as np
import pytest
from scidb.inspect import Inspector, render
from scidb.inspect.cli import _coerce_non_schema, main

from scidb import (
    AmbiguousVersionError,
    BaseVariable,
    NotFoundError,
    configure_database,
    for_each,
    scistack,
)

SCHEMA_KEYS = ["subject", "session"]


class P3Raw(BaseVariable):
    schema_version = 1


class P3Filt(BaseVariable):
    schema_version = 1


class P3Feat(BaseVariable):
    schema_version = 1


class P3Gain(BaseVariable):
    schema_version = 1


class P3Loaded(BaseVariable):
    schema_version = 1


def bandpass3(signal, low_hz):
    return signal * low_hz


def feature3(x, win):
    return float(np.sum(x)) * win


def gain3(signal, k):
    return signal * k


@scistack
def import3(filepath):
    with open(filepath) as fh:
        return float(fh.read().strip())


def _write_file(root, subject, session, value):
    d = Path(root) / f"sub{subject}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"ses{session}.txt").write_text(str(value))


def build_p3_db(db_path, data_root, red: bool = False):
    from scifor import PathInput

    db = configure_database(db_path, SCHEMA_KEYS)
    P3Raw.save(np.array([1.0, 2.0]), subject="S01", session="1")
    P3Raw.save(np.array([3.0, 4.0]), subject="S02", session="1")
    subjects = ["S01", "S02"]
    for_each(
        bandpass3,
        {"signal": P3Raw, "low_hz": 20},
        [P3Filt],
        subject=subjects,
        session=["1"],
    )
    for_each(
        feature3, {"x": P3Filt, "win": 3}, [P3Feat], subject=subjects, session=["1"]
    )
    for_each(
        gain3, {"signal": P3Raw, "k": 1}, [P3Gain], subject=subjects, session=["1"]
    )
    for_each(
        gain3, {"signal": P3Raw, "k": 2}, [P3Gain], subject=subjects, session=["1"]
    )

    _write_file(data_root, "S01", "1", 1.5)
    _write_file(data_root, "S02", "1", 2.5)
    for_each(
        import3,
        {
            "filepath": PathInput(
                "sub{subject}/ses{session}.txt", root_folder=str(data_root), name="sub{subject}/ses{session}.txt"
            )
        },
        [P3Loaded],
        subject=subjects,
        session=["1"],
    )

    if red:
        P3Raw.save(np.array([9.0]), subject="S03", session="1")
        _write_file(data_root, "S03", "1", 9.5)
    db.close()


@pytest.fixture(scope="module")
def green_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("p3_green")
    db_path, data_root = base / "p3.duckdb", base / "data"
    build_p3_db(db_path, data_root)
    return db_path, data_root


@pytest.fixture(scope="module")
def red_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("p3_red")
    db_path, data_root = base / "p3.duckdb", base / "data"
    build_p3_db(db_path, data_root, red=True)
    return db_path, data_root


@pytest.fixture
def insp(green_env):
    with Inspector.open(green_env[0]) as inspector:
        yield inspector


@pytest.fixture
def red_insp(red_env):
    with Inspector.open(red_env[0]) as inspector:
        yield inspector


class TestTrace:
    def test_full_upstream_chain(self, insp):
        t = insp.trace("P3Feat", subject="S01")
        assert len(t.nodes) == 3
        by_var = {n.variable: n for n in t.nodes}
        root = by_var["P3Feat"]
        assert t.root_record_id == root.record_id
        assert root.function_name == "feature3"
        assert root.constants == {"win": "3"}
        assert root.function_hash  # surfaced so old-hash lineage is visible
        assert root.run_count >= 1 and root.last_run
        assert by_var["P3Filt"].constants == {"low_hz": "20"}
        raw = by_var["P3Raw"]
        assert raw.function_name is None
        assert raw.saved is not None
        assert len(t.edges) == 2
        assert t.audit == []

    def test_ambiguous_variant_raises_with_candidates(self, insp):
        with pytest.raises(AmbiguousVersionError, match="2 P3Gain records"):
            insp.trace("P3Gain", subject="S01")

    def test_branch_param_disambiguates(self, insp):
        t = insp.trace("P3Gain", subject="S01", k=2)
        assert t.nodes[0].constants == {"k": "2"}

    def test_by_record_id(self, insp):
        rid = insp.trace("P3Feat", subject="S01").root_record_id
        t = insp.trace(record_id=rid)
        assert t.root_record_id == rid and len(t.nodes) == 3

    def test_include_audit(self, insp):
        t = insp.trace("P3Feat", subject="S01", include_audit=True)
        assert len(t.audit) >= 1
        assert t.audit[0].function_name == "feature3"

    def test_pathinput_loader_trace(self, insp):
        t = insp.trace("P3Loaded", subject="S01")
        root = t.nodes[0]
        assert root.function_name == "import3"
        assert list(root.path_inputs) == ["filepath"]
        assert root.inputs == []

    def test_no_match_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.trace("P3Feat", subject="S99")

    def test_unknown_record_id_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.trace(record_id="deadbeef00000000")

    def test_json_round_trip(self, insp):
        t = insp.trace("P3Feat", subject="S01", include_audit=True)
        parsed = json.loads(json.dumps(dataclasses.asdict(t), default=str))
        assert len(parsed["nodes"]) == 3


class TestRunsAndAudit:
    def test_runs_all(self, insp):
        runs = insp.runs()
        assert len(runs) == 5  # bandpass3, feature3, gain3 ×2, import3
        assert all(r.run_id and r.n_invocations >= 1 for r in runs)

    def test_runs_fn_filter_and_limit(self, insp):
        assert len(insp.runs(fn="gain3")) == 2
        assert len(insp.runs(limit=1)) == 1

    def test_audit(self, insp):
        rows = insp.audit("P3Feat", subject="S01")
        assert len(rows) >= 1
        assert rows[0].function_name == "feature3"
        assert rows[0].run_id is None  # audit rows carry no run metadata


class TestNodeState:
    def test_all_green(self, insp):
        states = insp.node_state()
        assert {s.function_name for s in states} == {
            "bandpass3",
            "feature3",
            "gain3",
            "import3",
        }
        assert all(s.state == "green" for s in states)
        assert all(s.state_basis == "stored_hash" for s in states)

    def test_single_fn_and_live_callable(self, insp):
        (st,) = insp.node_state("bandpass3")
        assert st.state == "green"
        (live,) = insp.node_state(bandpass3)
        assert live.state_basis == "live_fn" and live.state == "green"

    def test_red_with_missing_combos(self, red_insp):
        by_fn = {s.function_name: s for s in red_insp.node_state()}
        bp = by_fn["bandpass3"]
        assert bp.state == "red"
        assert bp.missing == 1  # the un-run S03 combo
        assert {"subject": "S03", "session": "1"} in bp.missing_combos
        # gain3 has two configs, each missing S03.
        assert by_fn["gain3"].state == "red"
        assert by_fn["gain3"].missing == 2
        # feature3's own input data (P3Filt) is unchanged → still green.
        assert by_fn["feature3"].state == "green"

    def test_unknown_fn_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.node_state("no_such_fn")


class TestPathInputState:
    def test_green_when_all_discovered_realized(self, insp):
        (st,) = insp.pathinput_state("import3")
        assert st.state == "green"
        assert st.state_basis == "discovery"
        assert st.up_to_date == 2 and st.missing == 0

    def test_new_file_turns_red(self, red_insp):
        (st,) = red_insp.pathinput_state("import3")
        assert st.state == "red"
        assert st.missing_combos == [{"subject": "S03", "session": "1"}]

    def test_grid_restriction_excludes_new_file(self, red_insp):
        (st,) = red_insp.pathinput_state("import3", subject=["S01", "S02"])
        assert st.state == "green"

    def test_non_loader_fn_raises(self, insp):
        with pytest.raises(NotFoundError, match="no PathInput inputs"):
            insp.pathinput_state("bandpass3")


class TestCoercion:
    def test_schema_keys_stay_verbatim_strings(self):
        out = _coerce_non_schema(
            {"subject": "01", "session": "1"}, ["subject", "session"]
        )
        assert out == {"subject": "01", "session": "1"}
        assert all(isinstance(v, str) for v in out.values())

    def test_non_schema_values_are_literal_evaled(self):
        out = _coerce_non_schema(
            {"low_hz": "20", "ratio": "0.5", "window": "hann", "ks": "[1, 2]"},
            ["subject"],
        )
        assert out == {"low_hz": 20, "ratio": 0.5, "window": "hann", "ks": [1, 2]}


class TestRenderers:
    def test_trace_render(self, insp):
        text = render.render_trace(insp.trace("P3Feat", subject="S01"))
        assert "P3Feat" in text and "feature3" in text
        assert "◀" in text and "win=3" in text
        assert "(raw save)" in text
        assert "fn_hash" in text

    def test_trace_render_ascii(self, insp):
        from scidb.inspect.render import ASCII_STYLE

        text = render.render_trace(
            insp.trace("P3Feat", subject="S01"), style=ASCII_STYLE
        )
        text.encode("ascii")

    def test_trace_render_with_audit(self, insp):
        text = render.render_trace(
            insp.trace("P3Feat", subject="S01", include_audit=True)
        )
        assert "runs that produced this record:" in text

    def test_node_states_render(self, red_insp):
        text = render.render_node_states(red_insp.node_state(), show_missing=True)
        assert "bandpass3" in text and "red" in text
        assert "subject=S03" in text

    def test_runs_table(self, insp):
        text = render.render_runs_table(insp.runs())
        assert "bandpass3" in text and "timestamp" in text


class TestCli:
    def test_trace_human(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "trace", "P3Feat", "subject=S01"]) == 0
        assert "feature3" in capsys.readouterr().out

    def test_trace_json(self, green_env, capsys):
        assert (
            main(
                ["--db", str(green_env[0]), "trace", "P3Feat", "subject=S01", "--json"]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert len(payload["nodes"]) == 3

    def test_trace_branch_param_string_is_coerced(self, green_env, capsys):
        # "k=2" arrives as a string; coercion makes it match the stored int 2.
        assert (
            main(
                [
                    "--db",
                    str(green_env[0]),
                    "trace",
                    "P3Gain",
                    "subject=S01",
                    "k=2",
                    "--json",
                ]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["nodes"][0]["constants"] == {"k": "2"}

    def test_trace_by_record_id_and_audit(self, green_env, capsys):
        assert (
            main(
                ["--db", str(green_env[0]), "trace", "P3Feat", "subject=S01", "--json"]
            )
            == 0
        )
        rid = json.loads(capsys.readouterr().out)["root_record_id"]
        assert (
            main(["--db", str(green_env[0]), "trace", "--record-id", rid, "--audit"])
            == 0
        )
        assert "runs that produced this record:" in capsys.readouterr().out

    def test_trace_without_args_fails(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "trace"]) == 1
        assert "record-id" in capsys.readouterr().err

    def test_runs(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "runs", "--json"]) == 0
        assert len(json.loads(capsys.readouterr().out)) == 5
        assert main(["--db", str(green_env[0]), "runs", "--fn", "gain3", "--json"]) == 0
        assert len(json.loads(capsys.readouterr().out)) == 2

    def test_state_json(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "state", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert all(s["state"] == "green" for s in payload)

    def test_state_missing_human(self, red_env, capsys):
        assert main(["--db", str(red_env[0]), "state", "--missing"]) == 0
        out = capsys.readouterr().out
        assert "red" in out and "subject=S03" in out

    def test_state_pathinput(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "state", "import3", "--pathinput"]) == 0
        assert "green" in capsys.readouterr().out

    def test_state_pathinput_requires_fn(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "state", "--pathinput"]) == 1
        assert "requires a function name" in capsys.readouterr().err

    def test_state_grid_without_pathinput_fails(self, green_env, capsys):
        assert (
            main(["--db", str(green_env[0]), "state", "bandpass3", "subject=S01"]) == 1
        )
        assert "--pathinput" in capsys.readouterr().err


class TestIntent:
    """`scidb intent <fn>` / `trace --intent`: intent vs fact from the CLI.

    The green database has never been opened by a GUI, so it has no intent
    store — the report shows fact alone, and says so. The store's read
    contract is scidb's (`scidb.intent.INTENT_COLUMNS`), so when a table IS
    present the inspector reads it without importing the GUI.
    """

    def test_fact_alone_when_no_store_exists(self, insp):
        report = insp.intent("bandpass3")
        assert report.function_name == "bandpass3"
        assert report.statements == []
        assert report.last_run is not None
        text = render.render_intent(report)
        assert "bandpass3" in text and "no statements" in text

    def test_a_store_row_is_read_and_resolved(self, tmp_path):
        """A statement written in the GUI's table shape resolves against the
        recorded fact, and a script origin reports it as not read.

        The inspector opens READ-ONLY, so the row is written first, through a
        plain DuckDB connection, into a database of this test's own.
        """
        import duckdb

        from scidb.intent import INTENT_COLUMNS, INTENT_TABLE

        db_path, data_root = tmp_path / "intent.duckdb", tmp_path / "data"
        build_p3_db(db_path, data_root)
        con = duckdb.connect(str(db_path))
        try:
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {INTENT_TABLE} ("
                "subject_kind VARCHAR, subject_ref VARCHAR, scope VARCHAR, aspect VARCHAR, "
                "aspect_key VARCHAR, value_json VARCHAR, origin VARCHAR, stated_at VARCHAR)"
            )
            con.execute(
                f"INSERT INTO {INTENT_TABLE} ({', '.join(INTENT_COLUMNS)}) VALUES "
                "('call_site', 'fn__bandpass3__0123456789abcdef', 'global', 'columns', "
                "'signal', '{\"columns\": [\"a\"], \"iterate\": false}', 'store', '2026')"
            )
        finally:
            con.close()

        with Inspector.open(db_path) as insp:
            as_gui = insp.intent("bandpass3", origin="gui")
            assert any(
                f.aspect == "columns" and f.key == "signal" and f.surface == "store"
                for f in as_gui.fields
            )
            as_script = insp.intent("bandpass3", origin="script")
            assert len(as_script.unread) == 1
            assert "not read by a script run" in render.render_intent(as_script)

    def test_cli_intent_and_trace_flag(self, green_env, capsys):
        assert main(["--db", str(green_env[0]), "intent", "bandpass3"]) == 0
        assert "intent vs fact" in capsys.readouterr().out
        assert (
            main(["--db", str(green_env[0]), "trace", "P3Feat", "subject=S01", "--intent"])
            == 0
        )
        out = capsys.readouterr().out
        assert "feature3" in out and "intent vs fact" in out
