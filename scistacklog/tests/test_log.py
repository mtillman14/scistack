"""Regression tests for the scistacklog Log facade.

These pin the format and behavior contracts the rest of scistack relies on:
- file line format: date + ms timestamp, padded short level, [layer], message
- console format: time-only timestamp, level prefix only at WARN+
- dual-sink level independence
- handler-level (not logger-level) filtering so pytest caplog always works
- the MATLAB-facing call shapes (positional msg, 0-3 numeric levels)
"""

import logging
import re

import pytest

from scistacklog import LAYERS, Log

FILE_LINE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} (DEBUG|INFO |WARN |ERROR) \[[a-z_]+\] "
)


@pytest.fixture(autouse=True)
def clean_log_state():
    """Each test starts and ends with a fully detached, default-level Log."""
    Log._reset_for_tests()
    yield
    Log._reset_for_tests()


def read_lines(path):
    return path.read_text(encoding="utf-8").splitlines()


def read_text_or_empty(path):
    """File content, or "" if the file was never created.

    The file sink opens lazily (FileHandler delay=True), so a log file whose
    every record was level-suppressed legitimately never exists on disk."""
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


# -- file sink format ------------------------------------------------------


def test_file_line_format(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.info("hello world")
    lines = read_lines(log_file)
    assert len(lines) == 1
    assert FILE_LINE.match(lines[0]), lines[0]
    assert lines[0].endswith("[scidb] hello world")


def test_file_uses_warn_not_warning(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.warn("careful")
    Log.error("boom")
    lines = read_lines(log_file)
    assert "WARN  [scidb] careful" in lines[0]
    assert "WARNING" not in lines[0]
    assert "ERROR [scidb] boom" in lines[1]


def test_percent_style_args(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.info("count=%d for %s", 7, "PSD")
    assert read_lines(log_file)[0].endswith("count=7 for PSD")


def test_exc_info_writes_traceback(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    try:
        raise ValueError("bad channel count")
    except ValueError:
        Log.error("iteration failed", exc_info=True)
    content = log_file.read_text(encoding="utf-8")
    assert "iteration failed" in content
    assert "Traceback (most recent call last):" in content
    assert "ValueError: bad channel count" in content


# -- console sink format ---------------------------------------------------


def test_console_format_time_only(capsys):
    Log.info("narrative line")
    err = capsys.readouterr().err.strip()
    assert re.match(r"^\d{2}:\d{2}:\d{2} \[scidb\] narrative line$", err), err


def test_console_warn_prefix(capsys):
    Log.warn("watch out")
    err = capsys.readouterr().err.strip()
    assert re.match(r"^\d{2}:\d{2}:\d{2} \[scidb\] WARN: watch out$", err), err


def test_console_stream_is_stderr_not_stdout(capsys):
    Log.info("to stderr")
    captured = capsys.readouterr()
    assert "to stderr" in captured.err
    assert captured.out == ""


# -- dual-sink levels ------------------------------------------------------


def test_dual_sink_level_independence(tmp_path, capsys):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.set_level("DEBUG", sink="file")
    Log.set_level("INFO", sink="console")
    Log.debug("internal detail")
    Log.info("visible narrative")
    err = capsys.readouterr().err
    assert "internal detail" not in err
    assert "visible narrative" in err
    content = log_file.read_text(encoding="utf-8")
    assert "internal detail" in content
    assert "visible narrative" in content


def test_default_level_is_info_on_both_sinks(tmp_path, capsys):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.debug("hidden by default")
    assert "hidden by default" not in capsys.readouterr().err
    assert "hidden by default" not in read_text_or_empty(log_file)
    assert Log.get_level() == Log.INFO


def test_get_level_reports_min_of_sinks():
    Log.set_level("ERROR", sink="console")
    Log.set_level("DEBUG", sink="file")
    assert Log.get_level("console") == Log.ERROR
    assert Log.get_level("file") == Log.DEBUG
    assert Log.get_level() == Log.DEBUG


def test_set_level_rejects_unknown_sink():
    with pytest.raises(ValueError):
        Log.set_level("INFO", sink="everywhere")


def test_env_var_sets_both_sinks(tmp_path, monkeypatch):
    monkeypatch.setenv("SCIDB_LOG_LEVEL", "DEBUG")
    Log._reset_for_tests()
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.debug("env-enabled detail")
    assert "env-enabled detail" in log_file.read_text(encoding="utf-8")
    assert Log.get_level("console") == Log.DEBUG
    assert Log.get_level("file") == Log.DEBUG


# -- caplog contract -------------------------------------------------------


def test_caplog_sees_debug_even_when_sinks_at_error(caplog):
    Log.set_level("ERROR", sink="both")
    with caplog.at_level(logging.DEBUG, logger="scidb"):
        Log.debug("hidden from sinks")
    assert any("hidden from sinks" in r.message for r in caplog.records)


def test_caplog_scidb_logger_name_unchanged(caplog):
    with caplog.at_level(logging.INFO, logger="scidb"):
        Log.info("legacy contract")
    assert caplog.records[0].name == "scidb"


# -- layer routing ---------------------------------------------------------


def test_layer_kwarg_routes_to_layer_logger(tmp_path, caplog):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with caplog.at_level(logging.INFO, logger="scifor"):
        Log.info("from the orchestrator", layer="scifor")
    assert "[scifor] from the orchestrator" in log_file.read_text(encoding="utf-8")
    assert caplog.records[0].name == "scifor"


def test_every_declared_layer_reaches_the_file(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    for layer in LAYERS:
        Log.info("ping", layer=layer)
    content = log_file.read_text(encoding="utf-8")
    for layer in LAYERS:
        assert f"[{layer}] ping" in content


def test_child_module_loggers_are_covered(tmp_path):
    """Plain logging.getLogger('<layer>.<module>') records land in the file
    tagged with the layer — this is how sciduckdb/scilineage/etc. log."""
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    logging.getLogger("scidb.discover").info("module-level record")
    logging.getLogger("scilineage.hashing").info("hash note")
    content = log_file.read_text(encoding="utf-8")
    assert "[scidb] module-level record" in content
    assert "[scilineage] hash note" in content


def test_unknown_layer_falls_back_to_scidb_with_warning(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with pytest.warns(UserWarning, match="Unknown scistack layer"):
        Log.info("odd origin", layer="not_a_layer")
    assert "[scidb] odd origin" in log_file.read_text(encoding="utf-8")


# -- lifecycle -------------------------------------------------------------


def test_set_path_repoints_mid_run(tmp_path):
    first, second = tmp_path / "a.log", tmp_path / "b.log"
    Log.set_path(str(first))
    Log.info("first message")
    Log.set_path(str(second))
    Log.info("second message")
    assert "first message" in first.read_text(encoding="utf-8")
    assert "second message" not in first.read_text(encoding="utf-8")
    assert "second message" in second.read_text(encoding="utf-8")
    assert Log.get_path() == str(second)


def test_set_path_none_detaches_file_sink(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.info("recorded")
    Log.set_path(None)
    Log.info("not recorded")
    assert "not recorded" not in log_file.read_text(encoding="utf-8")
    assert Log.get_path() is None


def test_attach_is_idempotent_no_duplicate_lines(tmp_path):
    Log.attach()
    Log.attach()
    Log.bridge_python_logging()
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.info("exactly once")
    assert log_file.read_text(encoding="utf-8").count("exactly once") == 1


# -- instrumentation: Log.step / Log.timer ---------------------------------


def test_step_entry_exit_at_debug(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.set_level("DEBUG", sink="file")
    with Log.step("resolve_empty_lists", layer="scifor"):
        pass
    content = log_file.read_text(encoding="utf-8")
    assert "→ resolve_empty_lists" in content
    assert "← resolve_empty_lists done in" in content
    assert "[scifor]" in content


def test_step_silent_at_info(tmp_path, capsys):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with Log.step("quiet_op"):
        pass
    assert "quiet_op" not in read_text_or_empty(log_file)
    assert "quiet_op" not in capsys.readouterr().err


def test_step_failure_logs_error_with_traceback_and_reraises(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with pytest.raises(ValueError, match="broken"):
        with Log.step("save_batch(PSD)"):
            raise ValueError("broken")
    content = log_file.read_text(encoding="utf-8")
    assert "✗ save_batch(PSD) failed after" in content
    assert "ValueError: broken" in content
    assert "Traceback (most recent call last):" in content


def test_timer_summary_at_info_with_timing_tag(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with Log.timer("save_batch(PSD)", extra="114 items") as t:
        with t.phase("canonical_hash"):
            pass
        with t.phase("commit"):
            pass
    content = log_file.read_text(encoding="utf-8")
    summary = [l for l in content.splitlines() if "[timing]" in l]
    assert len(summary) == 1
    assert "save_batch(PSD): 114 items, TOTAL=" in summary[0]
    assert "canonical_hash=" in summary[0]
    assert "commit=" in summary[0]


def test_timer_phase_table_at_debug_only(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with Log.timer("load(PSD)") as t:
        with t.phase("find"):
            pass
    at_info = log_file.read_text(encoding="utf-8")
    # Summary present, per-phase table suppressed at INFO.
    assert "[timing] load(PSD)" in at_info
    assert "\n  load(PSD) find" not in at_info

    Log.set_level("DEBUG", sink="file")
    with Log.timer("load(PSD)") as t:
        with t.phase("find"):
            pass
    at_debug = log_file.read_text(encoding="utf-8")
    assert "  load(PSD) find" in at_debug


def test_timings_matches_timer_summary_shape(tmp_path):
    """Log.timings is for callers that collect their own perf_counter deltas.

    save_batch and record_run hand-roll a `timings` dict; restructuring those
    hot paths into nested context managers would be a large change for no gain,
    so they call this instead. The output contract must be identical to timer()
    so one grep finds both.
    """
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.timings(
        "save_batch(PSD)",
        {"canonical_hash": 1.5, "commit": 0.25, "total": 2.0},
        extra="114 items",
    )
    summary = [
        l for l in log_file.read_text(encoding="utf-8").splitlines() if "[timing]" in l
    ]
    assert len(summary) == 1
    assert "save_batch(PSD): 114 items, TOTAL=2.000s" in summary[0]
    assert "canonical_hash=1.500s" in summary[0]
    assert "commit=0.250s" in summary[0]
    # 'total' is the total, not a phase of its own.
    assert "total=" not in summary[0]


def test_timings_total_defaults_to_sum_when_absent(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.timings("record_run(fn=f)", {"meta_fetch": 1.0, "commit": 2.0})
    assert "TOTAL=3.000s" in log_file.read_text(encoding="utf-8")


def test_timings_top_keeps_the_info_line_readable(tmp_path):
    """~18 phases do not fit one line; the costliest few go to INFO."""
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    phases = {f"p{i}": float(i) for i in range(10)}
    Log.timings("save_batch(X)", phases, top=3)
    summary = [
        l for l in log_file.read_text(encoding="utf-8").splitlines() if "[timing]" in l
    ][0]
    # Three most expensive present, cheapest absent, remainder counted.
    assert "p9=9.000s" in summary
    assert "p8=8.000s" in summary
    assert "p7=7.000s" in summary
    assert "p0=" not in summary
    assert "+7 more" in summary


def test_timings_phase_table_at_debug_only(tmp_path):
    """Every phase still reaches DEBUG even when `top` trims the INFO line."""
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.set_level("DEBUG", sink="file")
    Log.timings("save_batch(X)", {"cheap": 0.001, "dear": 9.0}, top=1)
    content = log_file.read_text(encoding="utf-8")
    assert "  save_batch(X) cheap" in content
    assert "  save_batch(X) dear" in content


def test_timings_accepts_pairs(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.timings("x", [("a", 1.0), ("b", 2.0)])
    assert "a=1.000s, b=2.000s" in log_file.read_text(encoding="utf-8")


def test_timer_emits_summary_even_when_body_raises(tmp_path):
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with pytest.raises(RuntimeError):
        with Log.timer("save_batch(X)"):
            raise RuntimeError("mid-save failure")
    assert "[timing] save_batch(X): TOTAL=" in log_file.read_text(encoding="utf-8")


def test_live_timer_announces_each_phase_as_it_starts(tmp_path):
    """A live timer speaks DURING the work, not only after it.

    The case this exists for: a figure save whose resolve ran for 25 minutes
    (scidb.log 2026-09-11 12:28 -> 12:54) and whose only instrumentation was a
    summary emitted on exit — so a save that timed out, or was still running
    when the log was read, described itself not at all.
    """
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with Log.timer("save_figure", live=True) as t:
        with t.phase("resolve", extra="2 figure(s)"):
            t.note("figure %d/%d", 1, 2)
    lines = read_lines(log_file)

    assert any("save_figure: resolve started — 2 figure(s)" in l for l in lines)
    assert any("save_figure: figure 1/2" in l for l in lines)
    assert any("save_figure: resolve done in" in l for l in lines)
    # The summary still comes last, and is still the only line carrying TOTAL=
    # — the shape MATLAB's timing archives grep for.
    assert "[timing] save_figure: TOTAL=" in lines[-1]
    assert len([l for l in lines if "TOTAL=" in l]) == 1


def test_a_quiet_timer_says_nothing_until_it_is_done(tmp_path):
    """The default is unchanged: one summary, no narration, no notes."""
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    with Log.timer("load(PSD)") as t:
        with t.phase("find"):
            t.note("this is not worth a line")
    lines = read_lines(log_file)

    assert len(lines) == 1
    assert "[timing] load(PSD): TOTAL=" in lines[0]
    assert "not worth a line" not in lines[0]


def test_positional_matlab_call_shapes(tmp_path):
    """MATLAB calls py.scidb.log.Log.info(msg) and set_level(level[, sink])."""
    log_file = tmp_path / "scidb.log"
    Log.set_path(str(log_file))
    Log.info("plain positional")
    Log.set_level("DEBUG")
    Log.set_level("INFO", "console")
    Log.set_level(0, "file")  # numeric 0-3 scale
    assert "plain positional" in log_file.read_text(encoding="utf-8")
    assert Log.get_level("file") == Log.DEBUG
    assert Log.get_level("console") == Log.INFO
