"""``scidb seams``: count hedged ('probable') failures in the debug log."""

from scidb.inspect.cli import seam_fallback_counts

_LINES = [
    "2026-11-01 09:00:00.000 WARN  [scifor] [seam_fallback] stage=combine_outputs "
    "fn=f output=Y TypeError: cannot concatenate — no check names this cause yet\n",
    "2026-11-02 09:00:00.000 WARN  [scifor] run=ab12 [seam_fallback] stage=combine_outputs "
    "fn=g output=Y TypeError: cannot concatenate — no check names this cause yet\n",
    "2026-11-02 09:30:00.000 WARN  [scifor] [seam_fallback] stage=save fn=f output=Z "
    "ValueError: bad — no check names this cause yet\n",
    "2026-11-02 09:31:00.000 INFO  [scifor] for_each(f) — 2 iterations\n",
]


def test_groups_by_stage_and_exception_most_frequent_first(tmp_path):
    log = tmp_path / "scistack-debug.log"
    log.write_text("".join(_LINES), encoding="utf-8")
    groups = seam_fallback_counts([log])
    assert [(g["stage"], g["exc_type"], g["count"]) for g in groups] == [
        ("combine_outputs", "TypeError", 2),
        ("save", "ValueError", 1),
    ]
    assert groups[0]["fns"] == ["f", "g"]
    assert groups[0]["last"] == "2026-11-02 09:00:00.000"


def test_since_skips_older_lines(tmp_path):
    log = tmp_path / "scistack-debug.log"
    log.write_text("".join(_LINES), encoding="utf-8")
    groups = seam_fallback_counts([log], since="2026-11-02")
    assert [(g["stage"], g["count"]) for g in groups] == [
        ("combine_outputs", 1),
        ("save", 1),
    ]


def test_missing_log_is_empty(tmp_path):
    assert seam_fallback_counts([tmp_path / "nope.log"]) == []
