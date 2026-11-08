"""Expected locations in the GUI (scistack_gui.services.expectation_service).

Every status is scidb's; these tests pin that the handlers reach it, that a
mark's inheritance reaches the canvas badges, and that the badge cache can
never serve a stale count (it is keyed on a database fingerprint).
"""

from scistack_gui.services import expectation_service


def post(client, path, body):
    return client.post(f"/api/expectation{path}", json=body)


def test_expectation_of_a_clean_variable(client):
    r = post(client, "", {"variable": "FilteredSignal"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["counts"]["present"] == 4
    assert body["tone_counts"]["problem"] == 0
    assert body["marks"] == []
    assert body["labels"]["missing_unexpected"] == "Missing unexpectedly"


def test_unknown_variable_is_404(client):
    assert post(client, "", {"variable": "Nope"}).status_code == 404


def test_mark_is_inherited_and_reaches_the_badges(client):
    names = ["RawSignal", "FilteredSignal"]
    before = post(client, "/badges", {"variables": names}).json()
    assert before["RawSignal"]["problems"] == 0

    r = post(client, "/mark", {
        "variable": "RawSignal", "keys": {"subject": "2"}, "expected": False, "reason": "withdrew",
    })
    assert r.status_code == 200, r.text

    after = post(client, "/badges", {"variables": names}).json()
    assert after["RawSignal"] == {"problems": 2, "marks": 1}
    # Downstream inherits the mark, but it is not FilteredSignal's own.
    assert after["FilteredSignal"] == {"problems": 2, "marks": 0}

    row = next(
        r for r in post(client, "", {"variable": "FilteredSignal"}).json()["rows"]
        if r["values"].get("subject") == "2"
    )
    assert row["status"] == "present_unexpected" and row["inherited_from"] == "RawSignal"

    assert post(client, "/unmark", {"variable": "RawSignal", "keys": {"subject": "2"}}).status_code == 200
    again = post(client, "/badges", {"variables": names}).json()
    assert again["RawSignal"] == {"problems": 0, "marks": 0}


def test_badges_are_cached_until_the_database_changes(client, populated_db, monkeypatch):
    calls = []
    real = expectation_service._inspector

    def counting(db):
        insp = real(db)
        original = insp.expectation

        def expectation(name):
            calls.append(name)
            return original(name)

        insp.expectation = expectation
        return insp

    monkeypatch.setattr(expectation_service, "_inspector", counting)
    expectation_service._BADGES.clear()
    expectation_service.badges(populated_db, ["RawSignal"])
    expectation_service.badges(populated_db, ["RawSignal"])
    assert calls == ["RawSignal"]
    from scidb.provenance.expectation import mark_expectation

    mark_expectation(populated_db, "RawSignal", False, "x", subject="1")
    expectation_service.badges(populated_db, ["RawSignal"])
    assert calls == ["RawSignal", "RawSignal"]


def test_a_bad_variable_badge_is_an_error_entry_not_a_failure(client):
    body = post(client, "/badges", {"variables": ["Nope", "RawSignal"]}).json()
    assert body["Nope"]["problems"] == 0 and "error" in body["Nope"]
    assert "error" not in body["RawSignal"]


def test_key_setting_refuses_values_on_a_per_parent_key(client):
    r = post(client, "/key", {"key": "session", "sharing": "per_parent", "values_text": "1..3"})
    assert r.status_code == 400
    ok = post(client, "/key", {"key": "session", "sharing": "shared", "values_text": "pre, post, mid"})
    assert ok.status_code == 200, ok.text
    body = post(client, "", {"variable": "RawSignal"}).json()
    assert body["counts"]["missing_unexpected"] == 2  # "mid" for both subjects


def test_coverage_and_follow(client):
    cov = post(client, "/coverage", {"by": ["subject"]}).json()
    assert {"RawSignal", "FilteredSignal"} <= set(cov["variables"])
    assert cov["values"] == [["1"], ["2"]]
    fol = post(client, "/follow", {"location": {"subject": "1", "session": "pre"}}).json()
    assert fol["lost_at"] == []
    assert {s["variable"] for s in fol["steps"]} >= {"RawSignal", "FilteredSignal"}
