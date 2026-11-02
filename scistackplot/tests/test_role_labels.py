"""The one owner of role words (docs/claude/studio-unification.md).

A figure and a table built from one design must describe it in the same
words: the verb is fixed, only the noun follows the output.
"""

import pytest

from scistackplot.role_labels import ROLE_LABELS, SURFACES, role_label_for, role_labels_payload
from scistackplot.spec import Role


def test_every_output_surface_names_every_role():
    for surface in ("figure", "table"):
        assert set(ROLE_LABELS[surface]) == set(Role)


def test_the_verb_is_shared_and_only_the_noun_changes():
    assert role_label_for(Role.GROUP, "figure") == role_label_for(Role.GROUP, "table") == "Compare"
    assert role_label_for(Role.COLLAPSE, "figure") == role_label_for(Role.COLLAPSE, "table") == "Average over"
    assert role_label_for(Role.ITERATE, "figure") == "Separate figures"
    assert role_label_for(Role.ITERATE, "table") == "Separate tables"
    assert role_label_for(Role.FACET, "figure") == "Separate panels"
    assert role_label_for(Role.FACET, "table") == "Separate sections"


def test_process_has_only_separate():
    # Unticked Schema Level keys reach the function together; they are not
    # averaged, so the canvas must never call them "Average over".
    assert ROLE_LABELS["process"] == {Role.ITERATE: "Separate"}


def test_a_missing_word_falls_back_to_the_figure():
    assert role_label_for(Role.COLLAPSE, "process") == "Average over"


def test_strings_are_accepted():
    assert role_label_for("iterate", "table") == "Separate tables"


def test_unknown_surface_is_refused():
    with pytest.raises(ValueError):
        role_label_for(Role.GROUP, "poster")
    with pytest.raises(ValueError):
        role_labels_payload("poster")


def test_payload_is_keyed_by_role_id():
    assert role_labels_payload("table") == {
        "group": "Compare", "facet": "Separate sections", "iterate": "Separate tables", "collapse": "Average over",
    }
    assert set(SURFACES) == set(ROLE_LABELS)
