"""What each role is CALLED, on every surface: the one owner
(docs/claude/studio-unification.md).

The roles themselves (:class:`~scistackplot.model.spec.Role`) are the library's
vocabulary and appear in specs and logs. These are the user's words. Plot
Studio, StatStudio (its rail and its step-by-step view) and the canvas's
Schema Level line all read them from here, so a figure and a table built
from one design describe it in the same words.

The verb says what happens to a key's levels, and only the noun changes
with the output (user decision 2026-11-01):

| Role | figure | table |
|---|---|---|
| GROUP | Compare | Compare |
| FACET | Separate panels | Separate sections |
| ITERATE | Separate figures | Separate tables |
| COLLAPSE | Average over | Average over |

``process`` is the canvas: a step only has the ITERATE idea (Schema Level's
ticked keys). Unticked keys reach the function together; they are not
averaged, so that surface has no COLLAPSE word.
"""

from __future__ import annotations

from scistacklog import Log

from .spec import Role

LAYER = "scistackplot"

#: The surfaces that show role names.
SURFACES = ("figure", "table", "process")

ROLE_LABELS: dict[str, dict[Role, str]] = {
    "figure": {
        Role.GROUP: "Compare",
        Role.FACET: "Separate panels",
        Role.ITERATE: "Separate figures",
        Role.COLLAPSE: "Average over",
    },
    "table": {
        Role.GROUP: "Compare",
        Role.FACET: "Separate sections",
        Role.ITERATE: "Separate tables",
        Role.COLLAPSE: "Average over",
    },
    "process": {
        Role.ITERATE: "Separate",
    },
}


def role_label_for(role: Role | str, surface: str = "figure") -> str:
    """The user's name for *role* on *surface* (``figure``, ``table`` or
    ``process``). A role the surface has no word for falls back to the
    figure's word, with a warning, rather than showing the library's name."""
    if surface not in ROLE_LABELS:
        raise ValueError(f"unknown role-label surface {surface!r}; expected one of {SURFACES}")
    role = Role(role) if not isinstance(role, Role) else role
    label = ROLE_LABELS[surface].get(role)
    if label is None:
        Log.warn("[role labels] %s has no word for %s; using the figure's", surface, role, layer=LAYER)
        return ROLE_LABELS["figure"][role]
    return label


def role_labels_payload(surface: str) -> dict[str, str]:
    """``{role id: label}`` for one surface, for a webview that renders the
    names it is handed."""
    if surface not in ROLE_LABELS:
        raise ValueError(f"unknown role-label surface {surface!r}; expected one of {SURFACES}")
    return {str(role): label for role, label in ROLE_LABELS[surface].items()}
