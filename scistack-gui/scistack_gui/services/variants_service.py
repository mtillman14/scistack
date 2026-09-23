"""
The bottom-up variant view for the GUI — a thin shell over ``scidb.inspect``.

Nothing here computes anything about variants. ``Inspector.topologies`` does,
and ``scidb variants <name>`` renders the same object, so the panel and the
terminal cannot answer differently (CLAUDE.md NOTE 3: the solution lives in
the owning layer). Even the ``load:`` verdict and the location sample are
imported (``inspect.api.variant_verdict`` / ``location_sample``) rather than
re-derived, because a rule written on this side of the wire would be a second
answer to "is this variant still alive" — which is the one question the view
exists to answer.

**Why this is not a subprocess.** ``provenance_service``'s docstring states
the reason and it is the same one: shelling out to ``scidb variants --json``
would open a SECOND connection to a single-writer DuckDB file, which is
exactly the write-lock contention the MATLAB run-ownership work removed
(docs/claude/matlab-run-database-ownership.md). Parity is kept instead by
building the CLI command first and giving the panel no question the command
cannot also answer — hence the ``command`` field in the reply.

**Top-down vs bottom-up.** ``provenance_service`` answers "where did this
variant come from"; you have to know the variant exists to ask it. This one
answers "what is in here, and which of it is still live" — the question a
user has when a variable grew more variants than they expected
(2026-09-22, a run that split ``grSides`` into two nodes).
"""

import dataclasses
import logging

from scidb.inspect.api import location_sample, variant_verdict

logger = logging.getLogger("scistack_gui.variants")

#: How many schema locations travel with each variant by default. A loader
#: with 450 of them must not bury the row; the panel asks for the rest by
#: re-requesting with ``max_locations=None``, the same way
#: ``scidb variants --locations`` does.
DEFAULT_MAX_LOCATIONS = 6


def variable_topologies(db, name: str, *, max_locations: int | None = DEFAULT_MAX_LOCATIONS) -> dict:
    """``Inspector.topologies(name)``, as JSON the Variants panel can render.

    Shape::

        {
          "variable": "GAITRiteLoaded",
          "command": "scidb variants GAITRiteLoaded",
          "topology_count": 1,
          "variant_count": 2,
          "topologies": [
            {
              "function_name": "loadGaitRiteOneFile",
              "input_types": [["gaitRitePath", "…"], ["gaitRiteConfig", "…"]],
              "output_type": "GAITRiteLoaded",
              "variants": [ {…VariantSummary…,
                             "verdict": "current",
                             "verdict_label": "load: CURRENT",
                             "locations": {"total": 450, "keys": [...],
                                           "sample": [{...}, ...]}} ]
            }
          ]
        }

    ``input_types`` stays a LIST of pairs rather than a dict: it is the
    topology key, ordered by ``Inspector.topologies``, and a dict would let a
    JSON round trip reorder the heading that names the node.

    A name that is not a variable raises ``NotFoundError``, which the handler
    maps to HTTP 400 — asking about a name that does not exist is a user typo,
    not a server fault. A real variable with no producing pipeline steps is
    not an error: it returns zero topologies, which is the true answer.
    """
    from scistack_gui.db import db_connection

    with db_connection("variable_topologies"):
        # NotFoundError propagates: see the handler's http_errors mapping.
        grouped = db.inspect.topologies(name)
        payload_topologies = []
        for (fn_name, input_types, output_type), group in grouped:
            variants = []
            for summary in group:
                verdict, label = variant_verdict(summary)
                row = dataclasses.asdict(summary)
                row["verdict"] = verdict
                row["verdict_label"] = label
                row["locations"] = location_sample(
                    db, summary.schema_ids, max_locations
                )
                # schema_ids can be hundreds of integers and the panel never
                # shows them — `locations` is the view of them. Dropped so a
                # loader's reply is a few KB rather than a few hundred.
                row.pop("schema_ids", None)
                variants.append(row)
            payload_topologies.append(
                {
                    "function_name": fn_name,
                    "input_types": [list(pair) for pair in input_types],
                    "output_type": output_type,
                    "variants": variants,
                }
            )

    n_variants = sum(len(t["variants"]) for t in payload_topologies)
    payload = {
        "variable": name,
        # Every screen has a terminal equivalent, printed on the screen: that
        # is what makes "the CLI powers the GUI" checkable rather than a claim.
        "command": f"scidb variants {name}",
        "topology_count": len(payload_topologies),
        "variant_count": n_variants,
        "topologies": payload_topologies,
    }
    not_current = [
        v
        for t in payload_topologies
        for v in t["variants"]
        if v["verdict"] != "current"
    ]
    logger.info(
        "[variants] %s: %d topology/ies, %d variant(s), %d not current%s",
        name,
        len(payload_topologies),
        n_variants,
        len(not_current),
        (
            " — " + ", ".join(sorted({v["verdict"] for v in not_current}))
            if not_current
            else ""
        ),
    )
    return payload


__all__ = ["variable_topologies", "DEFAULT_MAX_LOCATIONS"]
