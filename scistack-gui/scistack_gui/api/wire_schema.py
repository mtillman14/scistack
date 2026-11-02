"""The JSON Schema of every handler's request — the source of the frontend's types.

Each :class:`~scistack_gui.api.handlers.Handler` declares its request as a
pydantic model (``Handler.params``); the dispatch validates every request
against it. That makes the model the one owner of a request's shape. This
module turns the whole table into ONE JSON Schema document:

* ``$defs`` — every request model (and the models they nest);
* ``properties`` — method name -> ``$ref`` to its request model, so the
  TypeScript emitted from it has an ``ApiParams`` interface keyed by method.

``frontend/src/generated/api-schema.json`` is this document;
``npm run gen:types`` turns it into ``frontend/src/generated/api-types.ts``
(``json-schema-to-typescript``). ``tests/test_api_handlers.py`` holds the
committed JSON to :func:`api_json_schema`; the frontend's
``generatedTypes.test.ts`` holds the committed ``.ts`` to the JSON.

Bodiless handlers (``params is None``) are not listed: they take ``{}``.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic.json_schema import models_json_schema

from scistack_gui.api.tables import ALL_HANDLERS

#: Where the committed schema lives, relative to the scistack-gui folder.
SCHEMA_PATH = Path("frontend") / "src" / "generated" / "api-schema.json"


def api_json_schema() -> dict:
    """One JSON Schema for every handler's request model, keyed by method."""
    with_params = sorted(
        ((h.name, h.params) for h in ALL_HANDLERS if h.params is not None),
        key=lambda pair: pair[0],
    )
    models = sorted(
        {model for _, model in with_params}, key=lambda m: (m.__name__, m.__module__)
    )
    refs, document = models_json_schema(
        [(model, "validation") for model in models],
        ref_template="#/$defs/{model}",
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ApiParams",
        "description": (
            "GENERATED from scistack_gui.api.wire_schema.api_json_schema. "
            "The request params of every GUI backend method, by method name."
        ),
        "type": "object",
        "properties": {
            name: {"$ref": refs[(model, "validation")]["$ref"]} for name, model in with_params
        },
        "required": [name for name, _ in with_params],
        "additionalProperties": False,
        "$defs": {name: _untitled_properties(d) for name, d in document.get("$defs", {}).items()},
    }


def _untitled_properties(definition: dict) -> dict:
    """The model schema without pydantic's per-field ``title``s. A field
    title makes json-schema-to-typescript emit a type alias per field
    (``export type PipelineId = string | null``), which collide across
    models; the model's own title stays and names its interface."""
    properties = definition.get("properties")
    if not properties:
        return definition
    return {
        **definition,
        "properties": {
            name: {k: v for k, v in prop.items() if k != "title"}
            for name, prop in properties.items()
        },
    }


def schema_text() -> str:
    """The committed file's exact text (stable key order, trailing newline)."""
    return json.dumps(api_json_schema(), indent=2, sort_keys=True) + "\n"
