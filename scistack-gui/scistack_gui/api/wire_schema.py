"""The JSON Schema of every handler's request — the source of the frontend's types.

Each :class:`~scistack_gui.api.handlers.Handler` declares its request as a
pydantic model (``Handler.params``); the dispatch validates every request
against it. That makes the model the one owner of a request's shape. This
module turns the whole table into ONE JSON Schema document:

* ``$defs`` — every request model (and the models they nest);
* ``properties`` — method name -> ``$ref`` to its request model, so the
  TypeScript emitted from it has an ``ApiParams`` interface keyed by method;
* the spec wire models (:mod:`scistack_gui.api.spec_wire`: ``PlotSpec``,
  ``StatsSpec`` and their parts) as further ``$defs``, so the frontend's spec
  types are generated too.

``frontend/src/generated/api-schema.json`` is this document;
``npm run gen:types`` turns it into ``frontend/src/generated/api-types.ts``
(``json-schema-to-typescript``). ``tests/test_api_handlers.py`` holds the
committed JSON to :func:`api_json_schema`; the frontend's
``generatedTypes.test.ts`` holds the committed ``.ts`` to the JSON.

Bodiless handlers (``params is None``) are not listed: they take ``{}``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic.json_schema import models_json_schema

from scistack_gui.api.spec_wire import SPEC_MODELS
from scistack_gui.api.tables import ALL_HANDLERS

logger = logging.getLogger(__name__)

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
    logger.debug(
        "[wire_schema] %d request models, %d spec models", len(models), len(SPEC_MODELS)
    )
    refs, document = models_json_schema(
        [(model, "validation") for model in (*models, *SPEC_MODELS)],
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
        "$defs": {name: _definition(name, d) for name, d in document.get("$defs", {}).items()},
    }


def _definition(name: str, definition: dict) -> dict:
    """One ``$defs`` entry as the TypeScript generator should see it.

    A request model's free-form ``dict`` field (no declared shape) becomes
    TypeScript ``object``: json-schema-to-typescript would emit
    ``{[k: string]: unknown}``, which no named TypeScript interface is
    assignable to, though any JSON object is what the backend accepts. The
    spec wire models (the ``Wire`` suffix, :mod:`scistack_gui.api.spec_wire`)
    keep their index signatures: a variant selection IS read by key."""
    definition = _untyped_as_unknown(_untitled_properties(definition))
    if name.endswith("Wire"):
        return definition
    return _free_objects_as_ts_object(definition)


#: Keywords that give a schema a type. A property with none of them is
#: ``Any`` in Python (pydantic writes only its default).
_TYPING_KEYWORDS = ("type", "$ref", "anyOf", "allOf", "oneOf", "enum", "const", "items")


def _untyped_as_unknown(definition: dict) -> dict:
    """An ``Any`` field is TypeScript ``unknown``: with no type at all,
    json-schema-to-typescript would emit ``{[k: string]: unknown}``, an object."""
    properties = definition.get("properties")
    if not properties:
        return definition
    return {
        **definition,
        "properties": {
            name: (
                prop
                if any(key in prop for key in _TYPING_KEYWORDS)
                else {**prop, "tsType": "unknown"}
            )
            for name, prop in properties.items()
        },
    }


def _free_objects_as_ts_object(node: Any) -> Any:
    if isinstance(node, list):
        return [_free_objects_as_ts_object(item) for item in node]
    if not isinstance(node, dict):
        return node
    if (
        node.get("type") == "object"
        and node.get("additionalProperties") is True
        and "properties" not in node
    ):
        return {**node, "tsType": "object"}
    return {key: _free_objects_as_ts_object(value) for key, value in node.items()}


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
