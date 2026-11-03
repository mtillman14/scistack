"""Request fields that carry a plot spec or a stats spec.

``PlotSpecParam`` / ``StatsSpecParam`` are ``dict``s at runtime. The handlers
pass the dict on unchanged to the services, which read it with
``PlotSpec.from_dict`` / ``StatsSpec.from_dict``. On the way in, dispatch
validates the dict against its wire model
(:mod:`scistack_gui.api.spec_wire`), so a malformed spec is refused at the
boundary with pydantic's field paths (a 422 over HTTP), not deep inside a
service. The generated request types name the spec types (``spec: PlotSpec``)
instead of ``{[k: string]: unknown}``.

The wire models import scistackplot and scistackstats, which the GUI loads
only when a studio first needs them (every service imports them inside its
functions). So they are imported here on first use, not at module top:
:func:`_wire_model` is that one lazy import (design rule D8: a heavy
dependency kept off the startup path).
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from pydantic import BaseModel, GetCoreSchemaHandler, GetJsonSchemaHandler, ValidationError
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema, core_schema

logger = logging.getLogger(__name__)


def _wire_model(name: str) -> type[BaseModel]:
    # Lazy (D8): spec_wire imports scistackplot + scistackstats, kept off
    # the GUI's startup path; the first spec request loads them anyway.
    from scistack_gui.api import spec_wire

    return getattr(spec_wire, name)


class _CheckedSpec:
    """``Annotated`` marker: validate a dict against wire model ``model_name``,
    keep the dict exactly as sent."""

    def __init__(self, model_name: str) -> None:
        self.model_name = model_name

    def __get_pydantic_core_schema__(
        self, source: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(self._check, handler(source))

    def __get_pydantic_json_schema__(
        self, schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        return handler(_wire_model(self.model_name).__pydantic_core_schema__)

    def _check(self, value: dict[str, Any]) -> dict[str, Any]:
        try:
            _wire_model(self.model_name).model_validate(value)
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(p) for p in error['loc']) or '(spec)'}: {error['msg']}"
                for error in exc.errors()
            )
            logger.warning("[spec_params] %s refused: %s", self.model_name, problems)
            raise ValueError(f"not a valid {self.model_name.removesuffix('Wire')}: {problems}")
        return value


#: A ``PlotSpec.to_dict()``-shaped request field.
PlotSpecParam = Annotated[dict[str, Any], _CheckedSpec("PlotSpecWire")]
#: A ``StatsSpec.to_dict()``-shaped request field.
StatsSpecParam = Annotated[dict[str, Any], _CheckedSpec("StatsSpecWire")]
#: A spec part sent on its own: one ``DifferenceBar.to_dict()`` (derived
#: bars), one ``VariantSet.to_dict()`` (a Variants row), one
#: ``FactorVariable.to_dict()`` (a covariate).
DifferenceBarParam = Annotated[dict[str, Any], _CheckedSpec("DifferenceBarWire")]
VariantSetParam = Annotated[dict[str, Any], _CheckedSpec("VariantSetWire")]
FactorVariableParam = Annotated[dict[str, Any], _CheckedSpec("FactorVariableWire")]
