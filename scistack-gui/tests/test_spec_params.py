"""Spec-carrying request fields are validated at dispatch (P5 stage 5, step 4).

``scistack_gui.api.spec_params`` types every request field that carries a
plot or stats spec. Dispatch checks the dict against the spec's wire model
and hands the services the dict as sent. These tests guard:

* a real ``to_dict`` passes through unchanged, a malformed spec is refused
  with the field's path (and a WARNING);
* every spec-carrying field of every handler uses one of the two types,
  so no ``spec: dict`` slips back in;
* the generated request schema references the spec types;
* the wire models stay off the GUI's startup import path.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import typing
from typing import Any

import pytest
from pydantic import ValidationError

from scistack_gui.api.plot import SpecRequest, VariantSetsRequest
from scistack_gui.api.spec_params import (
    DifferenceBarParam,
    FactorVariableParam,
    PlotSpecParam,
    StatsSpecParam,
    VariantSetParam,
)
from scistack_gui.api.stats import ComputeRequest, PlotGuideRequest
from scistack_gui.api.tables import ALL_HANDLERS
from scistack_gui.api.wire_schema import api_json_schema
from scistackplot import PlotKind, PlotSpec
from scistackstats import StatsSpec

#: Request fields that carry a spec or a spec part, by name.
SPEC_FIELDS = {
    "spec", "plot_spec", "stats_spec", "seed", "derived_bars", "variant_sets", "covariates",
}


def test_a_real_spec_passes_through_unchanged():
    raw = PlotSpec(measures=["Force"], kind=PlotKind.BAR).to_dict()
    assert SpecRequest.model_validate({"spec": raw}).spec == raw
    stats = StatsSpec(design=PlotSpec(measures=["Force"])).to_dict()
    assert ComputeRequest.model_validate({"spec": stats}).spec == stats


def test_a_malformed_spec_is_refused_with_its_path(caplog):
    raw = {**PlotSpec(measures=["Force"]).to_dict(), "histogram": {"bin_count": "many"}}
    with caplog.at_level(logging.WARNING), pytest.raises(ValidationError) as info:
        SpecRequest.model_validate({"spec": raw})
    assert "histogram.bin_count" in str(info.value)
    assert "[spec_params] PlotSpecWire refused: histogram.bin_count" in caplog.text


def test_the_plot_guide_takes_a_plot_spec_not_a_table():
    """stats_plot_guide used to share ComputeRequest (a table's spec)."""
    raw = PlotSpec(measures=["Force"]).to_dict()
    assert PlotGuideRequest.model_validate({"spec": raw}).spec == raw
    with pytest.raises(ValidationError):
        ComputeRequest.model_validate({"spec": raw})


#: The validation markers, taken from the public types themselves.
MARKERS = {
    id(typing.get_args(p)[1])
    for p in (
        PlotSpecParam, StatsSpecParam, DifferenceBarParam, VariantSetParam, FactorVariableParam,
    )
}


def _is_spec_param(info: Any) -> bool:
    """Whether a field is (optionally) PlotSpecParam / StatsSpecParam."""

    def marked(annotation: Any) -> bool:
        if any(id(m) in MARKERS for m in getattr(annotation, "__metadata__", ())):
            return True
        return any(marked(arg) for arg in typing.get_args(annotation))

    return any(id(m) in MARKERS for m in info.metadata) or marked(info.annotation)


def test_every_spec_field_is_validated():
    plain = [
        f"{h.name}.{name}"
        for h in ALL_HANDLERS
        if h.params is not None
        for name in h.params.model_fields
        if name in SPEC_FIELDS and not _is_spec_param(h.params.model_fields[name])
    ]
    assert plain == []


def test_request_schema_names_the_spec_types():
    schema = api_json_schema()
    defs = schema["$defs"]

    def ref_of(method: str, field: str) -> str:
        model = defs[schema["properties"][method]["$ref"].removeprefix("#/$defs/")]
        prop = model["properties"][field]
        ref = prop.get("$ref") or next(a["$ref"] for a in prop["anyOf"] if "$ref" in a)
        return defs[ref.removeprefix("#/$defs/")]["title"]

    assert ref_of("plot_resolve", "spec") == "PlotSpec"
    assert ref_of("stats_compute", "spec") == "StatsSpec"
    assert ref_of("stats_plot_guide", "spec") == "PlotSpec"
    assert ref_of("stats_open", "seed") == "PlotSpec"


def test_the_wire_models_load_on_first_use_not_at_startup():
    code = (
        "import sys, scistack_gui.api.plot, scistack_gui.api.stats\n"
        "print('scistack_gui.api.spec_wire' in sys.modules)\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False", out.stderr



def test_a_spec_part_is_checked_on_its_own():
    """Derived bars, Variants rows and covariates cross without their spec."""
    good = VariantSetsRequest.model_validate(
        {"variable": "Force", "variant_sets": [{"selection": {"Code:f": "latest"}}]}
    )
    assert good.variant_sets == [{"selection": {"Code:f": "latest"}}]
    with pytest.raises(ValidationError) as info:
        VariantSetsRequest.model_validate(
            {"variable": "Force", "variant_sets": [{"selection": {}, "colour": "red"}]}
        )
    assert "colour" in str(info.value)


def test_free_form_and_untyped_fields_have_usable_ts_types():
    """A request's shapeless ``dict`` is TS ``object`` (a named interface fits
    it), an ``Any`` is ``unknown``; a spec part's own maps keep their index
    signatures, because the webview reads them by key."""
    defs = api_json_schema()["$defs"]
    preview = defs["SpecRequest"]["properties"]["preview"]
    assert any(member.get("tsType") == "object" for member in preview["anyOf"])
    assert defs["ParameterValue"]["properties"]["value"]["tsType"] == "unknown"
    selection = defs["VariantSetWire"]["properties"]["selection"]
    assert "tsType" not in selection
