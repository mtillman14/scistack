"""
Tests for glue nodes — transient in-memory reshaping between a variable and
the function consuming it (``scidb.glue``, Stage 1).

Design: ``docs/claude/free-code-glue-nodes.md``.

Covers:
- the column space is free: add / drop / rename / retype round-trips
- the row set is not: row-count, index and collapse-to-scalar all refuse
- hidden ``__record_id`` / ``__branch_params`` survive a glue untouched
- schema-key columns are visible but protected (dropped or retyped -> refuse)
- chains apply in order; a 2-parameter glue binds by stated binding, not name
- non-table (scalar) values skip the row contract
- fusion through ``for_each``: both loaded-column naming modes, Merge inputs,
  PathInput refusal, and the per-schema-key opt-in
- glue on a CONSTANT-fed parameter (the Parameter case): applied in prepare so
  the glued value is what lands in ``__constants``, the inverted language
  rule, and the EachOf recursion that used to drop glue entirely
"""

import logging

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import (
    BaseVariable,
    GlueChainOrderError,
    GlueLanguageMismatchError,
    GlueRowsChangedError,
    GlueSchemaKeysAlteredError,
    GlueSpec,
    GlueUnsupportedInputError,
    Merge,
    Parameter,
    PathInput,
    configure_database,
    for_each,
)
from scifor import EachOf
from scidb.glue import (
    apply_glue_chain,
    bulk_chain,
    chain_hash,
    check_run_language,
    normalize_glue,
    per_combo_chain,
)

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_glue.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# --- Variable types --------------------------------------------------------


class RawEMG(BaseVariable):
    """DataFrame-stored: the glue sees the user's own column names."""


class Force(BaseVariable):
    """Scalar-stored: the glue sees one data column named after the class."""


class Aux(BaseVariable):
    pass


class Analyzed(BaseVariable):
    pass


# --- Glue bodies -----------------------------------------------------------


def glue_drop_baseline(emg):
    return emg.drop(columns=["baseline"])


def glue_rename_signal(emg):
    return emg.rename(columns={"signal": "sig"})


def glue_add_double(emg):
    out = emg.copy()
    out["double"] = out["signal"] * 2
    return out


def glue_retype_signal(emg):
    out = emg.copy()
    out["signal"] = out["signal"].astype(str)
    return out


def glue_drop_rows(emg):
    return emg.iloc[:1]


def glue_collapse(emg):
    return float(emg["signal"].sum())


def glue_reindex(emg):
    return emg.set_index("signal")


def glue_drop_subject(emg):
    return emg.drop(columns=["subject"])


def glue_retype_subject(emg):
    out = emg.copy()
    out["subject"] = out["subject"].astype(int)
    return out


def glue_scale(value):
    return value * 10


def glue_combine(emg, offsets):
    """A 2-parameter glue: the piped table plus one extra bound input."""
    out = emg.copy()
    out["shifted"] = out["signal"] + offsets
    return out


def sum_signal(emg):
    return float(np.sum(emg["signal"]))


def pass_force(force):
    return float(force)


def sum_numeric(merged):
    return float(np.sum(merged.select_dtypes("number").values))


def pass_raw(raw):
    return raw


def _frame(n=3, *, hidden=True, subject="1"):
    df = pd.DataFrame(
        {
            "subject": [subject] * n,
            "signal": [1.0 * i for i in range(n)],
            "baseline": [0.5] * n,
        }
    )
    if hidden:
        df["__record_id"] = [f"rid{i}" for i in range(n)]
        df["__branch_params"] = ["{}"] * n
    return df


def _spec(fn, **kw):
    return GlueSpec(name=fn.__name__, fn=fn, **kw)


# ===========================================================================
# The column space is free
# ===========================================================================
class TestColumnSpaceIsFree:
    def test_drop_column(self):
        out = apply_glue_chain(
            _frame(), [_spec(glue_drop_baseline)], param="emg", schema_keys=SCHEMA
        )
        assert "baseline" not in out.columns
        assert "signal" in out.columns

    def test_rename_column(self):
        out = apply_glue_chain(
            _frame(), [_spec(glue_rename_signal)], param="emg", schema_keys=SCHEMA
        )
        assert "sig" in out.columns and "signal" not in out.columns

    def test_add_column(self):
        out = apply_glue_chain(
            _frame(), [_spec(glue_add_double)], param="emg", schema_keys=SCHEMA
        )
        assert out["double"].tolist() == [0.0, 2.0, 4.0]

    def test_retype_column(self):
        out = apply_glue_chain(
            _frame(), [_spec(glue_retype_signal)], param="emg", schema_keys=SCHEMA
        )
        assert out["signal"].tolist() == ["0.0", "1.0", "2.0"]

    def test_chain_of_two_applies_in_order(self):
        out = apply_glue_chain(
            _frame(),
            [_spec(glue_drop_baseline), _spec(glue_rename_signal)],
            param="emg",
            schema_keys=SCHEMA,
        )
        assert "baseline" not in out.columns
        assert "sig" in out.columns

    def test_empty_chain_is_identity(self):
        df = _frame()
        assert apply_glue_chain(df, [], param="emg") is df


# ===========================================================================
# Hidden bookkeeping columns
# ===========================================================================
class TestHiddenColumns:
    def test_record_id_survives_and_is_invisible_to_glue(self):
        seen = {}

        def glue_peek(emg):
            seen["cols"] = list(emg.columns)
            return emg

        out = apply_glue_chain(
            _frame(), [_spec(glue_peek)], param="emg", schema_keys=SCHEMA
        )
        # The glue never saw the internal columns...
        assert "__record_id" not in seen["cols"]
        assert "__branch_params" not in seen["cols"]
        # ...but they came back attached to the right rows.
        assert out["__record_id"].tolist() == ["rid0", "rid1", "rid2"]
        assert out["__branch_params"].tolist() == ["{}"] * 3

    def test_record_id_survives_a_column_drop(self):
        out = apply_glue_chain(
            _frame(), [_spec(glue_drop_baseline)], param="emg", schema_keys=SCHEMA
        )
        assert out["__record_id"].tolist() == ["rid0", "rid1", "rid2"]

    def test_glue_authored_internal_column_loses_to_bookkeeping(self):
        def glue_fake_rid(emg):
            out = emg.copy()
            out["__record_id"] = "spoofed"
            return out

        out = apply_glue_chain(
            _frame(), [_spec(glue_fake_rid)], param="emg", schema_keys=SCHEMA
        )
        assert out["__record_id"].tolist() == ["rid0", "rid1", "rid2"]

    def test_frame_without_hidden_columns_is_fine(self):
        out = apply_glue_chain(
            _frame(hidden=False),
            [_spec(glue_drop_baseline)],
            param="emg",
            schema_keys=SCHEMA,
        )
        assert list(out.columns) == ["subject", "signal"]


# ===========================================================================
# The row set is not free
# ===========================================================================
class TestRowPreservationContract:
    def test_dropping_rows_raises(self):
        with pytest.raises(GlueRowsChangedError) as exc:
            apply_glue_chain(
                _frame(), [_spec(glue_drop_rows)], param="emg", schema_keys=SCHEMA
            )
        msg = str(exc.value)
        assert "glue_drop_rows" in msg and "emg" in msg
        assert "3 row(s) in, 1 row(s) out" in msg

    def test_collapsing_to_a_scalar_raises(self):
        with pytest.raises(GlueRowsChangedError):
            apply_glue_chain(
                _frame(), [_spec(glue_collapse)], param="emg", schema_keys=SCHEMA
            )

    def test_reindexing_raises(self):
        with pytest.raises(GlueRowsChangedError):
            apply_glue_chain(
                _frame(), [_spec(glue_reindex)], param="emg", schema_keys=SCHEMA
            )

    def test_scalar_input_skips_the_row_contract(self):
        assert apply_glue_chain(4.0, [_spec(glue_scale)], param="v") == 40.0

    def test_array_input_skips_the_row_contract(self):
        out = apply_glue_chain(
            np.array([1.0, 2.0]), [_spec(glue_scale)], param="v"
        )
        assert out.tolist() == [10.0, 20.0]


# ===========================================================================
# Schema keys: visible but protected
# ===========================================================================
class TestSchemaKeyProtection:
    def test_schema_keys_are_visible_to_glue(self):
        seen = {}

        def glue_peek(emg):
            seen["cols"] = list(emg.columns)
            return emg

        apply_glue_chain(
            _frame(), [_spec(glue_peek)], param="emg", schema_keys=SCHEMA
        )
        assert "subject" in seen["cols"]

    def test_dropping_a_schema_key_raises(self):
        with pytest.raises(GlueSchemaKeysAlteredError) as exc:
            apply_glue_chain(
                _frame(), [_spec(glue_drop_subject)], param="emg", schema_keys=SCHEMA
            )
        assert "subject" in str(exc.value)

    def test_retyping_a_schema_key_raises(self):
        # int/str is the dangerous one: Step 5 stringifies schema values, so a
        # retyped key makes every combo filter miss and the run "succeeds"
        # having produced nothing.
        with pytest.raises(GlueSchemaKeysAlteredError) as exc:
            apply_glue_chain(
                _frame(), [_spec(glue_retype_subject)], param="emg", schema_keys=SCHEMA
            )
        assert "subject" in str(exc.value)

    def test_a_key_absent_from_the_table_is_not_required(self):
        # "session" is a schema key of the dataset but not a column of this
        # table (aggregation drops keys below the lowest iterated level).
        out = apply_glue_chain(
            _frame(), [_spec(glue_drop_baseline)], param="emg", schema_keys=SCHEMA
        )
        assert "session" not in out.columns


# ===========================================================================
# Chains, hashes and bindings
# ===========================================================================
class TestChains:
    def test_normalize_accepts_a_bare_callable(self):
        chains = normalize_glue({"emg": glue_drop_baseline})
        assert [s.name for s in chains["emg"]] == ["glue_drop_baseline"]

    def test_normalize_accepts_a_list(self):
        chains = normalize_glue({"emg": [glue_drop_baseline, glue_rename_signal]})
        assert len(chains["emg"]) == 2

    def test_chain_hash_is_order_sensitive(self):
        a, b = _spec(glue_drop_baseline), _spec(glue_rename_signal)
        assert chain_hash([a, b]) != chain_hash([b, a])

    def test_chain_hash_is_content_derived_not_name_derived(self):
        same_name_a = GlueSpec(name="glue_x", fn=glue_drop_baseline)
        same_name_b = GlueSpec(name="glue_x", fn=glue_rename_signal)
        assert same_name_a.hash != same_name_b.hash

    def test_mixed_language_chain_refused(self):
        with pytest.raises(GlueLanguageMismatchError):
            normalize_glue(
                {
                    "emg": [
                        _spec(glue_drop_baseline),
                        GlueSpec(
                            name="glue_m", language="matlab", source_text="function y=f(x)\ny=x;\nend"
                        ),
                    ]
                }
            )

    def test_matlab_glue_refused_in_a_python_run(self):
        chains = normalize_glue(
            {
                "emg": GlueSpec(
                    name="glue_m", language="matlab", source_text="function y=f(x)\ny=x;\nend"
                )
            }
        )
        with pytest.raises(GlueLanguageMismatchError):
            check_run_language(chains, "python")

    def test_bulk_glue_after_per_key_glue_refused(self):
        with pytest.raises(GlueChainOrderError):
            normalize_glue(
                {
                    "emg": [
                        _spec(glue_rename_signal, per_schema_key=True),
                        _spec(glue_drop_baseline),
                    ]
                }
            )

    def test_bulk_and_per_combo_split(self):
        chain = [
            _spec(glue_drop_baseline),
            _spec(glue_scale, per_schema_key=True),
        ]
        assert [s.name for s in bulk_chain(chain)] == ["glue_drop_baseline"]
        assert [s.name for s in per_combo_chain(chain)] == ["glue_scale"]

    def test_two_parameter_glue_binds_by_stated_binding_not_by_name(self):
        # The extra input is bound to the glue's own parameter name, and the
        # piped table lands on the first parameter — the glue's parameter
        # names match nothing in the consuming call.
        spec = GlueSpec(
            name="glue_combine", fn=glue_combine, extra_inputs={"offsets": 100.0}
        )
        out = apply_glue_chain(
            _frame(), [spec], param="not_called_emg", schema_keys=SCHEMA
        )
        assert out["shifted"].tolist() == [100.0, 101.0, 102.0]

    def test_two_parameter_glue_explicit_pipe_param(self):
        def glue_swapped(offsets, emg):
            out = emg.copy()
            out["shifted"] = out["signal"] + offsets
            return out

        spec = GlueSpec(
            name="glue_swapped",
            fn=glue_swapped,
            pipe_param="emg",
            extra_inputs={"offsets": 1.0},
        )
        out = apply_glue_chain(_frame(), [spec], param="emg", schema_keys=SCHEMA)
        assert out["shifted"].tolist() == [1.0, 2.0, 3.0]


# ===========================================================================
# Fusion through for_each
# ===========================================================================
def _seed_dataframe_variable(db):
    for subj in ("1", "2"):
        RawEMG.save(
            pd.DataFrame({"signal": [1.0, 2.0], "baseline": [0.5, 0.5]}),
            db=db,
            subject=subj,
            session="A",
        )


def _seed_scalar_variable(db):
    for i, subj in enumerate(("1", "2"), start=1):
        Force.save(float(i), db=db, subject=subj, session="A")
        Aux.save(float(i * 10), db=db, subject=subj, session="A")


class TestFusionThroughForEach:
    def test_dataframe_variable_reaches_glue_under_its_own_column_names(self, db):
        _seed_dataframe_variable(db)
        seen = []

        def glue_peek(emg):
            seen.append(list(emg.columns))
            return emg

        for_each(
            sum_signal,
            inputs={"emg": RawEMG},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"emg": _spec(glue_peek)},
            subject=[],
            session=[],
        )

        assert seen, "glue was never applied"
        # The user's own DataFrame column names, plus schema keys. No column
        # is named after the variable class.
        assert "signal" in seen[0] and "baseline" in seen[0]
        assert "RawEMG" not in seen[0]
        assert "subject" in seen[0]

    def test_scalar_variable_reaches_glue_as_one_column_named_after_the_class(
        self, db
    ):
        _seed_scalar_variable(db)
        seen = []

        def glue_peek(force):
            seen.append(list(force.columns))
            return force

        for_each(
            pass_force,
            inputs={"force": Force},
            outputs=[Analyzed],
            db=db,
            glue={"force": _spec(glue_peek)},
            subject=[],
            session=[],
        )

        assert seen, "glue was never applied"
        assert Force.view_name() in seen[0]

    def test_glue_changes_what_the_consuming_function_receives(self, db):
        _seed_dataframe_variable(db)
        received = []

        def analyze(emg):
            received.append(list(emg.columns))
            return float(np.sum(emg["signal"]))

        for_each(
            analyze,
            inputs={"emg": RawEMG},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"emg": _spec(glue_drop_baseline)},
            subject=[],
            session=[],
        )

        assert received, "consuming function was never called"
        assert all("baseline" not in cols for cols in received)

    def test_glue_output_is_never_saved(self, db):
        _seed_dataframe_variable(db)

        for_each(
            sum_signal,
            inputs={"emg": RawEMG},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"emg": _spec(glue_drop_baseline)},
            subject=[],
            session=[],
        )

        # Only the consumer's output landed; glue produced no variable.
        rows = db._duck._fetchall(
            "SELECT DISTINCT type FROM _record WHERE type IS NOT NULL"
        )
        types = {r[0] for r in rows}
        assert "Analyzed" in types
        assert not any(str(t).startswith("glue_") for t in types)

    def test_glue_on_a_merge_param(self, db):
        # A Merge has no single loaded table at the fusion point — it stays a
        # set of constituent frames until scifor joins them per combo — so its
        # chain is applied post-slice, on the merged frame.
        _seed_scalar_variable(db)
        seen = []

        def glue_peek(merged):
            seen.append(list(merged.columns))
            return merged

        for_each(
            sum_numeric,
            inputs={"merged": Merge(Force, Aux)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"merged": _spec(glue_peek)},
            subject=[],
            session=[],
        )

        assert seen, "glue was never applied to the Merge input"
        assert "Force" in seen[0] and "Aux" in seen[0]

    def test_glue_on_a_merge_param_warns_that_identity_is_untracked(self, db, caplog):
        # The known gap, asserted so it can't quietly become silent: a Merge's
        # constituents are loaded WITHOUT record ids, so there is no rid to
        # route through a virtual glue record. The glue still reshapes the
        # data; editing it will not invalidate downstream results. Anything
        # that makes this warning stop firing has either fixed the gap (update
        # this test) or hidden it (a regression).
        _seed_scalar_variable(db)

        with caplog.at_level(logging.WARNING, logger="scidb"):
            for_each(
                sum_numeric,
                inputs={"merged": Merge(Force, Aux)},
                outputs=[Analyzed],
                db=db,
                as_table=True,
                glue={"merged": _spec(glue_scale)},
                subject=[],
                session=[],
            )

        assert any(
            "cannot be recorded in the provenance graph" in r.message
            for r in caplog.records
        ), "the Merge identity gap was not reported"

    def test_glue_on_a_pathinput_param_is_refused(self, db):
        with pytest.raises(GlueUnsupportedInputError) as exc:
            for_each(
                pass_raw,
                inputs={"raw": PathInput("{subject}/{session}.csv", name="{subject}/{session}.csv")},
                outputs=[Analyzed],
                db=db,
                glue={"raw": _spec(glue_drop_baseline)},
                subject=["1"],
                session=["A"],
            )
        assert "PathInput" in str(exc.value)

    def test_row_changing_glue_fails_the_run(self, db):
        _seed_dataframe_variable(db)
        with pytest.raises(GlueRowsChangedError):
            for_each(
                sum_signal,
                inputs={"emg": RawEMG},
                outputs=[Analyzed],
                db=db,
                as_table=True,
                glue={"emg": _spec(glue_drop_rows)},
                subject=[],
                session=[],
            )

    def test_per_schema_key_glue_runs_on_the_sliced_value(self, db):
        _seed_scalar_variable(db)
        seen = []

        def glue_double(force):
            seen.append(force)
            return force * 2

        results = for_each(
            pass_force,
            inputs={"force": Force},
            outputs=[Analyzed],
            db=db,
            glue={"force": _spec(glue_double, per_schema_key=True)},
            subject=[],
            session=[],
        )

        # One call per combo, each with the already-sliced scalar — not the
        # whole two-row table.
        assert len(seen) == 2
        assert all(np.isscalar(v) or np.ndim(v) == 0 for v in seen)
        assert sorted(float(r) for r in results["Analyzed"]) == [2.0, 4.0]

    def test_glue_for_an_unknown_param_is_a_warning_not_a_crash(self, db):
        _seed_dataframe_variable(db)
        result = for_each(
            sum_signal,
            inputs={"emg": RawEMG},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"nope": _spec(glue_drop_baseline)},
            subject=[],
            session=[],
        )
        assert result is not None and not result.empty


# ===========================================================================
# Glue on a constant-fed parameter (the Parameter case)
# ===========================================================================
def scale_signal(emg, factor):
    return float(np.sum(emg["signal"]) * factor)


def glue_double_factor(factor):
    return factor * 2


def glue_pick_high(config):
    return config["high"]


class TestConstantGlue:
    """A ``Parameter``-fed parameter is glued in prepare, before the version
    keys are built, so the GLUED value is what lands in ``__constants``.

    That placement is the whole point: it makes an edited glue body invalidate
    downstream results through ``skip_computed``'s ordinary binding compare,
    with no virtual glue record — see ``apply_constant_glue``.
    """

    def test_a_single_valued_parameter_is_glued(self, db):
        _seed_dataframe_variable(db)
        results = for_each(
            scale_signal,
            inputs={"emg": RawEMG, "factor": Parameter(3)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"factor": _spec(glue_double_factor)},
            subject=[],
            session=[],
        )
        # sum(signal) = 3.0 per subject, factor 3 doubled to 6 -> 18.0
        assert sorted(float(r) for r in results["Analyzed"]) == [18.0, 18.0]

    def test_a_dict_valued_parameter_is_restructured(self, db):
        _seed_dataframe_variable(db)
        results = for_each(
            scale_signal,
            inputs={
                "emg": RawEMG,
                "factor": Parameter({"low": 1, "high": 10}),
            },
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"factor": _spec(glue_pick_high)},
            subject=[],
            session=[],
        )
        assert sorted(float(r) for r in results["Analyzed"]) == [30.0, 30.0]

    def test_every_value_of_a_multi_valued_parameter_is_glued(self, db):
        """The fan-out and the glue compose without either knowing about the
        other: ``for_each``'s Step 1 expands the EachOf into one recursive
        call per value, and each call glues its own concrete value."""
        _seed_dataframe_variable(db)
        results = for_each(
            scale_signal,
            inputs={"emg": RawEMG, "factor": Parameter(1, 2)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"factor": _spec(glue_double_factor)},
            subject=[],
            session=[],
        )
        # factors 1 and 2 doubled to 2 and 4; sum(signal) = 3.0 per subject.
        assert sorted(float(r) for r in results["Analyzed"]) == [6.0, 6.0, 12.0, 12.0]

    def test_editing_a_constant_glue_body_recomputes(self, db):
        """The reason for the placement. ``__constants`` holds the POST-glue
        value, so an edited body changes a binding ``skip_computed`` compares
        and the consumer recomputes — with no virtual glue record involved.

        If the raw Parameter value were recorded instead, the edit would leave
        every downstream record green and stale (§2)."""

        def glue_triple_factor(factor):
            return factor * 3

        _seed_dataframe_variable(db)
        first = for_each(
            scale_signal,
            inputs={"emg": RawEMG, "factor": Parameter(3)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            skip_computed=True,
            glue={"factor": GlueSpec(name="glue_f", fn=glue_double_factor)},
            subject=[],
            session=[],
        )
        assert sorted(float(r) for r in first["Analyzed"]) == [18.0, 18.0]

        # Same node NAME, edited body: 3 -> 9 instead of 3 -> 6.
        second = for_each(
            scale_signal,
            inputs={"emg": RawEMG, "factor": Parameter(3)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            skip_computed=True,
            glue={"factor": GlueSpec(name="glue_f", fn=glue_triple_factor)},
            subject=[],
            session=[],
        )
        assert sorted(float(r) for r in second["Analyzed"]) == [27.0, 27.0], (
            "the edited constant glue did not recompute"
        )

    def test_matlab_glue_on_a_constant_is_refused(self, db):
        """The inverted language rule. A constant-fed chain runs in prepare —
        Python on both run paths — so MATLAB glue cannot be applied there, and
        moving it to a per-combo site would record the pre-glue constant."""
        _seed_dataframe_variable(db)
        with pytest.raises(GlueLanguageMismatchError, match="Parameter"):
            for_each(
                scale_signal,
                inputs={"emg": RawEMG, "factor": Parameter(3)},
                outputs=[Analyzed],
                db=db,
                as_table=True,
                glue={
                    "factor": GlueSpec(
                        name="glue_m",
                        language="matlab",
                        source_text="function y=f(x)\ny=x*2;\nend",
                    )
                },
                subject=[],
                session=[],
            )

    def test_a_multi_type_variable_input_is_not_treated_as_a_constant(self):
        """An ``EachOf`` of variable CLASSES is a multi-type input, not a
        Parameter. Its data is loaded, so its glue belongs at the fusion point
        — the constant predicates must not claim it."""
        from scidb.glue import is_constant_axis, is_constant_input

        # Plain values: constants.
        assert is_constant_input(7)
        assert is_constant_input({"low": 1})
        assert is_constant_input(None)

        # Variable classes and multi-type inputs: not constants, either way.
        assert not is_constant_input(RawEMG)
        assert not is_constant_input(EachOf(RawEMG, Force))
        assert not is_constant_axis(EachOf(RawEMG, Force))

        # An unexpanded Parameter is an axis, not a value — for_each's Step 1
        # normally expands it before prepare, but the MATLAB bridge does not.
        assert not is_constant_input(Parameter(3))
        assert is_constant_axis(Parameter(3))
        assert is_constant_axis(Parameter(1, 2))

    def test_an_unexpanded_parameter_axis_is_glued_per_alternative(self):
        """The MATLAB bridge calls prepare directly, with no Step 1 in front
        of it, so an unexpanded Parameter can arrive. Gluing the EachOf object
        would hand the user's code the axis instead of a value."""
        from scidb.glue import apply_constant_glue, split_constant_chains

        inputs = {"factor": Parameter(1, 2)}
        chains = normalize_glue({"factor": _spec(glue_double_factor)})
        constant_chains, deferred = split_constant_chains(inputs, chains)

        assert set(constant_chains) == {"factor"}
        assert deferred == {}

        apply_constant_glue(inputs, constant_chains)
        assert isinstance(inputs["factor"], EachOf)
        assert list(inputs["factor"].alternatives) == [2, 4]


class TestGlueSurvivesEachOfExpansion:
    def test_glue_is_not_dropped_by_an_eachof_input(self, db):
        """``for_each``'s Step 1 recursion used to omit ``glue=``, so every
        alternative of an EachOf input ran with the glue silently dropped —
        nothing reshaped, no virtual record, and a successful-looking run."""
        _seed_dataframe_variable(db)
        seen = []

        def glue_peek(emg):
            seen.append(len(emg))
            return emg

        for_each(
            scale_signal,
            inputs={"emg": RawEMG, "factor": Parameter(1, 2)},
            outputs=[Analyzed],
            db=db,
            as_table=True,
            glue={"emg": _spec(glue_peek)},
            subject=[],
            session=[],
        )
        # Two alternatives of `factor` -> two recursive calls, each of which
        # must have applied the chain to the loaded emg table.
        assert len(seen) == 2
