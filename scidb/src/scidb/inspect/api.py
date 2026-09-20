"""Inspector — the read-only observability facade over a scidb database.

Every method returns plain dataclasses (JSON-serializable via
``dataclasses.asdict``) so the CLI, GUI, and MATLAB bridge all consume the
same shapes. Nothing here computes new state: each method is a thin shaping
layer over the core tables and the provenance_query primitives
(question → primitive mapping in docs/claude/observability-api-design.md).
"""

from __future__ import annotations

import functools
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ..exceptions import AmbiguousVersionError, NotFoundError
from ..log import Log

if TYPE_CHECKING:
    from ..database import DatabaseManager

# Internal (non-variable) entity types in _record, excluded from user-facing
# record counts everywhere. ``__glue__`` is a virtual provenance node for a
# glue chain's never-saved output — it has no data, no _record_save row and
# nothing loadable, so counting it would inflate every record total.
_INTERNAL_RECORD_TYPES = ("__constant__", "__pathinput__", "__glue__")
_NOT_INTERNAL_SQL = (
    "type NOT IN (" + ", ".join(f"'{t}'" for t in _INTERNAL_RECORD_TYPES) + ")"
)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class DbOverview:
    db_path: str
    db_size_bytes: int
    schema_keys: list[str]
    n_schema_locations: int
    n_variables: int
    n_records: int  # variable records only (constants/pathinputs excluded)
    n_excluded_records: int
    n_invocations: int  # real function calls (synthetic __save__ anchors excluded)
    n_runs: int
    last_save: str | None  # ISO timestamp of newest _record_save event
    last_run: str | None  # ISO timestamp of newest _run row


@dataclass
class VariableSummary:
    name: str
    schema_level: str | None
    dtype: str | None
    description: str
    record_count: int  # distinct non-excluded record_ids of this type
    excluded_count: int
    variant_count: int  # distinct (fn, constants, output) variants producing it
    last_saved: str | None


@dataclass
class VariableDetail(VariableSummary):
    data_columns: list[str] = field(default_factory=list)
    records_by_level: dict[str, int] = field(default_factory=dict)


@dataclass
class SchemaNode:
    key: str  # schema key name at this depth (e.g. "subject")
    value: str  # the key's value (e.g. "S01")
    schema_level: str | None  # set when this node is a realized _schema row
    schema_id: int | None
    record_count: int  # records at exactly this location
    children: list[SchemaNode] = field(default_factory=list)


@dataclass
class SchemaTree:
    schema_keys: list[str]
    roots: list[SchemaNode] = field(default_factory=list)


@dataclass
class RunRecord:
    timestamp: str
    user_id: str | None
    function_name: str
    where_clause: str | None  # display-only by design — never parsed
    run_id: str | None = None  # set by runs(); audit rows have no run_id
    n_invocations: int | None = None
    # Which surfaces the run read — gui / script / replay (rule 3 of
    # docs/claude/intent-and-fact.md); None on rows older than the column.
    origin: str | None = None


@dataclass
class RunRef:
    """One execution that produced a trace node's record.

    A record can have several: a re-run of the same recipe reuses its
    invocation and appends a ``_run`` row, so "which run produced this?" is a
    list. (A re-run under edited code or different run options is a different
    invocation AND a different record — the save metadata is part of the
    record_id — so it shows up as another record, not as another run here.)
    The per-run ``invocation_id`` / ``function_hash`` / ``run_options`` are
    carried anyway, so a second producer written by any other path is still
    told apart in place. This is the level ``trace`` could not reach before,
    and the reason "which run produced the records I am looking at?" needed
    hand-written SQL.
    """

    run_id: str
    timestamp: str
    user_id: str | None
    where_clause: str | None  # display-only by design — never parsed
    invocation_id: str
    function_hash: str | None
    run_options: str | None  # provenance_query.run_options_label


@dataclass
class TraceInput:
    param: str
    record_id: str
    variable: str


@dataclass
class TraceEdge:
    from_record_id: str
    to_record_id: str
    param: str


@dataclass
class TraceNode:
    record_id: str
    variable: str
    schema: dict[str, str]
    depth: int
    function_name: str | None  # None = raw / direct save
    function_hash: (
        str | None
    )  # surfaced deliberately: old-hash lineage rows are visible
    constants: dict[str, str]
    path_inputs: dict[str, str]  # param → spec string
    inputs: list[TraceInput]
    saved: str | None
    saved_by: str | None
    run_count: int
    last_run: str | None
    # --- down to the run (Stage 2) --------------------------------------
    #: The producing invocation ``function_hash`` above belongs to — the
    #: lowest-id producer, so the two always describe the same call.
    invocation_id: str | None = None
    #: EVERY producing invocation, ascending. The for_each save path writes
    #: exactly one (a non-identical re-run is a new record, not a second
    #: producer), so longer than one means another writer put it there;
    #: ``runs`` spans all of them either way.
    invocation_ids: list[str] = field(default_factory=list)
    #: The for_each call site (``provenance_query.config_call_id``). None for a
    #: raw save, a glue hop, and the synthetic ``__save__`` anchor.
    call_id: str | None = None
    #: ``distribute=…[, as_table=[…]]`` for ``invocation_id`` — the third
    #: variant axis, and the one that is invisible in constants and code.
    run_options: str | None = None
    #: The producing function's per-function version ordinal ("v1"/"v2"/…) for
    #: ``function_hash``. None when that function has only ever had one
    #: version: ``code_version_ordinals`` omits those on purpose (a single
    #: version is not a choice), and inventing "v1" here would imply one.
    code_version: str | None = None
    #: Every run that produced this record, oldest first. Populated only with
    #: ``include_runs=True`` — it is an extra join, and most trace callers are
    #: asking about the recipe rather than the executions.
    runs: list[RunRef] = field(default_factory=list)


@dataclass
class ProvenanceTree:
    root_record_id: str
    nodes: list[TraceNode]
    edges: list[TraceEdge]
    audit: list[RunRecord] = field(default_factory=list)
    #: The canonical pin this tree was resolved from (empty for a record_id or
    #: plain-metadata lookup). Echoed back so a JSON consumer can show what was
    #: selected without re-deriving it — and so a pin typed in the display
    #: dialect (``Code:fn=v2``) reports the ``__code__.fn`` it became.
    selection: dict[str, str] = field(default_factory=dict)
    #: Every record the pin matched, when more than the traced root did. A pin
    #: usually names one record per schema location; ``trace`` roots at one, and
    #: this says what else it named rather than leaving the other locations
    #: silently untraced.
    matched_record_ids: list[str] = field(default_factory=list)


@dataclass
class NodeStateSummary:
    function_name: str
    state: str  # green | red | unknown
    state_basis: str  # live_fn | stored_hash | discovery | none
    up_to_date: int
    missing: int
    missing_combos: list[dict] = field(default_factory=list)
    constants: dict | None = None  # set by pathinput_state (per-config results)


@dataclass
class RecordSummary:
    record_id: str
    variable: str
    schema: dict[str, str]
    timestamp: str
    user_id: str | None
    content_hash: str
    schema_version: int
    excluded: bool
    value_preview: str | None = None  # set by records(include_values=True)


@dataclass
class SqlResult:
    columns: list[str]
    rows: list[list]
    row_count: int


@dataclass
class PickCandidate:
    """One selectable record in `pick`: identity + everything a human needs
    to tell coexisting variants apart. Selection only — never data."""

    record_id: str
    variable: str
    schema: dict[str, str]
    branch_params: dict[str, str]  # namespaced fn.param → display string
    function_name: str | None  # producing function (None = raw save)
    saved: str | None


@dataclass
class ExclusionRecord:
    schema: dict[str, str]  # only the keys the exclusion names (rest = wildcard)
    reason: str
    changed_at: str
    changed_by: str | None


# ---------------------------------------------------------------------------
# Timing / logging decorator (NOTE 2: observe internals on every call)
# ---------------------------------------------------------------------------


def _timed(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = method(self, *args, **kwargs)
        ms = (time.perf_counter() - t0) * 1000.0
        n = len(result) if isinstance(result, list) else 1
        Log.debug(f"inspect.{method.__name__}: {ms:.1f} ms, {n} result(s)")
        return result

    return wrapper


def _iso(ts) -> str | None:
    """Timestamp (datetime / pd.Timestamp / None / NaT) → ISO string or None."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or ts is pd.NaT:
        return None
    try:
        if pd.isna(ts):
            return None
    except (TypeError, ValueError):
        pass
    return ts.isoformat(sep=" ") if hasattr(ts, "isoformat") else str(ts)


class Inspector:
    """Read-side facade over a DatabaseManager.

    Constructed from a live DatabaseManager (``db.inspect``) or standalone
    via ``Inspector.open(path)``, which discovers the schema keys from the
    database itself and opens strictly read-only.
    """

    def __init__(self, db: DatabaseManager, _owns_db: bool = False):
        self._db = db
        self._owns_db = _owns_db

    @classmethod
    def open(cls, db_path: str | Path) -> Inspector:
        """Open an existing database read-only, discovering its schema keys.

        A writer in another process holds an exclusive file lock (even
        against read-only opens), so lock contention is mapped to
        DatabaseLockedError with a plain-language message.
        """
        from sciduckdb import schema_keys_from_db

        from ..database import DatabaseManager
        from .mutate import lock_errors_mapped

        with lock_errors_mapped(db_path):
            keys = schema_keys_from_db(db_path)
            db = DatabaseManager(db_path, keys, read_only=True)
        Log.info(f"Inspector.open: {db_path} (read-only, schema_keys={keys})")
        return cls(db, _owns_db=True)

    def close(self) -> None:
        """Close the underlying connection if this Inspector opened it."""
        if self._owns_db:
            self._db._duck.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- internal helpers ---------------------------------------------------

    @property
    def _duck(self):
        return self._db._duck

    def _scalar(self, sql: str, params=None, default=0):
        """One-value query; returns default if a referenced table is missing."""
        try:
            rows = self._duck._fetchall(sql, params)
        except Exception as e:  # missing table on a pre-provenance database
            Log.debug(f"inspect scalar query failed ({e}); returning {default!r}")
            return default
        val = rows[0][0] if rows else None
        return default if val is None else val

    def _variant_counts(self) -> dict[str, int]:
        """output_type → number of distinct pipeline variants (single batch query)."""
        if not self._duck._table_exists("_invocation"):
            return {}
        counts: dict[str, int] = {}
        for entry in self._db.list_pipeline_variants():
            out = entry.get("output_type")
            if out:
                counts[out] = counts.get(out, 0) + 1
        return counts

    # -- facade methods -----------------------------------------------------

    @_timed
    def overview(self) -> DbOverview:
        db_path = str(self._db.dataset_db_path)
        try:
            size = Path(db_path).stat().st_size
        except OSError:
            size = 0
        not_internal = _NOT_INTERNAL_SQL
        return DbOverview(
            db_path=db_path,
            db_size_bytes=int(size),
            schema_keys=list(self._db.dataset_schema_keys),
            n_schema_locations=int(self._scalar("SELECT COUNT(*) FROM _schema")),
            n_variables=int(self._scalar("SELECT COUNT(*) FROM _variables")),
            n_records=int(
                self._scalar(f"SELECT COUNT(*) FROM _record WHERE {not_internal}")
            ),
            n_excluded_records=int(
                self._scalar(
                    f"SELECT COUNT(*) FROM _record WHERE {not_internal} "
                    f"AND COALESCE(excluded, FALSE)"
                )
            ),
            n_invocations=int(
                self._scalar(
                    "SELECT COUNT(*) FROM _invocation WHERE function_name <> '__save__'"
                )
            ),
            n_runs=int(self._scalar("SELECT COUNT(*) FROM _run")),
            last_save=_iso(
                self._scalar("SELECT MAX(timestamp) FROM _record_save", default=None)
            ),
            last_run=_iso(
                self._scalar("SELECT MAX(timestamp) FROM _run", default=None)
            ),
        )

    @_timed
    def variables(self) -> list[VariableSummary]:
        rows = self._duck._fetchall(
            "SELECT v.variable_name, v.schema_level, v.dtype, v.description, "
            "COUNT(DISTINCT CASE WHEN NOT COALESCE(r.excluded, FALSE) "
            "                    THEN r.record_id END) AS record_count, "
            "COUNT(DISTINCT CASE WHEN COALESCE(r.excluded, FALSE) "
            "                    THEN r.record_id END) AS excluded_count, "
            "MAX(rs.timestamp) AS last_saved "
            "FROM _variables v "
            "LEFT JOIN _record r ON r.type = v.variable_name "
            "LEFT JOIN _record_save rs ON rs.record_id = r.record_id "
            "GROUP BY v.variable_name, v.schema_level, v.dtype, v.description "
            "ORDER BY v.variable_name"
        )
        variant_counts = self._variant_counts()
        return [
            VariableSummary(
                name=name,
                schema_level=level,
                dtype=dtype,
                description=desc or "",
                record_count=int(n),
                excluded_count=int(n_ex),
                variant_count=variant_counts.get(name, 0),
                last_saved=_iso(last),
            )
            for name, level, dtype, desc, n, n_ex, last in rows
        ]

    @_timed
    def variable(self, name: str) -> VariableDetail:
        name = getattr(name, "__name__", name)
        summary = next((v for v in self.variables() if v.name == name), None)
        if summary is None:
            raise NotFoundError(
                f"Variable type {name!r} is not registered in this database"
            )

        # Data table columns (table name from _registered_types, fallback: name)
        table_name = self._scalar(
            "SELECT table_name FROM _registered_types WHERE type_name = ?",
            [name],
            default=name,
        )
        col_rows = self._duck._fetchall(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
        )

        level_rows = self._duck._fetchall(
            "SELECT s.schema_level, COUNT(DISTINCT r.record_id) "
            "FROM _record r JOIN _schema s ON r.schema_id = s.schema_id "
            "WHERE r.type = ? AND NOT COALESCE(r.excluded, FALSE) "
            "GROUP BY s.schema_level",
            [name],
        )

        return VariableDetail(
            **summary.__dict__,
            data_columns=[r[0] for r in col_rows],
            records_by_level={level: int(n) for level, n in level_rows},
        )

    @_timed
    def schema_tree(self) -> SchemaTree:
        keys = list(self._db.dataset_schema_keys)
        tree = SchemaTree(schema_keys=keys)
        if not self._duck._table_exists("_schema"):
            return tree

        key_cols = ", ".join(f'"{k}"' for k in keys)
        rows = self._duck._fetchall(
            f"SELECT schema_id, schema_level, {key_cols} FROM _schema"
        )
        counts = {}
        if self._duck._table_exists("_record"):
            counts = dict(
                self._duck._fetchall(
                    f"SELECT schema_id, COUNT(*) FROM _record "
                    f"WHERE {_NOT_INTERNAL_SQL} "
                    f"AND NOT COALESCE(excluded, FALSE) AND schema_id IS NOT NULL "
                    f"GROUP BY schema_id"
                )
            )

        # Build the hierarchy: each _schema row is a node at its deepest
        # non-NULL key; ancestors are synthesized when no _schema row exists
        # for them (schema_id=None).
        nodes: dict[tuple, SchemaNode] = {}

        def node_at(path: tuple) -> SchemaNode:
            if path in nodes:
                return nodes[path]
            key, value = path[-1]
            node = SchemaNode(
                key=key, value=value, schema_level=None, schema_id=None, record_count=0
            )
            nodes[path] = node
            if len(path) == 1:
                tree.roots.append(node)
            else:
                node_at(path[:-1]).children.append(node)
            return node

        for schema_id, schema_level, *values in rows:
            path = tuple(
                (k, str(v)) for k, v in zip(keys, values, strict=False) if v is not None
            )
            if not path:
                continue
            node = node_at(path)
            node.schema_id = int(schema_id)
            node.schema_level = schema_level
            node.record_count = int(counts.get(schema_id, 0))

        def sort_rec(children: list[SchemaNode]):
            children.sort(key=lambda n: (n.key, n.value))
            for c in children:
                sort_rec(c.children)

        sort_rec(tree.roots)
        return tree

    @_timed
    def pipeline(self, output_type: str | None = None, fn_registry: dict | None = None):
        """The type-level pipeline DAG (see graph.PipelineGraph).

        output_type: restrict to that variable and everything upstream of it.
        fn_registry: optional {fn_name: callable} — with the live functions,
            node state detects source edits (state_basis="live_fn"); without,
            state uses the most recently run stored hash ("stored_hash").
        """
        from .graph import build_pipeline_graph

        output_type = getattr(output_type, "__name__", output_type)
        return build_pipeline_graph(
            self._db,
            output_type=output_type,
            fn_registry=fn_registry,
        )

    @_timed
    def variants(self, name):
        """Coexisting variants of a variable type or of a function.

        ``name`` is matched as an output type first, then as a function name.
        A registered variable with no producing pipeline steps returns [].
        """
        from .graph import VariantSummary, _value_str

        name = getattr(name, "__name__", name)
        raw = self._db.list_pipeline_variants()

        def to_summary(v) -> VariantSummary:
            return VariantSummary(
                function_name=v["function_name"],
                call_id=v["call_id"],
                output_type=v["output_type"],
                output_num=v["output_num"],
                input_types=dict(v["input_types"]),
                constants={k: _value_str(val) for k, val in v["constants"].items()},
                record_count=int(v["record_count"]),
                run_options=v.get("run_options"),
            )

        matches = [v for v in raw if v["output_type"] == name]
        if not matches:
            matches = [v for v in raw if v["function_name"] == name]
        if not matches:
            known_type = self._scalar(
                "SELECT 1 FROM _variables WHERE variable_name = ?",
                [name],
                default=None,
            )
            if known_type is None:
                raise NotFoundError(
                    f"{name!r} is neither a variable type nor a recorded "
                    f"pipeline function in this database"
                )
            return []  # real variable, just no producing pipeline steps (raw saves)
        out = [to_summary(v) for v in matches]
        out.sort(
            key=lambda s: (
                s.output_type,
                s.function_name,
                s.output_num if s.output_num is not None else -1,
                sorted(s.constants.items()),
                s.run_options or "",
            )
        )
        n_run_sets = len({s.run_options for s in out if s.run_options})
        if n_run_sets > 1:
            Log.info(
                f"variants({name}): {len(out)} variant(s) across {n_run_sets} "
                f"run-option set(s) — rows identical but for the 'run options' "
                f"column are distinct variants, not duplicates"
            )
        return out

    def _resolve_pin(
        self, variable, record_id, metadata, selection=None
    ) -> tuple[str, list[str], dict]:
        """Resolve a variant pin (or an explicit record_id) to a root record.

        Returns ``(root_record_id, matched_record_ids, canonical_pin)``.

        Resolution is :func:`provenance_query.records_for_variant` and nothing
        beside it, so "trace the variant I pinned" and "load the variant I
        pinned" cannot name different records.

        Several matches mean two different things, and they are handled
        differently on purpose:

        * **without** an explicit ``selection`` — under-specified schema keys
          or coexisting variants — it raises with the candidates listed, which
          is what this method has always done and what the caller needs to fix;
        * **with** one, it is the normal shape of the answer: a variant spans
          schema locations, and ``--variant Code:grSides=v2`` with no
          ``subject=`` is the headline case rather than a mistake. It roots at
          the most recently saved match and returns every one.
        """
        from .. import provenance_query
        from ..variant import normalize_selection

        pins = normalize_selection(selection or {})
        if record_id:
            rows = self._duck._fetchall(
                "SELECT type FROM _record WHERE record_id = ?", [record_id]
            )
            if not rows:
                raise NotFoundError(f"Record {record_id!r} not found")
            if pins:
                # A pin alongside an explicit record_id is a contradiction, not
                # a narrowing: the record is already named, so the pin can only
                # agree or be ignored, and silently ignoring it would report
                # provenance for a variant the user did not ask about.
                raise ValueError(
                    f"--record-id names one record; a variant pin ({pins}) "
                    f"selects among several. Pass one or the other."
                )
            return record_id, [record_id], {}
        if variable is None:
            raise ValueError("Provide a variable type (plus metadata) or a record_id")

        type_name = getattr(variable, "__name__", variable)
        rids = provenance_query.records_for_variant(
            self._db, type_name, pins, **metadata
        )
        # What the pin became, including any non-schema kwarg folded into it —
        # echoed back so a caller can show the selection it actually resolved.
        nested = self._db._split_metadata(metadata)
        canonical = {
            **normalize_selection(nested.get("version") or {}),
            **pins,
        }
        if not rids:
            raise NotFoundError(
                f"No {type_name} record matches {metadata or '(any location)'}"
                + (f" with variant pin {canonical}" if canonical else "")
            )
        # NB: `pins`, not `canonical`. A branch param passed as a plain kwarg
        # (`trace Filtered low_hz=20`) is a *filter* on an otherwise
        # under-specified lookup and keeps raising as it always has; only an
        # explicit `selection=` / `--variant` says "I mean the variant, across
        # locations". Conflating the two would silently turn every existing
        # ambiguity error into a quietly-chosen record.
        if len(rids) > 1 and not pins:
            # No pin: ambiguity is under-specification, and the historical
            # behaviour is to refuse rather than pick. Unchanged.
            bp = provenance_query.branch_params_batch(self._duck, rids)
            chains = provenance_query.chain_batch(self._duck, rids)
            cands = [
                self._candidate_label(rid, bp, chains) for rid in rids[:5]
            ]
            raise AmbiguousVersionError(
                f"{len(rids)} {type_name} records match {metadata}: "
                + "; ".join(cands)
                + (f" (+{len(rids) - 5} more)" if len(rids) > 5 else "")
                + ". Narrow with schema keys / branch params, or pass record_id."
            )
        if len(rids) > 1:
            # A PIN names a variant, and a variant spans schema locations by
            # design — `--variant Code:grSides=v2` with no subject= is the
            # headline case, not a mistake. Refusing it would make the pin
            # useless exactly where it is most useful, so root at the most
            # recently saved match and say, in the result and in the log, that
            # there were others: `matched_record_ids` carries every one, so a
            # caller that wants the rest never has to re-derive the pin.
            saved = provenance_query.saved_at_batch(self._duck, rids)
            rids = sorted(rids, key=lambda r: (saved.get(r) or "", r), reverse=True)
            Log.info(
                f"{type_name} variant pin {canonical} matches {len(rids)} "
                f"record(s); tracing the most recently saved ({rids[0][:8]}…). "
                f"Narrow with schema keys to trace another."
            )
        return rids[0], rids, canonical

    def _candidate_label(self, rid: str, bp: dict, chains: dict) -> str:
        """One line describing a record in an ambiguity message: what tells it
        apart from the others (schema location, constants, code, run options)."""
        from .. import provenance_query

        node = (
            provenance_query._fetch_record_node(
                self._duck, rid, self._db.dataset_schema_keys,
                restore=self._db.restore_schema_value,
            )
            or {}
        )
        schema = " ".join(f"{k}={v}" for k, v in (node.get("schema") or {}).items())
        chain = chains.get(rid) or {}
        return (
            f"{rid[:8]}… ({schema}; {bp.get(rid, {})}"
            + (f"; code={chain.get('code')}" if chain.get("code") else "")
            + (f"; run={chain.get('run')}" if chain.get("run") else "")
            + ")"
        )

    def _resolve_record_id(self, variable, record_id, metadata) -> str:
        """Resolve (variable + metadata) or an explicit record_id to exactly
        one record — :meth:`_resolve_pin` without the pin."""
        return self._resolve_pin(variable, record_id, metadata)[0]

    @_timed
    def trace(
        self,
        variable=None,
        record_id: str | None = None,
        include_audit: bool = False,
        selection: dict | None = None,
        include_runs: bool = False,
        **metadata,
    ) -> ProvenanceTree:
        """Full upstream provenance of one record (provenance_query.pipeline).

        Resolve by variable + metadata (schema keys and branch params), by a
        variant ``selection`` (the dict a ``Variant`` carries, or the display
        spelling the plot picker uses), or pass record_id directly.
        ``include_audit`` appends the execution_audit rows for the root record;
        ``include_runs`` takes every node down to the runs that produced it.
        """
        from .. import provenance_query
        from .graph import _value_str

        duck = self._duck
        rid, matched, pins = self._resolve_pin(
            variable, record_id, metadata, selection
        )
        pipe = provenance_query.pipeline(self._db, rid)
        rids = [n["record_id"] for n in pipe["nodes"]]

        # Batch the per-node enrichments (never per-record loops — N+1 rule).
        # `all_producing` keeps every producer of every record; `producing`
        # is the one that owns `function_hash`, and the two agree by
        # construction (producing_invocation_batch is the plural's first item).
        all_producing = provenance_query.producing_invocations_batch(duck, rids)
        producing = {
            nrid: producers[0] for nrid, producers in all_producing.items()
        }
        saves = self._latest_saves_batch(rids)
        run_info = self._run_info_batch(rids)
        path_specs = {
            inv_id: provenance_query.invocation_path_inputs(duck, inv_id)
            for inv_id in {p[0] for p in producing.values()}
        }

        # Down-to-the-run enrichments, all keyed by invocation and all batched
        # over the WHOLE tree at once — the N+1 trap this codebase has hit
        # before (docs/claude memory: batched-provenance-hot-paths).
        every_inv = [inv[0] for producers in all_producing.values() for inv in producers]
        call_ids = provenance_query.invocation_call_ids_batch(duck, every_inv)
        run_opts = provenance_query.invocation_run_options_batch(duck, every_inv)
        ordinals = provenance_query.code_version_ordinals(
            duck, {inv[1] for producers in all_producing.values() for inv in producers}
        )
        inv_runs = (
            provenance_query.runs_for_invocations_batch(duck, every_inv)
            if include_runs
            else {}
        )

        nodes = []
        for n in pipe["nodes"]:
            nrid = n["record_id"]
            producers = all_producing.get(nrid, [])
            inv = producing.get(nrid)
            saved_ts, saved_by = saves.get(nrid, (None, None))
            n_runs, last_run = run_info.get(nrid, (0, None))
            runs: list[RunRef] = []
            for inv_id, _fn_name, fn_hash in producers:
                for run_id, ts, uid, where in inv_runs.get(inv_id, ()):
                    runs.append(
                        RunRef(
                            run_id=run_id,
                            timestamp=_iso(ts) or "",
                            user_id=uid,
                            where_clause=where,
                            invocation_id=inv_id,
                            function_hash=fn_hash,
                            run_options=run_opts.get(inv_id),
                        )
                    )
            runs.sort(key=lambda r: (r.timestamp, r.run_id))
            nodes.append(
                TraceNode(
                    record_id=nrid,
                    variable=n["variable_type"],
                    schema={k: str(v) for k, v in n["schema"].items()},
                    depth=int(n["depth"]),
                    function_name=n["function_name"],
                    function_hash=inv[2] if inv else None,
                    constants={k: _value_str(v) for k, v in n["constants"].items()},
                    path_inputs=dict(path_specs.get(inv[0], {})) if inv else {},
                    inputs=[
                        TraceInput(
                            param=i["param_name"],
                            record_id=i["record_id"],
                            variable=i["variable_type"],
                        )
                        for i in n["inputs"]
                    ],
                    saved=_iso(saved_ts),
                    saved_by=saved_by,
                    run_count=int(n_runs),
                    last_run=_iso(last_run),
                    invocation_id=inv[0] if inv else None,
                    invocation_ids=[p[0] for p in producers],
                    call_id=call_ids.get(inv[0]) if inv else None,
                    run_options=run_opts.get(inv[0]) if inv else None,
                    code_version=(
                        ordinals.get(inv[1], {}).get(inv[2]) if inv else None
                    ),
                    runs=runs,
                )
            )
        edges = [
            TraceEdge(
                from_record_id=e["from_record_id"],
                to_record_id=e["to_record_id"],
                param=e["param_name"],
            )
            for e in pipe["edges"]
        ]
        audit = []
        if include_audit:
            audit = [
                RunRecord(
                    timestamp=_iso(a["timestamp"]) or "",
                    user_id=a["user_id"],
                    function_name=a["function_name"],
                    where_clause=a["where_clause"],
                )
                for a in provenance_query.execution_audit(duck, rid)
            ]
        n_reproduced = sum(1 for n in nodes if len(n.invocation_ids) > 1)
        Log.info(
            f"trace({getattr(variable, '__name__', variable) or rid[:8]}): "
            f"{len(nodes)} node(s), {len(edges)} edge(s), "
            f"{sum(len(n.runs) for n in nodes)} run ref(s), "
            f"{n_reproduced} node(s) with more than one producing invocation"
            + (f", pin={pins}" if pins else "")
        )
        return ProvenanceTree(
            root_record_id=rid,
            nodes=nodes,
            edges=edges,
            audit=audit,
            selection={k: str(v) for k, v in pins.items()},
            matched_record_ids=list(matched),
        )

    def provenance(
        self,
        variable=None,
        selection: dict | None = None,
        record_id: str | None = None,
        include_runs: bool = True,
        include_audit: bool = False,
        **metadata,
    ) -> ProvenanceTree:
        """Provenance of a **pinned variant**, down to the run that produced it.

        The variant-pinned entry point :meth:`trace` never had: ``selection``
        is the vocabulary ``load()`` honours, so

            insp.provenance("GAITRiteSymmetry", {"__code__.grSides": "v2"})

        traces exactly the records

            GAITRiteSymmetry.load(**Variant(GAITRiteSymmetry,
                                            fn="grSides", code_version="v2"
                                            ).branch_params)

        would return. The display spelling (``{"Code:grSides": "v2"}``) is
        accepted too — one resolution for the Plot Studio picker, the GUI panel
        and the CLI.

        ``include_runs`` defaults True here and False on :meth:`trace`: this
        method exists to answer "which run produced this", and that is the
        join that answers it.
        """
        return self.trace(
            variable,
            record_id=record_id,
            include_audit=include_audit,
            selection=selection,
            include_runs=include_runs,
            **metadata,
        )

    def _latest_saves_batch(self, rids) -> dict:
        """{record_id: (timestamp, user_id)} of the newest save event."""
        if not rids:
            return {}
        placeholders = ", ".join(["?"] * len(rids))
        rows = self._duck._fetchall(
            f"SELECT record_id, timestamp, user_id FROM ("
            f"SELECT rs.*, ROW_NUMBER() OVER ("
            f"PARTITION BY record_id ORDER BY timestamp DESC) AS rn "
            f"FROM _record_save rs WHERE record_id IN ({placeholders})"
            f") WHERE rn = 1",
            list(rids),
        )
        return {rid: (ts, uid) for rid, ts, uid in rows}

    def _run_info_batch(self, rids) -> dict:
        """{record_id: (run_count, last_run_timestamp)} from the _run audit."""
        if not rids:
            return {}
        placeholders = ", ".join(["?"] * len(rids))
        rows = self._duck._fetchall(
            f"SELECT io.output_record_id, COUNT(*), MAX(run.timestamp) "
            f"FROM _invocation_output io "
            f"JOIN _run_invocation ri ON ri.invocation_id = io.invocation_id "
            f"JOIN _run run ON run.run_id = ri.run_id "
            f"WHERE io.output_record_id IN ({placeholders}) "
            f"GROUP BY io.output_record_id",
            list(rids),
        )
        return {rid: (n, ts) for rid, n, ts in rows}

    @_timed
    def runs(self, fn: str | None = None, limit: int = 50) -> list[RunRecord]:
        """The _run execution audit, newest first."""
        params: list = []
        where = ""
        if fn is not None:
            where = "WHERE r.function_name = ? "
            params.append(getattr(fn, "__name__", fn))
        params.append(int(limit))
        rows = self._duck._fetchall(
            "SELECT r.run_id, r.timestamp, r.user_id, r.function_name, "
            "r.where_clause, r.origin, COUNT(ri.invocation_id) "
            "FROM _run r "
            "LEFT JOIN _run_invocation ri ON ri.run_id = r.run_id "
            + where
            + "GROUP BY r.run_id, r.timestamp, r.user_id, r.function_name, "
            "r.where_clause, r.origin "
            "ORDER BY r.timestamp DESC LIMIT ?",
            params,
        )
        return [
            RunRecord(
                timestamp=_iso(ts) or "",
                user_id=uid,
                function_name=fn_name,
                where_clause=wc,
                run_id=run_id,
                n_invocations=int(n),
                origin=origin,
            )
            for run_id, ts, uid, fn_name, wc, origin, n in rows
        ]

    @_timed
    def audit(
        self, variable=None, record_id: str | None = None, **metadata
    ) -> list[RunRecord]:
        """Every run that (re)produced one record, oldest first."""
        from .. import provenance_query

        rid = self._resolve_record_id(variable, record_id, metadata)
        return [
            RunRecord(
                timestamp=_iso(a["timestamp"]) or "",
                user_id=a["user_id"],
                function_name=a["function_name"],
                where_clause=a["where_clause"],
            )
            for a in provenance_query.execution_audit(self._duck, rid)
        ]

    @_timed
    def node_state(
        self, fn=None, fn_registry: dict | None = None
    ) -> list[NodeStateSummary]:
        """Binary green/red per pipeline function (§9c semantics).

        fn: a name or callable to check one function (a callable is used as
        its own live registry entry — full source-edit detection); None = all
        recorded functions. Standalone (no callable/registry) uses the
        most-recently-run stored hash — see graph module docstring.
        """
        from ..state import _schema_id_to_combo
        from .graph import _node_states

        registry = dict(fn_registry or {})
        if fn is not None and callable(fn):
            registry.setdefault(getattr(fn, "__name__", str(fn)), fn)
        if fn is not None:
            names = [getattr(fn, "__name__", fn)]
            known = self._scalar(
                "SELECT 1 FROM _invocation WHERE function_name = ?",
                [names[0]],
                default=None,
            )
            if known is None:
                raise NotFoundError(
                    f"Function {names[0]!r} has no recorded invocations"
                )
        else:
            names = sorted(
                r[0]
                for r in self._duck._fetchall(
                    "SELECT DISTINCT function_name FROM _invocation "
                    "WHERE function_name <> '__save__'"
                )
            )

        states = _node_states(self._db, names, fn_registry=registry or None)
        out = []
        for name in names:
            st = states[name]
            out.append(
                NodeStateSummary(
                    function_name=name,
                    state=st["state"],
                    state_basis=st["basis"],
                    up_to_date=st["counts"].get("up_to_date", 0),
                    missing=st["counts"].get("missing", 0),
                    missing_combos=[
                        {
                            k: str(v)
                            for k, v in _schema_id_to_combo(self._db, sid).items()
                        }
                        for sid in st["missing_schema_ids"]
                        if sid is not None
                    ],
                )
            )
        return out

    @_timed
    def locations(
        self,
        variable,
        *,
        variant: dict | None = None,
        problems_only: bool = False,
        fn_registry: dict | None = None,
        **grid,
    ):
        """Per-location status for one variable under one variant (a
        :class:`~scidb.locations.LocationTree`).

        The granularity ``node_state`` (per function, binary) and
        ``schema_tree`` (per location, all variables, counts only) leave
        between them: *is my variable good at subject 03 trial 7, and if not,
        which locations are the problem?* Four states — green / amber (an input
        was re-saved since) / red (expected, absent) / grey (excluded, and so
        counted in neither the numerator nor the denominator). See
        ``docs/claude/schema-location-status.md``.

        Nothing is computed here: ``scidb.locations.location_states`` owns the
        rule, so this facade, the ``scidb locations`` command and the GUI's
        picker cannot disagree about what a colour means.

        variant: a branch-params filter in scidb's vocabulary
        (``{"bandpass.low_hz": 20}``, ``{"__code__": "latest"}``).
        problems_only: prune to the branches holding an amber or red leaf;
        counts are left intact, so a pruned node still reports ``1/8``.
        grid: iteration grid for PathInput discovery (lists per schema key).
        Only consulted for loader-produced variables, where discovery is the
        only live source for what *should* exist — which is what lets this
        surface see a file that was never loaded.
        """
        from ..locations import location_states, prune_to_problems

        name = getattr(variable, "__name__", variable)
        known = self._scalar(
            "SELECT 1 FROM _record WHERE type = ?", [name], default=None
        )
        if known is None:
            raise NotFoundError(f"No records of type {name!r}")
        tree = location_states(
            name,
            variant=variant,
            db=self._db,
            fn_registry=fn_registry,
            **grid,
        )
        return prune_to_problems(tree) if problems_only else tree

    @_timed
    def pathinput_state(self, fn_name: str, **grid) -> list[NodeStateSummary]:
        """On-demand discovery check for a PathInput loader (§10 in the
        design doc): should-run = PathInput.discover() ∩ grid − exclusions vs
        realized locations. One result per stored (spec, constants) config.

        The PathInput is reconstructed from the stored __pathinput__ spec, so
        this works standalone — but discover() walks the *current* filesystem,
        so run it where the data folders are reachable. grid values are lists
        (e.g. subject=["S01","S02"]); omitted keys are wildcards.
        """
        from ..locations import pathinput_configs
        from ..state import check_pathinput_node_state
        from .graph import _value_str

        fn_name = getattr(fn_name, "__name__", fn_name)
        duck = self._duck
        inv_rows = duck._fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?", [fn_name]
        )
        if not inv_rows:
            raise NotFoundError(f"Function {fn_name!r} has no recorded invocations")

        # One reconstruction, shared with scidb.locations — which needs exactly
        # the same specs-to-live-PathInput step to derive the discovery-based
        # denominator for a loader-produced variable.
        configs = pathinput_configs(duck, fn_name)
        if not configs:
            raise NotFoundError(
                f"Function {fn_name!r} has no PathInput inputs recorded — "
                f"use node_state() for variable-input functions"
            )

        def _stub():  # check_pathinput_node_state only reads fn.__name__
            pass

        _stub.__name__ = fn_name

        results = []
        for inputs, constants in configs:
            res = check_pathinput_node_state(_stub, [], inputs, db=self._db, **grid)
            results.append(
                NodeStateSummary(
                    function_name=fn_name,
                    state=res["state"],
                    state_basis="discovery",
                    up_to_date=res["counts"]["up_to_date"],
                    missing=res["counts"]["missing"],
                    missing_combos=[
                        {k: str(v) for k, v in c["schema_combo"].items()}
                        for c in res["combos"]
                        if c["state"] == "missing"
                    ],
                    constants={k: _value_str(v) for k, v in constants.items()},
                )
            )
        return results

    @_timed
    def records(
        self,
        variable,
        latest: bool = True,
        include_excluded: bool = False,
        include_values: bool = False,
        preview_len: int = 80,
        **metadata: Any,
    ) -> list[RecordSummary]:
        type_name = getattr(variable, "__name__", variable)
        nested = self._db._split_metadata(metadata)
        df = self._db._find_record(
            type_name,
            nested_metadata=nested,
            version_id="latest" if latest else "all",
            include_excluded=include_excluded,
        )
        keys = self._db.dataset_schema_keys
        out = []
        for _, row in df.iterrows():
            schema = {
                k: str(row[k])
                for k in keys
                if k in row.index and row[k] is not None and not pd.isna(row[k])
            }
            out.append(
                RecordSummary(
                    record_id=str(row["record_id"]),
                    variable=str(row["variable_name"]),
                    schema=schema,
                    timestamp=_iso(row["timestamp"]) or "",
                    user_id=None
                    if pd.isna(row.get("user_id"))
                    else str(row["user_id"]),
                    content_hash=str(row["content_hash"]),
                    schema_version=int(row["schema_version"]),
                    excluded=bool(row["excluded"]),
                )
            )
        if include_values and out:
            previews = self._value_previews(
                type_name, [r.record_id for r in out], preview_len
            )
            for r in out:
                r.value_preview = previews.get(r.record_id)
        return out

    def _value_previews(
        self, type_name: str, rids: list[str], preview_len: int
    ) -> dict[str, str]:
        """Compact per-record value previews straight from the data table
        (storage form — good enough to recognize a record, not a load API)."""
        table = self._scalar(
            "SELECT table_name FROM _registered_types WHERE type_name = ?",
            [type_name],
            default=type_name,
        )
        if not self._duck._table_exists(table):
            return {}
        placeholders = ", ".join(["?"] * len(rids))
        try:
            data = self._duck._fetchdf(
                f'SELECT * FROM "{table}" WHERE record_id IN ({placeholders})',
                list(rids),
            )
        except Exception as e:  # data table without record_id column etc.
            Log.debug(f"inspect: value preview unavailable for {type_name}: {e}")
            return {}
        if "record_id" not in data.columns:
            return {}
        data_cols = [c for c in data.columns if c not in ("record_id", "schema_id")]

        def clip(text: str) -> str:
            return text if len(text) <= preview_len else text[:preview_len] + "…"

        previews: dict[str, str] = {}
        for rid, group in data.groupby("record_id"):
            if len(group) == 1:
                pairs = ", ".join(f"{c}={group.iloc[0][c]}" for c in data_cols)
                previews[str(rid)] = clip(pairs)
            else:
                previews[str(rid)] = clip(
                    f"{len(group)} rows × {len(data_cols)} cols "
                    f"({', '.join(data_cols)})"
                )
        return previews

    @_timed
    def pick(
        self, variable, latest: bool = True, include_excluded: bool = False, **metadata
    ) -> list[PickCandidate]:
        """Candidates for record selection: latest records matching the
        metadata, enriched with branch params and producing function so
        coexisting variants are tellable apart. Batched enrichment
        (branch_params_batch / producing_invocation_batch — N+1 rule)."""
        from .. import provenance_query
        from .graph import _value_str

        recs = self.records(
            variable, latest=latest, include_excluded=include_excluded, **metadata
        )
        rids = [r.record_id for r in recs]
        bp = provenance_query.branch_params_batch(self._duck, rids)
        producing = provenance_query.producing_invocation_batch(self._duck, rids)
        out = []
        for r in recs:
            inv = producing.get(r.record_id)
            out.append(
                PickCandidate(
                    record_id=r.record_id,
                    variable=r.variable,
                    schema=r.schema,
                    branch_params={
                        k: _value_str(v) for k, v in bp.get(r.record_id, {}).items()
                    },
                    function_name=inv[1] if inv else None,
                    saved=r.timestamp or None,
                )
            )
        return out

    @_timed
    def exclusions(self) -> list[ExclusionRecord]:
        """Currently-excluded schema combinations (read side of the Phase 5
        write commands; the write side lives on mutate.Mutator)."""
        from ..exclusions import list_exclusions

        if not self._duck._table_exists("__scidb_schema_overrides"):
            return []
        df = list_exclusions(db=self._db)
        keys = self._db.dataset_schema_keys
        out = []
        for _, row in df.iterrows():
            schema = {
                k: str(row[k])
                for k in keys
                if k in row.index and row[k] is not None and not pd.isna(row[k])
            }
            out.append(
                ExclusionRecord(
                    schema=schema,
                    reason=str(row["reason"]),
                    changed_at=_iso(row["changed_at"]) or "",
                    changed_by=None
                    if pd.isna(row.get("changed_by"))
                    else str(row["changed_by"]),
                )
            )
        return out

    @_timed
    def sql(self, query: str) -> SqlResult:
        """Read-only escape hatch: arbitrary SQL, rendered/serialized as a
        table. Writes fail at the DuckDB level (read-only connection)."""

        def cell(value):
            if value is None:
                return None
            if isinstance(value, (str, int, float, bool)):
                return value
            # A LIST holding NULLs arrives as a masked array, whose str() is
            # numpy's "--" for the masked slots. Say NULL instead: "--" reads
            # like data and cost an investigation once (2026-09-15).
            if isinstance(value, np.ma.MaskedArray):
                return (
                    "["
                    + " ".join(
                        "NULL" if m else repr(v)
                        for v, m in zip(
                            value.data.tolist(),
                            np.ma.getmaskarray(value).tolist(),
                            strict=False,
                        )
                    )
                    + "]"
                )
            if hasattr(value, "isoformat"):
                return _iso(value)
            if hasattr(value, "item"):  # numpy scalar
                try:
                    return value.item()
                except (AttributeError, ValueError):
                    pass
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            return str(value)

        # _fetch_table, not _fetchdf: pandas turns a NULL DOUBLE into NaN, so a
        # NULL would be reported as the number `nan` by the one command whose
        # job is to say what the database actually holds.
        columns, rows = self._duck._fetch_table(query)
        return SqlResult(
            columns=[str(c) for c in columns],
            rows=[[cell(v) for v in row] for row in rows],
            row_count=len(rows),
        )

    # -- report (endpoint surface; see inspect/report.py) -------------------

    @_timed
    def report(self, fn: str | None = None, variable=None, all_versions: bool = False):
        """Collect every finalized endpoint (plot_/stat_) record into a
        ReportData manifest: figures with artifact paths + stamp
        verification, stats with parsed result JSON, per-entry warnings.
        Drafts never appear (no records). See inspect/report.py.
        """
        from .report import collect_report

        return collect_report(self, fn=fn, variable=variable, all_versions=all_versions)

    @_timed
    def write_report(
        self,
        out_dir,
        fn: str | None = None,
        variable=None,
        all_versions: bool = False,
        copy_artifacts: bool = True,
        embed: bool = True,
    ) -> Path:
        """Write a self-contained endpoint report folder: index.html (inline
        CSS, embedded/copied figures, per-test-family stats tables) +
        manifest.json + stats.csv (+ artifacts/ copies). Returns the path
        to index.html.
        """
        from .report import write_report

        return write_report(
            self,
            out_dir,
            fn=fn,
            variable=variable,
            all_versions=all_versions,
            copy_artifacts=copy_artifacts,
            embed=embed,
        )
