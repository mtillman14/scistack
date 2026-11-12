"""DatabaseManager: versions and variants — listing versions, excluding and including variants, provenance and invocation lookups.

Moved verbatim out of database.py (grug follow-up): DatabaseManager inherits
this mixin, so every method is still `db.<name>`. Each method's `self` is
annotated as DatabaseManager, so the IDE knows its attributes.
"""

from typing import TYPE_CHECKING

from importlib import metadata
from scidb.model.exceptions import AmbiguousVersionError
from scidb.model.exceptions import NotFoundError
from scidb.model.log import Log
from scidb.model.variable import BaseVariable
from scidb.provenance import query as provenance_query
from typing import Any

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.provenance.graph import constants_identity_key
class VersionsMixin:
    """DatabaseManager: versions and variants — listing versions, excluding and including variants, provenance and invocation lookups."""

    def list_versions(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        include_excluded: bool = False,
        **metadata,
    ) -> list[dict]:
        """
        List all versions at a schema location.

        Args:
            variable_class: The type to query
            include_excluded: If True, include excluded variants in results.
            **metadata: Schema metadata to match; non-schema keys are treated
                as branch_params filters.

        Returns:
            List of dicts with record_id, schema, branch_params, timestamp
            (plus "excluded" bool when include_excluded=True).
        """
        self._ensure_registered(variable_class, auto_register=True)

        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in metadata.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in metadata.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)

        try:
            records = self._find_record(
                variable_class.__name__,
                nested_metadata=nested_metadata,
                version_id="all",
                branch_params_filter=branch_params_filter,
                include_excluded=include_excluded,
            )
        except Exception as exc:
            # Still answered as "no versions" (callers and MATLAB rely on a
            # list), but no longer silently: an error here is not the same as
            # a location with nothing saved.
            Log.warn(
                f"list_versions({variable_class.__name__}): record lookup failed "
                f"({type(exc).__name__}: {exc}); reporting no versions"
            )
            return []

        # Every row's branch params from ONE upstream closure. This loop used
        # to walk each record's ancestry twice (once for its save kwargs, once
        # for branch_params) — the N+1 the batched helpers exist to avoid
        # (cleanup-audit F3).
        bp_map = provenance_query.branch_params_batch(
            self._duck, list(records["record_id"]) if len(records) else []
        )
        results = []
        for _, row in records.iterrows():
            # branch_params derived from the bipartite graph (§6), not a column.
            bp = bp_map.get(row["record_id"], {})
            _, nested = self._reconstruct_metadata_from_row(row, branch_params=bp)
            entry = {
                "record_id": row["record_id"],
                "schema": nested.get("schema", {}),
                "branch_params": bp,
                "timestamp": row["timestamp"],
            }
            if include_excluded:
                exc = row.get("excluded") if hasattr(row, "get") else row["excluded"]
                entry["excluded"] = bool(exc) if exc is not None else False
            results.append(entry)

        # Sort by timestamp descending
        results.sort(key=lambda x: x["timestamp"], reverse=True)
        return results

    def _resolve_record_id(
        self: "DatabaseManager",
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> str:
        """Resolve a record_id string or (variable_class, **kwargs) to a single record_id.

        Raises AmbiguousVersionError if multiple records match, NotFoundError if none.
        Always searches including excluded records.
        """
        if isinstance(record_id_or_type, str):
            return record_id_or_type

        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )
        if len(records) > 1:
            ids = records["record_id"].tolist()
            raise AmbiguousVersionError(
                f"{len(records)} records match for {variable_class.__name__} "
                f"with {kwargs}. "
                f"Pass a record_id directly or narrow with more branch parameters. "
                f"Matching record_ids: {ids}"
            )
        return records.iloc[0]["record_id"]

    def exclude_variant(
        self: "DatabaseManager",
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> int:
        """Mark variant(s) as excluded from automatic inclusion in for_each and load().

        If multiple variants match the provided metadata, ALL matching variants
        are excluded (e.g., all branch_params for a given schema combo).

        Usage:
            db.exclude_variant("abc123")                                  # by record_id (1 variant)
            db.exclude_variant(DetectedSpikes, subject="S01", low_hz=20)  # specific variant
            db.exclude_variant(DetectedSpikes, subject="S01")             # all variants for S01

        Returns:
            Number of variants excluded.
        """
        if isinstance(record_id_or_type, str):
            # Direct record_id provided - exclude just that one
            self._duck.execute(
                "UPDATE _record SET excluded = TRUE WHERE record_id = ?",
                [record_id_or_type],
            )
            return 1

        # Variable class + kwargs: find and exclude ALL matching records
        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )

        # Exclude all matching records
        record_ids = records["record_id"].tolist()
        for rid in record_ids:
            self._duck.execute(
                "UPDATE _record SET excluded = TRUE WHERE record_id = ?",
                [rid],
            )

        if len(record_ids) > 1:
            Log.info(
                f"Excluded {len(record_ids)} variant(s) of {variable_class.__name__} "
                f"matching {kwargs}"
            )
        return len(record_ids)

    def include_variant(
        self: "DatabaseManager",
        record_id_or_type: "str | type[BaseVariable]",
        **kwargs,
    ) -> int:
        """Re-include previously excluded variant(s).

        If multiple variants match the provided metadata, ALL matching variants
        are re-included.

        Usage:
            db.include_variant("abc123")                                  # by record_id (1 variant)
            db.include_variant(DetectedSpikes, subject="S01", low_hz=20)  # specific variant
            db.include_variant(DetectedSpikes, subject="S01")             # all variants for S01

        Returns:
            Number of variants re-included.
        """
        if isinstance(record_id_or_type, str):
            # Direct record_id provided - include just that one
            self._duck.execute(
                "UPDATE _record SET excluded = FALSE WHERE record_id = ?",
                [record_id_or_type],
            )
            return 1

        # Variable class + kwargs: find and include ALL matching records
        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in kwargs.items() if k not in schema_keys_set
        } or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )

        # Re-include all matching records
        record_ids = records["record_id"].tolist()
        for rid in record_ids:
            self._duck.execute(
                "UPDATE _record SET excluded = FALSE WHERE record_id = ?",
                [rid],
            )

        if len(record_ids) > 1:
            Log.info(
                f"Re-included {len(record_ids)} variant(s) of {variable_class.__name__} "
                f"matching {kwargs}"
            )
        return len(record_ids)

    def get_provenance(
        self: "DatabaseManager",
        variable_class: type[BaseVariable] | None,
        version: str | None = None,
        **metadata,
    ) -> dict | None:
        """
        Get the provenance (lineage) of a variable.

        Returns:
            Dict with function_name, function_hash, inputs, constants
            or None if no lineage recorded
        """
        if version:
            record_id = version
        else:
            var = next(self.load(variable_class, metadata, version_id="latest"))
            record_id = var.record_id
        return provenance_query.provenance(self._duck, record_id)

    def list_pipeline_variants(
        self: "DatabaseManager",
        output_type: str | None = None,
    ) -> list[dict]:
        """
        List all distinct pipeline step variants recorded in the database.

        Each entry represents a unique (function, constants, output_type)
        combination — a "branch" of the pipeline. Two for_each runs on the
        same function with different constants produce two separate entries.

        Derived entirely from the bipartite provenance graph (no version_keys).

        Args:
            output_type: Optional variable type name to filter results
                         (e.g. "Filtered"). If None, all types are returned.

        Returns:
            List of dicts with keys:
                function_name (str),
                output_type   (str),
                call_id       (str: 16 hex chars; identifies the for_each call
                               site that produced this variant.  Same across
                               cosmetic source edits to the fn body),
                input_types   (dict: param_name → type_name),
                constants     (dict: param_name → value),
                run_options   (str: the distribute/as_table set this variant
                               ran under — provenance_query.run_options_label),
                output_num    (int | None: which output of the PRODUCING
                               INVOCATION this record is — the PK half of
                               _invocation_output. That coincides with the
                               0-based signature slot only for an ordinary
                               multi-output call: a `distribute` run numbers
                               its SLICES, and a batch loader sharing one
                               invocation across runs gives a re-run's record
                               the next free slot. Do not index a signature
                               with it without checking the range, and never
                               for a single-output function — see
                               scistack_gui.services.graph_service.
                               _matlab_param_to_class_from_db),
                record_count  (int: distinct records for this variant)
        """

        return provenance_query.pipeline_variants(self._duck, output_type)

    def filter_variants_for_execution(
        self: "DatabaseManager",
        fn_name: str,
        call_id: str,
        schema_filter: dict[str, list] | None = None,
        constant_overrides: dict[str, Any] | None = None,
    ) -> list[dict]:
        """
        Filter variants for execution based on schema and constant selection.

        This method prepares variants for execution by:
        1. Getting variants for the specified function and call_id
        2. Filtering by schema_filter if provided
        3. Applying constant_overrides (replacing DB constants)
        4. Deduplicating the result

        Args:
            fn_name: Function name to filter.
            call_id: Call site identifier (16 hex chars).
            schema_filter: Optional dict of {schema_key: [selected_values]} to
                filter variants by. Only variants matching these schema values
                will be included.
            constant_overrides: Optional dict of {constant_name: value} to
                override database constants. When provided, these values replace
                the constants from the database for all matching variants.

        Returns:
            List of variant dicts ready for for_each execution::

                [
                    {
                        "input_types": {param: var_type},
                        "output_type": var_type,
                        "constants": {param: value},
                    }
                ]

            The list is deduplicated so identical variants appear only once.

        Example::

            # Get variants for a function, overriding threshold constant
            variants = db.filter_variants_for_execution(
                fn_name="process_signal",
                call_id="abc123def456789",
                schema_filter={"subject": [1, 2]},  # Only subjects 1 and 2
                constant_overrides={"threshold": 0.75},  # Override with 0.75
            )
        """

        # Get all variants for this function and call_id
        all_variants = self.list_pipeline_variants()
        fn_variants = [
            v
            for v in all_variants
            if v["function_name"] == fn_name and v.get("call_id") == call_id
        ]

        if not fn_variants:
            return []

        # Build result variants
        result_variants = []
        seen = set()  # For deduplication

        for variant in fn_variants:
            input_types = variant["input_types"]
            output_type = variant["output_type"]
            constants = dict(variant["constants"])  # Make a copy

            # Apply constant overrides
            if constant_overrides:
                for const_name, const_value in constant_overrides.items():
                    if const_name in constants:
                        constants[const_name] = const_value

            # Create variant dict
            result_variant = {
                "input_types": input_types,
                "output_type": output_type,
                "constants": constants,
            }

            # Deduplicate by converting to a hashable key. Constants are
            # canonicalized through scidb.provenance.graph.constants_identity_key:
            # a constant may be a dict (a config table declared in
            # scistack_entities.toml), and a bare tuple(sorted(...)) of those
            # items is unhashable.
            key = (
                output_type,
                tuple(sorted(input_types.items())),
                constants_identity_key(constants),
            )

            if key not in seen:
                seen.add(key)
                result_variants.append(result_variant)

        # Note: schema_filter is intentionally NOT applied here because
        # for_each handles schema filtering via **metadata_iterables.
        # The schema_filter parameter is kept for API compatibility with the
        # original plan, but filtering by schema happens at execution time,
        # not during variant selection.

        return result_variants

    def invocation_exists(self: "DatabaseManager", invocation_id: str) -> bool:
        """True if an invocation (unique function call) is already recorded.

        The §9c membership test: an invocation and its ``_invocation_output``
        rows are written together, so *invocation present ⇒ outputs present*.
        ``skip_computed`` and node-completeness both use this.
        """
        rows = self._duck.fetchall(
            "SELECT 1 FROM _invocation WHERE invocation_id = ? LIMIT 1",
            [invocation_id],
        )
        return bool(rows)

    def find_record_id(
        self: "DatabaseManager",
        variable_class: type,
        metadata: dict,
        branch_params_filter: dict | None = None,
    ) -> str | None:
        """Lightweight lookup returning the record_id of the latest record for
        a variable + metadata combination, without loading any data.

        Args:
            variable_class: The BaseVariable subclass to look up.
            metadata: Flat dict of schema keys (and optionally non-schema kwargs,
                matched as branch params against the graph).
            branch_params_filter: Optional namespaced branch_params dict
                (e.g. ``{"bandpass_filter.low_hz": 20}``) for variant
                disambiguation via suffix matching against graph-derived branch
                params (the producing invocation's constants).

        Returns None if no matching record exists.
        """
        nested = self._split_metadata(metadata)
        rows = self._find_record(
            variable_class.__name__,
            nested_metadata=nested,
            version_id="latest",
            branch_params_filter=branch_params_filter or None,
        )
        if rows.empty:
            return None
        return rows.iloc[0]["record_id"]
