"""FastAPI server wrapping a real DatabaseManager.

Run programmatically:
    app = create_app("/data/experiment.duckdb", ["subject", "session"], "/data/pipeline.db")
    uvicorn.run(app, host="0.0.0.0", port=8000)

Run via CLI:
    SCIDB_DATASET_DB_PATH=/data/experiment.duckdb \\
    SCIDB_DATASET_SCHEMA_KEYS='["subject","session"]' \\
    SCIDB_PIPELINE_DB_PATH=/data/pipeline.db \\
    scidb-server
"""

import json
import os
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from scidb.database import DatabaseManager
from scidb.variable import BaseVariable
from thunk.lineage import LineageRecord

from ._types import (
    CloseResponse,
    ErrorResponse,
    ExportToCsvRequest,
    ExportToCsvResponse,
    FindByLineageRequest,
    HasLineageRequest,
    HasLineageResponse,
    HealthResponse,
    ListVersionsRequest,
    ListVersionsResponse,
    PipelineStructureResponse,
    ProvenanceBySchemaRequest,
    ProvenanceBySchemaResponse,
    ProvenanceRequest,
    ProvenanceResponse,
    RegisterRequest,
    RegisterResponse,
    SaveEphemeralLineageRequest,
    SaveEphemeralLineageResponse,
    SaveResponse,
)
from .serialization import (
    decode_save_request,
    encode_multi,
    encode_response,
)


def _error_response(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(error=message).model_dump(),
    )


def create_app(
    dataset_db_path: str,
    dataset_schema_keys: list[str],
    lineage_mode: str = "strict",
) -> FastAPI:
    """Create a FastAPI application wrapping a DatabaseManager.

    Args:
        dataset_db_path: Path to DuckDB database file.
        dataset_schema_keys: List of metadata keys defining the dataset schema.
        lineage_mode: "strict" or "ephemeral".

    Returns:
        Configured FastAPI application.
    """
    app = FastAPI(title="SciStack Server", version="0.1.0")

    db = DatabaseManager(
        dataset_db_path=dataset_db_path,
        dataset_schema_keys=dataset_schema_keys,
        lineage_mode=lineage_mode,
    )
    # Store on app state so tests can access it
    app.state.db = db

    # Dynamic variable classes created via /register (server-side surrogates)
    # Maps type_name -> BaseVariable subclass
    _remote_types: dict[str, type[BaseVariable]] = {}

    def _get_or_create_type(
        type_name: str,
        table_name: str | None = None,
        schema_version: int = 1,
        has_custom_serialization: bool = False,
    ) -> type[BaseVariable]:
        """Get an existing type or create a dynamic surrogate."""
        if type_name in _remote_types:
            return _remote_types[type_name]

        # Check global BaseVariable registry
        existing = BaseVariable.get_subclass_by_name(type_name)
        if existing is not None:
            _remote_types[type_name] = existing
            return existing

        # Create a dynamic subclass
        attrs: dict[str, Any] = {"schema_version": schema_version}
        if table_name and table_name != type_name:
            attrs["table_name"] = classmethod(lambda cls, _tn=table_name: _tn)

        if has_custom_serialization:
            # Custom-serialized types: data is a DataFrame, stored as-is
            def _to_db(self):
                import pandas as pd
                if isinstance(self.data, pd.DataFrame):
                    return self.data.copy()
                return super(type(self), self).to_db()

            @classmethod  # type: ignore[misc]
            def _from_db(cls, df):
                return df.copy()

            attrs["to_db"] = _to_db
            attrs["from_db"] = _from_db

        cls = type(type_name, (BaseVariable,), attrs)
        _remote_types[type_name] = cls
        return cls

    # -----------------------------------------------------------------
    # Routes
    # -----------------------------------------------------------------

    @app.get("/api/v1/health")
    async def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.post("/api/v1/register")
    async def register(req: RegisterRequest) -> RegisterResponse:
        cls = _get_or_create_type(
            req.type_name,
            req.table_name,
            req.schema_version,
            req.has_custom_serialization,
        )
        db.register(cls)
        return RegisterResponse(ok=True)

    @app.post("/api/v1/save")
    async def save(request: Request) -> Response:
        body = await request.body()
        meta, data = decode_save_request(body)

        type_name = meta["type_name"]
        metadata = meta["metadata"]
        lineage_dict = meta.get("lineage")
        lineage_hash = meta.get("lineage_hash")
        index = meta.get("index")
        has_custom = meta.get("has_custom_serialization", False)

        cls = _get_or_create_type(type_name, has_custom_serialization=has_custom)

        lineage = LineageRecord.from_dict(lineage_dict) if lineage_dict else None

        instance = cls(data)
        record_id = db.save(
            instance,
            metadata,
            lineage=lineage,
            lineage_hash=lineage_hash,
            index=index,
        )

        return JSONResponse(
            content=SaveResponse(record_id=record_id).model_dump()
        )

    @app.post("/api/v1/load")
    async def load(request: Request) -> Response:
        body = await request.body()
        req = json.loads(body)
        type_name = req["type_name"]
        metadata = req["metadata"]
        version = req.get("version", "latest")
        loc = req.get("loc")
        iloc = req.get("iloc")

        cls = _get_or_create_type(type_name)

        try:
            var = db.load(cls, metadata, version=version, loc=loc, iloc=iloc)
        except Exception as e:
            if "not found" in str(e).lower():
                return _error_response(404, str(e))
            raise

        # Build response: envelope with data + JSON metadata header
        import pandas as pd
        from .serialization import serialize_data, encode_envelope

        header, data_body = serialize_data(var.data)
        # Attach variable metadata to header
        header["_record_id"] = var.record_id
        header["_metadata"] = var.metadata
        header["_content_hash"] = var.content_hash
        header["_lineage_hash"] = var.lineage_hash

        return Response(
            content=encode_envelope(header, data_body),
            media_type="application/octet-stream",
        )

    @app.post("/api/v1/load_all")
    async def load_all(request: Request) -> Response:
        body = await request.body()
        req = json.loads(body)
        type_name = req["type_name"]
        metadata = req["metadata"]

        cls = _get_or_create_type(type_name)

        # Collect all results
        results = list(db.load_all(cls, metadata))

        # Encode each variable as an envelope with metadata in the header
        from .serialization import serialize_data, encode_envelope
        import struct

        parts: list[bytes] = []
        for var in results:
            header, data_body = serialize_data(var.data)
            header["_record_id"] = var.record_id
            header["_metadata"] = var.metadata
            header["_content_hash"] = var.content_hash
            header["_lineage_hash"] = var.lineage_hash
            envelope = encode_envelope(header, data_body)
            parts.append(struct.pack(">I", len(envelope)) + envelope)

        packed = struct.pack(">I", len(results)) + b"".join(parts)
        return Response(content=packed, media_type="application/octet-stream")

    @app.post("/api/v1/list_versions")
    async def list_versions(req: ListVersionsRequest) -> ListVersionsResponse:
        cls = _get_or_create_type(req.type_name)
        versions = db.list_versions(cls, **req.metadata)
        return ListVersionsResponse(versions=versions)

    @app.post("/api/v1/provenance")
    async def provenance(req: ProvenanceRequest) -> ProvenanceResponse:
        cls = _get_or_create_type(req.type_name)
        result = db.get_provenance(cls, version=req.version, **req.metadata)
        return ProvenanceResponse(provenance=result)

    @app.post("/api/v1/provenance_by_schema")
    async def provenance_by_schema(
        req: ProvenanceBySchemaRequest,
    ) -> ProvenanceBySchemaResponse:
        records = db.get_provenance_by_schema(**req.schema_keys)
        return ProvenanceBySchemaResponse(records=records)

    @app.get("/api/v1/pipeline_structure")
    async def pipeline_structure() -> PipelineStructureResponse:
        structure = db.get_pipeline_structure()
        return PipelineStructureResponse(structure=structure)

    @app.post("/api/v1/has_lineage")
    async def has_lineage(req: HasLineageRequest) -> HasLineageResponse:
        result = db.has_lineage(req.record_id)
        return HasLineageResponse(has_lineage=result)

    @app.post("/api/v1/save_ephemeral_lineage")
    async def save_ephemeral_lineage(
        req: SaveEphemeralLineageRequest,
    ) -> SaveEphemeralLineageResponse:
        lineage = LineageRecord.from_dict(req.lineage)
        db.save_ephemeral_lineage(
            ephemeral_id=req.ephemeral_id,
            variable_type=req.variable_type,
            lineage=lineage,
            user_id=req.user_id,
            schema_keys=req.schema_keys,
        )
        return SaveEphemeralLineageResponse(ok=True)

    @app.post("/api/v1/export_to_csv")
    async def export_to_csv(req: ExportToCsvRequest) -> ExportToCsvResponse:
        cls = _get_or_create_type(req.type_name)
        count = db.export_to_csv(cls, req.path, **req.metadata)
        return ExportToCsvResponse(count=count)

    @app.post("/api/v1/find_by_lineage")
    async def find_by_lineage(request: Request) -> Response:
        body = await request.body()
        req = json.loads(body)
        lineage_hash = req["lineage_hash"]

        # Query PipelineDB directly by hash
        records = db._pipeline_db.find_by_lineage_hash(lineage_hash)
        if not records:
            return Response(content=b"", status_code=204)

        import struct
        from .serialization import serialize_data, encode_envelope

        results_data: list[bytes] = []
        for record in records:
            output_record_id = record["output_record_id"]
            output_type = record["output_type"]

            if output_record_id.startswith("ephemeral:"):
                continue

            cls = _get_or_create_type(output_type)
            try:
                var = db.load(cls, {}, version=output_record_id)
            except Exception:
                return Response(content=b"", status_code=204)

            header, data_body = serialize_data(var.data)
            header["_record_id"] = var.record_id
            header["_metadata"] = var.metadata
            header["_content_hash"] = var.content_hash
            header["_lineage_hash"] = var.lineage_hash
            envelope = encode_envelope(header, data_body)
            results_data.append(struct.pack(">I", len(envelope)) + envelope)

        if not results_data:
            return Response(content=b"", status_code=204)

        packed = struct.pack(">I", len(results_data)) + b"".join(results_data)
        return Response(content=packed, media_type="application/octet-stream")

    @app.post("/api/v1/close")
    async def close() -> CloseResponse:
        db.close()
        return CloseResponse(ok=True)

    return app


def main():
    """CLI entry point: scidb-server."""
    import uvicorn

    dataset_db_path = os.environ.get("SCIDB_DATASET_DB_PATH")
    schema_keys_json = os.environ.get("SCIDB_DATASET_SCHEMA_KEYS")
    lineage_mode = os.environ.get("SCIDB_LINEAGE_MODE", "strict")

    if not dataset_db_path or not schema_keys_json:
        raise SystemExit(
            "Required env vars: SCIDB_DATASET_DB_PATH, "
            "SCIDB_DATASET_SCHEMA_KEYS (JSON list)"
        )

    dataset_schema_keys = json.loads(schema_keys_json)

    app = create_app(
        dataset_db_path=dataset_db_path,
        dataset_schema_keys=dataset_schema_keys,
        lineage_mode=lineage_mode,
    )

    host = os.environ.get("SCIDB_HOST", "0.0.0.0")
    port = int(os.environ.get("SCIDB_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
