# app/services/buffer_service.py
import traceback

import geopandas as gpd
from fastapi import Body, File, Form, HTTPException, UploadFile

from app.schemas.PayloadSchemas import BufferTableParams, DBConnectionPayload
from app.services.process_service import log_start
from app.utils.dask_utils import parallel_map, submit_job
from app.utils.db_utils import get_engine_from_payload, load_gdf_from_db_by_engine
from app.utils.vector_utils import (
    CAP_MAPPING,
    JOIN_MAPPING,
    clean_geom,
    convert_distance,
    read_geojson_upload,
    save_vector_result,
)


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _buffer_chunk(gdf, distance=None, cap=1, join=1):
    """One slice of the layer. parallel_map fans these across the cluster."""
    out = gdf.copy()
    geom_col = out.geometry.name
    out[geom_col] = out.geometry.apply(clean_geom)
    out[geom_col] = out.geometry.buffer(distance, cap_style=cap, join_style=join)
    return out


def _run_buffer(gdf, distance, unit, cap, join, dissolve, name,
                db_connection=None, schema_name=None, output_layer=None):
    converted = convert_distance(distance, unit, gdf)

    result_gdf = parallel_map(
        gdf, _buffer_chunk,
        distance=converted,
        cap=CAP_MAPPING.get(cap, 1),
        join=JOIN_MAPPING.get(join, 1),
    )

    if dissolve:
        result_gdf = result_gdf.dissolve()

    out = save_vector_result(result_gdf, name, db_connection, schema_name, output_layer)
    out.update({"distance": distance, "unit": unit,
                "distance_in_crs_units": converted, "dissolved": dissolve})
    return out


def _run_buffer_table(db_connection, schema_name, table_name, where, distance,
                      unit, cap, join, dissolve, name, output_layer):
    engine = get_engine_from_payload(db_connection)
    gdf = load_gdf_from_db_by_engine(engine, schema_name, table_name, where)
    if gdf.empty:
        raise ValueError("No data found for given parameters")
    return _run_buffer(gdf, distance, unit, cap, join, dissolve, name,
                       db_connection, schema_name, output_layer)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def buffer_from_table(
    payload: DBConnectionPayload = Body(...),
    params: BufferTableParams = Body(...),
):
    try:
        log_id = log_start(
            input_layer=params.table_name,
            overlay_layer=None,
            tool_name="buffer",
            data_type="vector",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=payload.output_layer,
        )

        submit_job(
            _run_buffer_table, log_id,
            db_connection=payload.db_connection.model_dump(),
            schema_name=params.schema_name,
            table_name=params.table_name,
            where=params.where,
            distance=params.distance,
            unit=params.unit,
            cap=params.cap,
            join=params.join,
            dissolve=params.dissolve,
            name=payload.output_layer or f"buffer_{log_id[:8]}",
            output_layer=payload.output_layer,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Buffer job submitted. Poll /api/process/v1/status/{uuid}."}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def buffer_from_geojson(
    file: UploadFile = File(...),
    distance: float = Form(...),
    unit: str = Form("meters"),
    cap: str = Form("round"),
    join: str = Form("round"),
    dissolve: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: str = Form(None),
    output_layer: str = Form(None),
):
    try:
        gdf = await read_geojson_upload(file)

        log_id = log_start(
            input_layer=file.filename,
            overlay_layer=None,
            tool_name="buffer",
            data_type="vector",
            project_id=project_id,
            business_id=business_id,
            file_path=file_path,
            output_layer=output_layer,
        )

        submit_job(
            _run_buffer, log_id,
            gdf=gdf,
            distance=distance,
            unit=unit,
            cap=cap,
            join=join,
            dissolve=dissolve,
            name=output_layer or f"buffer_{log_id[:8]}",
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Buffer job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
