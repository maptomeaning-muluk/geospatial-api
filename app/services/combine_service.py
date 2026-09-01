# app/services/combine_service.py
import traceback
from typing import List

import geopandas as gpd
import pandas as pd
from fastapi import Body, File, Form, HTTPException, UploadFile

from app.schemas.PayloadSchemas import CombineTableParams, DBConnectionPayload
from app.services.process_service import log_start
from app.utils.dask_utils import client_context, submit_job
from app.utils.db_utils import get_engine_from_payload, load_gdf_from_db_by_engine
from app.utils.vector_utils import (
    clean_geom,
    ensure_crs,
    read_geojson_upload,
    save_vector_result,
)


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _load_one_table(db_connection, schema_name, table_name, where):
    """Each source table is read on its own worker, so N tables load at once."""
    engine = get_engine_from_payload(db_connection)
    gdf = load_gdf_from_db_by_engine(engine, schema_name, table_name, where)
    if len(gdf):
        gdf = gdf.copy()
        gdf["src_layer"] = table_name
    return gdf


def _merge_frames(frames, target_crs=None):
    frames = [f for f in frames if f is not None and len(f) > 0]
    if not frames:
        raise ValueError("Every input layer was empty")

    target_crs = target_crs or frames[0].crs
    aligned = []
    for f in frames:
        f = ensure_crs(f)
        if target_crs is not None and str(f.crs) != str(target_crs):
            f = f.to_crs(target_crs)
        if f.geometry.name != "geometry":
            f = f.rename_geometry("geometry")
        aligned.append(f)

    merged = pd.concat(aligned, ignore_index=True)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=target_crs)


def _run_combine(frames, dissolve, dissolve_field, name,
                 db_connection=None, schema_name=None, output_layer=None):
    combined = _merge_frames(frames)
    combined["geometry"] = combined.geometry.apply(clean_geom)

    if dissolve or dissolve_field:
        if dissolve_field:
            if dissolve_field not in combined.columns:
                raise ValueError(f"Field '{dissolve_field}' not found in the layer")
            combined = combined.dissolve(by=dissolve_field, as_index=False)
        else:
            combined = combined.dissolve()

    out = save_vector_result(combined, name, db_connection, schema_name, output_layer)
    out.update({"input_layers": len(frames),
                "dissolved": bool(dissolve or dissolve_field)})
    return out


def _run_combine_table(db_connection, schema_name, table_names, where,
                       dissolve, dissolve_field, name, output_layer):
    with client_context() as client:
        futures = [
            client.submit(_load_one_table, db_connection, schema_name, t, where,
                          pure=False)
            for t in table_names
        ]
        frames = client.gather(futures)
    return _run_combine(frames, dissolve, dissolve_field, name,
                        db_connection, schema_name, output_layer)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def combine_from_table(
    payload: DBConnectionPayload = Body(...),
    params: CombineTableParams = Body(...),
):
    try:
        log_id = log_start(
            input_layer=", ".join(params.table_names),
            overlay_layer=None,
            tool_name="combine",
            data_type="vector",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=payload.output_layer,
        )

        submit_job(
            _run_combine_table, log_id,
            db_connection=payload.db_connection.model_dump(),
            schema_name=params.schema_name,
            table_names=params.table_names,
            where=params.where,
            dissolve=params.dissolve,
            dissolve_field=params.dissolve_field,
            name=payload.output_layer or f"combine_{log_id[:8]}",
            output_layer=payload.output_layer,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Combine job submitted. Poll /api/process/v1/status/{uuid}."}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def combine_from_geojson(
    files: List[UploadFile] = File(...),
    dissolve: bool = Form(False),
    dissolve_field: str = Form(None),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: str = Form(None),
    output_layer: str = Form(None),
):
    try:
        if len(files) < 1:
            raise HTTPException(400, "Upload at least one GeoJSON file")

        frames = []
        for f in files:
            gdf = await read_geojson_upload(f)
            gdf["src_layer"] = f.filename
            frames.append(gdf)

        log_id = log_start(
            input_layer=", ".join(f.filename for f in files),
            overlay_layer=None,
            tool_name="combine",
            data_type="vector",
            project_id=project_id,
            business_id=business_id,
            file_path=file_path,
            output_layer=output_layer,
        )

        submit_job(
            _run_combine, log_id,
            frames=frames,
            dissolve=dissolve,
            dissolve_field=dissolve_field,
            name=output_layer or f"combine_{log_id[:8]}",
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Combine job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
