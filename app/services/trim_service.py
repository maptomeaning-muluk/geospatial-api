# app/services/trim_service.py
import traceback

import geopandas as gpd
from fastapi import Body, File, Form, HTTPException, UploadFile
from shapely.geometry import box

from app.schemas.PayloadSchemas import DBConnectionPayload, TrimTableParams
from app.services.process_service import log_start
from app.utils.dask_utils import parallel_map, submit_job
from app.utils.db_utils import get_engine_from_payload, load_gdf_from_db_by_engine
from app.utils.vector_utils import (
    clean_geom,
    read_geojson_upload,
    save_vector_result,
)


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _trim_chunk(gdf, mask=None, invert=False):
    """Clip a slice to the mask, or erase the mask out of it."""
    if len(gdf) == 0:
        return gdf
    if invert:
        out = gdf.copy()
        geom_col = out.geometry.name
        out[geom_col] = out.geometry.difference(mask)
        return out[~out.geometry.is_empty]
    return gpd.clip(gdf, mask, keep_geom_type=True)


def _build_mask(overlay_gdf=None, bbox=None, target_crs=None):
    if bbox is not None:
        if len(bbox) != 4:
            raise ValueError("bbox must be [minx, miny, maxx, maxy]")
        return box(*bbox)

    if overlay_gdf is None or overlay_gdf.empty:
        raise ValueError("Provide either an overlay layer or a bbox")

    if target_crs is not None and overlay_gdf.crs is not None \
            and str(overlay_gdf.crs) != str(target_crs):
        overlay_gdf = overlay_gdf.to_crs(target_crs)

    return overlay_gdf.union_all() if hasattr(overlay_gdf, "union_all") \
        else overlay_gdf.unary_union


def _run_trim(gdf, overlay_gdf, bbox, invert, name,
              db_connection=None, schema_name=None, output_layer=None):
    gdf = gdf.copy()
    gdf[gdf.geometry.name] = gdf.geometry.apply(clean_geom)

    mask = _build_mask(overlay_gdf, bbox, gdf.crs)

    result_gdf = parallel_map(gdf, _trim_chunk, mask=mask, invert=invert)

    out = save_vector_result(result_gdf, name, db_connection, schema_name,
                             output_layer)
    out.update({"operation": "erase" if invert else "clip",
                "input_features": int(len(gdf))})
    return out


def _run_trim_table(db_connection, schema_name, input_table, input_where,
                    overlay_table, overlay_where, bbox, invert, name,
                    output_layer):
    engine = get_engine_from_payload(db_connection)
    gdf = load_gdf_from_db_by_engine(engine, schema_name, input_table, input_where)
    if gdf.empty:
        raise ValueError("No data found for given parameters")

    overlay_gdf = None
    if overlay_table:
        overlay_gdf = load_gdf_from_db_by_engine(engine, schema_name,
                                                 overlay_table, overlay_where)
        if overlay_gdf.empty:
            raise ValueError("The overlay layer is empty")

    return _run_trim(gdf, overlay_gdf, bbox, invert, name,
                     db_connection, schema_name, output_layer)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def trim_from_table(
    payload: DBConnectionPayload = Body(...),
    params: TrimTableParams = Body(...),
):
    try:
        if not params.overlay_table and not params.bbox:
            raise HTTPException(400, "Provide either overlay_table or bbox")

        log_id = log_start(
            input_layer=params.input_table,
            overlay_layer=params.overlay_table,
            tool_name="trim",
            data_type="vector",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=payload.output_layer,
        )

        submit_job(
            _run_trim_table, log_id,
            db_connection=payload.db_connection.model_dump(),
            schema_name=params.schema_name,
            input_table=params.input_table,
            input_where=params.input_where,
            overlay_table=params.overlay_table,
            overlay_where=params.overlay_where,
            bbox=params.bbox,
            invert=params.invert,
            name=payload.output_layer or f"trim_{log_id[:8]}",
            output_layer=payload.output_layer,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Trim job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def trim_from_geojson(
    input_file: UploadFile = File(...),
    overlay_file: UploadFile = File(None),
    bbox: str = Form(None),          # "minx,miny,maxx,maxy"
    invert: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: str = Form(None),
    output_layer: str = Form(None),
):
    try:
        gdf = await read_geojson_upload(input_file)

        overlay_gdf = None
        parsed_bbox = None
        if overlay_file is not None:
            overlay_gdf = await read_geojson_upload(overlay_file)
        elif bbox:
            try:
                parsed_bbox = [float(v) for v in bbox.split(",")]
            except ValueError:
                raise HTTPException(400, "bbox must be 'minx,miny,maxx,maxy'")
        else:
            raise HTTPException(400, "Provide either an overlay_file or a bbox")

        log_id = log_start(
            input_layer=input_file.filename,
            overlay_layer=overlay_file.filename if overlay_file else None,
            tool_name="trim",
            data_type="vector",
            project_id=project_id,
            business_id=business_id,
            file_path=file_path,
            output_layer=output_layer,
        )

        submit_job(
            _run_trim, log_id,
            gdf=gdf,
            overlay_gdf=overlay_gdf,
            bbox=parsed_bbox,
            invert=invert,
            name=output_layer or f"trim_{log_id[:8]}",
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Trim job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
