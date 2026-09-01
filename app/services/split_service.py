# app/services/split_service.py
import traceback

import geopandas as gpd
from fastapi import Body, File, Form, HTTPException, UploadFile
from shapely.geometry import box

from app.schemas.PayloadSchemas import DBConnectionPayload, SplitTableParams
from app.services.process_service import log_start
from app.utils.dask_utils import client_context, submit_job
from app.utils.db_utils import get_engine_from_payload, load_gdf_from_db_by_engine
from app.utils.vector_utils import read_geojson_upload, save_vector_result


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _clip_to_tile(gdf, tile=None):
    """Clip one candidate subset to a single grid tile."""
    if len(gdf) == 0:
        return gdf
    return gpd.clip(gdf, tile, keep_geom_type=True)


def _split_by_attribute(gdf, split_field):
    if split_field not in gdf.columns:
        raise ValueError(f"Field '{split_field}' not found in the layer")
    parts = []
    for value in gdf[split_field].dropna().unique():
        subset = gdf[gdf[split_field] == value]
        if len(subset):
            parts.append((str(value), subset.reset_index(drop=True)))
    return parts


def _split_by_grid(gdf, rows, cols):
    """Cut the extent into rows x cols tiles; each tile is clipped in parallel."""
    minx, miny, maxx, maxy = gdf.total_bounds
    step_x = (maxx - minx) / cols
    step_y = (maxy - miny) / rows

    tiles, keys = [], []
    for r in range(rows):
        for c in range(cols):
            x0 = minx + c * step_x
            y0 = miny + r * step_y
            tiles.append(box(x0, y0, x0 + step_x, y0 + step_y))
            keys.append(f"r{r}c{c}")

    sindex = gdf.sindex
    with client_context() as client:
        futures = []
        for tile in tiles:
            candidates = gdf.iloc[sindex.query(tile, predicate="intersects")]
            futures.append(
                client.submit(_clip_to_tile, candidates, tile=tile, pure=False)
            )
        clipped_tiles = client.gather(futures)

    parts = []
    for key, clipped in zip(keys, clipped_tiles):
        if clipped is not None and len(clipped):
            parts.append((key, clipped.reset_index(drop=True)))
    return parts


def _split_to_parts(gdf):
    exploded = gdf.explode(index_parts=False, ignore_index=True)
    return [("parts", exploded)]


def _run_split(gdf, mode, split_field, grid_rows, grid_cols, name,
               db_connection=None, schema_name=None, output_layer=None):
    if mode == "attribute":
        parts = _split_by_attribute(gdf, split_field)
    elif mode == "grid":
        parts = _split_by_grid(gdf, grid_rows or 2, grid_cols or 2)
    else:
        parts = _split_to_parts(gdf)

    if not parts:
        raise ValueError("The split produced no non-empty outputs")

    outputs = []
    for key, part in parts:
        part_name = f"{name}_{key}"
        layer = f"{output_layer}_{key}" if output_layer else None
        entry = save_vector_result(part, part_name, db_connection,
                                   schema_name, layer)
        entry["key"] = key
        outputs.append(entry)

    return {"mode": mode, "parts": len(outputs), "outputs": outputs}


def _run_split_table(db_connection, schema_name, table_name, where, mode,
                     split_field, grid_rows, grid_cols, name, output_layer):
    engine = get_engine_from_payload(db_connection)
    gdf = load_gdf_from_db_by_engine(engine, schema_name, table_name, where)
    if gdf.empty:
        raise ValueError("No data found for given parameters")
    return _run_split(gdf, mode, split_field, grid_rows, grid_cols, name,
                      db_connection, schema_name, output_layer)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def split_from_table(
    payload: DBConnectionPayload = Body(...),
    params: SplitTableParams = Body(...),
):
    try:
        if params.mode == "attribute" and not params.split_field:
            raise HTTPException(400, "split_field is required when mode='attribute'")

        log_id = log_start(
            input_layer=params.table_name,
            overlay_layer=None,
            tool_name="split",
            data_type="vector",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=payload.output_layer,
        )

        submit_job(
            _run_split_table, log_id,
            db_connection=payload.db_connection.model_dump(),
            schema_name=params.schema_name,
            table_name=params.table_name,
            where=params.where,
            mode=params.mode.value,
            split_field=params.split_field,
            grid_rows=params.grid_rows,
            grid_cols=params.grid_cols,
            name=payload.output_layer or f"split_{log_id[:8]}",
            output_layer=payload.output_layer,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Split job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def split_from_geojson(
    file: UploadFile = File(...),
    mode: str = Form("attribute"),
    split_field: str = Form(None),
    grid_rows: int = Form(2),
    grid_cols: int = Form(2),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: str = Form(None),
    output_layer: str = Form(None),
):
    try:
        if mode == "attribute" and not split_field:
            raise HTTPException(400, "split_field is required when mode='attribute'")

        gdf = await read_geojson_upload(file)

        log_id = log_start(
            input_layer=file.filename,
            overlay_layer=None,
            tool_name="split",
            data_type="vector",
            project_id=project_id,
            business_id=business_id,
            file_path=file_path,
            output_layer=output_layer,
        )

        submit_job(
            _run_split, log_id,
            gdf=gdf,
            mode=mode,
            split_field=split_field,
            grid_rows=grid_rows,
            grid_cols=grid_cols,
            name=output_layer or f"split_{log_id[:8]}",
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Split job submitted. Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
