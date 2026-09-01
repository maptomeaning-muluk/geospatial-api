# app/services/raster_to_vector_service.py
import traceback

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from fastapi import Body, File, Form, HTTPException, UploadFile
from rasterio.features import shapes
from shapely.geometry import shape

from app.schemas.PayloadSchemas import DBConnectionPayload, RasterToVectorParams
from app.schemas.RasterPayloadSchemas import Rasters3ConnectionPayload
from app.services.process_service import log_start
from app.utils.dask_utils import client_context, submit_job
from app.utils.s3_utils import resolve_input_path
from app.utils.vector_utils import output_dir, save_vector_result, temp_path


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _polygonize_window(raster_path, band, window, field_name, ignore_value):
    """Vectorise one window of the raster. One task per window."""
    from rasterio.windows import Window

    col_off, row_off, width, height = window
    with rasterio.open(raster_path) as src:
        win = Window(col_off, row_off, width, height)
        data = src.read(band, window=win)
        transform = src.window_transform(win)
        nodata = src.nodata
        crs = src.crs

    skip = ignore_value if ignore_value is not None else nodata

    mask = np.ones(data.shape, dtype=bool)
    if skip is not None and skip == skip:          # not NaN
        mask &= data != skip
    if np.issubdtype(data.dtype, np.floating):
        mask &= ~np.isnan(data)

    if not mask.any():
        return gpd.GeoDataFrame({field_name: []}, geometry=[], crs=crs)

    source = data
    if source.dtype.name not in ("uint8", "int16", "int32", "float32", "float64"):
        source = source.astype("int32")

    geoms, values = [], []
    for geom, value in shapes(source, mask=mask, transform=transform):
        geoms.append(shape(geom))
        values.append(value)

    return gpd.GeoDataFrame({field_name: values}, geometry=geoms, crs=crs)


def _run_raster_to_vector(raster_file, band, field_name, ignore_value, dissolve,
                          name, db_connection=None, schema_name=None,
                          output_layer=None, access_key=None, secret_key=None):
    local_path = resolve_input_path(raster_file, output_dir(),
                                    access_key, secret_key)

    with rasterio.open(local_path) as src:
        if band > src.count:
            raise ValueError(f"Band {band} requested but raster has {src.count}")
        width, height, crs = src.width, src.height, src.crs

    # One window per 2048 px row block, vectorised in parallel.
    step = 2048 if width * height > 2048 * 2048 else max(height, 1)
    windows = [(0, row, width, min(step, height - row))
               for row in range(0, height, step)]

    with client_context() as client:
        futures = [
            client.submit(_polygonize_window, local_path, band, w, field_name,
                          ignore_value, pure=False)
            for w in windows
        ]
        pieces = [p for p in client.gather(futures) if p is not None and len(p)]

    if not pieces:
        raise ValueError("Vectorisation produced no features - "
                         "check the band and ignore_value.")

    merged = pd.concat(pieces, ignore_index=True)
    gdf = gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)

    if dissolve:
        gdf = gdf.dissolve(by=field_name, as_index=False)
        gdf = gdf.explode(index_parts=False, ignore_index=True)

    out = save_vector_result(gdf, name, db_connection, schema_name, output_layer)
    out.update({"source_raster": raster_file, "band": band,
                "windows": len(windows), "dissolved": dissolve})
    return out


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def raster_to_vector_from_file(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: RasterToVectorParams = Body(...),
):
    """Vectorise a raster given as a local path or an s3:// URI."""
    try:
        log_id = log_start(
            input_layer=params.raster_file,
            overlay_layer=None,
            tool_name="raster_to_vector",
            data_type="vector",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=payload.output_layer,
        )

        submit_job(
            _run_raster_to_vector, log_id,
            raster_file=params.raster_file,
            band=params.band,
            field_name=params.field_name,
            ignore_value=params.ignore_value,
            dissolve=params.dissolve,
            name=payload.output_layer or params.output_name or f"r2v_{log_id[:8]}",
            db_connection=payload.db_connection.model_dump(),
            schema_name=None,
            output_layer=payload.output_layer,
            access_key=raster_payload.awsAccessKeyId,
            secret_key=raster_payload.awsSecretAccessKey,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Raster to vector job submitted. "
                           "Poll /api/process/v1/status/{uuid}."}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def raster_to_vector_upload(
    file: UploadFile = File(...),
    band: int = Form(1),
    field_name: str = Form("value"),
    ignore_value: float = Form(None),
    dissolve: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: str = Form(None),
):
    """Vectorise an uploaded GeoTIFF."""
    try:
        local_path = temp_path(".tif", file.filename)
        with open(local_path, "wb") as fh:
            while chunk := await file.read(4 << 20):
                fh.write(chunk)

        log_id = log_start(
            input_layer=file.filename,
            overlay_layer=None,
            tool_name="raster_to_vector",
            data_type="vector",
            project_id=project_id,
            business_id=business_id,
            file_path=None,
            output_layer=output_name,
        )

        submit_job(
            _run_raster_to_vector, log_id,
            raster_file=local_path,
            band=band,
            field_name=field_name,
            ignore_value=ignore_value,
            dissolve=dissolve,
            name=output_name or f"r2v_{log_id[:8]}",
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Raster to vector job submitted. "
                           "Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
