# app/services/vector_to_raster_service.py
import os
import traceback

import numpy as np
import pandas as pd
import rasterio
from fastapi import Body, File, Form, HTTPException, UploadFile
from pyproj import CRS
from rasterio.features import rasterize
from rasterio.transform import from_origin

from app.schemas.PayloadSchemas import DBConnectionPayload, VectorToRasterParams
from app.schemas.RasterPayloadSchemas import (
    PixelUnit,
    Rasters3ConnectionPayload,
    StorageMethod,
)
from app.services.process_service import log_start
from app.utils.dask_utils import client_context, submit_job
from app.utils.db_utils import get_engine_from_payload, load_gdf_from_db_by_engine
from app.utils.s3_utils import store_output
from app.utils.vector_utils import read_geojson_upload, temp_path


# ---------------------------------------------------------------------------
# Runs on a Dask worker
# ---------------------------------------------------------------------------
def _rasterize_window(shapes, out_shape, transform, dtype="float32"):
    """Burn one horizontal band of the grid. One task per band."""
    return rasterize(
        shapes=shapes,
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype=dtype,
        all_touched=True,
    )


def _prepare_gdf(gdf, pixel_unit):
    if gdf.empty:
        raise ValueError("Input layer is empty.")
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)

    crs_obj = CRS(gdf.crs)
    if pixel_unit == "meters" and crs_obj.is_geographic:
        try:
            gdf = gdf.to_crs(gdf.estimate_utm_crs())
        except Exception:
            gdf = gdf.to_crs(epsg=3857)
    elif pixel_unit == "degrees" and crs_obj.is_projected:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _run_vector_to_raster(gdf, pixel_size, pixel_unit, attribute_field,
                          burn_value, output_name, storage_method,
                          business_id, project_id, bucket=None,
                          access_key=None, secret_key=None):
    gdf = _prepare_gdf(gdf, pixel_unit)

    minx, miny, maxx, maxy = gdf.total_bounds
    width = int(np.ceil((maxx - minx) / pixel_size))
    height = int(np.ceil((maxy - miny) / pixel_size))
    if width <= 0 or height <= 0:
        raise ValueError("Invalid raster dimensions - check pixel_size.")
    if width * height > 500_000_000:
        raise ValueError(
            f"Grid would be {width} x {height} px. Use a larger pixel_size."
        )
    transform = from_origin(minx, maxy, pixel_size, pixel_size)

    if attribute_field and attribute_field in gdf.columns:
        values = gdf[attribute_field]
        # pandas extension dtypes (StringDtype, nullable Int64) blow up
        # np.issubdtype, so ask pandas instead of numpy.
        if not pd.api.types.is_numeric_dtype(values):
            codes, _ = pd.factorize(values)
            values = codes + 1          # 0 stays "not burnt"
        pairs = list(zip(gdf.geometry, values))
    else:
        pairs = [(geom, burn_value) for geom in gdf.geometry]

    # Split the grid into horizontal bands and burn them across the cluster.
    band_height = max(256, height // max(1, len(pairs) // 5000 or 1))
    bands = [(row, min(band_height, height - row))
             for row in range(0, height, band_height)]

    with client_context() as client:
        futures = []
        for row_off, rows in bands:
            band_transform = from_origin(minx, maxy - row_off * pixel_size,
                                         pixel_size, pixel_size)
            futures.append(client.submit(
                _rasterize_window, pairs, (rows, width), band_transform, pure=False
            ))
        blocks = client.gather(futures)

    raster = np.zeros((height, width), dtype="float32")
    for (row_off, rows), block in zip(bands, blocks):
        raster[row_off:row_off + rows, :] = block

    out_path = temp_path(".tif", output_name)
    profile = {
        "driver": "GTiff", "width": width, "height": height, "count": 1,
        "dtype": "float32", "crs": gdf.crs, "transform": transform,
        "nodata": 0, "compress": "deflate", "tiled": True,
        "blockxsize": 256, "blockysize": 256,
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(raster, 1)

    stored = store_output(out_path, storage_method, business_id, project_id,
                          os.path.basename(out_path), bucket, access_key, secret_key)

    return {"output": stored, "width": width, "height": height,
            "pixel_size": pixel_size, "pixel_unit": pixel_unit,
            "crs": str(gdf.crs), "features": int(len(gdf))}


def _run_v2r_table(db_connection, schema_name, table_name, where, **kwargs):
    engine = get_engine_from_payload(db_connection)
    gdf = load_gdf_from_db_by_engine(engine, schema_name, table_name, where)
    if gdf.empty:
        raise ValueError("No data found for given parameters")
    return _run_vector_to_raster(gdf=gdf, **kwargs)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
async def vector_to_raster_table(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: VectorToRasterParams = Body(...),
    storage_method: StorageMethod = Body(StorageMethod.TEMP),
):
    try:
        log_id = log_start(
            input_layer=params.table_name,
            overlay_layer=None,
            tool_name="vector_to_raster",
            data_type="raster",
            project_id=payload.project_id,
            business_id=payload.business_id,
            file_path=payload.file_path,
            output_layer=params.output_name,
        )

        submit_job(
            _run_v2r_table, log_id,
            db_connection=payload.db_connection.model_dump(),
            schema_name=params.schema_name,
            table_name=params.table_name,
            where=params.where,
            pixel_size=params.pixel_size,
            pixel_unit=params.pixel_unit,
            attribute_field=params.attribute_field,
            burn_value=params.burn_value,
            output_name=params.output_name,
            storage_method=storage_method.value,
            business_id=payload.business_id,
            project_id=payload.project_id,
            bucket=raster_payload.bucketName,
            access_key=raster_payload.awsAccessKeyId,
            secret_key=raster_payload.awsSecretAccessKey,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Vector to raster job submitted. "
                           "Poll /api/process/v1/status/{uuid}."}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def vector_to_raster_geojson(
    file: UploadFile = File(...),
    pixel_size: float = Form(...),
    pixel_unit: PixelUnit = Form(PixelUnit.meters),
    attribute_field: str = Form(None),
    burn_value: int = Form(1),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: str = Form(None),
    storage_method: StorageMethod = Form(StorageMethod.TEMP),
    awsAccessKeyId: str = Form(None),
    awsSecretAccessKey: str = Form(None),
):
    try:
        gdf = await read_geojson_upload(file)

        log_id = log_start(
            input_layer=file.filename,
            overlay_layer=None,
            tool_name="vector_to_raster",
            data_type="raster",
            project_id=project_id,
            business_id=business_id,
            file_path=None,
            output_layer=output_name,
        )

        submit_job(
            _run_vector_to_raster, log_id,
            gdf=gdf,
            pixel_size=pixel_size,
            pixel_unit=pixel_unit.value,
            attribute_field=attribute_field,
            burn_value=burn_value,
            output_name=output_name,
            storage_method=storage_method.value,
            business_id=business_id,
            project_id=project_id,
            access_key=awsAccessKeyId,
            secret_key=awsSecretAccessKey,
        )

        return {"uuid": log_id, "status": "processing",
                "message": "Vector to raster job submitted. "
                           "Poll /api/process/v1/status/{uuid}."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
