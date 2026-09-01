"""Raster tool routes: Vector -> Raster and Raster -> Vector.

Like the vector routes, these are asynchronous: the endpoint returns a job
`uuid` immediately and the work runs on the Dask cluster. Poll
`GET /api/process/v1/status/{uuid}`.

Raster formats, measured against the running service:

* **GeoTIFF** with a CRS is the intended input - georeferencing is carried
  through to the output geometry.
* `.img`, `.vrt`, `.jp2` and any other GDAL-readable format work the same way.
* **PNG** has no CRS: it vectorises, but the coordinates are pixel offsets. The
  result carries `"georeferenced": false` and a `warning`.
* **JPEG** is lossy as well as ungeoreferenced - a 4-class test image came back
  as 80 polygons instead of 4. Do not use it for classified rasters.
"""

from typing import Optional

from fastapi import APIRouter, Body, File, Form, UploadFile

from app.schemas.PayloadSchemas import (
    DBConnectionPayload,
    RasterToVectorParams,
    VectorToRasterParams,
)
from app.schemas.RasterPayloadSchemas import (
    PixelUnit,
    Rasters3ConnectionPayload,
    StorageMethod,
)
from app.services import raster_to_vector_service, vector_to_raster_service

router = APIRouter()


# ------------------------------------------------------- Vector to Raster ----
@router.post(
    "/v1/vector-to-raster-from-table",
    summary="Vector to Raster - burn features into a grid (PostGIS table in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def vector_to_raster_from_table(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: VectorToRasterParams = Body(...),
    storage_method: StorageMethod = Body(
        StorageMethod.TEMP,
        description="temp = local file | own = bucket in raster_payload | alloc = bucket in .env",
    ),
):
    """
    Burn vector features into a raster grid.

    ### Input

    | | |
    |---|---|
    | Source | PostGIS table |
    | Geometry types | Point, LineString, Polygon, Multi* - all supported. `all_touched=True`, so thin lines and single points still produce pixels. |
    | `pixel_size` | pixel edge length, in `pixel_unit` |
    | `pixel_unit` | `meters` (a geographic layer is reprojected to its local UTM zone first) or `degrees` |
    | `attribute_field` | column to burn. Non-numeric columns are factorised to 1..N and the codes are burnt. |
    | `burn_value` | constant to burn when `attribute_field` is omitted (default 1) |
    | `storage_method` | `temp` local file, `own` bucket in `raster_payload`, `alloc` bucket from `.env` |

    A grid larger than 500 megapixels is refused - raise `pixel_size`.

    ### Output

    Immediate (HTTP 200):

    ```json
    {"uuid": "3f9c1e2b-...", "status": "processing", "message": "..."}
    ```

    On completion:

    ```json
    {"data": {"status": "completed",
              "result": {"output": "./output/output_ab12.tif",
                         "width": 178, "height": 186,
                         "pixel_size": 30.0, "pixel_unit": "meters",
                         "crs": "EPSG:32643", "features": 25}}}
    ```

    File out: **single-band float32 GeoTIFF**, deflate-compressed and tiled,
    `nodata=0`. With `storage_method` `own`/`alloc`, `output` is an
    `s3://bucket/key` URI instead of a local path.
    """
    return await vector_to_raster_service.vector_to_raster_table(
        payload=payload, raster_payload=raster_payload,
        params=params, storage_method=storage_method,
    )


@router.post(
    "/v1/vector-to-raster-from-geojson",
    summary="Vector to Raster - burn features into a grid (GeoJSON upload in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def vector_to_raster_from_geojson(
    file: UploadFile = File(..., description="GeoJSON FeatureCollection (any geometry type)"),
    pixel_size: float = Form(..., description="Pixel edge length in pixel_unit"),
    pixel_unit: PixelUnit = Form(PixelUnit.meters),
    attribute_field: Optional[str] = Form(
        None, description="Column to burn; non-numeric values are factorised to 1..N"),
    burn_value: int = Form(1, description="Constant burnt when attribute_field is omitted"),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: Optional[str] = Form(None, description="Output filename, e.g. pop_30m.tif"),
    storage_method: StorageMethod = Form(StorageMethod.TEMP),
    awsAccessKeyId: Optional[str] = Form(None),
    awsSecretAccessKey: Optional[str] = Form(None),
):
    """
    Burn an uploaded GeoJSON layer into a raster grid.

    ### Input

    `multipart/form-data`; `file` = GeoJSON FeatureCollection. Point,
    LineString, Polygon and Multi* all supported. With `pixel_unit=meters` a
    geographic layer is reprojected to its local UTM zone before burning.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion `result`
    holds `output`, `width`, `height`, `pixel_size`, `pixel_unit`, `crs` and
    `features`. File out: **single-band float32 GeoTIFF**.
    """
    return await vector_to_raster_service.vector_to_raster_geojson(
        file=file, pixel_size=pixel_size, pixel_unit=pixel_unit,
        attribute_field=attribute_field, burn_value=burn_value,
        business_id=business_id, project_id=project_id,
        output_name=output_name, storage_method=storage_method,
        awsAccessKeyId=awsAccessKeyId, awsSecretAccessKey=awsSecretAccessKey,
    )


# ------------------------------------------------------- Raster to Vector ----
@router.post(
    "/v1/raster-to-vector-from-file",
    summary="Raster to Vector - polygonize a raster (local path or s3:// in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def raster_to_vector_from_file(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: RasterToVectorParams = Body(...),
):
    """
    Trace contiguous same-value pixel regions into polygons.

    ### Input

    | | |
    |---|---|
    | `raster_file` | a local path **or** an `s3://bucket/key` URI |
    | Formats | **GeoTIFF** (intended), plus `.img`, `.vrt`, `.jp2`, any GDAL format. PNG/JPEG work but are not georeferenced - see below. |
    | `band` | 1-based band index (default 1) |
    | `field_name` | column that receives the pixel value (default `value`) |
    | `ignore_value` | pixels to skip; defaults to the raster's own nodata |
    | `dissolve` | merge polygons sharing a value, then explode to contiguous regions |

    The raster is vectorised in 2048-row windows, one Dask task per window.

    **Ungeoreferenced input.** A PNG/JPEG has no CRS, so the output coordinates
    are pixel offsets rather than real-world coordinates; the result carries
    `"georeferenced": false` and a `warning`. JPEG is also lossy - a 4-class
    test image produced 80 polygons instead of 4.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`. On completion:

    ```json
    {"data": {"status": "completed",
              "result": {"feature_count": 4, "crs": "EPSG:32643",
                         "band": 1, "windows": 1, "dissolved": true,
                         "georeferenced": true,
                         "source_raster": "/data/landcover.tif",
                         "file": "./output/<name>.geojson",
                         "table": "public.<output_layer>"}}}
    ```

    Geometry out: **Polygon**, one per contiguous same-value region, with the
    pixel value in `field_name`.
    """
    return await raster_to_vector_service.raster_to_vector_from_file(
        payload=payload, raster_payload=raster_payload, params=params,
    )


@router.post(
    "/v1/raster-to-vector-from-upload",
    summary="Raster to Vector - polygonize a raster (file upload in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def raster_to_vector_from_upload(
    file: UploadFile = File(..., description="GeoTIFF preferred. PNG/JPEG are accepted but "
                                             "have no CRS, and JPEG is lossy."),
    band: int = Form(1, description="1-based band index"),
    field_name: str = Form("value", description="Column that receives the pixel value"),
    ignore_value: Optional[float] = Form(
        None, description="Pixels to skip; defaults to the raster's nodata"),
    dissolve: bool = Form(False, description="Merge polygons sharing a value"),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: Optional[str] = Form(None),
):
    """
    Polygonize an uploaded raster.

    ### Input

    `multipart/form-data`; `file` is streamed to disk and opened with rasterio.
    **GeoTIFF** is the intended format. PNG and JPEG are readable but carry no
    CRS, so the output is in pixel coordinates and flagged
    `"georeferenced": false`; JPEG additionally shreds flat class blocks
    because it is lossy.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion `result`
    holds `feature_count`, `crs`, `band`, `windows`, `dissolved`,
    `georeferenced`, `file` and optionally `table`. Geometry out: **Polygon**.
    """
    return await raster_to_vector_service.raster_to_vector_upload(
        file=file, band=band, field_name=field_name,
        ignore_value=ignore_value, dissolve=dissolve,
        business_id=business_id, project_id=project_id, output_name=output_name,
    )


# ---------------------------------------------------------------------------
# Swagger "Try it out" bodies - see the note in geo_routes.py for why these
# live here rather than in Body(..., openapi_examples=...).
# ---------------------------------------------------------------------------
_DB = {"host": "localhost", "port": 5432, "dbname": "gisdb",
       "user": "gis", "password": "***"}

vector_to_raster_from_table.openapi_examples = {
    "attribute_30m": {
        "summary": "Burn a population column at 30 m, keep the file locally",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "raster_payload": {},
            "params": {"schema_name": "public", "table_name": "parcels",
                       "pixel_size": 30, "pixel_unit": "meters",
                       "attribute_field": "population"},
            "storage_method": "temp",
        },
    },
    "mask_to_s3": {
        "summary": "Burn a constant 1 as a presence mask, upload to S3",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "raster_payload": {"awsAccessKeyId": "AKIA...",
                               "awsSecretAccessKey": "***",
                               "bucketName": "my-bucket",
                               "region": "ap-south-1"},
            "params": {"schema_name": "public", "table_name": "parcels",
                       "pixel_size": 10, "pixel_unit": "meters",
                       "burn_value": 1, "output_name": "parcels_mask.tif"},
            "storage_method": "own",
        },
    },
}

raster_to_vector_from_file.openapi_examples = {
    "local_geotiff": {
        "summary": "Polygonize a local classified GeoTIFF into PostGIS",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1", "output_layer": "landcover_polygons"},
            "raster_payload": {},
            "params": {"raster_file": "/data/landcover.tif", "band": 1,
                       "field_name": "class_id", "dissolve": True},
        },
    },
    "from_s3": {
        "summary": "Polygonize straight from S3",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "raster_payload": {"awsAccessKeyId": "AKIA...",
                               "awsSecretAccessKey": "***",
                               "region": "ap-south-1"},
            "params": {"raster_file": "s3://my-bucket/landcover.tif",
                       "band": 1, "field_name": "class_id"},
        },
    },
}
