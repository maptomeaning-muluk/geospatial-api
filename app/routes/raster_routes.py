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
@router.post("/v1/vector-to-raster-from-table")
async def vector_to_raster_from_table(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: VectorToRasterParams = Body(...),
    storage_method: StorageMethod = Body(StorageMethod.TEMP),
):
    return await vector_to_raster_service.vector_to_raster_table(
        payload=payload, raster_payload=raster_payload,
        params=params, storage_method=storage_method,
    )


@router.post("/v1/vector-to-raster-from-geojson")
async def vector_to_raster_from_geojson(
    file: UploadFile = File(...),
    pixel_size: float = Form(...),
    pixel_unit: PixelUnit = Form(PixelUnit.meters),
    attribute_field: Optional[str] = Form(None),
    burn_value: int = Form(1),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: Optional[str] = Form(None),
    storage_method: StorageMethod = Form(StorageMethod.TEMP),
    awsAccessKeyId: Optional[str] = Form(None),
    awsSecretAccessKey: Optional[str] = Form(None),
):
    return await vector_to_raster_service.vector_to_raster_geojson(
        file=file, pixel_size=pixel_size, pixel_unit=pixel_unit,
        attribute_field=attribute_field, burn_value=burn_value,
        business_id=business_id, project_id=project_id,
        output_name=output_name, storage_method=storage_method,
        awsAccessKeyId=awsAccessKeyId, awsSecretAccessKey=awsSecretAccessKey,
    )


# ------------------------------------------------------- Raster to Vector ----
@router.post("/v1/raster-to-vector-from-file")
async def raster_to_vector_from_file(
    payload: DBConnectionPayload = Body(...),
    raster_payload: Rasters3ConnectionPayload = Body(...),
    params: RasterToVectorParams = Body(...),
):
    return await raster_to_vector_service.raster_to_vector_from_file(
        payload=payload, raster_payload=raster_payload, params=params,
    )


@router.post("/v1/raster-to-vector-from-upload")
async def raster_to_vector_from_upload(
    file: UploadFile = File(...),
    band: int = Form(1),
    field_name: str = Form("value"),
    ignore_value: Optional[float] = Form(None),
    dissolve: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    output_name: Optional[str] = Form(None),
):
    return await raster_to_vector_service.raster_to_vector_upload(
        file=file, band=band, field_name=field_name,
        ignore_value=ignore_value, dissolve=dissolve,
        business_id=business_id, project_id=project_id, output_name=output_name,
    )
