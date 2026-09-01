from typing import List, Literal, Optional

from fastapi import APIRouter, Body, File, Form, UploadFile

from app.schemas.PayloadSchemas import (
    BufferTableParams,
    CombineTableParams,
    DBConnectionPayload,
    SplitTableParams,
    TrimTableParams,
)
from app.services import buffer_service, combine_service, split_service, trim_service

router = APIRouter()


# ---------------------------------------------------------------- Buffer ----
@router.post("/v1/buffer-from-table")
async def buffer_from_table(
    payload: DBConnectionPayload = Body(...),
    params: BufferTableParams = Body(...),
):
    return await buffer_service.buffer_from_table(payload=payload, params=params)


@router.post("/v1/buffer-from-geojson")
async def buffer_from_geojson(
    file: UploadFile = File(...),
    distance: float = Form(...),
    unit: Literal["degrees", "meters", "kilometers", "feet", "inches", "miles",
                  "nautical miles", "yards", "millimeters"] = Form("meters"),
    cap: Literal["round", "flat", "square"] = Form("round"),
    join: Literal["round", "miter", "bevel"] = Form("round"),
    dissolve: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    return await buffer_service.buffer_from_geojson(
        file=file, distance=distance, unit=unit, cap=cap, join=join,
        dissolve=dissolve, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# ----------------------------------------------------------------- Split ----
@router.post("/v1/split-from-table")
async def split_from_table(
    payload: DBConnectionPayload = Body(...),
    params: SplitTableParams = Body(...),
):
    return await split_service.split_from_table(payload=payload, params=params)


@router.post("/v1/split-from-geojson")
async def split_from_geojson(
    file: UploadFile = File(...),
    mode: Literal["attribute", "grid", "parts"] = Form("attribute"),
    split_field: Optional[str] = Form(None),
    grid_rows: int = Form(2),
    grid_cols: int = Form(2),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    return await split_service.split_from_geojson(
        file=file, mode=mode, split_field=split_field, grid_rows=grid_rows,
        grid_cols=grid_cols, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# --------------------------------------------------------------- Combine ----
@router.post("/v1/combine-from-table")
async def combine_from_table(
    payload: DBConnectionPayload = Body(...),
    params: CombineTableParams = Body(...),
):
    return await combine_service.combine_from_table(payload=payload, params=params)


@router.post("/v1/combine-from-geojson")
async def combine_from_geojson(
    files: List[UploadFile] = File(...),
    dissolve: bool = Form(False),
    dissolve_field: Optional[str] = Form(None),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    return await combine_service.combine_from_geojson(
        files=files, dissolve=dissolve, dissolve_field=dissolve_field,
        business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# ------------------------------------------------------------------ Trim ----
@router.post("/v1/trim-from-table")
async def trim_from_table(
    payload: DBConnectionPayload = Body(...),
    params: TrimTableParams = Body(...),
):
    return await trim_service.trim_from_table(payload=payload, params=params)


@router.post("/v1/trim-from-geojson")
async def trim_from_geojson(
    input_file: UploadFile = File(...),
    overlay_file: Optional[UploadFile] = File(None),
    bbox: Optional[str] = Form(None),
    invert: bool = Form(False),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    return await trim_service.trim_from_geojson(
        input_file=input_file, overlay_file=overlay_file, bbox=bbox,
        invert=invert, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )
