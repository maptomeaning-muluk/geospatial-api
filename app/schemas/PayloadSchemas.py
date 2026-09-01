from typing import Optional, Literal, List
from pydantic import BaseModel, Field
from enum import Enum


class DBConnectionDetails(BaseModel):
    host: str
    port: int
    dbname: str
    user: str
    password: str


class DBConnectionPayload(BaseModel):
    db_connection: DBConnectionDetails
    project_id: str
    business_id: str
    file_path: Optional[str] = None       # example: "s3://my-bucket/business123/output/"
    output_layer: Optional[str] = None    # output table name written back to PostGIS


class BufferTableParams(BaseModel):
    schema_name: str
    table_name: str
    where: Optional[str] = None
    distance: float
    cap: Literal["round", "flat", "square"] = "round"
    join: Literal["round", "miter", "bevel"] = "round"
    unit: Literal[
        "degrees",
        "meters",
        "kilometers",
        "feet",
        "inches",
        "miles",
        "nautical miles",
        "yards",
        "millimeters",
    ] = "meters"
    dissolve: bool = False


class SplitMode(str, Enum):
    attribute = "attribute"
    grid = "grid"
    parts = "parts"


class SplitTableParams(BaseModel):
    schema_name: str
    table_name: str
    where: Optional[str] = None
    mode: SplitMode = SplitMode.attribute
    split_field: Optional[str] = None            # required for mode = attribute
    grid_rows: Optional[int] = Field(default=2, ge=1, le=100)
    grid_cols: Optional[int] = Field(default=2, ge=1, le=100)


class CombineTableParams(BaseModel):
    schema_name: str
    table_names: List[str] = Field(min_length=1)   # one or more tables to merge
    where: Optional[str] = None
    dissolve: bool = False
    dissolve_field: Optional[str] = None


class TrimTableParams(BaseModel):
    schema_name: str
    input_table: str
    input_where: Optional[str] = None
    overlay_table: Optional[str] = None            # polygon table used as the mask
    overlay_where: Optional[str] = None
    bbox: Optional[List[float]] = None             # [minx, miny, maxx, maxy] alternative
    invert: bool = False                           # True = erase the mask instead


class VectorToRasterParams(BaseModel):
    schema_name: str
    table_name: str
    where: Optional[str] = None
    output_name: Optional[str] = None
    pixel_size: float = Field(gt=0, description="Pixel size")
    pixel_unit: Literal["meters", "degrees"] = "meters"
    attribute_field: Optional[str] = None
    burn_value: int = 1


class RasterToVectorParams(BaseModel):
    raster_file: str                               # local path or s3://bucket/key.tif
    band: int = Field(default=1, ge=1)
    field_name: str = "value"
    ignore_value: Optional[float] = None           # skip these pixels (default: nodata)
    dissolve: bool = False                         # merge polygons sharing a value
    output_name: Optional[str] = None
