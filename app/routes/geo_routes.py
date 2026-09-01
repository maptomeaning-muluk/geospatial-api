"""Vector tool routes: Buffer, Split, Combine, Trim.

Every endpoint here is asynchronous. It validates the request, writes a row to
the `processing` table and hands the work to the Dask cluster, then returns
**200** with a job `uuid` - usually within a few milliseconds. Poll
`GET /api/process/v1/status/{uuid}` for progress and the result.

Each tool comes in two flavours:

* `-from-table`   reads an existing PostGIS table (JSON body)
* `-from-geojson` takes an uploaded GeoJSON file (multipart form)

The docstring on each route is what Swagger renders, so the input/output
contract is stated once, next to the code that implements it.
"""

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
@router.post(
    "/v1/buffer-from-table",
    summary="Buffer - grow or shrink geometries (PostGIS table in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def buffer_from_table(
    payload: DBConnectionPayload = Body(...),
    params: BufferTableParams = Body(...),
):
    """
    Grow (or shrink, with a negative distance) geometries by a fixed distance.

    ### Input

    | | |
    |---|---|
    | Source | PostGIS table. The geometry column may be named anything - it is looked up in `information_schema`. |
    | Geometry types | Point, LineString, Polygon and their Multi* forms |
    | `distance` | number; negative values erode |
    | `unit` | `meters` `kilometers` `feet` `inches` `miles` `nautical miles` `yards` `millimeters` `degrees` |
    | `cap` | `round` `flat` `square` - **`flat`/`square` on Point input yields ZERO features** (GEOS needs a direction for a flat cap). Use `round` for points. |
    | `join` | `round` `miter` `bevel` |
    | `dissolve` | merge all buffers into one feature |

    Note: on a geographic CRS the distance uses a single `1/111319.9` degree
    factor, so east-west buffers run short away from the equator (-5% at
    latitude 18.5, -36% at 50). See the README.

    ### Output

    Immediate (HTTP 200):

    ```json
    {"uuid": "3f9c1e2b-...", "status": "processing", "message": "..."}
    ```

    On completion, from `GET /api/process/v1/status/{uuid}`:

    ```json
    {"data": {"status": "completed",
              "result": {"feature_count": 25, "crs": "EPSG:4326",
                         "file": "./output/<name>.geojson",
                         "table": "public.<output_layer>",
                         "distance": 500, "unit": "meters",
                         "dissolved": false}}}
    ```

    Geometry out: **Polygon** (MultiPolygon when `dissolve=true`). `file` is
    always written; `table` appears only when `output_layer` was given.
    """
    return await buffer_service.buffer_from_table(payload=payload, params=params)


@router.post(
    "/v1/buffer-from-geojson",
    summary="Buffer - grow or shrink geometries (GeoJSON upload in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def buffer_from_geojson(
    file: UploadFile = File(..., description="GeoJSON FeatureCollection (.geojson/.json, UTF-8). "
                                             "GeoPackage/Shapefile are NOT accepted here."),
    distance: float = Form(..., description="Buffer distance in `unit`; negative erodes"),
    unit: Literal["degrees", "meters", "kilometers", "feet", "inches", "miles",
                  "nautical miles", "yards", "millimeters"] = Form("meters"),
    cap: Literal["round", "flat", "square"] = Form(
        "round", description="flat/square on Point input yields zero features"),
    join: Literal["round", "miter", "bevel"] = Form("round"),
    dissolve: bool = Form(False, description="Merge all buffers into one feature"),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None,
                                       description="Also write the result to this PostGIS table"),
):
    """
    Buffer an uploaded GeoJSON layer.

    ### Input

    `multipart/form-data`. `file` must be a **GeoJSON FeatureCollection** - a
    `.gpkg`/`.shp`/`.zip` upload is rejected with `400 Invalid GeoJSON file.`
    Accepts Point, LineString, Polygon and Multi* geometries. A layer with no
    CRS is assumed to be EPSG:4326.

    Parameters match `buffer-from-table`; the same `cap="flat"` on Point
    caveat applies.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion
    `result` holds `feature_count`, `crs`, `file` and (with `output_layer`)
    `table`. Geometry out: **Polygon**.
    """
    return await buffer_service.buffer_from_geojson(
        file=file, distance=distance, unit=unit, cap=cap, join=join,
        dissolve=dissolve, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# ----------------------------------------------------------------- Split ----
@router.post(
    "/v1/split-from-table",
    summary="Split - one dataset into many (PostGIS table in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def split_from_table(
    payload: DBConnectionPayload = Body(...),
    params: SplitTableParams = Body(...),
):
    """
    Break one layer into many outputs.

    ### Input

    | | |
    |---|---|
    | Source | PostGIS table |
    | Geometry types | Point, LineString, Polygon, Multi* - all modes |
    | `mode=attribute` | one output per distinct value of `split_field` (**required**) |
    | `mode=grid` | tile the extent into `grid_rows` x `grid_cols`; each tile is clipped on its own Dask task |
    | `mode=parts` | explode Multi* geometries into single parts (one output) |

    Omitting `split_field` with `mode=attribute` returns `400`.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`. On completion `result`
    is a summary plus one entry per part:

    ```json
    {"mode": "attribute", "parts": 3,
     "outputs": [{"key": "D0", "feature_count": 10, "crs": "EPSG:4326",
                  "file": "./output/split_D0.geojson",
                  "table": "public.parcels_D0"}]}
    ```

    Geometry out: **same type as the input**, one dataset per part. With
    `output_layer` set, tables are named `<output_layer>_<key>`, where key is
    the attribute value or `r<row>c<col>`.
    """
    return await split_service.split_from_table(payload=payload, params=params)


@router.post(
    "/v1/split-from-geojson",
    summary="Split - one dataset into many (GeoJSON upload in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def split_from_geojson(
    file: UploadFile = File(..., description="GeoJSON FeatureCollection"),
    mode: Literal["attribute", "grid", "parts"] = Form("attribute"),
    split_field: Optional[str] = Form(None, description="Required when mode=attribute"),
    grid_rows: int = Form(2, description="Used when mode=grid"),
    grid_cols: int = Form(2, description="Used when mode=grid"),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    """
    Split an uploaded GeoJSON layer - see `split-from-table` for the modes.

    ### Input

    `multipart/form-data`; `file` = GeoJSON FeatureCollection. All geometry
    types supported. Omitting `split_field` with `mode=attribute` returns `400`.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion
    `result.outputs` lists one entry per part with `key`, `feature_count`,
    `file` and optionally `table`. Geometry out: **same type as the input**.
    """
    return await split_service.split_from_geojson(
        file=file, mode=mode, split_field=split_field, grid_rows=grid_rows,
        grid_cols=grid_cols, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# --------------------------------------------------------------- Combine ----
@router.post(
    "/v1/combine-from-table",
    summary="Combine - merge several tables into one (PostGIS tables in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def combine_from_table(
    payload: DBConnectionPayload = Body(...),
    params: CombineTableParams = Body(...),
):
    """
    Merge several layers into one.

    ### Input

    | | |
    |---|---|
    | Source | one or more PostGIS tables (`table_names`), each read on its own Dask task |
    | Geometry types | Point, LineString, Polygon, Multi*; **mixed types are allowed** and produce a mixed layer |
    | `dissolve` | union everything into one feature |
    | `dissolve_field` | union grouped by this column |

    Attribute schemas are unioned, all inputs are reprojected to the CRS of the
    first, and a `src_layer` column records which table each feature came from.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`. On completion:

    ```json
    {"data": {"status": "completed",
              "result": {"feature_count": 26, "crs": "EPSG:4326",
                         "input_layers": 2, "dissolved": false,
                         "file": "./output/<name>.geojson",
                         "table": "public.<output_layer>"}}}
    ```

    Geometry out: **same type(s) as the inputs**.
    """
    return await combine_service.combine_from_table(payload=payload, params=params)


@router.post(
    "/v1/combine-from-geojson",
    summary="Combine - merge several uploads into one (GeoJSON uploads in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def combine_from_geojson(
    files: List[UploadFile] = File(..., description="Two or more GeoJSON FeatureCollections"),
    dissolve: bool = Form(False),
    dissolve_field: Optional[str] = Form(None),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    """
    Merge several uploaded GeoJSON layers into one.

    ### Input

    `multipart/form-data` with the `files` field repeated once per layer. Each
    must be a GeoJSON FeatureCollection. Mixed geometry types are allowed.
    `src_layer` records the source filename per feature.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion `result`
    holds `feature_count`, `input_layers`, `dissolved`, `crs`, `file` and
    optionally `table`. Geometry out: **same type(s) as the inputs**.
    """
    return await combine_service.combine_from_geojson(
        files=files, dissolve=dissolve, dissolve_field=dissolve_field,
        business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# ------------------------------------------------------------------ Trim ----
@router.post(
    "/v1/trim-from-table",
    summary="Trim - clip to a mask, or erase the mask (PostGIS table in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def trim_from_table(
    payload: DBConnectionPayload = Body(...),
    params: TrimTableParams = Body(...),
):
    """
    Keep what falls inside a mask, or (with `invert=true`) cut the mask out.

    ### Input

    | | |
    |---|---|
    | Input layer | PostGIS table - Point, LineString, Polygon, Multi* |
    | Mask | **`overlay_table` must be (Multi)Polygon.** A point/line mask fails. |
    | `bbox` | `[minx, miny, maxx, maxy]` - an alternative to `overlay_table`, always valid |
    | `invert` | `false` = clip (keep inside), `true` = erase (keep outside) |

    Exactly one of `overlay_table` or `bbox` is required; omitting both returns
    `400`. Clip and erase partition the source: over 25 parcels a clip keeping
    9 leaves an erase of 16.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`. On completion:

    ```json
    {"data": {"status": "completed",
              "result": {"feature_count": 9, "crs": "EPSG:4326",
                         "operation": "clip", "input_features": 25,
                         "file": "./output/<name>.geojson",
                         "table": "public.<output_layer>"}}}
    ```

    `operation` is `clip` or `erase`. Geometry out: **same type as the input**.
    """
    return await trim_service.trim_from_table(payload=payload, params=params)


@router.post(
    "/v1/trim-from-geojson",
    summary="Trim - clip to a mask, or erase the mask (GeoJSON upload in)",
    response_description="Job accepted; poll /api/process/v1/status/{uuid}",
)
async def trim_from_geojson(
    input_file: UploadFile = File(..., description="GeoJSON layer to trim (any geometry type)"),
    overlay_file: Optional[UploadFile] = File(
        None, description="GeoJSON mask - must be (Multi)Polygon. Omit if using bbox."),
    bbox: Optional[str] = Form(
        None, description="Alternative to overlay_file: 'minx,miny,maxx,maxy'"),
    invert: bool = Form(False, description="false = clip, true = erase"),
    business_id: str = Form(...),
    project_id: str = Form(...),
    file_path: Optional[str] = Form(None),
    output_layer: Optional[str] = Form(None),
):
    """
    Clip or erase an uploaded GeoJSON layer.

    ### Input

    `multipart/form-data`. `input_file` may be any geometry type; the mask is
    either `overlay_file` (**(Multi)Polygon only**) or a `bbox` string
    `"minx,miny,maxx,maxy"`. Supplying neither returns `400`.

    ### Output

    Immediate `{"uuid": ..., "status": "processing"}`; on completion `result`
    holds `feature_count`, `operation` (`clip`/`erase`), `input_features`,
    `crs`, `file` and optionally `table`. Geometry out: **same type as the
    input**.
    """
    return await trim_service.trim_from_geojson(
        input_file=input_file, overlay_file=overlay_file, bbox=bbox,
        invert=invert, business_id=business_id, project_id=project_id,
        file_path=file_path, output_layer=output_layer,
    )


# ---------------------------------------------------------------------------
# Swagger "Try it out" bodies.
#
# These cannot go in Body(..., openapi_examples=...): FastAPI merges several
# Body params into one generated Body_* model and drops their per-field
# examples. They are attached to the endpoint function here and injected into
# the schema by custom_openapi() in main.py, which keeps the example next to
# the route it documents.
# ---------------------------------------------------------------------------
_DB = {"host": "localhost", "port": 5432, "dbname": "gisdb",
       "user": "gis", "password": "***"}

buffer_from_table.openapi_examples = {
    "buffer_500m": {
        "summary": "500 m buffer on highways, written back to PostGIS",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1", "output_layer": "roads_buffer_500m"},
            "params": {"schema_name": "public", "table_name": "roads",
                       "where": "class = 'highway'", "distance": 500,
                       "unit": "meters", "cap": "round", "join": "round",
                       "dissolve": False},
        },
    },
    "dissolved_km": {
        "summary": "1 km buffer, dissolved into a single feature",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "params": {"schema_name": "public", "table_name": "parcels",
                       "distance": 1, "unit": "kilometers", "dissolve": True},
        },
    },
}

split_from_table.openapi_examples = {
    "by_attribute": {
        "summary": "One output per district",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1", "output_layer": "parcels"},
            "params": {"schema_name": "public", "table_name": "parcels",
                       "mode": "attribute", "split_field": "district"},
        },
    },
    "by_grid": {
        "summary": "Tile the extent into a 3x3 grid",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "params": {"schema_name": "public", "table_name": "parcels",
                       "mode": "grid", "grid_rows": 3, "grid_cols": 3},
        },
    },
}

combine_from_table.openapi_examples = {
    "merge": {
        "summary": "Append three layers into one table",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1", "output_layer": "all_boundaries"},
            "params": {"schema_name": "public",
                       "table_names": ["parcels", "wards", "zones"]},
        },
    },
    "dissolve": {
        "summary": "Merge, then dissolve by district",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "params": {"schema_name": "public", "table_names": ["parcels"],
                       "dissolve": True, "dissolve_field": "district"},
        },
    },
}

trim_from_table.openapi_examples = {
    "clip_by_table": {
        "summary": "Clip parcels to a boundary polygon table",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1", "output_layer": "parcels_clipped"},
            "params": {"schema_name": "public", "input_table": "parcels",
                       "overlay_table": "boundary"},
        },
    },
    "erase": {
        "summary": "Erase water bodies from landuse",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "params": {"schema_name": "public", "input_table": "landuse",
                       "overlay_table": "water", "invert": True},
        },
    },
    "bbox": {
        "summary": "Clip to a bounding box (no overlay table needed)",
        "value": {
            "payload": {"db_connection": _DB, "project_id": "p1",
                        "business_id": "b1"},
            "params": {"schema_name": "public", "input_table": "parcels",
                       "bbox": [73.80, 18.50, 73.83, 18.53]},
        },
    },
}
