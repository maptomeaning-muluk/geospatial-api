# app/utils/vector_utils.py
import json
import os
import uuid

import geopandas as gpd
from fastapi import HTTPException
from shapely import make_valid

# Degrees per metre at the equator - the same conversion table the rest of the
# service family uses, so a "meters" buffer on a 4326 layer behaves identically.
CONVERSION_FACTOR = {
    "degrees": 1,
    "meters": 1 / 111319.9,
    "kilometers": 1000 / 111319.9,
    "feet": 0.3048 / 111319.9,
    "inches": 0.0254 / 111319.9,
    "miles": 1609.34 / 111319.9,
    "nautical miles": 1852 / 111319.9,
    "yards": 0.9144 / 111319.9,
    "millimeters": 0.001 / 111319.9,
}

CAP_MAPPING = {"round": 1, "flat": 2, "square": 3}
JOIN_MAPPING = {"round": 1, "miter": 2, "bevel": 3}


def convert_distance(distance: float, unit: str, gdf=None) -> float:
    """Convert a distance to the units of the layer's CRS.

    A projected CRS already works in metres, so only a geographic CRS needs the
    degree conversion.
    """
    if gdf is not None and gdf.crs is not None and gdf.crs.is_projected:
        factors = {"degrees": 1, "meters": 1, "kilometers": 1000,
                   "feet": 0.3048, "inches": 0.0254, "miles": 1609.34,
                   "nautical miles": 1852, "yards": 0.9144, "millimeters": 0.001}
        if unit not in factors:
            raise HTTPException(400, "Unsupported unit for distance")
        return distance * factors[unit]

    if unit not in CONVERSION_FACTOR:
        raise HTTPException(400, "Unsupported unit for distance")
    return distance * CONVERSION_FACTOR[unit]


async def read_geojson_upload(upload_file) -> gpd.GeoDataFrame:
    """Turn an uploaded GeoJSON into a GeoDataFrame."""
    contents = await upload_file.read()
    try:
        geojson_data = json.loads(contents.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Invalid GeoJSON file.")

    features = geojson_data.get("features", [])
    if not features:
        raise HTTPException(400, "Invalid or empty GeoJSON file.")

    gdf = gpd.GeoDataFrame.from_features(features)
    if gdf.empty:
        raise HTTPException(404, "No features found in the uploaded GeoJSON.")
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    return gdf


def clean_geom(geom):
    """Repair an invalid geometry."""
    try:
        if geom is None:
            return None
        if not geom.is_valid:
            fixed = make_valid(geom)
            return fixed if fixed.is_valid else geom.buffer(0)
        return geom
    except Exception:
        return geom.buffer(0)


def drop_empty(gdf):
    geom = gdf.geometry
    return gdf[geom.notna() & ~geom.is_empty]


def ensure_crs(gdf, epsg=4326):
    if gdf.crs is None:
        gdf.set_crs(epsg=epsg, inplace=True)
    return gdf


def gdf_to_response(gdf):
    """The JSON body every vector endpoint returns."""
    return json.loads(gdf.to_json())


def output_dir() -> str:
    d = os.environ.get("OUTPUT_DIR", "./output")
    os.makedirs(d, exist_ok=True)
    return d


def temp_path(suffix: str = ".tif", name: str = None) -> str:
    filename = name or f"output_{uuid.uuid4().hex[:12]}{suffix}"
    if not filename.endswith(suffix):
        filename += suffix
    return os.path.join(output_dir(), filename)


def save_vector_result(gdf, name, db_connection=None, schema_name=None,
                       output_layer=None):
    """Write the result out and describe where it went.

    Always writes a GeoJSON into OUTPUT_DIR; additionally writes a PostGIS table
    when `output_layer` is supplied.
    """
    from app.utils.db_utils import get_engine_from_payload, write_gdf_to_db

    gdf = ensure_crs(drop_empty(gdf))
    result = {"feature_count": int(len(gdf)),
              "crs": str(gdf.crs) if gdf.crs else None}

    geojson_path = os.path.join(output_dir(), f"{name}.geojson")
    gdf.to_file(geojson_path, driver="GeoJSON")
    result["file"] = geojson_path

    if output_layer and db_connection is not None:
        engine = get_engine_from_payload(db_connection)
        result["table"] = write_gdf_to_db(
            gdf, engine, schema_name or "public", output_layer
        )
    return result
