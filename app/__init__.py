"""Package init.

Repairs a machine-wide PROJ_LIB / GDAL_DATA before geopandas or rasterio are
imported anywhere in the app.

A host PostgreSQL/PostGIS or QGIS install commonly exports these for every
process on the box. Our wheels ship their own PROJ database, and mixing the two
makes every CRS lookup fail with:

    proj.db contains DATABASE.LAYOUT.VERSION.MINOR = 2
    whereas a number >= 6 is expected

Unsetting the variables is not enough - GDAL then falls back to a DLL-relative
search that can still find the foreign install - so a foreign value is pointed
at the bundled data instead. Values already inside this Python installation are
left alone, and if nothing is set we stay out of the way.

Living in __init__ means `import app.anything` fixes it, including on a Dask
worker importing app.services.*.
"""
import os
import sys
import sysconfig


def _site_dirs():
    paths = sysconfig.get_paths()
    return [p for p in (paths.get("purelib"), paths.get("platlib")) if p]


def _is_foreign(value: str) -> bool:
    roots = [os.path.realpath(p) for p in _site_dirs() + [sys.prefix]]
    real = os.path.realpath(value)
    return not any(real.startswith(root) for root in roots)


def _bundled(*relative_parts) -> str | None:
    for root in _site_dirs():
        candidate = os.path.join(root, *relative_parts)
        if os.path.isdir(candidate):
            return candidate
    return None


def _fix_proj_env() -> None:
    proj_dir = _bundled("rasterio", "proj_data") or _bundled("pyproj", "proj_dir",
                                                             "share", "proj")
    gdal_dir = _bundled("rasterio", "gdal_data")

    for var, replacement in (("PROJ_LIB", proj_dir), ("PROJ_DATA", proj_dir),
                             ("GDAL_DATA", gdal_dir)):
        value = os.environ.get(var)
        if not value or not _is_foreign(value):
            continue
        if replacement:
            os.environ[var] = replacement
        else:
            os.environ.pop(var, None)


_fix_proj_env()
