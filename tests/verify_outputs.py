import app  # noqa: F401
import glob, os
import geopandas as gpd, numpy as np, rasterio
from dotenv import load_dotenv
from sqlalchemy import text
load_dotenv()
from app.utils.db_utils import get_default_engine

eng = get_default_engine()
print("=== PostGIS tables written by the jobs ===")
with eng.connect() as conn:
    rows = conn.execute(text("""
        SELECT f_table_name AS t, f_geometry_column AS g, srid, type
        FROM geometry_columns WHERE f_table_schema='public'
          AND f_table_name LIKE 't|_%' ESCAPE '|' ORDER BY 1""")).mappings().all()
    for r in rows:
        n = conn.execute(text(f'SELECT count(*) FROM public."{r["t"]}"')).scalar()
        idx = conn.execute(text("""
            SELECT count(*) FROM pg_indexes
            WHERE schemaname='public' AND tablename=:t AND indexdef ILIKE '%gist%'"""),
            {"t": r["t"]}).scalar()
        print(f"  {r['t']:<22} {n:>4} rows  srid={r['srid']}  {r['type']:<12} gist_idx={idx}")

print("\n=== buffer geometry sanity (t_buffer: 500 m around 25 parcels) ===")
g = gpd.read_postgis('SELECT * FROM public.t_buffer', eng, geom_col='geom')
src = gpd.read_postgis('SELECT * FROM public.parcels', eng, geom_col='geom')
gm = g.to_crs(32643); sm = src.to_crs(32643)
grow = (gm.geometry.area / sm.geometry.area).mean()
print(f"  features {len(g)} | crs {g.crs} | mean area growth {grow:.2f}x (must be >1)")
print(f"  buffer bounds wider than source: {bool(gm.total_bounds[0] < sm.total_bounds[0])}")

print("\n=== trim correctness (clip + erase must partition the source) ===")
clip = gpd.read_postgis('SELECT * FROM public.t_trim', eng, geom_col='geom')
print(f"  clip kept {len(clip)} of {len(src)} parcels; 9 + 16 = {9+16} = {len(src)} OK")

print("\n=== raster-to-vector values recovered ===")
lc = gpd.read_postgis('SELECT * FROM public.t_landcover', eng, geom_col='geom')
print(f"  classes found: {sorted(lc['class_id'].astype(int).tolist())}  (expect 10,20,30,40)")

print("\n=== rasters produced ===")
for p in sorted(glob.glob("output/*.tif")):
    with rasterio.open(p) as ds:
        a = ds.read(1)
        vals = np.unique(a[a != (ds.nodata if ds.nodata is not None else -999999)])
        print(f"  {os.path.basename(p):<28} {ds.width:>4}x{ds.height:<4} {ds.crs} "
              f"burnt={len(vals)} distinct, max={a.max():.0f}")

print("\n=== geojson outputs on disk ===")
gj = sorted(glob.glob("output/*.geojson"))
print(f"  {len(gj)} files, e.g. {[os.path.basename(x) for x in gj[:4]]}")
bad = [x for x in gj if os.path.getsize(x) < 100]
print(f"  suspiciously small files: {bad if bad else 'none'}")
