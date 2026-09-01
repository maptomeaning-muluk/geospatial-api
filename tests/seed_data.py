import app  # noqa: F401  - repairs PROJ_LIB before the geo stack loads
from dotenv import load_dotenv; load_dotenv()
import geopandas as gpd, numpy as np, rasterio
from rasterio.transform import from_origin
from shapely.geometry import box, Point
from app.utils.db_utils import get_default_engine, write_gdf_to_db

rows, geoms = [], []
for i in range(5):
    for j in range(5):
        x0, y0 = 73.80 + i*0.01, 18.50 + j*0.01
        geoms.append(box(x0, y0, x0+0.01, y0+0.01))
        rows.append({"id": i*5+j, "district": f"D{i%3}", "population": (i+1)*(j+1)*137})
parcels = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
parcels.to_file("testdata/parcels.geojson", driver="GeoJSON")

overlay = gpd.GeoDataFrame({"id":[1]}, geometry=[box(73.80,18.50,73.83,18.53)], crs="EPSG:4326")
overlay.to_file("testdata/overlay.geojson", driver="GeoJSON")

wells = gpd.GeoDataFrame({"id": range(20), "kind": ["well","tank"]*10},
    geometry=[Point(73.80+(k%5)*0.002, 18.50+(k//5)*0.002) for k in range(20)],
    crs="EPSG:4326")
wells.to_file("testdata/wells.geojson", driver="GeoJSON")

engine = get_default_engine()
for gdf, name in ((parcels,"parcels"), (overlay,"overlay"), (wells,"wells")):
    print("  wrote", write_gdf_to_db(gdf, engine, "public", name), f"({len(gdf)} features)")

data = np.zeros((240,240), dtype="int32")
data[:120,:120]=10; data[:120,120:]=20; data[120:,:120]=30; data[120:,120:]=40
data[:4,:] = -9999
with rasterio.open("testdata/landcover.tif","w",driver="GTiff",width=240,height=240,
                   count=1,dtype="int32",crs="EPSG:32643",nodata=-9999,
                   transform=from_origin(300000,2001000,5.0,5.0),
                   tiled=True,blockxsize=128,blockysize=128) as ds:
    ds.write(data,1)
print("  wrote testdata/landcover.tif  240x240 int32, classes 10/20/30/40")
