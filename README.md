# GeoSpatial API

Buffer, Split, Combine, Trim, Raster→Vector and Vector→Raster over FastAPI, Dask
and PostGIS. Same layout as `python_services`.

Every tool endpoint returns a `uuid` immediately and runs the work on the Dask
cluster, so you can submit more jobs and poll
`GET /api/process/v1/status/{uuid}` for progress and results.

---

## Commands

### Run with Docker (everything: PostGIS + Dask + API)

```bash
cp .env.example .env                       # edit DB_PASSWORD / JWT_TOKEN / aws keys
docker compose up -d --build               # start the whole stack
docker compose up -d --scale dask-worker=6 # more compute, any time
docker compose logs -f api                 # follow the API log
docker compose ps                          # what is running
docker compose down                        # stop
docker compose down -v                     # stop and wipe the database
```

### Run locally (no Docker)

```bash
python -m venv .venv
.venv\Scripts\activate                     # Windows
source .venv/bin/activate                  # Linux / macOS

pip install -r requirements.txt
cp .env.example .env                       # set DB_HOST=localhost

# a PostGIS to point at, if you do not have one:
docker run -d --name gis-pg -p 5432:5432 \
  -e POSTGRES_DB=gisdb -e POSTGRES_USER=gis -e POSTGRES_PASSWORD=gis \
  postgis/postgis:16-3.4

python main.py                             # reads PORT from .env (default 5000)
# or, with reload while developing:
uvicorn main:app --reload --host 0.0.0.0 --port 5000
```

With `DASK_SCHEDULER_ADDRESS` blank the app starts its own LocalCluster, so a
laptop needs nothing else. The PostGIS extension and the `processing` table are
created on startup.

### Verify it is up

```bash
curl http://localhost:5000/health
curl -H "Authorization: Bearer $JWT_TOKEN" http://localhost:5000/api/process/v1/cluster
```

| | |
|---|---|
| API + Swagger | <http://localhost:5000/docs> |
| ReDoc | <http://localhost:5000/redoc> |
| Dask dashboard | <http://localhost:8787> |

### Run the tests

```bash
python tests/seed_data.py                  # demo tables + testdata/ files
uvicorn main:app --port 5000               # in another shell

export PYTHONPATH=.                        # Windows: set PYTHONPATH=.
python tests/test_api.py                   # 35 endpoint checks
python tests/test_concurrency.py           # submission is non-blocking
python tests/verify_outputs.py             # geometry actually written
```

---

## Layout

```
main.py                              app entry point
app/
  middleware/auth_middleware.py      Bearer token check (JWT_TOKEN in .env)
  migration/table_mig.py             creates PostGIS + the processing table
  routes/
    geo_routes.py                    buffer, split, combine, trim
    raster_routes.py                 vector-to-raster, raster-to-vector
    process_routes.py                job status
  schemas/
    PayloadSchemas.py                DB payload + per-tool params
    RasterPayloadSchemas.py          S3 payload, storage method, pixel unit
  services/
    buffer_service.py                one file per tool
    split_service.py
    combine_service.py
    trim_service.py
    vector_to_raster_service.py
    raster_to_vector_service.py
    process_service.py               log_start / log_end / status queries
  utils/
    db_utils.py                      engine from payload, read/write PostGIS
    dask_utils.py                    client, submit_job, parallel_map
    s3_utils.py                      download / upload / storage method
    vector_utils.py                  units, geojson upload, save output
```

---

## Endpoints

Each tool has two flavours — `-from-table` reads an existing PostGIS table,
`-from-geojson` takes an uploaded file.

| Method | Path |
|---|---|
| POST | `/api/vector/v1/buffer-from-table` · `/buffer-from-geojson` |
| POST | `/api/vector/v1/split-from-table` · `/split-from-geojson` |
| POST | `/api/vector/v1/combine-from-table` · `/combine-from-geojson` |
| POST | `/api/vector/v1/trim-from-table` · `/trim-from-geojson` |
| POST | `/api/raster/v1/vector-to-raster-from-table` · `-from-geojson` |
| POST | `/api/raster/v1/raster-to-vector-from-file` · `-from-upload` |
| GET | `/api/process/v1/status/{uuid}` — **job status** |
| GET | `/api/process/v1/all-process` |
| GET | `/api/process/v1/{business_id}` — filter by project/tool/status |
| GET | `/api/process/v1/cluster` — Dask workers and threads |
| GET | `/health` |

All endpoints except `/health` and the docs need `Authorization: Bearer <JWT_TOKEN>`.
Leave `JWT_TOKEN` blank in `.env` to disable auth while developing.

---

## What each endpoint accepts

Everything in this section was measured against the running service, not
assumed.

### Input format per endpoint

| Endpoint | Accepts |
|---|---|
| `*-from-table` | Any PostGIS table with a `geometry`/`geography` column. The column may be named anything — it is looked up in `information_schema`. |
| `*-from-geojson` | **GeoJSON only** (a `FeatureCollection`, `.geojson`/`.json`, UTF-8). A `.gpkg`/`.shp`/`.zip` upload is rejected with `400 Invalid GeoJSON file.` Convert first, or load into PostGIS and use the `-from-table` form. |
| `raster-to-vector-from-file` | A local path or an `s3://bucket/key` URI to any GDAL-readable raster. |
| `raster-to-vector-from-upload` | An uploaded raster file (see raster formats below). |

### Vector geometry types

| Tool | Point | Line | Polygon | Multi\* | Notes |
|---|:--:|:--:|:--:|:--:|---|
| Buffer | ✅ | ✅ | ✅ | ✅ | See the cap-style trap below. |
| Split (`attribute`) | ✅ | ✅ | ✅ | ✅ | Splits on any column. |
| Split (`grid`) | ✅ | ✅ | ✅ | ✅ | Clips to tiles, keeping the input's geometry type. |
| Split (`parts`) | ✅ | ✅ | ✅ | ✅ | Only meaningful for Multi\* input — explodes to single parts. |
| Combine | ✅ | ✅ | ✅ | ✅ | Mixed types allowed; the result is a mixed layer. `dissolve` is really only meaningful within one type. |
| Trim — **input** | ✅ | ✅ | ✅ | ✅ | |
| Trim — **mask** | ❌ | ❌ | ✅ | ✅ | The mask must be **(Multi)Polygon**. A point/line mask fails with `'mask' should be ... (Multi)Polygon`. A `bbox` always works. |
| Vector→Raster | ✅ | ✅ | ✅ | ✅ | Burns with `all_touched=True`, so thin lines and single points still produce pixels. |

**Buffer cap-style trap.** `cap="flat"` or `"square"` on **Point** input returns
**zero features**. That is GEOS behaviour, not a bug — a flat cap is only
defined for a geometry with direction. Use `cap="round"` for points.

### Raster formats

| Input | Works | What you get |
|---|:--:|---|
| **GeoTIFF** (`.tif`/`.tiff`, with CRS) | ✅ | The intended input. CRS and transform are carried through to the output geometry. |
| `.img`, `.vrt`, `.jp2`, any GDAL format | ✅ | Read through rasterio/GDAL; georeferencing preserved. |
| **PNG** (no CRS) | ⚠️ | Vectorises, but the coordinates are **pixel offsets**, not real-world. The result carries `"georeferenced": false` and a `warning`. |
| **JPEG** | ⚠️❌ | Same CRS problem, **plus JPEG is lossy**: a 4-class test image came back as **80 polygons instead of 4**, because compression noise breaks up the flat colour blocks. Do not use JPEG for classified rasters. |

Output rasters are always single-band `float32` GeoTIFF, deflate-compressed and
tiled.

### Output geometry

| Tool | Produces |
|---|---|
| Buffer | Polygon (MultiPolygon when `dissolve=true`) |
| Split | Same type as the input, one dataset per part |
| Combine | Same type(s) as the inputs |
| Trim | Same type as the input |
| Raster→Vector | Polygon, one per contiguous same-value region (`dissolve=true` merges by value) |
| Vector→Raster | Single-band float32 GeoTIFF |

---

## Example

Submit a buffer over an existing PostGIS table:

```bash
curl -X POST http://localhost:5000/api/vector/v1/buffer-from-table \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" -d '{
  "payload": {
    "db_connection": {"host": "localhost", "port": 5432, "dbname": "gisdb",
                      "user": "gis", "password": "***"},
    "project_id": "p1", "business_id": "b1",
    "output_layer": "roads_buffer_500m"
  },
  "params": {
    "schema_name": "public", "table_name": "roads",
    "where": "class = '\''highway'\''",
    "distance": 500, "unit": "meters", "dissolve": false
  }
}'
# -> {"uuid": "9a2e...", "status": "processing", "message": "..."}

curl http://localhost:5000/api/process/v1/status/9a2e... \
  -H "Authorization: Bearer $JWT_TOKEN"
# -> {"data": {"status": "completed", "result": {"table": "public.roads_buffer_500m",
#              "feature_count": 128411, "file": "./output/roads_buffer_500m.geojson"}}}
```

Or from an upload:

```bash
curl -X POST http://localhost:5000/api/vector/v1/buffer-from-geojson \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -F "file=@roads.geojson" -F "distance=500" -F "unit=meters" \
  -F "business_id=b1" -F "project_id=p1"
```

---

## Request / response format

Same shape for every tool: submit, get a `uuid`, poll.

### 1. Submit

**`-from-table`** endpoints take `application/json` with two objects,
`payload` (where to connect, who is asking, where to write) and `params`
(the tool's own settings):

```json
{
  "payload": {
    "db_connection": {"host": "localhost", "port": 5432, "dbname": "gisdb",
                      "user": "gis", "password": "***"},
    "project_id": "p1",
    "business_id": "b1",
    "file_path": null,
    "output_layer": "roads_buffer_500m"
  },
  "params": {
    "schema_name": "public", "table_name": "roads",
    "where": "class = 'highway'",
    "distance": 500, "unit": "meters", "dissolve": false
  }
}
```

The raster endpoints add a third object, `raster_payload`, plus
`storage_method` (`temp` | `own` | `alloc`).

**`-from-geojson` / `-from-upload`** endpoints take `multipart/form-data`:
the file field plus each parameter as a form field.

### 2. Immediate response — HTTP 200, in milliseconds

```json
{
  "uuid": "3f9c1e2b-7a41-4d0e-9c33-2b8a1f5e6d70",
  "status": "processing",
  "message": "Buffer job submitted. Poll /api/process/v1/status/{uuid}."
}
```

Nothing has run yet. Keep submitting other jobs.

### 3. Poll — `GET /api/process/v1/status/{uuid}`

```json
{
  "message": "Processing record fetched successfully",
  "data": {
    "uuid": "3f9c1e2b-...",
    "tool_name": "buffer",
    "data_type": "vector",
    "status": "completed",
    "business_id": "b1",
    "project_id": "p1",
    "input_layer": "roads",
    "overlay_layer": null,
    "output_layer": "roads_buffer_500m",
    "start_time": "2026-09-01T12:45:26",
    "end_time": "2026-09-01T12:45:29",
    "message": "Operation completed successfully",
    "result": {
      "feature_count": 25,
      "crs": "EPSG:4326",
      "file": "./output/roads_buffer_500m.geojson",
      "table": "public.roads_buffer_500m"
    }
  }
}
```

`status` is `processing` | `completed` | `failed`. While processing, `result`
is `null`. On failure, `message` carries the reason.

### The `result` block per tool

| Tool | `result` fields |
|---|---|
| Buffer | `feature_count` `crs` `file` `table`* `distance` `unit` `distance_in_crs_units` `dissolved` |
| Split | `mode` `parts` `outputs[]` — each with `key` `feature_count` `crs` `file` `table`* |
| Combine | `feature_count` `crs` `file` `table`* `input_layers` `dissolved` |
| Trim | `feature_count` `crs` `file` `table`* `operation` (`clip`/`erase`) `input_features` |
| Vector→Raster | `output` `width` `height` `pixel_size` `pixel_unit` `crs` `features` |
| Raster→Vector | `feature_count` `crs` `file` `table`* `band` `windows` `dissolved` `georeferenced` `source_raster` |

\* `table` only when `output_layer` was supplied.

### Errors

| Code | When |
|---|---|
| `400` | Missing a required combination — e.g. `mode=attribute` without `split_field`, or Trim with neither `overlay_table` nor `bbox`. Also a non-GeoJSON upload. |
| `401` | Missing or wrong `Authorization: Bearer <JWT_TOKEN>` |
| `404` | Unknown job uuid |
| `422` | Body fails validation — missing field, bad enum (e.g. `unit: "parsecs"`) |
| `500` | Unexpected server error while submitting |

A failure *inside* the job is not an HTTP error — submission already returned
`200`. The job ends with `status: "failed"` and the reason in `message`.

---

## Where Dask is used

- `submit_job()` hands each job to the cluster and returns straight away — the
  API never blocks on GIS work.
- `parallel_map()` splits a GeoDataFrame into `DASK_CHUNK_SIZE` row chunks and
  buffers / clips them concurrently (Buffer, Trim).
- Split fans one task per grid tile; Combine loads all source tables at once;
  Vector→Raster burns the grid in horizontal bands; Raster→Vector polygonizes
  one task per raster window.

Tune with `DASK_WORKERS`, `DASK_THREADS`, `DASK_MEMORY_LIMIT`, `DASK_CHUNK_SIZE`
in `.env`.

---

## Output

Results always land as a GeoJSON in `OUTPUT_DIR`. Additionally:

- give `output_layer` and the result is written to PostGIS too (schema, table
  and GiST index created automatically);
- for rasters, `storage_method` picks `temp` (local file), `own` (the bucket in
  the request) or `alloc` (the bucket in `.env`).

---

## Testing

```bash
python tests/seed_data.py          # PostGIS fixtures + testdata/ files
uvicorn main:app --port 5000       # in another shell

export PYTHONPATH=.                # Windows: set PYTHONPATH=.
python tests/test_api.py           # 35 endpoint checks
python tests/test_concurrency.py   # proves submission is non-blocking
python tests/verify_outputs.py     # checks the geometry actually written
```

`test_api.py` asserts on each job's **result**, not just that it completed.

---

## Known limitation: buffer distance on a geographic CRS

`convert_distance()` turns metres into degrees with a single factor
(`1/111319.9`), carried over from `python_services`. That factor is only exact
for latitude, so an east-west buffer comes out short the further you are from
the equator:

| latitude | requested | actual E-W |
|---|---|---|
| 0 | 500 m | 500 m |
| 18.5 (Pune) | 500 m | 474 m (-5%) |
| 35 | 500 m | 410 m (-18%) |
| 50 | 500 m | 321 m (-36%) |

Measured, not estimated: a 500 m buffer over the test parcels comes back
497 m north-south and 474 m east-west.

If you want true metric buffers, project to UTM first and back afterwards -
the same thing `vector_to_raster` already does via `estimate_utm_crs()`:

```python
# in buffer_service._run_buffer
src_crs = gdf.crs
if unit != "degrees" and gdf.crs and gdf.crs.is_geographic:
    gdf = gdf.to_crs(gdf.estimate_utm_crs())     # metres
    converted = distance                          # no degree conversion needed
result_gdf = parallel_map(gdf, _buffer_chunk, distance=converted, ...)
result_gdf = result_gdf.to_crs(src_crs)
```

Left as-is so results stay consistent with the existing `python_services`
buffer; flip it if you would rather have the accuracy.
