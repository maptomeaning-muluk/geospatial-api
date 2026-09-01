# GeoSpatial API

Buffer, Split, Combine, Trim, Raster→Vector and Vector→Raster over FastAPI, Dask
and PostGIS. Same layout as `python_services`.

Every tool endpoint returns a `uuid` immediately and runs the work on the Dask
cluster, so you can submit more jobs and poll
`GET /api/process/v1/status/{uuid}` for progress and results.

---

## Run

**Docker (everything)**

```bash
cp .env.example .env      # then edit DB_PASSWORD / JWT_TOKEN / aws keys
docker compose up -d --build
docker compose up -d --scale dask-worker=6      # more compute
```

| | |
|---|---|
| API + Swagger | <http://localhost:5000/docs> |
| Dask dashboard | <http://localhost:8787> |

**Local**

```bash
pip install -r requirements.txt
cp .env.example .env       # set DB_HOST=localhost
python main.py             # or: uvicorn main:app --reload --port 5000
```

With `DASK_SCHEDULER_ADDRESS` blank the app starts its own LocalCluster, so a
laptop needs nothing else. The `processing` table is created on startup.

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
