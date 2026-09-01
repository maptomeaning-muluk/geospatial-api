"""Exercise every endpoint against a running server and print a report.

Each tool job is checked against an expected result, not just "completed" -
a job can finish successfully while producing nothing useful.
"""
import app  # noqa: F401  - repairs PROJ_LIB before the geo stack loads
import os
import sys
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE = os.environ.get("TEST_BASE", "http://127.0.0.1:5000")
TOKEN = os.environ.get("JWT_TOKEN", "local-dev-token")
c = httpx.Client(base_url=BASE, timeout=180.0,
                 headers={"Authorization": f"Bearer {TOKEN}"})

DB = {"host": os.environ["DB_HOST"], "port": int(os.environ["DB_PORT"]),
      "dbname": os.environ["DB_NAME"], "user": os.environ["DB_USER"],
      "password": os.environ["DB_PASSWORD"]}
PAY = {"db_connection": DB, "project_id": "p1", "business_id": "b1"}

RESULTS = []


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name:<40} {detail}")


def wait(uuid, timeout=180):
    end = time.time() + timeout
    last = {}
    while time.time() < end:
        r = c.get(f"/api/process/v1/status/{uuid}")
        if r.status_code != 200:
            return {"status": f"HTTP {r.status_code}", "message": r.text[:200]}
        last = r.json()["data"]
        if last["status"] in ("completed", "failed"):
            return last
        time.sleep(0.4)
    return last


def submit(name, path, *, json_body=None, files=None, data=None, expect=None):
    t0 = time.perf_counter()
    r = c.post(path, json=json_body, files=files, data=data)
    submit_ms = (time.perf_counter() - t0) * 1000

    if r.status_code != 200:
        record(name, False, f"submit HTTP {r.status_code}: {r.text[:150]}")
        return None
    body = r.json()
    if "uuid" not in body or body.get("status") != "processing":
        record(name, False, f"bad submit response: {body}")
        return None

    rec = wait(body["uuid"])
    if rec.get("status") != "completed":
        record(name, False, f"{rec.get('status')}: {str(rec.get('message'))[:140]}")
        return None

    res = rec.get("result") or {}
    bits = []
    if "feature_count" in res:
        bits.append(f"{res['feature_count']} feat")
    if "parts" in res:
        bits.append(f"{res['parts']} parts")
    if "width" in res:
        bits.append(f"{res['width']}x{res['height']}px")
    if "table" in res:
        bits.append(str(res["table"]))
    if "output" in res:
        bits.append(os.path.basename(str(res["output"])))

    problems = []
    for key, want in (expect or {}).items():
        got = res.get(key)
        if callable(want):
            if not want(got):
                problems.append(f"{key}={got!r} rejected")
        elif got != want:
            problems.append(f"{key}: want {want!r} got {got!r}")

    detail = f"{submit_ms:4.0f}ms | " + ", ".join(bits)
    if problems:
        record(name, False, detail + "  <-- " + "; ".join(problems))
        return None
    record(name, True, detail)
    return rec


def f(path):
    return open(os.path.join("testdata", path), "rb")


positive = lambda v: isinstance(v, (int, float)) and v > 0          # noqa: E731
is_tif = lambda v: bool(v) and str(v).endswith(".tif")              # noqa: E731


print("\n=============== 1. INFRASTRUCTURE ===============")
r = c.get("/health")
record("GET /health", r.status_code == 200, r.text[:55])

r = httpx.get(f"{BASE}/api/process/v1/all-process", timeout=30)
record("auth: no token -> 401", r.status_code == 401)
r = httpx.get(f"{BASE}/api/process/v1/all-process",
              headers={"Authorization": "Bearer wrong"}, timeout=30)
record("auth: bad token -> 401", r.status_code == 401)

r = c.get("/api/process/v1/cluster")
info = r.json()["data"]
record("GET /api/process/v1/cluster",
       r.status_code == 200 and info.get("workers", 0) > 0,
       f"{info.get('workers')} workers / {info.get('threads')} threads")

spec = httpx.get(f"{BASE}/openapi.json", timeout=30).json()
record("GET /openapi.json", len(spec["paths"]) >= 15, f"{len(spec['paths'])} paths")
record("GET /docs", httpx.get(f"{BASE}/docs", timeout=30).status_code == 200)


print("\n=============== 2. BUFFER ===============")
submit("buffer-from-table", "/api/vector/v1/buffer-from-table",
       json_body={"payload": {**PAY, "output_layer": "t_buffer"},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "distance": 500, "unit": "meters",
                             "dissolve": False}},
       expect={"feature_count": 25, "table": "public.t_buffer"})

submit("buffer-from-table (where+dissolve)", "/api/vector/v1/buffer-from-table",
       json_body={"payload": {**PAY, "output_layer": "t_buffer_d1"},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "where": "district = 'D1'", "distance": 1,
                             "unit": "kilometers", "dissolve": True}},
       expect={"feature_count": 1})

submit("buffer-from-geojson", "/api/vector/v1/buffer-from-geojson",
       files={"file": ("wells.geojson", f("wells.geojson"))},
       data={"distance": "250", "unit": "meters", "cap": "round",
             "join": "round", "dissolve": "false",
             "business_id": "b1", "project_id": "p1"},
       expect={"feature_count": 20})


print("\n=============== 3. SPLIT ===============")
submit("split-from-table (attribute)", "/api/vector/v1/split-from-table",
       json_body={"payload": {**PAY, "output_layer": "t_split"},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "mode": "attribute", "split_field": "district"}},
       expect={"parts": 3})

submit("split-from-table (grid 2x2)", "/api/vector/v1/split-from-table",
       json_body={"payload": {**PAY},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "mode": "grid", "grid_rows": 2, "grid_cols": 2}},
       expect={"parts": 4})

submit("split-from-geojson (parts)", "/api/vector/v1/split-from-geojson",
       files={"file": ("parcels.geojson", f("parcels.geojson"))},
       data={"mode": "parts", "business_id": "b1", "project_id": "p1"},
       expect={"parts": 1})


print("\n=============== 4. COMBINE ===============")
submit("combine-from-table (2 tables)", "/api/vector/v1/combine-from-table",
       json_body={"payload": {**PAY, "output_layer": "t_combined"},
                  "params": {"schema_name": "public",
                             "table_names": ["parcels", "overlay"]}},
       expect={"feature_count": 26, "input_layers": 2})

submit("combine-from-table (dissolve)", "/api/vector/v1/combine-from-table",
       json_body={"payload": {**PAY},
                  "params": {"schema_name": "public", "table_names": ["parcels"],
                             "dissolve": True, "dissolve_field": "district"}},
       expect={"feature_count": 3})

submit("combine-from-geojson (2 files)", "/api/vector/v1/combine-from-geojson",
       files=[("files", ("parcels.geojson", f("parcels.geojson"))),
              ("files", ("overlay.geojson", f("overlay.geojson")))],
       data={"business_id": "b1", "project_id": "p1"},
       expect={"feature_count": 26})


print("\n=============== 5. TRIM ===============")
submit("trim-from-table (overlay clip)", "/api/vector/v1/trim-from-table",
       json_body={"payload": {**PAY, "output_layer": "t_trim"},
                  "params": {"schema_name": "public", "input_table": "parcels",
                             "overlay_table": "overlay"}},
       expect={"feature_count": 9, "operation": "clip"})

submit("trim-from-table (erase)", "/api/vector/v1/trim-from-table",
       json_body={"payload": {**PAY},
                  "params": {"schema_name": "public", "input_table": "parcels",
                             "overlay_table": "overlay", "invert": True}},
       expect={"feature_count": 16, "operation": "erase"})

submit("trim-from-table (bbox)", "/api/vector/v1/trim-from-table",
       json_body={"payload": {**PAY},
                  "params": {"schema_name": "public", "input_table": "parcels",
                             "bbox": [73.80, 18.50, 73.83, 18.53]}},
       expect={"feature_count": 9})

submit("trim-from-geojson (overlay file)", "/api/vector/v1/trim-from-geojson",
       files={"input_file": ("parcels.geojson", f("parcels.geojson")),
              "overlay_file": ("overlay.geojson", f("overlay.geojson"))},
       data={"business_id": "b1", "project_id": "p1"},
       expect={"feature_count": 9})

submit("trim-from-geojson (bbox)", "/api/vector/v1/trim-from-geojson",
       files={"input_file": ("parcels.geojson", f("parcels.geojson"))},
       data={"bbox": "73.80,18.50,73.83,18.53",
             "business_id": "b1", "project_id": "p1"},
       expect={"feature_count": 9})


print("\n=============== 6. VECTOR -> RASTER ===============")
submit("v2r-from-table (attribute)", "/api/raster/v1/vector-to-raster-from-table",
       json_body={"payload": PAY, "raster_payload": {},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "pixel_size": 30, "pixel_unit": "meters",
                             "attribute_field": "population"},
                  "storage_method": "temp"},
       expect={"width": positive, "height": positive, "features": 25,
               "output": is_tif})

submit("v2r-from-table (burn value, degrees)",
       "/api/raster/v1/vector-to-raster-from-table",
       json_body={"payload": PAY, "raster_payload": {},
                  "params": {"schema_name": "public", "table_name": "parcels",
                             "pixel_size": 0.0005, "pixel_unit": "degrees",
                             "burn_value": 7},
                  "storage_method": "temp"},
       expect={"pixel_unit": "degrees", "features": 25, "width": positive})

submit("v2r-from-geojson (text attribute)",
       "/api/raster/v1/vector-to-raster-from-geojson",
       files={"file": ("parcels.geojson", f("parcels.geojson"))},
       data={"pixel_size": "50", "pixel_unit": "meters",
             "attribute_field": "district", "business_id": "b1",
             "project_id": "p1", "storage_method": "temp"},
       expect={"features": 25, "output": is_tif})


print("\n=============== 7. RASTER -> VECTOR ===============")
raster_abs = os.path.abspath("testdata/landcover.tif")
submit("r2v-from-file (s3/local path)", "/api/raster/v1/raster-to-vector-from-file",
       json_body={"payload": {**PAY, "output_layer": "t_landcover"},
                  "raster_payload": {},
                  "params": {"raster_file": raster_abs, "band": 1,
                             "field_name": "class_id", "dissolve": True}},
       expect={"feature_count": 4, "band": 1, "table": "public.t_landcover"})

submit("r2v-from-upload", "/api/raster/v1/raster-to-vector-from-upload",
       files={"file": ("landcover.tif", f("landcover.tif"))},
       data={"band": "1", "field_name": "class_id", "dissolve": "true",
             "business_id": "b1", "project_id": "p1"},
       expect={"feature_count": 4})


print("\n=============== 8. PROCESS / STATUS ===============")
r = c.get("/api/process/v1/all-process", params={"limit": 200})
records = r.json()["data"]
record("GET /api/process/v1/all-process", r.status_code == 200,
       f"{len(records)} records")

r = c.get("/api/process/v1/b1", params={"tool_name": "buffer"})
record("GET /api/process/v1/{business_id}",
       r.status_code == 200 and len(r.json()["data"]) >= 3,
       f"{len(r.json()['data'])} buffer jobs")

r = c.get("/api/process/v1/b1", params={"status": "completed"})
record("filter by status", r.status_code == 200,
       f"{len(r.json()['data'])} completed")

r = c.get("/api/process/v1/status/00000000-0000-0000-0000-000000000000")
record("status of unknown uuid -> 404", r.status_code == 404)


print("\n=============== 9. ERROR HANDLING ===============")
r = c.post("/api/vector/v1/buffer-from-table",
           json={"payload": PAY, "params": {"schema_name": "public",
                                            "table_name": "parcels"}})
record("missing required field -> 422", r.status_code == 422)

r = c.post("/api/vector/v1/split-from-table",
           json={"payload": PAY,
                 "params": {"schema_name": "public", "table_name": "parcels",
                            "mode": "attribute"}})
record("split without split_field -> 400", r.status_code == 400,
       str(r.json().get("detail"))[:55])

r = c.post("/api/vector/v1/trim-from-table",
           json={"payload": PAY,
                 "params": {"schema_name": "public", "input_table": "parcels"}})
record("trim without mask -> 400", r.status_code == 400,
       str(r.json().get("detail"))[:55])

r = c.post("/api/vector/v1/buffer-from-table",
           json={"payload": PAY,
                 "params": {"schema_name": "public", "table_name": "nope",
                            "distance": 10, "unit": "meters"}})
if r.status_code == 200:
    rec = wait(r.json()["uuid"])
    record("bad table -> job marked failed", rec["status"] == "failed",
           str(rec.get("message"))[:60])
else:
    record("bad table -> job marked failed", False, f"HTTP {r.status_code}")

r = c.post("/api/vector/v1/buffer-from-table",
           json={"payload": PAY,
                 "params": {"schema_name": "public", "table_name": "parcels",
                            "distance": 10, "unit": "parsecs"}})
record("invalid unit -> 422", r.status_code == 422)

r = c.post("/api/raster/v1/raster-to-vector-from-file",
           json={"payload": PAY, "raster_payload": {},
                 "params": {"raster_file": "/no/such/file.tif"}})
if r.status_code == 200:
    rec = wait(r.json()["uuid"])
    record("missing raster -> job marked failed", rec["status"] == "failed",
           str(rec.get("message"))[:60])
else:
    record("missing raster -> job marked failed", False, f"HTTP {r.status_code}")


print("\n" + "=" * 60)
passed = sum(1 for _, ok, _ in RESULTS if ok)
failed = [n for n, ok, _ in RESULTS if not ok]
print(f"  {passed}/{len(RESULTS)} passed")
if failed:
    print("  FAILED:")
    for n in failed:
        print(f"    - {n}")
print("=" * 60)
sys.exit(1 if failed else 0)
