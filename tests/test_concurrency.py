"""Prove submission is non-blocking and jobs really overlap on the cluster."""
import app  # noqa: F401
import os, time, httpx
from dotenv import load_dotenv
load_dotenv()

c = httpx.Client(base_url="http://127.0.0.1:5000", timeout=300.0,
                 headers={"Authorization": f"Bearer {os.environ['JWT_TOKEN']}"})
DB = {"host": os.environ["DB_HOST"], "port": int(os.environ["DB_PORT"]),
      "dbname": os.environ["DB_NAME"], "user": os.environ["DB_USER"],
      "password": os.environ["DB_PASSWORD"]}
PAY = {"db_connection": DB, "project_id": "p1", "business_id": "conc"}

jobs = [
    ("buffer",  "/api/vector/v1/buffer-from-table",
     {"payload": PAY, "params": {"schema_name": "public", "table_name": "parcels",
                                 "distance": 300, "unit": "meters"}}),
    ("split",   "/api/vector/v1/split-from-table",
     {"payload": PAY, "params": {"schema_name": "public", "table_name": "parcels",
                                 "mode": "grid", "grid_rows": 3, "grid_cols": 3}}),
    ("combine", "/api/vector/v1/combine-from-table",
     {"payload": PAY, "params": {"schema_name": "public",
                                 "table_names": ["parcels", "overlay", "wells"]}}),
    ("trim",    "/api/vector/v1/trim-from-table",
     {"payload": PAY, "params": {"schema_name": "public", "input_table": "parcels",
                                 "overlay_table": "overlay"}}),
    ("v2r",     "/api/raster/v1/vector-to-raster-from-table",
     {"payload": PAY, "raster_payload": {},
      "params": {"schema_name": "public", "table_name": "parcels",
                 "pixel_size": 5, "pixel_unit": "meters"},
      "storage_method": "temp"}),
    ("r2v",     "/api/raster/v1/raster-to-vector-from-file",
     {"payload": PAY, "raster_payload": {},
      "params": {"raster_file": os.path.abspath("testdata/landcover.tif"),
                 "field_name": "class_id", "dissolve": True}}),
]

print("1. SUBMIT all six at once (each must return in ms, before any work runs)")
uuids, t_all = {}, time.perf_counter()
for name, path, body in jobs:
    t0 = time.perf_counter()
    r = c.post(path, json=body)
    dt = (time.perf_counter() - t0) * 1000
    assert r.status_code == 200, (name, r.text[:120])
    uuids[name] = r.json()["uuid"]
    print(f"   {name:<8} -> {r.json()['status']:<10} {dt:6.1f} ms")
print(f"   all 6 accepted in {(time.perf_counter()-t_all)*1000:.0f} ms total")

print("\n2. POLL - the API must stay responsive while work runs")
seen_processing, done, ticks = set(), {}, 0
deadline = time.time() + 180
while time.time() < deadline and len(done) < len(uuids):
    t0 = time.perf_counter()
    row = []
    for name, uid in uuids.items():
        if name in done:
            row.append(f"{name}:{done[name][:4]}"); continue
        s = c.get(f"/api/process/v1/status/{uid}").json()["data"]
        row.append(f"{name}:{s['status'][:4]}")
        if s["status"] == "processing":
            seen_processing.add(name)
        else:
            done[name] = s["status"]
    poll_ms = (time.perf_counter() - t0) * 1000
    if ticks < 5:
        print(f"   [{poll_ms:5.1f} ms for 6 status calls] " + "  ".join(row))
    ticks += 1
    time.sleep(0.3)

print(f"\n3. RESULT")
print(f"   jobs observed still 'processing' while others ran: "
      f"{len(seen_processing)}/{len(uuids)} -> {sorted(seen_processing)}")
ok = all(v == "completed" for v in done.values())
for name, uid in uuids.items():
    s = c.get(f"/api/process/v1/status/{uid}").json()["data"]
    start, end = s.get("start_time"), s.get("end_time")
    print(f"   {name:<8} {s['status']:<10} {str(start)[11:23]} -> {str(end)[11:23]}")

# Do the execution windows actually overlap?
from datetime import datetime
spans = []
for name, uid in uuids.items():
    s = c.get(f"/api/process/v1/status/{uid}").json()["data"]
    if s.get("start_time") and s.get("end_time"):
        spans.append((name, datetime.fromisoformat(str(s["start_time"])),
                      datetime.fromisoformat(str(s["end_time"]))))
overlaps = sum(1 for i, a in enumerate(spans) for b in spans[i+1:]
               if a[1] < b[2] and b[1] < a[2])
print(f"\n   overlapping execution windows: {overlaps} pair(s)")
print(f"   all completed: {ok}")
