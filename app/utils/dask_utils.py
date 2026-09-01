# app/utils/dask_utils.py
"""Dask client + the two helpers the services use.

`submit_job`   - hand a job to the cluster and return its uuid straight away,
                 so the caller can keep working and poll /api/process for status.
`parallel_map` - split a GeoDataFrame across the cluster for the heavy geometry
                 work, then stitch the pieces back together.
"""
import os
import traceback
from contextlib import contextmanager

import geopandas as gpd
import pandas as pd

_client = None


def get_client():
    """One client per process. Connects to DASK_SCHEDULER_ADDRESS when set,
    otherwise starts a LocalCluster so a laptop needs nothing extra."""
    global _client
    if _client is not None:
        return _client

    from dask.distributed import Client, LocalCluster

    address = os.environ.get("DASK_SCHEDULER_ADDRESS", "").strip()
    if address:
        _client = Client(address, timeout="30s")
    else:
        cluster = LocalCluster(
            n_workers=int(os.environ.get("DASK_WORKERS", 4)),
            threads_per_worker=int(os.environ.get("DASK_THREADS", 2)),
            memory_limit=os.environ.get("DASK_MEMORY_LIMIT", "2GB"),
            processes=True,
        )
        _client = Client(cluster)
    print(f"[dask] connected: {_client.dashboard_link}")
    return _client


def close_client():
    global _client
    if _client is not None:
        _client.close()
        _client = None


def on_worker() -> bool:
    """True when this code is running inside a Dask task."""
    try:
        from distributed import get_worker
        get_worker()
        return True
    except Exception:
        return False


@contextmanager
def client_context():
    """A client that works from the API process *and* from inside a worker.

    A job runs on a worker; when that job wants to fan its own work out it must
    not call get_client() - with no external scheduler that would try to start a
    second LocalCluster inside the worker ("Nanny failed to start"). The correct
    pattern is worker_client(), which lends the running task the worker's own
    client and secedes its thread so the pool is not blocked while it waits.
    """
    if on_worker():
        from distributed import worker_client
        with worker_client() as wc:
            yield wc
    else:
        yield get_client()


# ---------------------------------------------------------------------------
# Fire-and-forget job submission
# ---------------------------------------------------------------------------
def submit_job(func, log_id, **kwargs):
    """Run `func(**kwargs)` on the cluster; mark the processing row done/failed.

    Returns immediately - the HTTP handler answers with `log_id` while the
    worker keeps running.
    """
    client = get_client()
    future = client.submit(_run_and_log, func, log_id, kwargs, pure=False)
    # Keep a reference so Dask does not garbage-collect the task.
    _keep_alive.append(future)
    _keep_alive[:] = [f for f in _keep_alive if not f.done()][-500:]
    return log_id


_keep_alive = []


def _run_and_log(func, log_id, kwargs):
    """Executed on the worker. Owns the outcome of the processing row."""
    from app.services.process_service import log_end

    try:
        result = func(**kwargs)
        log_end(log_id, success=True,
                message="Operation completed successfully",
                result=result)
        return result
    except Exception as e:
        traceback.print_exc()
        log_end(log_id, success=False, message=str(e))
        raise


# ---------------------------------------------------------------------------
# Parallel GeoDataFrame processing
# ---------------------------------------------------------------------------
def parallel_map(gdf, func, chunk_size=None, **kwargs):
    """Apply `func(chunk, **kwargs)` to slices of `gdf` across the cluster.

    `func` must be a module-level function so Dask can pickle it. Small frames
    are done inline - shipping them to a worker would cost more than the work.
    """
    if chunk_size is None:
        chunk_size = int(os.environ.get("DASK_CHUNK_SIZE", 20000))

    if len(gdf) <= chunk_size:
        return func(gdf, **kwargs)

    chunks = [gdf.iloc[i:i + chunk_size] for i in range(0, len(gdf), chunk_size)]
    with client_context() as client:
        futures = [client.submit(func, chunk, pure=False, **kwargs) for chunk in chunks]
        results = client.gather(futures)

    results = [r for r in results if r is not None and len(r) > 0]
    if not results:
        return gpd.GeoDataFrame(geometry=[], crs=gdf.crs)

    merged = pd.concat(results, ignore_index=True)
    return gpd.GeoDataFrame(merged, geometry=results[0].geometry.name, crs=gdf.crs)


def cluster_info():
    """Small dict for the /api/process/v1/cluster endpoint."""
    try:
        client = get_client()
        info = client.scheduler_info()
        workers = info.get("workers", {})
        return {
            "scheduler": info.get("address"),
            "dashboard": client.dashboard_link,
            "workers": len(workers),
            "threads": sum(w.get("nthreads", 0) for w in workers.values()),
        }
    except Exception as e:
        return {"error": str(e)}
