"""Job monitoring routes.

Every tool endpoint returns a `uuid` straight away; these routes are how you
find out what happened to it. State lives in the `processing` table, so status
survives an API restart and is visible from any replica.

Job lifecycle: `processing` -> `completed` | `failed`.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services import process_service
from app.utils.dask_utils import cluster_info

router = APIRouter()


@router.get(
    "/v1/status/{job_uuid}",
    summary="Job status - poll this with the uuid a tool returned",
    response_description="The processing record, including `result` once complete",
)
def get_job_status(job_uuid: str):
    """
    Look up one job by the `uuid` a tool endpoint returned.

    ### Input

    Path parameter `job_uuid` - the `uuid` from the tool response.

    ### Output

    ```json
    {"message": "Processing record fetched successfully",
     "data": {"uuid": "3f9c1e2b-...",
              "tool_name": "buffer",
              "data_type": "vector",
              "status": "completed",
              "business_id": "b1", "project_id": "p1",
              "input_layer": "parcels", "overlay_layer": null,
              "output_layer": "roads_buffer_500m",
              "start_time": "2026-09-01T12:45:26",
              "end_time": "2026-09-01T12:45:29",
              "message": "Operation completed successfully",
              "result": {"feature_count": 25, "crs": "EPSG:4326",
                         "file": "./output/roads_buffer_500m.geojson",
                         "table": "public.roads_buffer_500m"}}}
    ```

    `status` is `processing`, `completed` or `failed`. While it is
    `processing`, `result` is `null`. On `failed`, `message` carries the
    reason and `result` stays `null`.

    Unknown uuid returns `404`.
    """
    record = process_service.get_processing_by_uuid(job_uuid)
    return {"message": "Processing record fetched successfully", "data": record}


@router.get(
    "/v1/all-process",
    summary="Recent jobs across every business",
    response_description="Processing records, newest first",
)
def get_all_processing(limit: int = Query(100, ge=1, le=1000,
                                          description="Max records to return")):
    """
    List recent jobs, newest first.

    ### Input

    Query parameter `limit` (1-1000, default 100).

    ### Output

    ```json
    {"message": "All processing records fetched successfully",
     "data": [{"uuid": "...", "tool_name": "buffer", "status": "completed",
               "start_time": "...", "end_time": "...", "result": {...}}]}
    ```

    Same record shape as `/v1/status/{uuid}`, as a list.
    """
    try:
        records = process_service.get_all_processing(limit)
        return {"message": "All processing records fetched successfully",
                "data": records}
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/v1/cluster",
    summary="Dask cluster capacity",
    response_description="Scheduler address, dashboard link, worker and thread counts",
)
def get_cluster_info():
    """
    Report the compute backing the service - useful for checking that workers
    actually came up before you submit a large job.

    ### Input

    None.

    ### Output

    ```json
    {"message": "Cluster info",
     "data": {"scheduler": "tcp://127.0.0.1:57516",
              "dashboard": "http://127.0.0.1:8787/status",
              "workers": 4, "threads": 8}}
    ```

    If the cluster is unreachable, `data` is `{"error": "..."}` instead.
    """
    return {"message": "Cluster info", "data": cluster_info()}


@router.get(
    "/v1/{business_id}",
    summary="Jobs for one business, with optional filters",
    response_description="Matching processing records, newest first",
)
def get_processing_records(
    business_id: str,
    project_id: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None, description="vector | raster"),
    tool_name: Optional[str] = Query(
        None, description="buffer | split | combine | trim | "
                          "vector_to_raster | raster_to_vector"),
    status: Optional[str] = Query(None, description="processing | completed | failed"),
    output_layer: Optional[str] = Query(None),
):
    """
    Filter a business's jobs.

    ### Input

    Path parameter `business_id`, plus any combination of the query filters
    `project_id`, `data_type`, `tool_name`, `status`, `output_layer`. Filters
    left unset are ignored.

    ### Output

    ```json
    {"message": "Filtered processing records fetched successfully",
     "data": [{"uuid": "...", "tool_name": "buffer", "status": "completed", ...}]}
    ```

    An empty list (not a `404`) when nothing matches.
    """
    try:
        filters = {
            "business_id": business_id,
            "project_id": project_id,
            "data_type": data_type,
            "tool_name": tool_name,
            "status": status,
            "output_layer": output_layer,
        }
        filters = {k: v for k, v in filters.items() if v is not None}

        records = process_service.get_processing_records_by_filters(filters)
        return {"message": "Filtered processing records fetched successfully",
                "data": records}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
