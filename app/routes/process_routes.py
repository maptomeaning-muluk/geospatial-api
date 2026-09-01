from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.services import process_service
from app.utils.dask_utils import cluster_info

router = APIRouter()


@router.get("/v1/status/{job_uuid}")
def get_job_status(job_uuid: str):
    """Job status - poll this with the uuid a tool endpoint returned."""
    record = process_service.get_processing_by_uuid(job_uuid)
    return {"message": "Processing record fetched successfully", "data": record}


@router.get("/v1/all-process")
def get_all_processing(limit: int = Query(100, ge=1, le=1000)):
    try:
        records = process_service.get_all_processing(limit)
        return {"message": "All processing records fetched successfully",
                "data": records}
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/v1/cluster")
def get_cluster_info():
    """Dask cluster capacity - workers, threads, dashboard link."""
    return {"message": "Cluster info", "data": cluster_info()}


@router.get("/v1/{business_id}")
def get_processing_records(
    business_id: str,
    project_id: Optional[str] = Query(None),
    data_type: Optional[str] = Query(None),
    tool_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    output_layer: Optional[str] = Query(None),
):
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
