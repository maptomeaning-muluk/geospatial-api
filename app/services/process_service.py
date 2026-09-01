# app/services/process_service.py
import json
import os
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import text

from app.utils.db_utils import get_default_engine

load_dotenv()

ist_time = ZoneInfo("Asia/Kolkata")


def log_start(input_layer, overlay_layer, tool_name, data_type,
              project_id=None, business_id=None, file_path=None,
              output_layer=None):
    """Create the processing row and return its uuid - this is the job id."""
    engine = get_default_engine()
    log_id = str(uuid.uuid4())
    with engine.begin() as cursor:
        cursor.execute(text("""
            INSERT INTO processing
                (input_layer, overlay_layer, tool_name, data_type, status, uuid,
                 project_id, business_id, file_path, start_time, output_layer)
            VALUES
                (:input_layer, :overlay_layer, :tool_name, :data_type, 'processing',
                 :uuid, :project_id, :business_id, :file_path, :start_time,
                 :output_layer)
        """), {
            "input_layer": input_layer,
            "overlay_layer": overlay_layer,
            "tool_name": tool_name,
            "data_type": data_type,
            "uuid": log_id,
            "project_id": project_id,
            "business_id": business_id,
            "file_path": file_path,
            "start_time": datetime.now(ist_time),
            "output_layer": output_layer,
        })
    return log_id


def log_end(log_id, success=True, message=None, result=None):
    """Close the processing row. Called by the Dask worker when the job ends."""
    engine = get_default_engine()
    with engine.begin() as cursor:
        cursor.execute(text("""
            UPDATE processing
            SET status = :status,
                end_time = :end_time,
                message = :message,
                result = :result
            WHERE uuid = :uuid
        """), {
            "status": "completed" if success else "failed",
            "end_time": datetime.now(ist_time),
            "message": message,
            "result": json.dumps(result, default=str) if result is not None else None,
            "uuid": log_id,
        })


def get_processing_by_uuid(job_uuid: str):
    """Job status - what the client polls after submitting."""
    engine = get_default_engine()
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT * FROM processing WHERE uuid = :uuid"),
                {"uuid": job_uuid},
            )
            row = result.fetchone()
            if not row:
                raise HTTPException(404, f"No processing record for uuid {job_uuid}")
            record = dict(zip(result.keys(), row))
            if record.get("result"):
                try:
                    record["result"] = json.loads(record["result"])
                except Exception:
                    pass
            return record
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error while fetching processing record: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_all_processing(limit: int = 100):
    engine = get_default_engine()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT * FROM processing
                ORDER BY created_at DESC
                LIMIT :limit;
            """), {"limit": limit})
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Error while fetching processing records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_processing_records_by_filters(filters: dict):
    engine = get_default_engine()
    try:
        with engine.connect() as connection:
            conditions = []
            params = {}
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = :{key}")
                    params[key] = value

            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = f"""
                SELECT * FROM processing
                WHERE {where_clause}
                ORDER BY created_at DESC;
            """
            result = connection.execute(text(query), params)
            rows = result.fetchall()
            if not rows:
                return []
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Error while fetching processing records: {e}")
        raise HTTPException(status_code=500, detail=str(e))
