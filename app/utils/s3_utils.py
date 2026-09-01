# app/utils/s3_utils.py
import os
import uuid
from typing import Optional
from urllib.parse import urlparse

import boto3
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("awsAccessKeyId")
AWS_SECRET_ACCESS_KEY = os.getenv("awsSecretAccessKey")
AWS_REGION = os.getenv("region")
BUCKET_NAME = os.getenv("bucketName")


def _get_s3_client(access_key: Optional[str] = None,
                   secret_key: Optional[str] = None,
                   region: Optional[str] = None):
    return boto3.client(
        "s3",
        aws_access_key_id=access_key or AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secret_key or AWS_SECRET_ACCESS_KEY,
        region_name=region or AWS_REGION,
    )


def parse_s3_uri(uri: str):
    """s3://bucket/folder/file.tif -> ('bucket', 'folder/file.tif')"""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise HTTPException(400, f"Not an S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def download_from_s3(uri: str, dest_dir: str,
                     access_key: Optional[str] = None,
                     secret_key: Optional[str] = None) -> str:
    """Download an S3 object and return the local path."""
    bucket, key = parse_s3_uri(uri)
    os.makedirs(dest_dir, exist_ok=True)
    local_path = os.path.join(dest_dir, os.path.basename(key))
    s3 = _get_s3_client(access_key, secret_key)
    try:
        s3.download_file(bucket, key, local_path)
    except Exception as e:
        raise HTTPException(500, f"Failed to download {uri}: {e}")
    return local_path


def upload_file_to_s3(local_path: str, folder_prefix: str,
                      output_filename: Optional[str] = None,
                      bucket: Optional[str] = None,
                      access_key: Optional[str] = None,
                      secret_key: Optional[str] = None) -> str:
    """Upload a local file and return its s3:// URI."""
    bucket = bucket or BUCKET_NAME
    if not bucket:
        raise HTTPException(400, "No S3 bucket configured (set bucketName in .env)")

    filename = output_filename or f"output_{uuid.uuid4()}{os.path.splitext(local_path)[1]}"
    key = f"{folder_prefix.rstrip('/')}/{filename}"
    s3 = _get_s3_client(access_key, secret_key)
    try:
        s3.upload_file(local_path, bucket, key)
    except Exception as e:
        raise HTTPException(500, f"Failed to upload raster: {e}")
    return f"s3://{bucket}/{key}"


def resolve_input_path(path: str, dest_dir: str,
                       access_key: Optional[str] = None,
                       secret_key: Optional[str] = None) -> str:
    """Accept either a local path or an s3:// URI and always return a local path."""
    if str(path).startswith("s3://"):
        return download_from_s3(path, dest_dir, access_key, secret_key)
    if not os.path.isfile(path):
        raise HTTPException(404, f"File not found: {path}")
    return path


def store_output(local_path: str, storage_method: str, business_id: str,
                 project_id: str, output_name: Optional[str] = None,
                 bucket: Optional[str] = None,
                 access_key: Optional[str] = None,
                 secret_key: Optional[str] = None) -> str:
    """Keep the file locally (temp) or push it to S3 (own / alloc)."""
    if storage_method == "temp":
        return local_path
    prefix = f"python_service/{business_id}/{project_id}"
    return upload_file_to_s3(local_path, prefix, output_name,
                             bucket=bucket, access_key=access_key,
                             secret_key=secret_key)
