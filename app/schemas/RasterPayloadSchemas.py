from typing import Optional
from pydantic import BaseModel
from enum import Enum


class StorageMethod(str, Enum):
    TEMP = "temp"      # keep on the server, return a local path
    OWN = "own"        # upload to the bucket named in the request
    ALLOC = "alloc"    # upload to the bucket configured in .env


class PixelUnit(str, Enum):
    meters = "meters"
    degrees = "degrees"


class Rasters3ConnectionPayload(BaseModel):
    awsAccessKeyId: Optional[str] = None
    awsSecretAccessKey: Optional[str] = None
    bucketName: Optional[str] = None
    region: Optional[str] = None
