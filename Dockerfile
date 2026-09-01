# One image, three roles: api, dask scheduler, dask worker.
# The workers must run the SAME image as the API, because Dask ships the job
# function by module path and the worker has to import app.services.* itself.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# geopandas/rasterio wheels bundle their own GDAL/GEOS/PROJ - no system GDAL,
# which also avoids the usual version-skew breakage.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl libexpat1 libgomp1 libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY app ./app

RUN mkdir -p /app/output

EXPOSE 5000 8786 8787

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD curl -fsS http://localhost:5000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
