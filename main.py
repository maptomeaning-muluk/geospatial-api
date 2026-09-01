# main.py
import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

load_dotenv()

from app.middleware.auth_middleware import AuthMiddleware  # noqa: E402
from app.migration.table_mig import ensure_processing_table  # noqa: E402
from app.routes import geo_routes, process_routes, raster_routes  # noqa: E402

port = int(os.environ.get("PORT", 5000))

# Make sure the processing table (the job log) exists before serving
ensure_processing_table()

app = FastAPI(title="GeoSpatial API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(AuthMiddleware)

app.include_router(geo_routes.router, prefix="/api/vector",
                   tags=["Vector Operations"])
app.include_router(raster_routes.router, prefix="/api/raster",
                   tags=["Raster Operations"])
app.include_router(process_routes.router, prefix="/api/process",
                   tags=["Processing Operations"])


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok", "service": "GeoSpatial API"}


@app.on_event("startup")
def startup_dask():
    """Bring the Dask cluster up with the app so the first job is not slow."""
    from app.utils.dask_utils import get_client
    try:
        get_client()
    except Exception as e:
        print(f"[dask] cluster not ready at startup: {e}")


@app.on_event("shutdown")
def shutdown_dask():
    from app.utils.dask_utils import close_client
    from app.utils.db_utils import dispose_engines
    close_client()
    dispose_engines()


# Inject HTTP Bearer Auth into Swagger
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="GeoSpatial API",
        version="1.0.0",
        description=(
            "Buffer, Split, Combine, Trim, Raster to Vector and Vector to Raster.\n\n"
            "Every tool endpoint returns a `uuid` immediately and runs the work on a "
            "Dask cluster. Poll `GET /api/process/v1/status/{uuid}` for the result.\n\n"
            "Each tool comes in two flavours: `-from-table` reads an existing PostGIS "
            "table, `-from-geojson` takes an uploaded file."
        ),
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
    }
    for path in openapi_schema["paths"].values():
        for method in path.values():
            method.setdefault("security", [{"BearerAuth": []}])
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
