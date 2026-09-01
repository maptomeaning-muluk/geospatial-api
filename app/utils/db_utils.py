# app/utils/db_utils.py
import os
import threading

import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine, text


# Engines are pooled and cached per (connection, pid). Building one per call
# means a fresh TCP + auth handshake on every query (~90 ms) and no pooling at
# all; the cache keeps a real connection pool alive instead. Keyed by pid so a
# forked Dask worker never reuses a socket inherited from its parent.
_engines = {}
_engine_lock = threading.Lock()


def _conn_values(payload):
    def get_value(key):
        return getattr(payload, key) if hasattr(payload, key) else payload.get(key)
    return {k: get_value(k) for k in ("host", "port", "dbname", "user", "password")}


def get_engine_from_payload(payload):
    """Pooled engine for a connection given as a pydantic model or a dict."""
    cfg = _conn_values(payload)
    key = (os.getpid(), cfg["host"], str(cfg["port"]), cfg["dbname"], cfg["user"])

    with _engine_lock:
        engine = _engines.get(key)
        if engine is None:
            def get_conn(cfg=cfg):
                return psycopg2.connect(
                    host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
                    user=cfg["user"], password=cfg["password"],
                )

            engine = create_engine(
                "postgresql+psycopg2://",
                creator=get_conn,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=1800,
            )
            _engines[key] = engine
        return engine


def get_default_engine():
    """Engine for the service's own database, from .env."""
    return get_engine_from_payload({
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    })


def dispose_engines():
    with _engine_lock:
        for engine in _engines.values():
            try:
                engine.dispose()
            except Exception:
                pass
        _engines.clear()


def load_gdf_from_db_by_engine(engine, schema: str, table: str,
                               where: str = "") -> gpd.GeoDataFrame:
    query = f'SELECT * FROM "{schema}"."{table}"'
    if where:
        query += f" WHERE {where}"
    gdf = gpd.read_postgis(query, con=engine, geom_col=get_geom_column(engine, schema, table))
    return gdf


def get_geom_column(engine, schema: str, table: str, default: str = "geom") -> str:
    """Find the geometry column so tables using 'geometry' also work."""
    sql = text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
          AND udt_name IN ('geometry', 'geography')
        ORDER BY ordinal_position LIMIT 1
    """)
    with engine.connect() as conn:
        found = conn.execute(sql, {"schema": schema, "table": table}).scalar()
    return found or default


def write_gdf_to_db(gdf, engine, schema: str, table: str, if_exists: str = "replace"):
    """Write a GeoDataFrame back to PostGIS, creating the schema/table/index."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    gdf = gdf.copy()
    if gdf.geometry.name != "geom":
        gdf = gdf.rename_geometry("geom")

    gdf.to_postgis(name=table, con=engine, schema=schema,
                   if_exists=if_exists, index=False)

    # to_postgis already builds its own GiST index (idx_<table>_geom); adding a
    # second one just doubles the write cost and the disk footprint.
    with engine.begin() as conn:
        has_index = conn.execute(text("""
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = :schema AND tablename = :table
              AND indexdef ILIKE '%USING gist%'
        """), {"schema": schema, "table": table}).scalar()
        if not has_index:
            conn.execute(text(
                f'CREATE INDEX "{table}_geom_idx" '
                f'ON "{schema}"."{table}" USING GIST (geom)'
            ))
    return f"{schema}.{table}"
