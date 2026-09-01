# app/migration/table_mig.py
from sqlalchemy import text

from app.utils.db_utils import get_default_engine


def ensure_processing_table():
    """Create PostGIS, the processing table and its enums. Runs on startup."""
    engine = get_default_engine()
    with engine.begin() as cursor:
        cursor.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        cursor.execute(text("""
            DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type
                                   WHERE typname = 'processing_status_enum') THEN
                        CREATE TYPE processing_status_enum AS ENUM
                            ('processing', 'completed', 'failed');
                    END IF;

                    IF NOT EXISTS (SELECT 1 FROM pg_type
                                   WHERE typname = 'data_type_enum') THEN
                        CREATE TYPE data_type_enum AS ENUM ('vector', 'raster');
                    END IF;
                END
            $$;

            CREATE TABLE IF NOT EXISTS processing (
                created_at TIMESTAMP DEFAULT now(),
                processing_row_id SERIAL PRIMARY KEY,
                data_type data_type_enum NOT NULL DEFAULT 'vector',
                tool_name VARCHAR(255) NOT NULL,
                status processing_status_enum NOT NULL DEFAULT 'processing',
                uuid VARCHAR(255) UNIQUE NOT NULL,
                business_id VARCHAR,
                project_id VARCHAR,
                file_path VARCHAR(255),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                message TEXT,
                result TEXT,
                input_layer TEXT,
                overlay_layer TEXT,
                output_layer TEXT
            );

            ALTER TABLE processing ADD COLUMN IF NOT EXISTS result TEXT;
            CREATE INDEX IF NOT EXISTS processing_uuid_idx ON processing (uuid);
        """))
    print("[migration] processing table ready")
