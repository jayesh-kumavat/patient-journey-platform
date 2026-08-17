from datetime import datetime, timezone
import logging
from sqlalchemy import create_engine, text
from typing import Optional
import pandas as pd
from config.settings import get_db_url

logger = logging.getLogger(__name__)

ALLOWED_TABLES = {
    "stg_patients", "stg_physicians", "stg_prescriptions", "stg_diagnoses", "stg_claims",
    "dim_patient", "dim_physician", "fact_prescription", "fact_patient_journey", "therapy_switches",
    "anomaly_results", "pipeline_runs", "dq_scores",
}
ALLOWED_COLUMNS = {"patient_id", "physician_id", "prescription_id", "diagnosis_id", "claim_id", "ingested_at"}


def _safe(name: str, allowed: set) -> str:
    if name not in allowed:
        raise ValueError(f"Invalid identifier: {name}")
    return name


def get_engine():
    return create_engine(get_db_url(), echo=False)

def get_last_watermark(engine, table_name: str) -> Optional[str]:
    try:
        t = _safe(table_name, ALLOWED_TABLES)
        result = pd.read_sql(text("SELECT MAX(ingested_at) as last_watermark FROM " + t), engine)
        val = result["last_watermark"].iloc[0]
        return val if val else None
    except Exception:
        return None


def load_incremental(engine, source_table: str, target_table: str, key_column: str) -> int:
    src = _safe(source_table, ALLOWED_TABLES)
    tgt = _safe(target_table, ALLOWED_TABLES)
    col = _safe(key_column, ALLOWED_COLUMNS)

    watermark = get_last_watermark(engine, tgt)

    if watermark:
        df = pd.read_sql(text("SELECT * FROM " + src + " WHERE ingested_at > :wm"), engine, params={"wm": watermark})
    else:
        df = pd.read_sql(text("SELECT * FROM " + src), engine)

    if df.empty:
        logger.info(f"No new records in {src}")
        return 0

    with engine.connect() as conn:
        keys = df[col].tolist()
        if keys:
            placeholders = ",".join([f":k{i}" for i in range(len(keys))])
            params = {f"k{i}": k for i, k in enumerate(keys)}
            conn.execute(text("DELETE FROM " + tgt + " WHERE " + col + " IN (" + placeholders + ")"), params)
        conn.commit()

    df.to_sql(target_table, engine, if_exists="append", index=False)
    logger.info(f"Incremental load: {len(df)} new records -> {target_table}")
    return len(df)


def record_pipeline_run(engine, pipeline_name: str, status: str, records_processed: int, records_failed: int = 0):
    now = datetime.now(timezone.utc)
    run_id = f"{pipeline_name}_{now.strftime('%Y%m%d_%H%M%S')}"
    with engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at, completed_at, records_processed, records_failed) "
                "VALUES (:rid, :name, :status, :started, :completed, :processed, :failed)"
            ),
            {
                "rid": run_id, "name": pipeline_name, "status": status,
                "started": now,
                "completed": now,
                "processed": records_processed, "failed": records_failed,
            }
        )
        conn.commit()