"""
Incremental loading - only process records newer than the last watermark.
Used when we don't want to do a full refresh every time.
"""

from config.settings import get_db_url
from datetime import datetime
from typing import Optional
import pandas as pd
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

def get_engine():
    return create_engine(get_db_url(), echo=False)

def get_last_watermark(engine, table_name: str) -> Optional[str]:
    try:
        result = pd.read_sql(f"SELECT MAX(ingested_at) as last_watermark FROM {table_name}", engine)
        val = result["last_watermark"].iloc[0]
        return val if val else None
    except Exception:
        return None


def load_incremental(engine, source_table: str, target_table: str, key_column: str) -> int:
    watermark = get_last_watermark(engine, target_table)

    if watermark:
        query = f"SELECT * FROM {source_table} WHERE ingested_at > :wm"
        df = pd.read_sql(query, engine, params={"wm": watermark})
    else:
        df = pd.read_sql(f"SELECT * FROM {source_table}", engine)

    if df.empty:
        logger.info(f"No new records in {source_table}")
        return 0

    # delete-then-insert upsert pattern, not ideal but works fine for less volume
    with engine.connect() as conn:
        keys = df[key_column].tolist()
        if keys:
            placeholders = ",".join([f":k{i}" for i in range(len(keys))])
            params = {f"k{i}": k for i, k in enumerate(keys)}
            conn.execute(text(f"DELETE FROM {target_table} WHERE {key_column} IN ({placeholders})"), params)
        conn.commit()

    df.to_sql(target_table, engine, if_exists="append", index=False)
    logger.info(f"Incremental load: {len(df)} new records -> {target_table}")
    return len(df)


def record_pipeline_run(engine, pipeline_name: str, status: str, records_processed: int, records_failed: int = 0):
    run_id = f"{pipeline_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    with engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at, completed_at, records_processed, records_failed) "
                "VALUES (:rid, :name, :status, :started, :completed, :processed, :failed)"
            ),
            {
                "rid": run_id, "name": pipeline_name, "status": status,
                "started": datetime.utcnow().isoformat(),
                "completed": datetime.utcnow().isoformat(),
                "processed": records_processed, "failed": records_failed,
            }
        )
        conn.commit()
