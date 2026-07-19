"""
Ingestion layer - validates CSVs and loads them into staging tables.
"""

from pathlib import Path
import logging
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float
from typing import List, Dict
from datetime import datetime, timezone
from src.ingestion.schema_validator import validate_and_quarantine
from config.settings import get_db_url, RAW_DIR
import pandas as pd

logger = logging.getLogger(__name__)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(get_db_url(), echo=False)
    return _engine


def create_staging_tables(engine):
    metadata = MetaData()

    Table("stg_patients", metadata,
          Column("patient_id", String, primary_key=True),
          Column("gender", String),
          Column("date_of_birth", String),
          Column("region", String),
          Column("enrollment_date", String),
          Column("ingested_at", String))

    Table("stg_physicians", metadata,
          Column("physician_id", String, primary_key=True),
          Column("specialty", String),
          Column("region", String),
          Column("years_experience", Integer),
          Column("ingested_at", String))

    Table("stg_prescriptions", metadata,
          Column("prescription_id", String, primary_key=True),
          Column("patient_id", String),
          Column("physician_id", String),
          Column("drug_name", String),
          Column("prescription_date", String),
          Column("quantity", Integer),
          Column("refills", Integer),
          Column("therapy_area", String),
          Column("ingested_at", String))

    Table("stg_diagnoses", metadata,
          Column("diagnosis_id", String, primary_key=True),
          Column("patient_id", String),
          Column("icd_code", String),
          Column("diagnosis_date", String),
          Column("physician_id", String),
          Column("severity", String),
          Column("ingested_at", String))

    Table("stg_claims", metadata,
          Column("claim_id", String, primary_key=True),
          Column("patient_id", String),
          Column("claim_date", String),
          Column("amount", Float),
          Column("status", String),
          Column("payer", String),
          Column("ingested_at", String))

    Table("pipeline_runs", metadata,
          Column("run_id", String, primary_key=True),
          Column("pipeline_name", String),
          Column("status", String),
          Column("started_at", String),
          Column("completed_at", String),
          Column("records_processed", Integer),
          Column("records_failed", Integer))

    metadata.create_all(engine)
    logger.info("Staging tables created/verified")


def load_records_to_staging(engine, records: List[Dict], table_name: str) -> int:
    if not records:
        return 0

    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["ingested_at"] = ingested_at

    df = pd.DataFrame(records)
    pk = df.columns[0]
    df = df.drop_duplicates(subset=[pk], keep="last")

    # TODO: switch to upsert instead of replace - this is fine for now
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logger.info(f"Loaded {len(df)} records -> {table_name}")
    return len(df)


def ingest_file(filepath: Path, dataset_type: str, engine=None) -> Dict:
    engine = engine or get_engine()

    table_map = {
        "patients": "stg_patients",
        "physicians": "stg_physicians",
        "prescriptions": "stg_prescriptions",
        "diagnoses": "stg_diagnoses",
        "claims": "stg_claims",
    }
    if dataset_type not in table_map:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

    logger.info(f"Ingesting {filepath.name} ({dataset_type})")
    valid_records = validate_and_quarantine(filepath, dataset_type)
    loaded = load_records_to_staging(engine, valid_records, table_map[dataset_type])

    return {
        "file": str(filepath),
        "dataset_type": dataset_type,
        "records_loaded": loaded,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def ingest_all(data_dir=None) -> List[Dict]:
    data_dir = data_dir or RAW_DIR
    engine = get_engine()
    create_staging_tables(engine)

    files_to_load = [
        ("patients.csv", "patients"),
        ("physicians.csv", "physicians"),
        ("prescriptions.csv", "prescriptions"),
        ("diagnoses.csv", "diagnoses"),
        ("claims.csv", "claims"),
    ]

    results = []
    for filename, dtype in files_to_load:
        fp = data_dir / filename
        if fp.exists():
            results.append(ingest_file(fp, dtype, engine))
        else:
            logger.warning(f"File not found, skipping: {fp}")

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = ingest_all()
    for r in results:
        print(f"  {r['dataset_type']}: {r['records_loaded']} records")
