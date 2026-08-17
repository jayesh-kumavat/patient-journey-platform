import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Numeric, Date, TIMESTAMP
from typing import List, Dict
from pathlib import Path
import pandas as pd
from config.settings import get_db_url, RAW_DIR
from src.ingestion.schema_validator import validate_and_quarantine

logger = logging.getLogger(__name__)

STAGING_DTYPES = {
    "stg_patients": {"date_of_birth": Date(), "enrollment_date": Date(), "ingested_at": TIMESTAMP()},
    "stg_physicians": {"years_experience": Integer(), "ingested_at": TIMESTAMP()},
    "stg_prescriptions": {"prescription_date": Date(), "quantity": Integer(), "refills": Integer(), "ingested_at": TIMESTAMP()},
    "stg_diagnoses": {"diagnosis_date": Date(), "ingested_at": TIMESTAMP()},
    "stg_claims": {"claim_date": Date(), "amount": Numeric(10, 2), "ingested_at": TIMESTAMP()},
    "pipeline_runs": {"started_at": TIMESTAMP(), "completed_at": TIMESTAMP(), "records_processed": Integer(), "records_failed": Integer()},
}

_engine_store: dict = {}

def get_engine():
    if "engine" not in _engine_store:
        _engine_store["engine"] = create_engine(get_db_url(), echo=False)
    return _engine_store["engine"]


def create_staging_tables(engine):
    metadata = MetaData()

    Table("stg_patients", metadata,
        Column("patient_id", String(20), primary_key=True),
        Column("gender", String(10)),
        Column("date_of_birth", Date),
        Column("region", String(50)),
        Column("enrollment_date", Date),
        Column("ingested_at", TIMESTAMP)
    )

    Table("stg_physicians", metadata,
        Column("physician_id", String(20), primary_key=True),
        Column("specialty", String(50)),
        Column("region", String(50)),
        Column("years_experience", Integer),
        Column("ingested_at", TIMESTAMP)
    )

    Table("stg_prescriptions", metadata,
        Column("prescription_id", String(20), primary_key=True),
        Column("patient_id", String(20)),
        Column("physician_id", String(20)),
        Column("drug_name", String(100)),
        Column("prescription_date", Date),
        Column("quantity", Integer),
        Column("refills", Integer),
        Column("therapy_area", String(50)),
        Column("ingested_at", TIMESTAMP)
    )

    Table("stg_diagnoses", metadata,
        Column("diagnosis_id", String(20), primary_key=True),
        Column("patient_id", String(20)),
        Column("icd_code", String(20)),
        Column("diagnosis_date", Date),
        Column("physician_id", String(20)),
        Column("severity", String(20)),
        Column("ingested_at", TIMESTAMP)
    )

    Table("stg_claims", metadata,
        Column("claim_id", String(20), primary_key=True),
        Column("patient_id", String(20)),
        Column("claim_date", Date),
        Column("amount", Numeric(10, 2)),
        Column("status", String(20)),
        Column("payer", String(50)),
        Column("ingested_at", TIMESTAMP)
    )

    Table("pipeline_runs", metadata,
        Column("run_id", String(100), primary_key=True),
        Column("pipeline_name", String(100)),
        Column("status", String(20)),
        Column("started_at", TIMESTAMP),
        Column("completed_at", TIMESTAMP),
        Column("records_processed", Integer),
        Column("records_failed", Integer)
    )

    metadata.create_all(engine)
    logger.info("Staging tables created/verified")


def load_records_to_staging(engine, records: List[Dict], table_name: str) -> int:
    if not records:
        return 0

    ingested_at = datetime.now(timezone.utc)
    for r in records:
        r["ingested_at"] = ingested_at

    df = pd.DataFrame(records)
    pk = df.columns[0]
    df = df.drop_duplicates(subset=[pk], keep="last")


    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=STAGING_DTYPES.get(table_name, {}))
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
        "timestamp": datetime.now(timezone.utc).isoformat(),    }


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


