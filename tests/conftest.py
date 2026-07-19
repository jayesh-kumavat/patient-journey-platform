"""Shared test fixtures."""

from pathlib import Path
import os
from urllib.parse import quote_plus
import pytest
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# test database config - uses a separate DB so we don't trash dev data
TEST_DB_NAME = "patient_journey_test"
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
TEST_DB_URL = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{TEST_DB_NAME}"


def _ensure_test_db_exists():
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname="postgres", user=DB_USER, password=DB_PASSWORD)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB_NAME,))
        if not cur.fetchone():
            from psycopg2.extensions import quote_ident
            cur.execute(f"CREATE DATABASE {quote_ident(TEST_DB_NAME, cur)}")
        cur.close()
    finally:
        if conn:
            conn.close()


_ensure_test_db_exists()


@pytest.fixture(scope="session")
def test_engine():
    engine = create_engine(TEST_DB_URL, echo=False)
    yield engine

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()
    engine.dispose()


@pytest.fixture
def test_data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def sample_patients():
    return [
        {"patient_id": "PAT-000001", "gender": "M", "date_of_birth": "1980-05-15", "region": "North", "enrollment_date": "2020-01-01"},
        {"patient_id": "PAT-000002", "gender": "F", "date_of_birth": "1975-11-20", "region": "South", "enrollment_date": "2019-06-15"},
        {"patient_id": "PAT-000003", "gender": "Other", "date_of_birth": "1990-03-10", "region": "East", "enrollment_date": "2021-03-01"},
    ]


@pytest.fixture
def sample_prescriptions():
    return [
        {"prescription_id": "RX-0000001", "patient_id": "PAT-000001", "physician_id": "PHY-00001",
         "drug_name": "DrugA-Onc", "prescription_date": "2023-01-15", "quantity": 30, "refills": 2, "therapy_area": "Oncology"},
        {"prescription_id": "RX-0000002", "patient_id": "PAT-000001", "physician_id": "PHY-00001",
         "drug_name": "DrugB-Onc", "prescription_date": "2023-03-20", "quantity": 60, "refills": 1, "therapy_area": "Oncology"},
        {"prescription_id": "RX-0000003", "patient_id": "PAT-000002", "physician_id": "PHY-00002",
         "drug_name": "DrugA-Card", "prescription_date": "2023-02-10", "quantity": 90, "refills": 3, "therapy_area": "Cardiology"},
        {"prescription_id": "RX-0000004", "patient_id": "PAT-000003", "physician_id": "PHY-00001",
         "drug_name": "DrugA-Onc", "prescription_date": "2023-04-01", "quantity": 700, "refills": 0, "therapy_area": "Oncology"},
    ]


@pytest.fixture
def sample_physicians():
    return [
        {"physician_id": "PHY-00001", "specialty": "Oncologist", "region": "North", "years_experience": 15},
        {"physician_id": "PHY-00002", "specialty": "Cardiologist", "region": "South", "years_experience": 8},
        {"physician_id": "PHY-00003", "specialty": "Neurologist", "region": "East", "years_experience": 22},
    ]
