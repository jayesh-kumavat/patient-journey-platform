"""API endpoint tests."""

import pandas as pd
import sys
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tests.conftest import TEST_DB_URL
from src.ingestion.data_ingest import create_staging_tables, load_records_to_staging
from src.processing.spark_transform import (
    create_warehouse_tables, transform_patients, transform_prescriptions,
    build_patient_journey, detect_therapy_switches,
)


@pytest.fixture(scope="module")
def setup_api_db():
    engine = create_engine(TEST_DB_URL, echo=False)

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()

    create_staging_tables(engine)
    create_warehouse_tables(engine)

    patients = [
        {"patient_id": "PAT-000001", "gender": "M", "date_of_birth": "1980-05-15",
         "region": "North", "enrollment_date": "2020-01-01"},
        {"patient_id": "PAT-000002", "gender": "F", "date_of_birth": "1975-11-20",
         "region": "South", "enrollment_date": "2019-06-15"},
    ]
    physicians = [
        {"physician_id": "PHY-00001", "specialty": "Oncologist",
         "region": "North", "years_experience": 15},
    ]
    prescriptions = [
        {"prescription_id": "RX-0000001", "patient_id": "PAT-000001", "physician_id": "PHY-00001",
         "drug_name": "DrugA-Onc", "prescription_date": "2023-01-15", "quantity": 30,
         "refills": 2, "therapy_area": "Oncology"},
        {"prescription_id": "RX-0000002", "patient_id": "PAT-000001", "physician_id": "PHY-00001",
         "drug_name": "DrugB-Onc", "prescription_date": "2023-03-20", "quantity": 60,
         "refills": 1, "therapy_area": "Oncology"},
    ]

    load_records_to_staging(engine, patients, "stg_patients")
    load_records_to_staging(engine, physicians, "stg_physicians")
    load_records_to_staging(engine, prescriptions, "stg_prescriptions")

    transform_patients(engine)
    transform_prescriptions(engine)
    build_patient_journey(engine)
    detect_therapy_switches(engine)

    # insert a fake anomaly for testing
    pd.DataFrame([{
        "physician_id": "PHY-00001", "anomaly_score": -0.8,
        "anomaly_type": "prescription_volume", "total_prescriptions": 500,
        "avg_quantity": 150.0, "max_quantity": 700, "unique_patients": 200,
        "detection_date": "2024-01-15", "confidence": 0.85,
        "explanation": "High prescription volume (500)",
    }]).to_sql("anomaly_results", engine, if_exists="replace", index=False)

    return engine


@pytest.fixture
def client(setup_api_db, monkeypatch):
    engine = setup_api_db
    mock_engine = lambda: engine
    monkeypatch.setattr("src.api.routes.patients.get_engine", mock_engine)
    monkeypatch.setattr("src.api.routes.anomalies.get_engine", mock_engine)
    monkeypatch.setattr("src.api.routes.kpis.get_engine", mock_engine)

    from src.api.main import app
    return TestClient(app)


# basic endpoints

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"


def test_root_has_endpoints(client):
    assert "endpoints" in client.get("/").json()


# patient endpoints

def test_patient_journey(client):
    r = client.get("/patients/PAT-000001/journey")
    assert r.status_code == 200
    assert r.json()["total_events"] > 0


def test_patient_not_found(client):
    assert client.get("/patients/PAT-999999/journey").status_code == 404


def test_list_patients(client):
    r = client.get("/patients/")
    assert r.json()["count"] > 0


def test_filter_patients_by_region(client):
    r = client.get("/patients/?region=North")
    for p in r.json()["patients"]:
        assert p["region"] == "North"


# anomaly endpoints

def test_get_anomalies(client):
    r = client.get("/anomalies/")
    assert r.json()["count"] > 0


def test_anomalies_confidence_filter(client):
    r = client.get("/anomalies/?min_confidence=0.5")
    for a in r.json()["anomalies"]:
        assert a["confidence"] >= 0.5


def test_physician_flagged(client):
    r = client.get("/anomalies/physician/PHY-00001")
    assert r.json()["is_anomalous"] is True


def test_physician_not_flagged(client):
    r = client.get("/anomalies/physician/PHY-99999")
    assert r.json()["is_anomalous"] is False


def test_anomaly_summary(client):
    data = client.get("/anomalies/summary").json()
    assert data["total_anomalies"] > 0


# KPI endpoints

def test_therapy_switching(client):
    r = client.get("/kpis/therapy-switching")
    assert "switches" in r.json()


def test_kpi_summary(client):
    data = client.get("/kpis/summary").json()
    assert "total_patients" in data
    assert "total_prescriptions" in data


def test_drop_off(client):
    data = client.get("/kpis/drop-off").json()
    assert "drop_off_rate" in data
