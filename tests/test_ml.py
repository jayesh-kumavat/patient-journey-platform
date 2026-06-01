"""Tests for the ML anomaly detection pipeline."""

from pathlib import Path
import tempfile
import random
import pandas as pd
import sys
import pytest
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.data_ingest import create_staging_tables, load_records_to_staging
from src.processing.spark_transform import create_warehouse_tables, transform_prescriptions
from src.ml.train_anomaly_model import prepare_physician_features, train_isolation_forest, save_model
from src.ml.run_inference import create_anomaly_table, load_model, score_physicians, generate_explanations


@pytest.fixture(scope="module")
def ml_engine():
    from tests.conftest import TEST_DB_URL
    engine = create_engine(TEST_DB_URL, echo=False)

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()

    create_staging_tables(engine)
    create_warehouse_tables(engine)

    random.seed(42)

    physicians = [
        {"physician_id": f"PHY-{i:05d}", "specialty": "Oncologist",
         "region": "North", "years_experience": 10}
        for i in range(50)
    ]
    patients = [
        {"patient_id": f"PAT-{i:06d}", "gender": "M", "date_of_birth": "1980-01-01",
         "region": "North", "enrollment_date": "2020-01-01"}
        for i in range(200)
    ]

    rxs = []
    for i in range(5000):
        phy = random.choice(physicians)
        is_anomalous = phy["physician_id"] in ["PHY-00001", "PHY-00002"]
        qty = random.randint(500, 1000) if is_anomalous else random.randint(10, 90)

        rxs.append({
            "prescription_id": f"RX-{i:07d}",
            "patient_id": random.choice(patients)["patient_id"],
            "physician_id": phy["physician_id"],
            "drug_name": random.choice(["DrugA", "DrugB", "DrugC"]),
            "prescription_date": f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "quantity": qty,
            "refills": random.randint(0, 5),
            "therapy_area": "Oncology",
        })

    load_records_to_staging(engine, patients, "stg_patients")
    load_records_to_staging(engine, physicians, "stg_physicians")
    load_records_to_staging(engine, rxs, "stg_prescriptions")
    transform_prescriptions(engine)
    return engine


@pytest.fixture
def model_dir():
    d = Path(tempfile.mkdtemp())
    yield d
    import shutil
    shutil.rmtree(d, ignore_errors=True)


def test_feature_extraction(ml_engine):
    features = prepare_physician_features(ml_engine)
    assert len(features) > 0
    assert "total_prescriptions" in features.columns
    assert "avg_quantity" in features.columns


def test_model_detects_anomalies(ml_engine):
    features = prepare_physician_features(ml_engine)
    model, scaler = train_isolation_forest(features)

    X = scaler.transform(
        features[["total_prescriptions", "avg_quantity", "max_quantity", "unique_patients", "unique_drugs"]].fillna(0)
    )
    preds = model.predict(X)

    assert (preds == -1).sum() > 0
    assert (preds == -1).sum() < len(features)


def test_model_save_and_load(ml_engine, model_dir, monkeypatch):
    monkeypatch.setattr("src.ml.train_anomaly_model.MODELS_DIR", model_dir)
    monkeypatch.setattr("src.ml.run_inference.MODELS_DIR", model_dir)

    features = prepare_physician_features(ml_engine)
    model, scaler = train_isolation_forest(features)
    save_model(model, scaler)

    assert (model_dir / "isolation_forest.pkl").exists()
    loaded_model, loaded_scaler = load_model()
    assert loaded_model is not None


def test_scoring_confidence_range(ml_engine, model_dir, monkeypatch):
    monkeypatch.setattr("src.ml.train_anomaly_model.MODELS_DIR", model_dir)
    monkeypatch.setattr("src.ml.run_inference.MODELS_DIR", model_dir)

    features = prepare_physician_features(ml_engine)
    model, scaler = train_isolation_forest(features)
    save_model(model, scaler)

    scored = score_physicians(ml_engine, model, scaler)
    assert scored["confidence"].between(0, 1).all()


def test_explanations_high_volume():
    row = pd.Series({
        "total_prescriptions": 500, "avg_quantity": 150,
        "max_quantity": 700, "unique_patients": 200,
    })
    explanation = generate_explanations(row)
    assert "High" in explanation


def test_explanations_normal_physician():
    row = pd.Series({
        "total_prescriptions": 50, "avg_quantity": 30,
        "max_quantity": 90, "unique_patients": 20,
    })
    explanation = generate_explanations(row)
    assert "outlier" in explanation.lower()
