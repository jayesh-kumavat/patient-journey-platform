"""Tests for data quality checks."""

import pytest
from pathlib import Path
import sys
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.data_ingest import create_staging_tables, load_records_to_staging
from src.quality.dq_expectations import (
    check_completeness, check_uniqueness, check_referential_integrity,
    check_row_count, check_statistical_distribution,
    run_all_quality_checks, generate_dq_report,
)
from src.quality.dq_report_generator import save_dq_report


@pytest.fixture(scope="module")
def dq_engine():
    from tests.conftest import TEST_DB_URL
    engine = create_engine(TEST_DB_URL, echo=False)

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()

    create_staging_tables(engine)

    # setting up test data with some intentional quality issues
    patients = [
        {"patient_id": f"PAT-{i:06d}", "gender": "M", "date_of_birth": "1980-01-01",
         "region": "North", "enrollment_date": "2020-01-01"}
        for i in range(100)
    ]
    physicians = [
        {"physician_id": f"PHY-{i:05d}", "specialty": "Oncologist",
         "region": "North", "years_experience": 10}
        for i in range(20)
    ]
    # some prescriptions have null patient_id (every 20th one)
    prescriptions = [
        {
            "prescription_id": f"RX-{i:07d}",
            "patient_id": f"PAT-{i % 100:06d}" if i % 20 != 0 else None,
            "physician_id": f"PHY-{i % 20:05d}",
            "drug_name": "DrugA",
            "prescription_date": "2023-06-15",
            "quantity": 30 if i % 50 != 0 else 999,  # outliers every 50th
            "refills": 2,
            "therapy_area": "Oncology",
        }
        for i in range(500)
    ]

    load_records_to_staging(engine, patients, "stg_patients")
    load_records_to_staging(engine, physicians, "stg_physicians")
    load_records_to_staging(engine, prescriptions, "stg_prescriptions")
    return engine


def test_completeness_perfect_column(dq_engine):
    c = check_completeness(dq_engine, "stg_patients", "patient_id")
    assert c.passed
    assert c.score == 1.0


def test_completeness_with_nulls(dq_engine):
    c = check_completeness(dq_engine, "stg_prescriptions", "patient_id")
    assert c.score < 1.0
    assert c.score > 0.9


def test_uniqueness_on_pk(dq_engine):
    assert check_uniqueness(dq_engine, "stg_patients", "patient_id").passed


def test_referential_integrity(dq_engine):
    c = check_referential_integrity(
        dq_engine, "stg_prescriptions", "physician_id", "stg_physicians", "physician_id"
    )
    assert c.passed


def test_row_count_pass_and_fail(dq_engine):
    assert check_row_count(dq_engine, "stg_patients", 50).passed
    assert not check_row_count(dq_engine, "stg_patients", 500).passed


def test_distribution_catches_outliers(dq_engine):
    c = check_statistical_distribution(dq_engine, "stg_prescriptions", "quantity", 1, 500)
    assert c.score < 1.0


def test_full_dq_report(dq_engine):
    checks = run_all_quality_checks(dq_engine)
    report = generate_dq_report(checks)
    assert 0 <= report["overall_score"] <= 1
    assert report["total_checks"] > 0


def test_report_saves_to_disk(dq_engine, tmp_path, monkeypatch):
    monkeypatch.setattr("src.quality.dq_report_generator.REPORTS_DIR", tmp_path)
    report = generate_dq_report(run_all_quality_checks(dq_engine))
    path = save_dq_report(report, dq_engine)
    assert path.exists()
