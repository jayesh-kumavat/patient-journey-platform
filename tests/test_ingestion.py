"""Tests for data generation, validation, and ingestion."""

import sys
from pathlib import Path
import csv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.data_generator import (
    generate_patients, generate_physicians, generate_prescriptions,
    generate_diagnoses, generate_claims, inject_dirty_data, save_to_csv,
)
from src.ingestion.schema_validator import validate_record, validate_file, validate_and_quarantine, load_schemas
from src.ingestion.data_ingest import create_staging_tables, load_records_to_staging, ingest_file


class TestDataGenerator:
    def test_patients_basic(self):
        patients = generate_patients(100)
        assert len(patients) == 100
        for p in patients:
            assert "patient_id" in p
            assert p["gender"] in ("M", "F", "Other")

    def test_patient_ids_unique(self):
        patients = generate_patients(1000)
        ids = [p["patient_id"] for p in patients]
        assert len(ids) == len(set(ids))

    def test_physicians(self):
        physicians = generate_physicians(50)
        assert len(physicians) == 50
        assert all("physician_id" in p for p in physicians)

    def test_prescriptions_reference_valid_patients(self):
        patients = generate_patients(10)
        physicians = generate_physicians(5)
        prescriptions = generate_prescriptions(patients, physicians, 100)
        assert len(prescriptions) == 100

        patient_ids = {p["patient_id"] for p in patients}
        for rx in prescriptions:
            assert rx["patient_id"] in patient_ids

    def test_diagnoses(self):
        patients = generate_patients(10)
        physicians = generate_physicians(5)
        assert len(generate_diagnoses(patients, physicians, 200)) == 200

    def test_claims(self):
        patients = generate_patients(10)
        assert len(generate_claims(patients, 150)) == 150

    def test_dirty_data_adds_records(self):
        patients = generate_patients(100)
        dirty = inject_dirty_data(patients, 0.1)
        assert len(dirty) == 110

    def test_csv_output(self, test_data_dir):
        patients = generate_patients(50)
        path = save_to_csv(patients, "test.csv", test_data_dir)
        assert path.exists()
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 50


class TestSchemaValidator:
    def test_schemas_load(self):
        schemas = load_schemas()
        assert "patients" in schemas
        assert "prescriptions" in schemas

    def test_valid_patient_passes(self):
        schemas = load_schemas()
        record = {
            "patient_id": "PAT-000001", "gender": "M",
            "date_of_birth": "1980-05-15", "region": "North",
            "enrollment_date": "2020-01-01",
        }
        valid, err = validate_record(record, schemas["patients"])
        assert valid

    def test_invalid_gender_fails(self):
        schemas = load_schemas()
        record = {
            "patient_id": "PAT-000001", "gender": "X",
            "date_of_birth": "1980-05-15", "region": "North",
        }
        valid, _ = validate_record(record, schemas["patients"])
        assert not valid

    def test_file_with_mixed_records(self, test_data_dir):
        records = [
            {"patient_id": "PAT-000001", "gender": "M", "date_of_birth": "1980-05-15",
             "region": "North", "enrollment_date": "2020-01-01"},
            {"patient_id": "PAT-000002", "gender": "INVALID", "date_of_birth": "1975-11-20",
             "region": "South", "enrollment_date": "2019-06-15"},
        ]
        filepath = test_data_dir / "patients.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

        valid, invalid = validate_file(filepath, "patients")
        assert len(valid) == 1
        assert len(invalid) == 1


class TestDataIngest:
    def test_staging_tables_created(self, test_engine):
        create_staging_tables(test_engine)
        from sqlalchemy import inspect
        tables = inspect(test_engine).get_table_names()
        assert "stg_patients" in tables
        assert "stg_prescriptions" in tables

    def test_load_records(self, test_engine, sample_patients):
        create_staging_tables(test_engine)
        count = load_records_to_staging(test_engine, sample_patients, "stg_patients")
        assert count == 3

    def test_ingest_file_end_to_end(self, test_engine, test_data_dir, sample_patients):
        create_staging_tables(test_engine)

        # write sample data to CSV
        filepath = test_data_dir / "patients.csv"
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sample_patients[0].keys())
            writer.writeheader()
            writer.writerows(sample_patients)

        result = ingest_file(filepath, "patients", test_engine)
        assert result["records_loaded"] == 3
