"""Tests for the warehouse transformation layer."""

import pytest
from pathlib import Path
import sys
from sqlalchemy import inspect
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.processing.spark_transform import (
    create_warehouse_tables, deduplicate_dataframe, normalize_dates,
    transform_patients, transform_physicians, transform_prescriptions,
    build_patient_journey, detect_therapy_switches,
)
from src.processing.incremental_loader import get_last_watermark, record_pipeline_run
from src.ingestion.data_ingest import create_staging_tables, load_records_to_staging


class TestDeduplication:
    def test_removes_dupes(self):
        df = pd.DataFrame({"id": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]})
        result = deduplicate_dataframe(df, "id")
        assert len(result) == 3
        assert result[result["id"] == "A"]["value"].iloc[0] == 3

    def test_no_dupes_unchanged(self):
        df = pd.DataFrame({"id": ["A", "B", "C"], "value": [1, 2, 3]})
        assert len(deduplicate_dataframe(df, "id")) == 3


class TestDateNormalization:
    def test_valid_dates_kept(self):
        df = pd.DataFrame({"d": ["2023-01-15", "2023-02-20"]})
        result = normalize_dates(df, ["d"])
        assert len(result) == 2

    def test_bad_dates_dropped(self):
        df = pd.DataFrame({"d": ["2023-01-15", "not-a-date", "2023-03-10"]})
        result = normalize_dates(df, ["d"])
        assert len(result) == 2


class TestWarehouseTransforms:
    @pytest.fixture(autouse=True)
    def setup(self, test_engine, sample_patients, sample_prescriptions, sample_physicians):
        create_staging_tables(test_engine)
        create_warehouse_tables(test_engine)
        load_records_to_staging(test_engine, sample_patients, "stg_patients")
        load_records_to_staging(test_engine, sample_physicians, "stg_physicians")
        load_records_to_staging(test_engine, sample_prescriptions, "stg_prescriptions")
        self.engine = test_engine

    def test_warehouse_tables_exist(self):
        tables = inspect(self.engine).get_table_names()
        assert "dim_patient" in tables
        assert "fact_prescription" in tables
        assert "therapy_switches" in tables

    def test_patients_transform(self):
        result = transform_patients(self.engine)
        assert len(result) == 3
        assert all(result["is_current"] == 1)

    def test_physicians_transform(self):
        result = transform_physicians(self.engine)
        assert len(result) == 3

    def test_prescriptions_transform(self):
        result = transform_prescriptions(self.engine)
        assert len(result) == 4
        assert all(result["quantity"] > 0)

    def test_patient_journey_ordering(self):
        transform_prescriptions(self.engine)
        journey = build_patient_journey(self.engine)
        assert len(journey) > 0

        for _, group in journey.groupby("patient_id"):
            assert list(group["sequence_num"]) == sorted(group["sequence_num"])

    def test_therapy_switches_detected(self):
        transform_prescriptions(self.engine)
        switches = detect_therapy_switches(self.engine)
        assert len(switches) > 0
        assert "from_drug" in switches.columns


class TestIncrementalLoader:
    @pytest.fixture(autouse=True)
    def setup(self, test_engine, sample_patients):
        from sqlalchemy import text
        create_staging_tables(test_engine)
        load_records_to_staging(test_engine, sample_patients, "stg_patients")
        self.engine = test_engine

    def test_watermark_returns_value(self):
        wm = get_last_watermark(self.engine, "stg_patients")
        assert wm is not None

    def test_watermark_empty_table(self):
        from sqlalchemy import text
        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM stg_patients"))
            conn.commit()
        assert get_last_watermark(self.engine, "stg_patients") is None

    def test_pipeline_run_recorded(self):
        record_pipeline_run(self.engine, "test_run", "success", 100, 5)
        result = pd.read_sql(
            "SELECT * FROM pipeline_runs WHERE pipeline_name = 'test_run'", self.engine
        )
        assert len(result) == 1
