import logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import pytest
from sqlalchemy import text


from src.ingestion.data_generator import (
    generate_patients,
    generate_physicians,
    generate_prescriptions,
    generate_diagnoses,
    generate_claims,
    save_to_csv,
)

from src.ingestion.data_ingest import (
    create_staging_tables,
    load_records_to_staging,
)


from src.quality.dq_expectations import (
    run_all_quality_checks,
    generate_dq_report,
    check_completeness,
    check_uniqueness,
    check_row_count,
)

from src.processing.spark_transform import (
    create_warehouse_tables,
    transform_patients,
    transform_physicians,
    transform_prescriptions,
    build_patient_journey,
    detect_therapy_switches,
)

from src.ml.train_anomaly_model import (
    prepare_physician_features,
    train_isolation_forest,
    save_model,
)

from src.ml.run_inference import (
    create_anomaly_table,
    load_model,
    score_physicians,
)

logger = logging.getLogger(__name__)


# test parameters
TEST_PATIENTS = 100
TEST_PHYSICIANS = 50
TEST_PRESCRIPTIONS = 500
TEST_DIAGNOSES = 200
TEST_CLAIMS = 300


# fixtures
@pytest.fixture(scope="module")
def integration_engine():
    from tests.conftest import TEST_DB_URL
    from sqlalchemy import create_engine

    engine = create_engine(TEST_DB_URL, echo=False)

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()

    yield engine

    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()
    engine.dispose()


@pytest.fixture(scope="module")
def test_data_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("integration_data")


@pytest.fixture(scope="module")
def generated_data():
    patients = generate_patients(TEST_PATIENTS)
    physicians = generate_physicians(TEST_PHYSICIANS)
    prescriptions = generate_prescriptions(patients, physicians, TEST_PRESCRIPTIONS)
    diagnoses = generate_diagnoses(patients, physicians, TEST_DIAGNOSES)
    claims = generate_claims(patients, TEST_CLAIMS)

    return {
        "patients": patients,
        "physicians": physicians,
        "prescriptions": prescriptions,
        "diagnoses": diagnoses,
        "claims": claims,
    }


@pytest.fixture(scope="module")
def model_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("models")


class TestDataGeneration:
    def test_patient_generation(self, generated_data):
        patients = generated_data["patients"]
        assert len(patients) == TEST_PATIENTS
        assert all("patient_id" in p for p in patients)
        assert all(p["patient_id"].startswith("PAT-") for p in patients)

    def test_physician_generation(self, generated_data):
        physicians = generated_data["physicians"]
        assert len(physicians) == TEST_PHYSICIANS
        assert all("physician_id" in p for p in physicians)

    def test_prescription_generation(self, generated_data):
        prescriptions = generated_data["prescriptions"]
        assert len(prescriptions) == TEST_PRESCRIPTIONS
        assert all("prescription_id" in p for p in prescriptions)
        assert all("therapy_area" in p for p in prescriptions)

    def test_csv_export(self, generated_data, test_data_dir):
        filepath = save_to_csv(generated_data["patients"], "patients.csv", test_data_dir)
        assert filepath.exists()
        df = pd.read_csv(filepath)
        assert len(df) == TEST_PATIENTS



class TestIngestion:

    def test_create_staging_tables(self, integration_engine):
        create_staging_tables(integration_engine)

        with integration_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            ))
            tables = [row[0] for row in result]

        assert "stg_patients" in tables
        assert "stg_physicians" in tables
        assert "stg_prescriptions" in tables

    def test_load_patients(self, integration_engine, generated_data):
        loaded = load_records_to_staging(
            integration_engine, generated_data["patients"], "stg_patients"
        )
        assert loaded == TEST_PATIENTS

        df = pd.read_sql("SELECT COUNT(*) as cnt FROM stg_patients", integration_engine)
        assert df["cnt"].iloc[0] == TEST_PATIENTS

    def test_load_physicians(self, integration_engine, generated_data):
        loaded = load_records_to_staging(
            integration_engine, generated_data["physicians"], "stg_physicians"
        )
        assert loaded == TEST_PHYSICIANS

    def test_load_prescriptions(self, integration_engine, generated_data):
        loaded = load_records_to_staging(
            integration_engine, generated_data["prescriptions"], "stg_prescriptions"
        )
        assert loaded == TEST_PRESCRIPTIONS

    def test_load_diagnoses(self, integration_engine, generated_data):
        loaded = load_records_to_staging(
            integration_engine, generated_data["diagnoses"], "stg_diagnoses"
        )
        assert loaded == TEST_DIAGNOSES

    def test_load_claims(self, integration_engine, generated_data):
        loaded = load_records_to_staging(
            integration_engine, generated_data["claims"], "stg_claims"
        )
        assert loaded == TEST_CLAIMS



class TestQualityChecks:

    def test_completeness_checks(self, integration_engine):
        result = check_completeness(integration_engine, "stg_prescriptions", "patient_id")
        assert result.score > 0
        assert result.check_type == "completeness"

    def test_uniqueness_checks(self, integration_engine):
        result = check_uniqueness(integration_engine, "stg_patients", "patient_id")
        assert result.score > 0.95

    def test_row_count_checks(self, integration_engine):
        result = check_row_count(integration_engine, "stg_patients", min_rows=50)
        assert result.passed is True
        assert result.score == 1.0

    def test_full_quality_report(self, integration_engine):
        checks = run_all_quality_checks(engine=integration_engine)
        report = generate_dq_report(checks)

        assert report["total_checks"] > 0
        assert report["overall_score"] > 0
        assert report["passed"] > 0

        logger.info(
            f"DQ Report: {report['passed']}/{report['total_checks']} passed "
            f"(score: {report['overall_score']:.1%})"
        )



class TestTransforms:
    def test_create_warehouse_tables(self, integration_engine):
        create_warehouse_tables(integration_engine)

        with integration_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            ))
            tables = [row[0] for row in result]

        assert "dim_patient" in tables
        assert "dim_physician" in tables
        assert "fact_prescription" in tables

    def test_transform_patients(self, integration_engine):
        df = transform_patients(integration_engine)

        assert len(df) > 0
        assert "effective_from" in df.columns
        assert "effective_to" in df.columns
        assert "is_current" in df.columns
        assert all(df["is_current"] == 1)

    def test_transform_physicians(self, integration_engine):
        df = transform_physicians(integration_engine)
        assert len(df) == TEST_PHYSICIANS

    def test_transform_prescriptions(self, integration_engine):
        df = transform_prescriptions(integration_engine)
        assert len(df) > 0

        assert df["quantity"].dtype in ["int64", "int32"]
        assert df["refills"].dtype in ["int64", "int32"]

    def test_build_patient_journey(self, integration_engine):
        df = build_patient_journey(integration_engine)
        assert len(df) > 0
        assert "sequence_num" in df.columns
        assert "event_type" in df.columns

    def test_detect_therapy_switches(self, integration_engine):
        df = detect_therapy_switches(integration_engine)
        assert len(df) > 0
        assert "from_drug" in df.columns
        assert "to_drug" in df.columns

    def test_warehouse_row_counts(self, integration_engine):
        tables_to_check = [
            ("dim_patient", TEST_PATIENTS),
            ("dim_physician", TEST_PHYSICIANS),
            ("fact_prescription", TEST_PRESCRIPTIONS),
            ("fact_patient_journey", TEST_PRESCRIPTIONS + TEST_DIAGNOSES),
            ("therapy_switches", 1),
        ]

        for table_name, min_rows in tables_to_check:
            df = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", integration_engine)
            count = df["cnt"].iloc[0]
            assert count >= min_rows, f"{table_name} has {count} rows, expected >= {min_rows}"



class TestModelTraining:

    def test_prepare_features(self, integration_engine):
        features_df = prepare_physician_features(integration_engine)

        assert len(features_df) > 0
        assert "physician_id" in features_df.columns
        assert "total_prescriptions" in features_df.columns
        assert "avg_quantity" in features_df.columns
        assert "unique_patients" in features_df.columns

    def test_train_model(self, integration_engine, model_dir, monkeypatch):
        import config.settings
        monkeypatch.setattr(config.settings, "MODELS_DIR", model_dir)

        import src.ml.train_anomaly_model as tam
        monkeypatch.setattr(tam, "MODELS_DIR", model_dir)

        features_df = prepare_physician_features(integration_engine)
        model, scaler = train_isolation_forest(features_df)

        assert model is not None
        assert scaler is not None

        model_path = save_model(model, scaler)
        assert model_path.exists()
        assert (model_dir / "isolation_forest.pkl").exists()
        assert (model_dir / "scaler.pkl").exists()




class TestInference:

    def test_create_anomaly_table(self, integration_engine):
        create_anomaly_table(integration_engine)

        with integration_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'anomaly_results'"
            ))
            tables = [row[0] for row in result]
        assert "anomaly_results" in tables

    def test_score_physicians(self, integration_engine, model_dir, monkeypatch):
        import src.ml.run_inference as ri
        monkeypatch.setattr(ri, "MODELS_DIR", model_dir)

        model, scaler = load_model()
        scored = score_physicians(integration_engine, model, scaler)

        assert len(scored) > 0
        assert "anomaly_score" in scored.columns
        assert "is_anomaly" in scored.columns
        assert "confidence" in scored.columns

        n_anomalies = scored["is_anomaly"].sum()
        logger.info(f"Inference flagged {n_anomalies}/{len(scored)} physicians")

    def test_anomaly_results_stored(self, integration_engine, model_dir, monkeypatch):
        import src.ml.run_inference as ri
        monkeypatch.setattr(ri, "MODELS_DIR", model_dir)

        model, scaler = load_model()
        scored = score_physicians(integration_engine, model, scaler)

        anomalies = scored[scored["is_anomaly"]].copy()
        if not anomalies.empty:
            anomalies["anomaly_type"] = "prescription_volume"
            anomalies["detection_date"] = datetime.now(timezone.utc).date()
            output_cols = [
                "physician_id", "anomaly_score", "anomaly_type",
                "total_prescriptions", "avg_quantity", "max_quantity",
                "unique_patients", "detection_date", "confidence", "explanation"
            ]
            anomalies[output_cols].to_sql(
                "anomaly_results", integration_engine, if_exists="replace", index=False
            )

        df = pd.read_sql("SELECT COUNT(*) as cnt FROM anomaly_results", integration_engine)
        assert df["cnt"].iloc[0] > 0, "anomaly_results should have rows after inference"

        logger.info("Full pipeline e2e test passed")


class TestFullPipelineE2E:

    def test_end_to_end(self, integration_engine, generated_data, model_dir, monkeypatch):
        import config.settings
        import src.ml.train_anomaly_model as tam
        import src.ml.run_inference as ri

        monkeypatch.setattr(config.settings, "MODELS_DIR", model_dir)
        monkeypatch.setattr(tam, "MODELS_DIR", model_dir)
        monkeypatch.setattr(ri, "MODELS_DIR", model_dir)

        # 1. Ingest
        create_staging_tables(integration_engine)
        load_records_to_staging(integration_engine, generated_data["patients"], "stg_patients")
        load_records_to_staging(integration_engine, generated_data["physicians"], "stg_physicians")
        load_records_to_staging(integration_engine, generated_data["prescriptions"], "stg_prescriptions")
        load_records_to_staging(integration_engine, generated_data["diagnoses"], "stg_diagnoses")
        load_records_to_staging(integration_engine, generated_data["claims"], "stg_claims")

        # 2. Quality
        checks = run_all_quality_checks(engine=integration_engine)
        report = generate_dq_report(checks)
        assert report["overall_score"] > 0, "DQ score must be > 0"

        # 3. Transform
        create_warehouse_tables(integration_engine)
        transform_patients(integration_engine)
        transform_physicians(integration_engine)
        transform_prescriptions(integration_engine)
        build_patient_journey(integration_engine)
        detect_therapy_switches(integration_engine)

        for table in ["dim_patient", "dim_physician", "fact_prescription", "therapy_switches"]:
            df = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", integration_engine)
            assert df["cnt"].iloc[0] > 0, f"{table} should be populated"

        # 4. Train
        features_df = prepare_physician_features(integration_engine)
        model, scaler = train_isolation_forest(features_df)
        model_path = save_model(model, scaler)
        assert model_path.exists(), "Model file should be created"

        # 5. Inference
        create_anomaly_table(integration_engine)
        model, scaler = load_model()
        scored = score_physicians(integration_engine, model, scaler)

        anomalies = scored[scored["is_anomaly"]].copy()
        if not anomalies.empty:
            anomalies["anomaly_type"] = "prescription_volume"
            anomalies["detection_date"] = datetime.now(timezone.utc).date()
            anomalies["explanation"] = "Statistical outlier"
            output_cols = [
                "physician_id", "anomaly_score", "anomaly_type",
                "total_prescriptions", "avg_quantity", "max_quantity",
                "unique_patients", "detection_date", "confidence", "explanation"
            ]
            anomalies[output_cols].to_sql(
                "anomaly_results", integration_engine, if_exists="replace", index=False
            )

        df = pd.read_sql("SELECT COUNT(*) as cnt FROM anomaly_results", integration_engine)
        assert df["cnt"].iloc[0] > 0, "anomaly_results should have rows after inference"

        logger.info("Full pipeline e2e test passed")

