import logging
from pathlib import Path
import sys
import time
import argparse


sys.path.insert(0, str(Path(__file__).resolve().parent))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")]
)
logger = logging.getLogger("pipeline")


def step_generate():
    from src.ingestion.data_generator import generate_all_data
    return generate_all_data()


def step_ingest():
    # full load — drops and recreates staging tables on every run
    from src.ingestion.data_ingest import ingest_all
    results = ingest_all()
    total = sum(r["records_loaded"] for r in results)
    logger.info(f"Ingested {total} total records")
    return results

    # incremental load - only processes new records since the last run using ingested_at as watermark
    #
    # from sqlalchemy import create_engine
    # from config.settings import get_db_url
    # from src.processing.incremental_loader import load_incremental
    #
    # engine = create_engine(get_db_url())
    # counts = {
    #     "patients": load_incremental(engine, "stg_patients", "dim_patient", "patient_id"),
    #     "physicians": load_incremental(engine, "stg_physicians", "dim_physician", "physician_id"),
    #     "prescriptions": load_incremental(engine, "stg_prescriptions", "fact_prescription", "prescription_id"),
    # }
    # total = sum(counts.values())
    # logger.info(f"Incremental ingest: {counts}  total={total}")
    # return [{"records_loaded": total}]


def step_validate():
    from sqlalchemy import create_engine
    from config.settings import get_db_url
    from src.quality.dq_expectations import run_all_quality_checks, generate_dq_report
    from src.quality.dq_report_generator import save_dq_report

    engine = create_engine(get_db_url())
    checks = run_all_quality_checks(engine)
    report = generate_dq_report(checks)
    save_dq_report(report, engine)
    logger.info(f"DQ Score: {report['overall_score']:.0%}")
    return report

def step_transform():
    from src.processing.spark_transform import run_all_transformations
    return run_all_transformations()


def step_train():
    from src.ml.train_anomaly_model import train_and_save
    return train_and_save()


def step_infer():
    from src.ml.run_inference import run_inference
    return run_inference()


def run_full():
    start = time.time()
    logger.info("=" * 40)
    logger.info("STARTING FULL PIPELINE RUN")
    logger.info("=" * 40)

    from config.settings import ensure_database_exists
    ensure_database_exists()

    records_processed = 0
    records_failed = 0
    status = "success"

    try:
        step_generate()
        ingest_results = step_ingest()
        records_processed = sum(r["records_loaded"] for r in ingest_results)
        step_validate()
        step_transform()
        step_train()
        step_infer()
    except Exception as e:
        status = "failed"
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        try:
            from sqlalchemy import create_engine
            from config.settings import get_db_url
            from src.processing.incremental_loader import record_pipeline_run
            engine = create_engine(get_db_url())
            record_pipeline_run(engine, "full_pipeline", status, records_processed, records_failed)
        except Exception as e:
            logger.warning(f"Could not record pipeline run: {e}")

    elapsed = time.time() - start
    logger.info(f"Pipeline complete in {elapsed:.1f}s")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patient Journey Pipeline")
    parser.add_argument("--generate", action="store_true", help="Generate synthetic data")
    parser.add_argument("--ingest", action="store_true", help="Ingest raw CSVs")
    parser.add_argument("--validate", action="store_true", help="Run DQ checks")
    parser.add_argument("--transform", action="store_true", help="Run warehouse transforms")
    parser.add_argument("--train", action="store_true", help="Train anomaly model")
    parser.add_argument("--infer", action="store_true", help="Run inference")
    parser.add_argument("--all", action="store_true", help="Run everything end-to-end")

    args = parser.parse_args()

    if args.all or not any(vars(args).values()):
        run_full()
    else:
        if args.generate: step_generate()
        if args.ingest: step_ingest()
        if args.validate: step_validate()
        if args.transform: step_transform()
        if args.train: step_train()
        if args.infer: step_infer()