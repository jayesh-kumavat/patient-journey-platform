"""
Airflow DAG - runs the full pipeline daily.
Schedule: 6am UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


default_args = {
    "owner": "Jayesh",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["jayeshkumavat42@gmail.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    "patient_journey_pipeline",
    default_args=default_args,
    description="End-to-end patient journey analytics pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["patient-journey", "data-engineering"],
)


def task_generate(**ctx):
    from src.ingestion.data_generator import generate_all_data
    files = generate_all_data()
    ctx["ti"].xcom_push(key="generated_files", value={k: str(v) for k, v in files.items()})


def task_ingest(**ctx):
    from src.ingestion.data_ingest import ingest_all
    results = ingest_all()
    total = sum(r["records_loaded"] for r in results)
    return f"Ingested {total} records"


def task_quality(**ctx):
    from src.quality.dq_expectations import run_all_quality_checks, generate_dq_report
    from src.quality.dq_report_generator import save_dq_report
    from sqlalchemy import create_engine
    from config.settings import get_db_url

    engine = create_engine(get_db_url())
    checks = run_all_quality_checks(engine)
    report = generate_dq_report(checks)
    save_dq_report(report, engine)

    # fail the task if quality is too low
    if report["overall_score"] < 0.8:
        raise ValueError(f"DQ score below threshold: {report['overall_score']:.0%}")


def task_transform(**ctx):
    from src.processing.spark_transform import run_all_transformations
    return run_all_transformations()


def task_train(**ctx):
    from src.ml.train_anomaly_model import train_and_save
    return train_and_save()


def task_infer(**ctx):
    from src.ml.run_inference import run_inference
    return run_inference()


# task definitions
t1 = PythonOperator(task_id="generate_data", python_callable=task_generate, dag=dag)
t2 = PythonOperator(task_id="ingest", python_callable=task_ingest, dag=dag)
t3 = PythonOperator(task_id="quality_checks", python_callable=task_quality, dag=dag)
t4 = PythonOperator(task_id="transform", python_callable=task_transform, dag=dag)
t5 = PythonOperator(task_id="train_model", python_callable=task_train, dag=dag)
t6 = PythonOperator(task_id="inference", python_callable=task_infer, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
