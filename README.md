# Patient Journey Analytics Platform

End-to-end data pipeline for pharma patient journey analytics. Ingests synthetic patient/prescription/claims data, runs quality checks, builds a star schema warehouse, trains an anomaly detection model on physician prescribing patterns, and serves everything through an API + dashboard.

## Setup

```bash
git clone https://github.com/jayesh-kumavat/patient-journey-platform
cd patient-journey-platform

python -m venv venv
venv\Scripts\activate   # linux/mac: source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

You'll need a Postgres instance running. Copy `.env.example` to `.env` and fill in your password, then run the full pipeline:

```bash
python run_pipeline.py --all
```

The pipeline will automatically create the `patient_journey` database if it doesn't exist.

Takes about 5 minutes to generate data, ingest, validate, transform, train, and score.

## API

```bash
uvicorn src.api.main:app --reload --port 8000
```

Endpoints:
- `GET /patients/{id}/journey` - full patient timeline
- `GET /anomalies/` - flagged physicians
- `GET /kpis/therapy-switching` - therapy switch data
- `GET /kpis/summary` - high level numbers
- `GET /kpis/drop-off` - patient drop-off rates

Visit: `http://localhost:8000/docs` - for more details

## Dashboard

```bash
streamlit run src/dashboard/app.py
```

## Docker

```bash
docker-compose up -d
```

This spins up postgres, the API, and the dashboard. You'll still need to run the pipeline to populate data:

```bash
docker exec -it <api-container> python run_pipeline.py --all
```

## Tests

```bash
pytest tests/ -v
```

The test suite auto-creates the `patient_journey_test` database if it doesn't exist (handled in `conftest.py`).

## Project structure

```
config/             settings + JSON schemas for validation
src/ingestion/      data generation, CSV validation, staging loader
src/processing/     warehouse transforms (star schema, SCD-2, therapy switches)
src/quality/        data quality checks and reporting
src/ml/             isolation forest training + inference
src/api/            FastAPI endpoints
src/dashboard/      streamlit app
sql/                DDL and analytical queries
dags/               airflow DAG
tests/              pytest suite
```

## Notes

- The anomaly model uses Isolation Forest with 5% contamination rate. Thresholds in `run_inference.py` were tuned by looking at the actual data distribution.
- DQ checks will fail the Airflow DAG if overall score drops below 80%.
