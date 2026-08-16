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

You'll need a Postgres instance running. Create a `.env` file in the project root with the following variables, then run the full pipeline:

```env
ALERT_EMAIL=your_email_here

DB_HOST=localhost
DB_PORT=5432
DB_NAME=patient_journey
DB_USER=postgres
DB_PASSWORD=your_password_here
```

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
# All tests
pytest tests/ -v

# Unit tests only
pytest tests/test_transforms.py tests/test_ingestion.py tests/test_ml.py -v

# Integration test (full pipeline e2e)
pytest tests/test_pipeline_integration.py -v

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

The test suite auto-creates the `patient_journey_test` database if it doesn't exist (handled in `conftest.py`).

### Integration tests cover:
- Data generation (correct volumes and formats)
- Ingestion into staging tables
- DQ checks (completeness, uniqueness, referential integrity)
- Warehouse transforms (dim_patient SCD-2, fact_prescription, therapy_switches)
- ML model training (feature extraction, Isolation Forest)
- Inference (physician scoring, anomaly results stored)


## CI

- **CI** (`.github/workflows/ci.yml`): Runs on PRs — sets up PostgreSQL service container, installs deps, runs `pytest tests/ -v`

Add `POSTGRES_PASSWORD` as a repository secret under `Settings -> Secrets and variables -> Actions -> New repository secret`.

## Documentation

- `docs/data_dictionary.md` — Full documentation of all tables (staging, warehouse, operational) with columns, types, business logic, and ER diagram

## Notes

- The anomaly model uses Isolation Forest with 5% contamination rate. Thresholds in `run_inference.py` were tuned by looking at the actual data distribution.
- DQ checks will fail the Airflow DAG if overall score drops below 80%.