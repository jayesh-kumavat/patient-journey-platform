# Data Dictionary — Patient Journey Platform

This document describes all tables in the Patient Journey Platform, organized by layer.

---

## Staging Layer

Raw data landing zone. Records arrive from CSV ingestion with minimal transformation. Each table includes an `ingested_at` timestamp for lineage tracking.

### stg_patients

Source data for patient demographics.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_id | VARCHAR(20) | Unique patient identifier (format: `PAT-XXXXXX`) | PK |
| gender | VARCHAR(10) | Patient gender (`M`, `F`, `Other`) | |
| date_of_birth | DATE | Patient date of birth | |
| region | VARCHAR(50) | Geographic region (`North`, `South`, `East`, `West`, `Central`) | |
| enrollment_date | DATE | Date patient enrolled in the program | |
| ingested_at | TIMESTAMP | Timestamp when the record was loaded | Default: CURRENT_TIMESTAMP |

---

### stg_physicians

Source data for physician information.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| physician_id | VARCHAR(20) | Unique physician identifier (format: `PHY-XXXXX`) | PK |
| specialty | VARCHAR(50) | Medical specialty (e.g., Oncologist, Cardiologist) | |
| region | VARCHAR(50) | Geographic region where physician practices | |
| years_experience | INTEGER | Years of professional experience | |
| ingested_at | TIMESTAMP | Timestamp when the record was loaded | Default: CURRENT_TIMESTAMP |

---

### stg_prescriptions

Raw prescription records. May contain dirty data (null fields, bad dates, duplicates) which the DQ layer catches.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| prescription_id | VARCHAR(20) | Unique prescription identifier (format: `RX-XXXXXXX`) | PK |
| patient_id | VARCHAR(20) | Patient who received the prescription | FK → stg_patients |
| physician_id | VARCHAR(20) | Prescribing physician | FK → stg_physicians |
| drug_name | VARCHAR(100) | Name of the prescribed drug (e.g., `DrugA-Onc`) | |
| prescription_date | DATE | Date the prescription was written | |
| quantity | INTEGER | Number of units prescribed | |
| refills | INTEGER | Number of refills authorized (0-5) | |
| therapy_area | VARCHAR(50) | Therapeutic category (Oncology, Cardiology, etc.) | |
| ingested_at | TIMESTAMP | Timestamp when the record was loaded | Default: CURRENT_TIMESTAMP |

**Business Logic:** Quantity values > 500 are considered anomalous and flagged by the ML model. ~3% dirty records injected during generation for DQ testing.

---

### stg_diagnoses

Patient diagnosis records linked to ICD codes.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| diagnosis_id | VARCHAR(20) | Unique diagnosis identifier (format: `DX-XXXXXXX`) | PK |
| patient_id | VARCHAR(20) | Patient who received the diagnosis | FK → stg_patients |
| icd_code | VARCHAR(20) | ICD-10 diagnosis code (e.g., `C34.1`, `I25.1`) | |
| diagnosis_date | DATE | Date of diagnosis | |
| physician_id | VARCHAR(20) | Diagnosing physician | FK → stg_physicians |
| severity | VARCHAR(20) | Severity level (`mild`, `moderate`, `severe`) | |
| ingested_at | TIMESTAMP | Timestamp when the record was loaded | Default: CURRENT_TIMESTAMP |

---

### stg_claims

Insurance claim records for patient encounters.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| claim_id | VARCHAR(20) | Unique claim identifier (format: `CLM-XXXXXXX`) | PK |
| patient_id | VARCHAR(20) | Patient associated with the claim | FK → stg_patients |
| claim_date | DATE | Date the claim was filed | |
| amount | NUMERIC(10,2) | Claim dollar amount ($50 - $15,000 typical range) | |
| status | VARCHAR(20) | Claim status (`approved`, `denied`, `pending`) | |
| payer | VARCHAR(50) | Insurance payer (InsuranceA/B/C, Medicare, Medicaid) | |
| ingested_at | TIMESTAMP | Timestamp when the record was loaded | Default: CURRENT_TIMESTAMP |

---

## Warehouse Layer

Transformed, cleansed data organized in a star schema. Data flows from staging through deduplication, date normalization, and type coercion.

### dim_patient (SCD Type 2)

Patient dimension with Slowly Changing Dimension Type 2 support. When patient attributes change, the current row is expired and a new row is inserted.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_id | VARCHAR(20) | Natural/business key for the patient | |
| gender | VARCHAR(10) | Patient gender | |
| date_of_birth | DATE | Patient date of birth | |
| region | VARCHAR(50) | Geographic region | |
| enrollment_date | DATE | Program enrollment date | |
| effective_from | DATE | Start date for this version of the record | NOT NULL |
| effective_to | DATE | End date for this version (`9999-12-31` if current) | Default: 9999-12-31 |
| is_current | SMALLINT | Flag indicating if this is the active record (1=yes, 0=no) | Default: 1 |

**Business Logic:** On each pipeline run, new records are inserted with `is_current=1` and `effective_from=today`. In a full SCD-2 implementation, changed records would expire the old row (`is_current=0`, `effective_to=yesterday`) before inserting the new version.

---

### dim_physician

Physician dimension (Type 1 — overwrites on change).

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| physician_id | VARCHAR(20) | Unique physician identifier | PK |
| specialty | VARCHAR(50) | Medical specialty | |
| region | VARCHAR(50) | Practice region | |
| years_experience | INTEGER | Years of experience (coerced to int, default 0) | |

---

### dim_therapy

Therapy area reference dimension. Computed from `fact_prescription` during the transform step.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| therapy_area | VARCHAR(50) | Therapeutic category (e.g., Oncology, Cardiology) | PK |
| drug_count | INTEGER | Number of distinct drugs available in this therapy area | |

**Business Logic:** Populated by `transform_therapy()` which aggregates `COUNT(DISTINCT drug_name)` per therapy area from `fact_prescription`. Used by the dashboard for filter dropdowns and to understand therapy coverage breadth. This is a computed dimension — derived from fact data, not from a source system.

---

### fact_prescription

Central fact table for prescription events. Deduplicated, date-normalized, and type-coerced from staging.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| prescription_id | VARCHAR(20) | Unique prescription identifier | PK |
| patient_id | VARCHAR(20) | Patient receiving the prescription | FK → dim_patient |
| physician_id | VARCHAR(20) | Prescribing physician | FK → dim_physician |
| drug_name | VARCHAR(100) | Prescribed drug name | |
| prescription_date | DATE | Date prescription was written (normalized to YYYY-MM-DD) | |
| quantity | INTEGER | Units prescribed (coerced to int, default 0) | |
| refills | INTEGER | Authorized refills (coerced to int, default 0) | |
| therapy_area | VARCHAR(50) | Therapeutic category | |

**Indexes:** `idx_rx_patient` (patient_id), `idx_rx_physician` (physician_id), `idx_rx_date` (prescription_date)

---

### fact_patient_journey

Unified event timeline combining prescriptions and diagnoses per patient. Used to analyze patient treatment pathways.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_id | VARCHAR(20) | Patient this event belongs to | |
| event_type | VARCHAR(50) | Event category (`prescription`, `diagnosis`) | |
| event_date | DATE | When the event occurred | |
| event_detail | VARCHAR(200) | Event-specific detail (drug name or ICD code) | |
| therapy_area | VARCHAR(50) | Associated therapy area (empty for diagnoses) | |
| sequence_num | INTEGER | Chronological sequence number within the patient's journey | |

**Business Logic:** Events are ordered by `event_date` per patient, and `sequence_num` provides a 1-based ordering. This enables therapy pathway and treatment sequence analysis.

**Index:** `idx_journey_patient` (patient_id)

---

### therapy_switches

Records when a patient changes from one drug to another. Derived from consecutive prescriptions per patient.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| patient_id | VARCHAR(20) | Patient who switched therapy | |
| from_drug | VARCHAR(100) | Drug the patient was previously on | |
| to_drug | VARCHAR(100) | Drug the patient switched to | |
| switch_date | DATE | Date of the new prescription (switch point) | |
| therapy_area | VARCHAR(50) | Therapy area of the new drug | |

**Business Logic:** A switch is detected when a patient's drug_name changes between consecutive prescriptions (ordered by prescription_date). Same-drug refills do not count as switches.

---

### anomaly_results

Output of the ML anomaly detection pipeline. Contains physicians flagged for unusual prescribing patterns.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| physician_id | VARCHAR(20) | Flagged physician | FK → dim_physician |
| anomaly_score | DOUBLE PRECISION | Isolation Forest decision function score (lower = more anomalous) | |
| anomaly_type | VARCHAR(50) | Category of anomaly (`prescription_volume`) | |
| total_prescriptions | INTEGER | Total prescriptions by this physician | |
| avg_quantity | NUMERIC(10,2) | Average quantity per prescription | |
| max_quantity | INTEGER | Maximum single prescription quantity | |
| unique_patients | INTEGER | Number of distinct patients seen | |
| detection_date | DATE | Date the anomaly was detected | |
| confidence | NUMERIC(5,4) | Normalized confidence score (0-1, higher = more confident) | |
| explanation | TEXT | Human-readable explanation of why physician was flagged | |

**Business Logic:** The Isolation Forest model is trained with `contamination=0.05` (expects ~5% anomalous). Explanations are generated based on threshold rules: total_prescriptions > 200, avg_quantity > 100, max_quantity > 400, unique_patients > 100.

**Index:** `idx_anomaly_physician` (physician_id)

---

## Operational Tables

Tables supporting pipeline orchestration and monitoring.

### pipeline_runs

Tracks execution of each pipeline run for observability and debugging.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| run_id | VARCHAR(100) | Unique run identifier (UUID or timestamp-based) | PK |
| pipeline_name | VARCHAR(100) | Name of the pipeline (e.g., `ingestion`, `transforms`, `ml_training`) | |
| status | VARCHAR(20) | Execution status (`running`, `success`, `failed`) | |
| started_at | TIMESTAMP | When the pipeline run began | |
| completed_at | TIMESTAMP | When the pipeline run finished (NULL if still running) | |
| records_processed | INTEGER | Number of records successfully processed | |
| records_failed | INTEGER | Number of records that failed processing | |

---

### dq_scores

Stores results of data quality check runs. One row per execution of the quality suite.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| run_id | VARCHAR(100) | Unique DQ run identifier | PK |
| run_timestamp | TIMESTAMP | When the quality checks were executed | |
| total_checks | INTEGER | Total number of DQ checks run | |
| passed_checks | INTEGER | Number of checks that passed | |
| overall_score | NUMERIC(5,4) | Ratio of passed/total (0.0 to 1.0) | |
| report_path | VARCHAR(500) | File path to the detailed JSON report | |

**Business Logic:** A score threshold of 0.80 is typically used as a gate — if overall_score < 0.80, the pipeline halts and alerts are sent.

---

## Data Flow

1. **Generation** → Raw CSVs (patients, physicians, prescriptions, diagnoses, claims)
2. **Ingestion** → Staging tables (`stg_*`) with validation and quarantine
3. **Quality** → DQ checks produce scores stored in `dq_scores`
4. **Transform** → Star schema tables (`dim_*`, `fact_*`, `therapy_switches`)
5. **ML Training** → Feature engineering from `fact_prescription`, model artifacts saved
6. **ML Inference** → Scoring physicians, results written to `anomaly_results`
