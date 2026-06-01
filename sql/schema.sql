-- Schema for patient journey analytics

-- staging
CREATE TABLE IF NOT EXISTS stg_patients (
    patient_id VARCHAR(20) PRIMARY KEY,
    gender VARCHAR(10),
    date_of_birth DATE,
    region VARCHAR(50),
    enrollment_date DATE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_physicians (
    physician_id VARCHAR(20) PRIMARY KEY,
    specialty VARCHAR(50),
    region VARCHAR(50),
    years_experience INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_prescriptions (
    prescription_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    physician_id VARCHAR(20),
    drug_name VARCHAR(100),
    prescription_date DATE,
    quantity INTEGER,
    refills INTEGER,
    therapy_area VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_diagnoses (
    diagnosis_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    icd_code VARCHAR(20),
    diagnosis_date DATE,
    physician_id VARCHAR(20),
    severity VARCHAR(20),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_claims (
    claim_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    claim_date DATE,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    payer VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- warehouse
CREATE TABLE IF NOT EXISTS dim_patient (
    patient_key SERIAL PRIMARY KEY,
    patient_id VARCHAR(20),
    gender VARCHAR(10),
    date_of_birth DATE,
    region VARCHAR(50),
    enrollment_date DATE,
    effective_from DATE NOT NULL,
    effective_to DATE DEFAULT '9999-12-31',
    is_current INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_physician (
    physician_id VARCHAR(20) PRIMARY KEY,
    specialty VARCHAR(50),
    region VARCHAR(50),
    years_experience INTEGER
);

CREATE TABLE IF NOT EXISTS fact_prescription (
    prescription_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    physician_id VARCHAR(20),
    drug_name VARCHAR(100),
    prescription_date DATE,
    quantity INTEGER,
    refills INTEGER,
    therapy_area VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fact_patient_journey (
    journey_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20),
    event_type VARCHAR(50),
    event_date DATE,
    event_detail VARCHAR(200),
    therapy_area VARCHAR(50),
    sequence_num INTEGER
);

CREATE TABLE IF NOT EXISTS therapy_switches (
    switch_id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20),
    from_drug VARCHAR(100),
    to_drug VARCHAR(100),
    switch_date DATE,
    therapy_area VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS anomaly_results (
    anomaly_id SERIAL PRIMARY KEY,
    physician_id VARCHAR(20),
    anomaly_score FLOAT,
    anomaly_type VARCHAR(50),
    total_prescriptions INTEGER,
    avg_quantity FLOAT,
    max_quantity INTEGER,
    unique_patients INTEGER,
    detection_date DATE,
    confidence FLOAT,
    explanation TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    pipeline_name VARCHAR(100),
    status VARCHAR(20),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    records_processed INTEGER,
    records_failed INTEGER
);

CREATE TABLE IF NOT EXISTS dq_scores (
    run_id VARCHAR(100) PRIMARY KEY,
    run_timestamp TIMESTAMP,
    total_checks INTEGER,
    passed_checks INTEGER,
    overall_score FLOAT,
    report_path VARCHAR(500)
);

-- indexes
CREATE INDEX IF NOT EXISTS idx_rx_patient ON fact_prescription(patient_id);
CREATE INDEX IF NOT EXISTS idx_rx_physician ON fact_prescription(physician_id);
CREATE INDEX IF NOT EXISTS idx_rx_date ON fact_prescription(prescription_date);
CREATE INDEX IF NOT EXISTS idx_journey_patient ON fact_patient_journey(patient_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_physician ON anomaly_results(physician_id);
