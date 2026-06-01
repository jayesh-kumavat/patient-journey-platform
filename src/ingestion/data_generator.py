"""
Generates synthetic pharma data.
Run this first before anything else in the pipeline.
"""

from faker import Faker
from typing import List, Dict
import random
import csv
from config.settings import RAW_DIR
from pathlib import Path
import logging

fake = Faker()
Faker.seed(42)
random.seed(42)

logger = logging.getLogger(__name__)

# therapy areas and their associated drugs
THERAPY_AREAS = ["Oncology", "Cardiology", "Neurology", "Immunology", "Diabetes", "Respiratory"]

DRUGS = {
    "Oncology": ["DrugA-Onc", "DrugB-Onc", "DrugC-Onc"],
    "Cardiology": ["DrugA-Card", "DrugB-Card", "DrugC-Card"],
    "Neurology": ["DrugA-Neuro", "DrugB-Neuro"],
    "Immunology": ["DrugA-Imm", "DrugB-Imm", "DrugC-Imm"],
    "Diabetes": ["DrugA-Diab", "DrugB-Diab", "DrugC-Diab", "DrugD-Diab"],
    "Respiratory": ["DrugA-Resp", "DrugB-Resp"],
}

SPECIALTIES = ["Oncologist", "Cardiologist", "Neurologist", "Immunologist", "Endocrinologist", "Pulmonologist"]
REGIONS = ["North", "South", "East", "West", "Central"]

ICD_CODES = ["C34.1", "I25.1", "G30.0", "M05.7", "E11.9", "J44.1", "C50.9", "I48.0", "G20", "E10.9"]


def generate_patients(n=5000) -> List[Dict]:
    patients = []
    for i in range(n):
        patients.append({
            "patient_id": f"PAT-{i+1:06d}",
            "gender": random.choice(["M", "F", "Other"]),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
            "region": random.choice(REGIONS),
            "enrollment_date": fake.date_between(start_date="-5y", end_date="-1y").isoformat(),
        })
    return patients


def generate_physicians(n=500) -> List[Dict]:
    return [
        {
            "physician_id": f"PHY-{i+1:05d}",
            "specialty": random.choice(SPECIALTIES),
            "region": random.choice(REGIONS),
            "years_experience": random.randint(1, 35),
        }
        for i in range(n)
    ]


def generate_prescriptions(patients, physicians, n=50000) -> List[Dict]:
    prescriptions = []
    for i in range(n):
        therapy = random.choice(THERAPY_AREAS)
        physician = random.choice(physicians)

        # spike some quantities high so the anomaly model has something to find
        if i % 500 == 0:
            quantity = random.randint(500, 1000)
        else:
            quantity = random.randint(1, 90)

        prescriptions.append({
            "prescription_id": f"RX-{i+1:07d}",
            "patient_id": random.choice(patients)["patient_id"],
            "physician_id": physician["physician_id"],
            "drug_name": random.choice(DRUGS[therapy]),
            "prescription_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "quantity": quantity,
            "refills": random.randint(0, 5),
            "therapy_area": therapy,
        })
    return prescriptions


def generate_diagnoses(patients, physicians, n=30000):
    diagnoses = []
    for i in range(n):
        diagnoses.append({
            "diagnosis_id": f"DX-{i+1:07d}",
            "patient_id": random.choice(patients)["patient_id"],
            "icd_code": random.choice(ICD_CODES),
            "diagnosis_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "physician_id": random.choice(physicians)["physician_id"],
            "severity": random.choice(["mild", "moderate", "severe"]),
        })
    return diagnoses


def generate_claims(patients, n=40000):
    claims = []
    for i in range(n):
        claims.append({
            "claim_id": f"CLM-{i+1:07d}",
            "patient_id": random.choice(patients)["patient_id"],
            "claim_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "amount": round(random.uniform(50, 15000), 2),
            "status": random.choice(["approved", "denied", "pending"]),
            "payer": random.choice(["InsuranceA", "InsuranceB", "InsuranceC", "Medicare", "Medicaid"]),
        })
    return claims


def inject_dirty_data(records, dirty_pct=0.05):
    """Sprinkle in some garbage records so the DQ checks have something to catch."""
    dirty_count = int(len(records) * dirty_pct)
    dirty_records = []

    for _ in range(dirty_count):
        record = records[random.randint(0, len(records) - 1)].copy()
        corruption = random.choice(["null_field", "bad_date", "duplicate"])

        if corruption == "null_field":
            # null out a random non-PK field
            keys = [k for k in record.keys() if k != list(record.keys())[0]]
            record[random.choice(keys)] = None
        elif corruption == "bad_date":
            date_keys = [k for k in record.keys() if "date" in k]
            if date_keys:
                record[random.choice(date_keys)] = "invalid-date"
        # else: duplicate - just append as-is

        dirty_records.append(record)

    return records + dirty_records


def save_to_csv(records, filename, output_dir=None) -> Path:
    output_dir = output_dir or RAW_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / filename

    if not records:
        return filepath

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    logger.info(f"Wrote {len(records)} records -> {filepath.name}")
    return filepath


def generate_all_data(output_dir=None):
    """Main entry point - generates everything."""
    logger.info("Starting data generation...")

    patients = generate_patients(5000)
    physicians = generate_physicians(500)
    prescriptions = generate_prescriptions(patients, physicians, 50000)
    diagnoses = generate_diagnoses(patients, physicians, 30000)
    claims = generate_claims(patients, 40000)

    prescriptions = inject_dirty_data(prescriptions, 0.03)
    diagnoses = inject_dirty_data(diagnoses, 0.02)
    claims = inject_dirty_data(claims, 0.04)

    files = {
        "patients": save_to_csv(patients, "patients.csv", output_dir),
        "physicians": save_to_csv(physicians, "physicians.csv", output_dir),
        "prescriptions": save_to_csv(prescriptions, "prescriptions.csv", output_dir),
        "diagnoses": save_to_csv(diagnoses, "diagnoses.csv", output_dir),
        "claims": save_to_csv(claims, "claims.csv", output_dir),
    }

    total = sum(len(x) for x in [patients, physicians, prescriptions, diagnoses, claims])
    logger.info(f"Generated {total:,} total records across {len(files)} files")
    return files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    generate_all_data()
