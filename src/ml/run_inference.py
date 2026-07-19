"""
Scoring pipeline - loads the trained model and flags anomalous physicians.
"""

import pickle
from datetime import datetime, timezone
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from typing import Dict
import numpy as np
import pandas as pd
from config.settings import get_db_url, MODELS_DIR

logger = logging.getLogger(__name__)

FEATURE_COLS = ["total_prescriptions", "avg_quantity", "max_quantity", "unique_patients", "unique_drugs"]

EXPLANATION_THRESHOLDS = {
    "total_prescriptions": 200,
    "avg_quantity": 100,
    "max_quantity": 400,
    "unique_patients": 100,
}


def create_anomaly_table(engine):
    metadata = MetaData()
    Table("anomaly_results", metadata,
          Column("anomaly_id", Integer, primary_key=True, autoincrement=True),
          Column("physician_id", String),
          Column("anomaly_score", Float),
          Column("anomaly_type", String),
          Column("total_prescriptions", Integer),
          Column("avg_quantity", Float),
          Column("max_quantity", Integer),
          Column("unique_patients", Integer),
          Column("detection_date", String),
          Column("confidence", Float),
          Column("explanation", String))
    metadata.create_all(engine)


def load_model():
    model_path = MODELS_DIR / "isolation_forest.pkl"
    scaler_path = MODELS_DIR / "scaler.pkl"

    if not model_path.exists() or not scaler_path.exists():
        raise FileNotFoundError(f"Model files not found in {MODELS_DIR} - run training first")

    with open(model_path, "rb") as f:
        model = pickle.load(f)
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    return model, scaler


def score_physicians(engine, model, scaler) -> pd.DataFrame:
    query = """
        SELECT physician_id,
               COUNT(*) as total_prescriptions,
               AVG(quantity) as avg_quantity,
               MAX(quantity) as max_quantity,
               COUNT(DISTINCT patient_id) as unique_patients,
               COUNT(DISTINCT drug_name) as unique_drugs
        FROM fact_prescription
        GROUP BY physician_id
    """
    df = pd.read_sql(query, engine)

    X = df[FEATURE_COLS].fillna(0)
    X_scaled = scaler.transform(X)

    df["anomaly_score"] = model.decision_function(X_scaled)
    df["is_anomaly"] = model.predict(X_scaled) == -1

    # normalize confidence to 0-1 range
    max_score = np.abs(df["anomaly_score"]).max()
    df["confidence"] = np.abs(df["anomaly_score"]) / max_score if max_score > 0 else 0

    n_flagged = df["is_anomaly"].sum()
    logger.info(f"Scored {len(df)} physicians, {n_flagged} flagged as anomalous")
    return df


def generate_explanations(row: pd.Series) -> str:
    reasons = []

    if row["total_prescriptions"] > EXPLANATION_THRESHOLDS["total_prescriptions"]:
        reasons.append(f"High prescription volume ({int(row['total_prescriptions'])})")
    if row["avg_quantity"] > EXPLANATION_THRESHOLDS["avg_quantity"]:
        reasons.append(f"High avg quantity ({row['avg_quantity']:.0f})")
    if row["max_quantity"] > EXPLANATION_THRESHOLDS["max_quantity"]:
        reasons.append(f"Very high max quantity ({int(row['max_quantity'])})")
    if row["unique_patients"] > EXPLANATION_THRESHOLDS["unique_patients"]:
        reasons.append(f"Unusually many patients ({int(row['unique_patients'])})")

    if not reasons:
        return "Statistical outlier in prescribing pattern"
    return "; ".join(reasons)


def run_inference() -> Dict:
    engine = create_engine(get_db_url())
    create_anomaly_table(engine)

    model, scaler = load_model()
    scored = score_physicians(engine, model, scaler)

    # save only the anomalous ones
    anomalies = scored[scored["is_anomaly"]].copy()
    anomalies["anomaly_type"] = "prescription_volume"
    anomalies["detection_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    anomalies["explanation"] = anomalies.apply(generate_explanations, axis=1)

    output_cols = [
        "physician_id", "anomaly_score", "anomaly_type", "total_prescriptions",
        "avg_quantity", "max_quantity", "unique_patients", "detection_date",
        "confidence", "explanation"
    ]
    anomalies[output_cols].to_sql("anomaly_results", engine, if_exists="replace", index=False)

    return {"total_scored": len(scored), "anomalies_detected": len(anomalies)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_inference()
    print(f"Inference done: {result}")
