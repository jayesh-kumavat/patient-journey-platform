"""
Trains an Isolation Forest to detect anomalous physician prescribing patterns.
Also does a simple z-score check on daily volumes.
"""

import pickle
import logging
from typing import Dict, Tuple
import pandas as pd
from sklearn.ensemble import IsolationForest
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from config.settings import get_db_url, MODELS_DIR, ANOMALY_THRESHOLD
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

FEATURE_COLS = ["total_prescriptions", "avg_quantity", "max_quantity", "unique_patients", "unique_drugs"]


def prepare_physician_features(engine) -> pd.DataFrame:
    query = """
        SELECT
            physician_id,
            COUNT(*) as total_prescriptions,
            AVG(quantity) as avg_quantity,
            MAX(quantity) as max_quantity,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT drug_name) as unique_drugs
        FROM fact_prescription
        GROUP BY physician_id
    """
    df = pd.read_sql(query, engine)
    logger.info(f"Built features for {len(df)} physicians")
    return df


def train_isolation_forest(features_df: pd.DataFrame) -> Tuple[IsolationForest, StandardScaler]:
    X = features_df[FEATURE_COLS].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # contamination=0.05 means we expect ~5% of physicians to be anomalous
    # tuned this based on looking at the data distribution
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,
        random_state=42,
        max_samples="auto",
    )
    model.fit(X_scaled)

    scores = model.decision_function(X_scaled)
    n_anomalies = (scores < ANOMALY_THRESHOLD).sum()
    logger.info(f"Model trained - flagged {n_anomalies}/{len(X)} physicians ({n_anomalies/len(X):.1%})")

    return model, scaler


def compute_zscore_anomalies(engine, window=30) -> pd.DataFrame:
    query = """
        SELECT prescription_date, COUNT(*) as daily_volume
        FROM fact_prescription
        GROUP BY prescription_date
        ORDER BY prescription_date
    """
    df = pd.read_sql(query, engine)
    df["prescription_date"] = pd.to_datetime(df["prescription_date"])

    df["rolling_mean"] = df["daily_volume"].rolling(window=window, min_periods=1).mean()
    df["rolling_std"] = df["daily_volume"].rolling(window=window, min_periods=1).std().fillna(1)
    df["z_score"] = (df["daily_volume"] - df["rolling_mean"]) / df["rolling_std"]
    df["is_anomaly"] = df["z_score"].abs() > 2.5

    anomaly_days = df["is_anomaly"].sum()
    logger.info(f"Z-score check: {anomaly_days} anomalous days out of {len(df)}")
    return df


def save_model(model, scaler) -> Path:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODELS_DIR / "isolation_forest.pkl"
    scaler_path = MODELS_DIR / "scaler.pkl"

    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)

    logger.info(f"Saved model to {model_path}")
    return model_path


def train_and_save() -> Dict:
    engine = create_engine(get_db_url())

    features_df = prepare_physician_features(engine)
    model, scaler = train_isolation_forest(features_df)
    model_path = save_model(model, scaler)
    zscore_df = compute_zscore_anomalies(engine)

    return {
        "model_path": str(model_path),
        "physicians_analyzed": len(features_df),
        "zscore_anomaly_days": int(zscore_df["is_anomaly"].sum()),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = train_and_save()
    print(f"Training complete: {result}")
