from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text

from config.settings import get_db_url

router = APIRouter()


def get_engine():
    return create_engine(get_db_url())


@router.get("/therapy-switching")
def therapy_switching(therapy_area: Optional[str] = None, limit: int = Query(100, le=1000)):
    engine = get_engine()

    if therapy_area:
        df = pd.read_sql(
            text("SELECT * FROM therapy_switches WHERE therapy_area = :ta LIMIT :lim"),
            engine, params={"ta": therapy_area, "lim": limit}
        )
    else:
        df = pd.read_sql(f"SELECT * FROM therapy_switches LIMIT {limit}", engine)

    summary = {}
    if not df.empty:
        summary = {
            "total_switches": len(df),
            "unique_patients": int(df["patient_id"].nunique()),
            "top_from": df["from_drug"].value_counts().head(5).to_dict(),
            "top_to": df["to_drug"].value_counts().head(5).to_dict(),
        }

    return {"switches": df.to_dict(orient="records"), "summary": summary}


@router.get("/summary")
def kpi_summary():
    engine = get_engine()
    try:
        patients = pd.read_sql("SELECT COUNT(*) c FROM dim_patient WHERE is_current=1", engine)["c"].iloc[0]
        rxs = pd.read_sql("SELECT COUNT(*) c FROM fact_prescription", engine)["c"].iloc[0]
        switches = pd.read_sql("SELECT COUNT(*) c FROM therapy_switches", engine)["c"].iloc[0]
        anomalies = pd.read_sql("SELECT COUNT(*) c FROM anomaly_results", engine)["c"].iloc[0]

        by_therapy = pd.read_sql(
            "SELECT therapy_area, COUNT(*) as vol FROM fact_prescription GROUP BY therapy_area", engine
        )

        return {
            "total_patients": int(patients),
            "total_prescriptions": int(rxs),
            "total_therapy_switches": int(switches),
            "total_anomalies_detected": int(anomalies),
            "prescriptions_by_therapy": by_therapy.set_index("therapy_area")["vol"].to_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drop-off")
def drop_off_rate(therapy_area: Optional[str] = None):
    """Patients with <= 2 prescriptions are considered drop-offs."""
    engine = get_engine()

    if therapy_area:
        q = text(
            "SELECT patient_id, therapy_area, COUNT(*) as total_rx FROM fact_prescription "
            "WHERE therapy_area = :ta GROUP BY patient_id, therapy_area"
        )
        df = pd.read_sql(q, engine, params={"ta": therapy_area})
    else:
        q = "SELECT patient_id, therapy_area, COUNT(*) as total_rx FROM fact_prescription GROUP BY patient_id, therapy_area"
        df = pd.read_sql(q, engine)
    if df.empty:
        return {"drop_off_rate": 0}

    dropoffs = df[df["total_rx"] <= 2]
    return {
        "total_patients_analyzed": len(df),
        "drop_off_count": len(dropoffs),
        "drop_off_rate": round(len(dropoffs) / len(df), 4),
    }
