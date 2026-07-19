from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text

from config.settings import get_db_url

router = APIRouter()


def get_engine():
    return create_engine(get_db_url())


@router.get("/")
def get_anomalies(
    date: Optional[str] = None,
    min_confidence: float = Query(0.0, ge=0, le=1),
    limit: int = Query(50, le=500),
):
    engine = get_engine()
    params = {"lim": limit, "dt": date, "conf": min_confidence}

    if date and min_confidence > 0:
        query = "SELECT * FROM anomaly_results WHERE detection_date = :dt AND confidence >= :conf ORDER BY anomaly_score ASC LIMIT :lim"
    elif date:
        query = "SELECT * FROM anomaly_results WHERE detection_date = :dt ORDER BY anomaly_score ASC LIMIT :lim"
    elif min_confidence > 0:
        query = "SELECT * FROM anomaly_results WHERE confidence >= :conf ORDER BY anomaly_score ASC LIMIT :lim"
    else:
        query = "SELECT * FROM anomaly_results ORDER BY anomaly_score ASC LIMIT :lim"

    df = pd.read_sql(text(query), engine, params=params)
    return {
        "anomalies": df.to_dict(orient="records"),
        "count": len(df),
        "filters": {"date": date, "min_confidence": min_confidence},
    }


@router.get("/physician/{physician_id}")
def get_physician_anomaly(physician_id: str):
    engine = get_engine()
    df = pd.read_sql(
        text("SELECT * FROM anomaly_results WHERE physician_id = :pid"),
        engine, params={"pid": physician_id}
    )
    if df.empty:
        return {"physician_id": physician_id, "is_anomalous": False, "details": None}
    return {"physician_id": physician_id, "is_anomalous": True, "details": df.iloc[0].to_dict()}


@router.get("/summary")
def anomaly_summary():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM anomaly_results", engine)
    if df.empty:
        return {"total_anomalies": 0}

    return {
        "total_anomalies": len(df),
        "avg_confidence": round(float(df["confidence"].mean()), 3),
        "by_type": df["anomaly_type"].value_counts().to_dict(),
        "top_anomalies": df.nsmallest(5, "anomaly_score")[
            ["physician_id", "anomaly_score", "explanation"]
        ].to_dict(orient="records"),
    }
