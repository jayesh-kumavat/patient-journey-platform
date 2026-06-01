from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine, text

from config.settings import get_db_url

router = APIRouter()


def get_engine():
    return create_engine(get_db_url())


@router.get("/{patient_id}/journey")
def get_patient_journey(patient_id: str):
    engine = get_engine()

    journey = pd.read_sql(
        text("SELECT * FROM fact_patient_journey WHERE patient_id = :pid ORDER BY sequence_num"),
        engine, params={"pid": patient_id}
    )
    if journey.empty:
        raise HTTPException(status_code=404, detail=f"No journey data for patient {patient_id}")

    # grab patient demographics too
    pat = pd.read_sql(
        text("SELECT * FROM dim_patient WHERE patient_id = :pid AND is_current = 1"),
        engine, params={"pid": patient_id}
    )
    patient_info = pat.iloc[0].to_dict() if not pat.empty else {}

    return {
        "patient_id": patient_id,
        "patient_info": patient_info,
        "journey_events": journey.to_dict(orient="records"),
        "total_events": len(journey),
    }


@router.get("/")
def list_patients(region: Optional[str] = None, limit: int = Query(default=50, le=500)):
    engine = get_engine()

    if region:
        df = pd.read_sql(
            text("SELECT * FROM dim_patient WHERE is_current = 1 AND region = :r LIMIT :lim"),
            engine, params={"r": region, "lim": limit}
        )
    else:
        df = pd.read_sql(
            text("SELECT * FROM dim_patient WHERE is_current = 1 LIMIT :lim"),
            engine, params={"lim": limit}
        )

    return {"patients": df.to_dict(orient="records"), "count": len(df)}
