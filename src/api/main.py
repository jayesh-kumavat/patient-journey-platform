"""FastAPI app for the patient journey platform."""

from fastapi import FastAPI
from src.api.routes.patients import router as patients_router
from src.api.routes.kpis import router as kpis_router
from src.api.routes.anomalies import router as anomalies_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Patient Journey Analytics", version="1.0.0")

# need to update this in production to restrict origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(patients_router, prefix="/patients", tags=["Patients"])
app.include_router(anomalies_router, prefix="/anomalies", tags=["Anomalies"])
app.include_router(kpis_router, prefix="/kpis", tags=["KPIs"])


@app.get("/health")
def health():
    return {"status": "healthy", "service": "patient-journey-api"}


@app.get("/")
def root():
    return {
        "service": "Patient Journey Analytics Platform",
        "docs": "/docs",
        "endpoints": [
            "/patients/{patient_id}/journey",
            "/anomalies",
            "/kpis/therapy-switching",
            "/kpis/summary",
        ]
    }
