"""Saves DQ reports to JSON and tracks scores in the DB."""

import json
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import text, MetaData, Table, Column, String, Float, Integer
from config.settings import BASE_DIR

logger = logging.getLogger(__name__)

REPORTS_DIR = BASE_DIR / "data" / "reports"


def create_dq_scores_table(engine):
    meta = MetaData()
    Table("dq_scores", meta,
          Column("run_id", String, primary_key=True),
          Column("run_timestamp", String),
          Column("total_checks", Integer),
          Column("passed_checks", Integer),
          Column("overall_score", Float),
          Column("report_path", String))
    meta.create_all(engine)


def save_dq_report(report, engine=None):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"dq_report_{ts}.json"

    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"DQ report saved: {path.name}")

    if engine:
        create_dq_scores_table(engine)
        run_id = f"dq_{ts}"
        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO dq_scores (run_id, run_timestamp, total_checks, passed_checks, overall_score, report_path) "
                    "VALUES (:rid, :ts, :total, :passed, :score, :path)"
                ),
                {"rid": run_id, "ts": report["timestamp"], "total": report["total_checks"],
                 "passed": report["passed"], "score": report["overall_score"], "path": str(path)}
            )
            conn.commit()

    return path


def get_dq_trend(engine, n=10):
    try:
        return pd.read_sql(
            f"SELECT run_timestamp, overall_score FROM dq_scores ORDER BY run_timestamp DESC LIMIT {n}",
            engine
        )
    except Exception:
        return pd.DataFrame(columns=["run_timestamp", "overall_score"])
