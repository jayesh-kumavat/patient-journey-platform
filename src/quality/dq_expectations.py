"""
Data quality checks.
Runs completeness, uniqueness, referential integrity, and distribution checks against the staging tables.
"""

import logging
from datetime import datetime, timezone
from config.settings import get_db_url
from typing import List, Dict
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class DataQualityCheck:

    def __init__(self, name, check_type, table, column=None, threshold=0.95):
        self.name = name
        self.check_type = check_type
        self.table = table
        self.column = column
        self.threshold = threshold
        self.passed = False
        self.score = 0.0
        self.details = ""

    def to_dict(self):
        return {
            "name": self.name,
            "check_type": self.check_type,
            "table": self.table,
            "column": self.column,
            "threshold": float(self.threshold),
            "passed": bool(self.passed),
            "score": float(self.score),
            "details": self.details,
        }


def check_completeness(engine, table, column, threshold=0.95):
    check = DataQualityCheck(f"completeness_{table}_{column}", "completeness", table, column, threshold)

    df = pd.read_sql(f"SELECT {column} FROM {table}", engine)
    total = len(df)
    if total == 0:
        check.score = 0.0
        check.details = "Table is empty"
        return check

    non_null = df[column].notna().sum()
    check.score = non_null / total
    check.details = f"{non_null}/{total} non-null ({check.score:.1%})"
    check.passed = check.score >= threshold
    return check


def check_uniqueness(engine, table, column, threshold=0.99):
    check = DataQualityCheck(f"uniqueness_{table}_{column}", "uniqueness", table, column, threshold)

    df = pd.read_sql(f"SELECT {column} FROM {table}", engine)
    total = len(df)
    if total == 0:
        check.score = 0.0
        return check

    unique_count = df[column].nunique()
    check.score = unique_count / total
    check.details = f"{unique_count}/{total} unique ({check.score:.1%})"
    check.passed = check.score >= threshold
    return check


def check_referential_integrity(engine, child_table, child_col, parent_table, parent_col):
    check = DataQualityCheck(
        f"ref_integrity_{child_table}_{child_col}",
        "referential_integrity", child_table, child_col
    )

    child_vals = set(pd.read_sql(f"SELECT DISTINCT {child_col} FROM {child_table}", engine)[child_col].dropna())
    parent_vals = set(pd.read_sql(f"SELECT DISTINCT {parent_col} FROM {parent_table}", engine)[parent_col].dropna())

    orphans = child_vals - parent_vals
    total = len(child_vals)
    check.score = 1 - (len(orphans) / total) if total > 0 else 1.0
    check.details = f"{len(orphans)} orphan values out of {total}"
    check.passed = check.score >= check.threshold
    return check


def check_row_count(engine, table, min_rows=1):
    check = DataQualityCheck(f"row_count_{table}", "row_count", table, threshold=1.0)
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", engine)["cnt"].iloc[0]
    check.score = 1.0 if count >= min_rows else 0.0
    check.details = f"{count} rows (need at least {min_rows})"
    check.passed = count >= min_rows
    return check


def check_statistical_distribution(engine, table, column, min_val=None, max_val=None):
    check = DataQualityCheck(f"distribution_{table}_{column}", "distribution", table, column)

    df = pd.read_sql(f"SELECT {column} FROM {table}", engine)
    df[column] = pd.to_numeric(df[column], errors="coerce")
    total = len(df[column].dropna())

    if total == 0:
        check.score = 0.0
        check.details = "No numeric values"
        return check

    stats = df[column].describe()
    outliers = 0
    if min_val is not None:
        outliers += (df[column] < min_val).sum()
    if max_val is not None:
        outliers += (df[column] > max_val).sum()

    check.score = 1 - (outliers / total)
    check.details = f"mean={stats['mean']:.1f}, std={stats['std']:.1f}, {outliers} outliers"
    check.passed = check.score >= check.threshold
    return check


def run_all_quality_checks(engine=None) -> List[DataQualityCheck]:
    engine = engine or create_engine(get_db_url())
    results = []

    # completeness
    completeness_checks = [
        ("stg_prescriptions", "patient_id"),
        ("stg_prescriptions", "physician_id"),
        ("stg_prescriptions", "drug_name"),
        ("stg_diagnoses", "patient_id"),
        ("stg_diagnoses", "icd_code"),
        ("stg_claims", "patient_id"),
        ("stg_claims", "amount"),
    ]
    for table, col in completeness_checks:
        try:
            results.append(check_completeness(engine, table, col))
        except Exception as e:
            logger.error(f"Completeness check failed for {table}.{col}: {e}")

    # uniqueness on PKs
    uniqueness_checks = [
        ("stg_prescriptions", "prescription_id"),
        ("stg_diagnoses", "diagnosis_id"),
        ("stg_claims", "claim_id"),
        ("stg_patients", "patient_id"),
    ]
    for table, col in uniqueness_checks:
        try:
            results.append(check_uniqueness(engine, table, col))
        except Exception as e:
            logger.error(f"Uniqueness check failed for {table}.{col}: {e}")

    # minimum row counts
    for table, min_rows in [("stg_patients", 100), ("stg_physicians", 50), ("stg_prescriptions", 1000)]:
        try:
            results.append(check_row_count(engine, table, min_rows))
        except Exception as e:
            logger.error(f"Row count check failed for {table}: {e}")

    # quantity should be between 1 and 500
    try:
        results.append(check_statistical_distribution(engine, "stg_prescriptions", "quantity", min_val=1, max_val=500))
    except Exception as e:
        logger.error(f"Distribution check failed: {e}")

    # prescriptions should reference valid patients
    try:
        results.append(check_referential_integrity(
            engine, "stg_prescriptions", "patient_id", "stg_patients", "patient_id"))
    except Exception as e:
        logger.error(f"RI check failed: {e}")

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    if total:
        logger.info(f"DQ results: {passed}/{total} checks passed ({passed/total:.0%})")

    return results


def generate_dq_report(checks: List[DataQualityCheck]) -> Dict:
    total = len(checks)
    passed = sum(1 for c in checks if c.passed)
    failed_details = [c.to_dict() for c in checks if not c.passed]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": total,
        "passed": passed,
        "failed_count": total - passed,
        "overall_score": passed / total if total > 0 else 0,
        "failed_checks": failed_details,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    checks = run_all_quality_checks()
    report = generate_dq_report(checks)
    print(f"\nDQ Score: {report['overall_score']:.0%}")
    if report["failed_checks"]:
        print("Failed checks:")
        for fc in report["failed_checks"]:
            print(f"  ✗ {fc['name']}: {fc['details']}")
