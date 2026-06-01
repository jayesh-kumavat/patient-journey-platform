"""
Schema validation using jsonschema.
Records that fail go to quarantine CSVs.
"""

import logging
from datetime import datetime
from typing import List, Dict, Tuple
import csv
from pathlib import Path
import json
from config.settings import QUARANTINE_DIR
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

SCHEMAS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "schemas" / "data_schemas.json"

_schemas_cache = None

def load_schemas() -> Dict:
    global _schemas_cache
    if _schemas_cache is None:
        with open(SCHEMAS_PATH) as f:
            _schemas_cache = json.load(f)
    return _schemas_cache


def validate_record(record: Dict, schema: Dict) -> Tuple[bool, str]:
    try:
        cleaned = {}
        props = schema.get("properties", {})

        for key, value in record.items():
            if value is None or value == "" or value == "None":
                cleaned[key] = None
            elif props.get(key, {}).get("type") == "integer":
                try:
                    cleaned[key] = int(float(value))
                except (ValueError, TypeError):
                    cleaned[key] = value 
            elif props.get(key, {}).get("type") == "number":
                try:
                    cleaned[key] = float(value)
                except (ValueError, TypeError):
                    cleaned[key] = value
            else:
                cleaned[key] = value

        validate(instance=cleaned, schema=schema)
        return True, ""
    except ValidationError as e:
        return False, str(e.message)


def validate_file(filepath: Path, dataset_type: str) -> Tuple[List[Dict], List[Dict]]:
    schemas = load_schemas()
    if dataset_type not in schemas:
        raise ValueError(f"No schema defined for: {dataset_type}")

    schema = schemas[dataset_type]
    valid = []
    invalid = []

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=1):
            ok, error = validate_record(row, schema)
            if ok:
                valid.append(row)
            else:
                row["_validation_error"] = error
                row["_row_number"] = row_num
                invalid.append(row)

    logger.info(f"{filepath.name}: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid


def quarantine_records(invalid_records: List[Dict], source_file: str) -> Path:
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = QUARANTINE_DIR / f"quarantine_{source_file}_{ts}.csv"

    if not invalid_records:
        return out_path

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=invalid_records[0].keys())
        writer.writeheader()
        writer.writerows(invalid_records)

    logger.warning(f"Quarantined {len(invalid_records)} records -> {out_path.name}")
    return out_path


def validate_and_quarantine(filepath: Path, dataset_type: str) -> List[Dict]:
    valid_records, invalid_records = validate_file(filepath, dataset_type)
    if invalid_records:
        quarantine_records(invalid_records, filepath.stem)
    return valid_records
