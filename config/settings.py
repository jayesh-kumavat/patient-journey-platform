"""Central config. Reads from .env file."""

import logging
import os
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
MODELS_DIR = BASE_DIR / "models"

# db config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "patient_journey")
DB_USER = os.getenv("DB_USER", "postgres")

from urllib.parse import quote_plus


def get_db_url() -> str:
    """Build and return the database URL at call time."""
    password = os.getenv("DB_PASSWORD", "")
    return f"postgresql://{DB_USER}:{quote_plus(password)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def ensure_database_exists():
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, quote_ident

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname="postgres",
            user=DB_USER, password=os.getenv("DB_PASSWORD", ""),
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        if not cur.fetchone():
            safe_db_name = quote_ident(DB_NAME, cur)
            cur.execute("CREATE DATABASE " + safe_db_name)
        cur.close()
    except Exception as e:
        logger.warning(f"Could not ensure database exists: {e}")
    finally:
        if conn:
            conn.close()


# aws config for prod
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "patient-journey-landing")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "-0.5"))
