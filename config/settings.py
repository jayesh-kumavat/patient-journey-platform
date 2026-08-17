import logging
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
MODELS_DIR = BASE_DIR / "models"


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "patient_journey")
DB_USER = os.getenv("DB_USER", "postgres")


def get_db_url() -> str:
    password = os.getenv("DB_PASSWORD", "")
    return f"postgresql://{DB_USER}:{quote_plus(password)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def ensure_database_exists():
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from psycopg2 import sql

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
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        cur.close()
    except Exception as e:
        logger.warning(f"Could not ensure database exists: {e}")
    finally:
        if conn:
            conn.close()


