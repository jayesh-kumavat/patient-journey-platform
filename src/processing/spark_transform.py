"""
Warehouse transforms - takes staging data and builds star schema tables.
"""

# Use this Pyspark transformation if data grows beyond what pandas can handle in memory (~10M). Would run on EMR/Glue in prod.
#
# from pyspark.sql import SparkSession, Window
# from pyspark.sql import functions as F
# from pyspark.sql.types import IntegerType, StringType
#
# def get_spark():
#     return SparkSession.builder \
#         .appName("PatientJourneyTransforms") \
#         .config("spark.jars", "/path/to/postgresql-42.6.0.jar") \
#         .getOrCreate()
#
# JDBC_URL = "jdbc:postgresql://localhost:5432/patient_journey"
# JDBC_PROPS = {"user": "postgres", "password": "<password>", "driver": "org.postgresql.Driver"}
#
#
# def spark_transform_patients(spark):
#     df = spark.read.jdbc(JDBC_URL, "stg_patients", properties=JDBC_PROPS)
#     df = df.dropDuplicates(["patient_id"])
#     df = df.filter(F.to_date(F.col("date_of_birth")).isNotNull())
#     df = df.filter(F.to_date(F.col("enrollment_date")).isNotNull())
#     df = df.withColumn("effective_from", F.current_date().cast(StringType())) \
#            .withColumn("effective_to", F.lit("9999-12-31")) \
#            .withColumn("is_current", F.lit(1))
#     df = df.drop("ingested_at")
#     df.write.jdbc(JDBC_URL, "dim_patient", mode="overwrite", properties=JDBC_PROPS)
#     return df.count()
#
#
# def spark_transform_physicians(spark):
#     df = spark.read.jdbc(JDBC_URL, "stg_physicians", properties=JDBC_PROPS)
#     df = df.dropDuplicates(["physician_id"])
#     df = df.withColumn("years_experience", F.col("years_experience").cast(IntegerType()))
#     df = df.fillna(0, subset=["years_experience"])
#     df = df.drop("ingested_at")
#     df.write.jdbc(JDBC_URL, "dim_physician", mode="overwrite", properties=JDBC_PROPS)
#     return df.count()
#
#
# def spark_transform_prescriptions(spark):
#     df = spark.read.jdbc(JDBC_URL, "stg_prescriptions", properties=JDBC_PROPS)
#     df = df.dropDuplicates(["prescription_id"])
#     df = df.filter(F.to_date(F.col("prescription_date")).isNotNull())
#     df = df.withColumn("quantity", F.col("quantity").cast(IntegerType())) \
#            .withColumn("refills", F.col("refills").cast(IntegerType()))
#     df = df.fillna(0, subset=["quantity", "refills"])
#     df = df.drop("ingested_at")
#     df.write.jdbc(JDBC_URL, "fact_prescription", mode="overwrite", properties=JDBC_PROPS)
#     return df.count()
#
#
# def spark_build_patient_journey(spark):
#     rx = spark.read.jdbc(JDBC_URL, "fact_prescription", properties=JDBC_PROPS) \
#         .select(
#             F.col("patient_id"),
#             F.lit("prescription").alias("event_type"),
#             F.col("prescription_date").alias("event_date"),
#             F.col("drug_name").alias("event_detail"),
#             F.col("therapy_area"),
#         )
#
#     # try reading diagnoses, might not exist yet
#     try:
#         dx = spark.read.jdbc(JDBC_URL, "stg_diagnoses", properties=JDBC_PROPS) \
#             .select(
#                 F.col("patient_id"),
#                 F.lit("diagnosis").alias("event_type"),
#                 F.col("diagnosis_date").alias("event_date"),
#                 F.col("icd_code").alias("event_detail"),
#                 F.lit("").alias("therapy_area"),
#             )
#         journey = rx.unionByName(dx)
#     except Exception:
#         journey = rx
#
#     # add sequence number per patient ordered by date
#     w = Window.partitionBy("patient_id").orderBy("event_date")
#     journey = journey.withColumn("sequence_num", F.row_number().over(w))
#
#     journey.write.jdbc(JDBC_URL, "fact_patient_journey", mode="overwrite", properties=JDBC_PROPS)
#     return journey.count()
#
#
# def spark_detect_therapy_switches(spark):
#     df = spark.read.jdbc(JDBC_URL, "fact_prescription", properties=JDBC_PROPS)
#
#     w = Window.partitionBy("patient_id").orderBy("prescription_date")
#     df = df.withColumn("prev_drug", F.lag("drug_name").over(w))
#     df = df.withColumn("prev_therapy", F.lag("therapy_area").over(w))
#
#     switches = df.filter(
#         (F.col("prev_drug").isNotNull()) & (F.col("drug_name") != F.col("prev_drug"))
#     ).select(
#         F.col("patient_id"),
#         F.col("prev_drug").alias("from_drug"),
#         F.col("drug_name").alias("to_drug"),
#         F.col("prescription_date").alias("switch_date"),
#         F.col("therapy_area"),
#     )
#
#     switches.write.jdbc(JDBC_URL, "therapy_switches", mode="overwrite", properties=JDBC_PROPS)
#     return switches.count()
#
#
# def spark_run_all():
#     spark = get_spark()
#     counts = {
#         "dim_patient": spark_transform_patients(spark),
#         "dim_physician": spark_transform_physicians(spark),
#         "fact_prescription": spark_transform_prescriptions(spark),
#         "fact_patient_journey": spark_build_patient_journey(spark),
#         "therapy_switches": spark_detect_therapy_switches(spark),
#     }
#     spark.stop()
#     return counts


from datetime import datetime
from config.settings import get_db_url
from typing import List, Dict
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float
import logging

logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(get_db_url(), echo=False)


def create_warehouse_tables(engine):
    metadata = MetaData()

    Table("dim_patient", metadata,
          Column("patient_key", Integer, primary_key=True, autoincrement=True),
          Column("patient_id", String),
          Column("gender", String),
          Column("date_of_birth", String),
          Column("region", String),
          Column("enrollment_date", String),
          Column("effective_from", String),
          Column("effective_to", String),
          Column("is_current", Integer))

    Table("dim_physician", metadata,
          Column("physician_id", String, primary_key=True),
          Column("specialty", String),
          Column("region", String),
          Column("years_experience", Integer))

    Table("dim_therapy", metadata,
          Column("therapy_area", String, primary_key=True),
          Column("drug_count", Integer))

    Table("fact_prescription", metadata,
          Column("prescription_id", String, primary_key=True),
          Column("patient_id", String),
          Column("physician_id", String),
          Column("drug_name", String),
          Column("prescription_date", String),
          Column("quantity", Integer),
          Column("refills", Integer),
          Column("therapy_area", String))

    Table("fact_patient_journey", metadata,
          Column("journey_id", Integer, primary_key=True, autoincrement=True),
          Column("patient_id", String),
          Column("event_type", String),
          Column("event_date", String),
          Column("event_detail", String),
          Column("therapy_area", String),
          Column("sequence_num", Integer))

    Table("therapy_switches", metadata,
          Column("switch_id", Integer, primary_key=True, autoincrement=True),
          Column("patient_id", String),
          Column("from_drug", String),
          Column("to_drug", String),
          Column("switch_date", String),
          Column("therapy_area", String))

    metadata.create_all(engine)
    logger.info("Warehouse tables ready")


def deduplicate_dataframe(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=[key_column], keep="last")
    removed = before - len(df)
    if removed:
        logger.info(f"Dedup: removed {removed} rows on {key_column}")
    return df


def normalize_dates(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    for col in date_columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
        bad_count = df[col].isna().sum()
        if bad_count > 0:
            logger.warning(f"Dropping {bad_count} rows with unparseable {col}")
            df = df.dropna(subset=[col])
    return df


def transform_patients(engine) -> pd.DataFrame:
    """Build dim_patient with SCD Type-2 fields.
    Proper SCD-2 would need to diff against existing dim rows and expire old ones.
    """
    df = pd.read_sql("SELECT * FROM stg_patients", engine)
    df = deduplicate_dataframe(df, "patient_id")
    df = normalize_dates(df, ["date_of_birth", "enrollment_date"])

    df["effective_from"] = datetime.utcnow().strftime("%Y-%m-%d")
    df["effective_to"] = "9999-12-31"
    df["is_current"] = 1

    df = df.drop(columns=["ingested_at"], errors="ignore")
    df.to_sql("dim_patient", engine, if_exists="replace", index=False)
    logger.info(f"dim_patient: {len(df)} rows loaded")
    return df


def transform_physicians(engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM stg_physicians", engine)
    df = deduplicate_dataframe(df, "physician_id")
    df["years_experience"] = pd.to_numeric(df["years_experience"], errors="coerce").fillna(0).astype(int)
    df = df.drop(columns=["ingested_at"], errors="ignore")
    df.to_sql("dim_physician", engine, if_exists="replace", index=False)
    logger.info(f"dim_physician: {len(df)} rows")
    return df


def transform_prescriptions(engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM stg_prescriptions", engine)
    df = deduplicate_dataframe(df, "prescription_id")
    df = normalize_dates(df, ["prescription_date"])

    # coerce numeric cols
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["refills"] = pd.to_numeric(df["refills"], errors="coerce").fillna(0).astype(int)

    df = df.drop(columns=["ingested_at"], errors="ignore")
    df.to_sql("fact_prescription", engine, if_exists="replace", index=False)
    logger.info(f"fact_prescription: {len(df)} rows")
    return df


def build_patient_journey(engine) -> pd.DataFrame:
    rx_df = pd.read_sql(
        "SELECT patient_id, 'prescription' as event_type, prescription_date as event_date, "
        "drug_name as event_detail, therapy_area FROM fact_prescription", engine
    )

    try:
        dx_df = pd.read_sql(
            "SELECT patient_id, 'diagnosis' as event_type, diagnosis_date as event_date, "
            "icd_code as event_detail, '' as therapy_area FROM stg_diagnoses", engine
        )
    except Exception:
        dx_df = pd.DataFrame(columns=rx_df.columns)

    journey = pd.concat([rx_df, dx_df], ignore_index=True)
    journey = journey.sort_values(["patient_id", "event_date"])
    journey["sequence_num"] = journey.groupby("patient_id").cumcount() + 1

    journey.to_sql("fact_patient_journey", engine, if_exists="replace", index=False)
    logger.info(f"Patient journey: {len(journey)} events across {journey['patient_id'].nunique()} patients")
    return journey


def detect_therapy_switches(engine) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT patient_id, drug_name, prescription_date, therapy_area "
        "FROM fact_prescription ORDER BY patient_id, prescription_date", engine
    )

    switches = []
    for pid, group in df.groupby("patient_id"):
        group = group.sort_values("prescription_date")
        drugs = group["drug_name"].tolist()
        dates = group["prescription_date"].tolist()
        areas = group["therapy_area"].tolist()

        for i in range(1, len(drugs)):
            if drugs[i] != drugs[i-1]:
                switches.append({
                    "patient_id": pid,
                    "from_drug": drugs[i-1],
                    "to_drug": drugs[i],
                    "switch_date": dates[i],
                    "therapy_area": areas[i],
                })

    result = pd.DataFrame(switches)
    if not result.empty:
        result.to_sql("therapy_switches", engine, if_exists="replace", index=False)
    logger.info(f"Found {len(result)} therapy switches")
    return result


def run_all_transformations() -> Dict:
    engine = get_engine()
    create_warehouse_tables(engine)

    counts = {
        "dim_patient": len(transform_patients(engine)),
        "dim_physician": len(transform_physicians(engine)),
        "fact_prescription": len(transform_prescriptions(engine)),
        "fact_patient_journey": len(build_patient_journey(engine)),
        "therapy_switches": len(detect_therapy_switches(engine)),
    }
    logger.info(f"All transforms complete: {counts}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_all_transformations()
