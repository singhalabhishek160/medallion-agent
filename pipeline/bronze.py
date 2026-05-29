"""
Bronze Layer - Raw ingestion into PostgreSQL.
"""
import hashlib
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sha2, concat_ws, col, current_timestamp


def get_spark():
    """Create a local Spark session with PostgreSQL JDBC driver."""
    return (
        SparkSession.builder
        .appName("medallion-bronze")
        .master("local[*]")
        .getOrCreate()
    )


def get_jdbc_props():
    return {
        "user": "pipeline",
        "password": "pipeline123",
        "driver": "org.postgresql.Driver"
    }


JDBC_URL = "jdbc:postgresql://localhost:5432/medallion"


def ingest(csv_path: str = "data/raw_tickets.csv"):
    """Read raw CSV and write to bronze.raw_tickets with lineage metadata."""
    spark = get_spark()
    
    print(f"[BRONZE] Reading {csv_path}...")
    
    # Read everything as string (schema-on-read)
    df = spark.read.csv(csv_path, header=True, inferSchema=False)
    
    print(f"[BRONZE] Read {df.count()} rows")
    
    # Add lineage metadata
    df = df.withColumn("_source_file", lit(csv_path))
    df = df.withColumn("_ingested_at", current_timestamp())
    df = df.withColumn("_row_hash", sha2(concat_ws("|", *[col(c) for c in df.columns if not c.startswith("_")]), 256))
    
    # Get existing hashes to skip duplicates
    try:
        existing = spark.read.jdbc(JDBC_URL, "bronze.raw_tickets", properties=get_jdbc_props())
        existing_hashes = set(existing.select("_row_hash").distinct().toPandas()["_row_hash"].tolist())
        print(f"[BRONZE] Found {len(existing_hashes)} existing rows")
    except Exception:
        existing_hashes = set()
        print("[BRONZE] Fresh table, no existing data")
    
    # Filter out already ingested rows
    if existing_hashes:
        from pyspark.sql.functions import col as spark_col
        df = df.filter(~spark_col("_row_hash").isin(existing_hashes))
    
    new_count = df.count()
    if new_count == 0:
        print("[BRONZE] No new rows to ingest. Already up to date.")
        spark.stop()
        return
    
    # Write to PostgreSQL
    print(f"[BRONZE] Writing {new_count} new rows to bronze.raw_tickets...")
    df.write.jdbc(
        url=JDBC_URL,
        table="bronze.raw_tickets",
        mode="append",
        properties=get_jdbc_props()
    )
    
    print(f"[BRONZE] Done! Ingested {new_count} rows.")
    spark.stop()


if __name__ == "__main__":
    ingest()
