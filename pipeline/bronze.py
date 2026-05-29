"""
Bronze Layer - Raw ingestion into PostgreSQL.
"""
import os
import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "medallion"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}

BRONZE_COLS = [
    "ticket_id", "created_at", "resolved_at", "category", "priority",
    "status", "building", "description", "submitted_by", "assigned_to",
    "resolution_notes", "cost", "sla_hours",
]


def _row_hash(row: dict) -> str:
    val = "|".join(str(row.get(c, "")) for c in BRONZE_COLS)
    return hashlib.sha256(val.encode()).hexdigest()


def ingest(csv_path: str = "data/raw_tickets.csv"):
    print(f"[BRONZE] Reading {csv_path}...")
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    print(f"[BRONZE] Read {len(df)} rows")

    df["_source_file"] = csv_path
    df["_row_hash"] = df.apply(lambda r: _row_hash(r.to_dict()), axis=1)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT _row_hash FROM bronze.raw_tickets")
    existing_hashes = {r[0] for r in cur.fetchall()}
    print(f"[BRONZE] {len(existing_hashes)} existing rows in bronze")

    new_df = df[~df["_row_hash"].isin(existing_hashes)]
    if new_df.empty:
        print("[BRONZE] No new rows to ingest. Already up to date.")
        cur.close()
        conn.close()
        return

    insert_cols = ["_source_file", "_row_hash"] + BRONZE_COLS
    rows = [tuple(row.get(c, "") for c in insert_cols) for _, row in new_df.iterrows()]

    execute_values(cur, f"""
        INSERT INTO bronze.raw_tickets ({', '.join(insert_cols)})
        VALUES %s
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[BRONZE] Done! Ingested {len(new_df)} new rows.")


if __name__ == "__main__":
    ingest()
