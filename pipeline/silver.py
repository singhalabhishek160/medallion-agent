"""
Silver Layer - Clean, deduplicate, type-cast, validate.
"""
import os
import re
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

# Rule: Normalise free-text categories to a fixed enum.
# Why: The raw data has 49+ distinct category strings for what are really ~12 types
# (e.g. "a/c", "Air Conditioning", "HVAC", "cooling" all mean the same thing).
# Without normalisation, gold aggregations split the same problem type across many rows,
# making SLA breach rates and cost rollups meaningless.
CATEGORY_MAP = {
    "power issue": "Power", "power": "Power", "electrical": "Power", "electric": "Power",
    "fire safety": "Fire Safety", "fire/safety": "Fire Safety", "fire": "Fire Safety",
    "a/c": "HVAC", "hvac": "HVAC", "heating": "HVAC", "cooling": "HVAC",
    "air conditioning": "HVAC", "cold": "HVAC",
    "plumbing": "Plumbing", "water": "Plumbing", "leak": "Plumbing", "pipe burst": "Plumbing",
    "elevator": "Elevator", "lift": "Elevator",
    "pest": "Pest Control", "pest control": "Pest Control",
    "security": "Security", "access": "Security",
    "cleaning": "Cleaning", "janitorial": "Cleaning",
    "structural": "Structural", "roof": "Structural", "roofing": "Structural",
    "general": "General Maintenance", "maintenance": "General Maintenance",
    "it support": "IT Support", "network": "IT Support",
    "other": "Other", "???": "Other", "null": "Other", "unknown": "Other",
    "test": "Other", "asdf": "Other",
}

# Rule: Collapse priority variants to 4 canonical values + UNKNOWN.
# Why: Raw data has "crit", "CRITICAL", "urgent", "hi", "HIGH" for the same severity.
# A consistent enum is required for SLA breach rate calculations and triage dashboards.
PRIORITY_MAP = {
    "low": "LOW", "medium": "MEDIUM", "med": "MEDIUM",
    "high": "HIGH", "critical": "CRITICAL", "urgent": "CRITICAL", "crit": "CRITICAL",
}

# Rule: Try 5 date formats before giving up and returning None.
# Why: The source system exported dates inconsistently — ISO 8601, human-readable
# ("12-Feb-2025 03:21"), and plain date strings all appear. Coercing to a single
# TIMESTAMP type is required to compute resolution_hours and detect SLA breaches.
# Unparseable values (e.g. "asap", "???") are NULL'd rather than rejected, so the
# row is still usable for non-time-based analytics.
DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    "%d-%b-%Y %H:%M", "%d-%b-%Y", "%Y-%m-%d",
]


def normalize_category(cat: str) -> str:
    if not cat or cat.strip() == "":
        return "Other"
    c = cat.strip().lower()
    if c in CATEGORY_MAP:
        return CATEGORY_MAP[c]
    for key, val in CATEGORY_MAP.items():
        if len(key) > 3 and key in c:
            return val
    return cat.strip().title()


def normalize_priority(p: str) -> str:
    return PRIORITY_MAP.get((p or "").strip().lower(), "UNKNOWN")


def normalize_status(s: str) -> str:
    s = (s or "").strip().lower()
    if "open" in s:        return "Open"
    if "resolved" in s:    return "Resolved"
    if "closed" in s:      return "Closed"
    if "progress" in s:    return "In Progress"
    if "pending" in s:     return "Pending"
    if "escalat" in s:     return "Escalated"
    return "Unknown"


def parse_date(val: str):
    if not val or val.strip() == "":
        return None
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(val.strip(), format=fmt)
        except (ValueError, TypeError):
            continue
    return None


def parse_cost(val: str):
    # Rule: Strip currency symbols and non-numeric chars, coerce to float.
    # Why: Raw values include "$1,200", "1200.00", "TBD", "error". Financial
    # rollups in gold require a numeric type; sentinel strings become NULL.
    if not val or val.strip() == "":
        return None
    cleaned = re.sub(r"[^0-9.]", "", val)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_sla_hours(val: str):
    # Rule: Strip non-numeric chars and coerce to int.
    # Why: Raw values include "8h", "8 hours", "8". A consistent integer is
    # needed to compute is_sla_breached = resolution_hours > sla_hours.
    if not val or val.strip() == "":
        return None
    cleaned = re.sub(r"[^0-9]", "", val)
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def transform():
    print("[SILVER] Reading from bronze.raw_tickets...")
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM bronze.raw_tickets", conn)
    print(f"[SILVER] Read {len(df)} rows from bronze")

    df = df.drop_duplicates(subset=["ticket_id"], keep="first")
    print(f"[SILVER] After dedup: {len(df)} unique tickets")

    df["category"] = df["category"].apply(normalize_category)
    df["priority"] = df["priority"].apply(normalize_priority)
    df["status"] = df["status"].apply(normalize_status)
    df["created_at"] = df["created_at"].apply(parse_date)
    df["resolved_at"] = df["resolved_at"].apply(parse_date)
    df["cost"] = df["cost"].apply(parse_cost)
    df["sla_hours"] = df["sla_hours"].apply(parse_sla_hours)

    df["resolution_hours"] = df.apply(
        lambda r: round((r["resolved_at"] - r["created_at"]).total_seconds() / 3600, 2)
        if pd.notna(r["resolved_at"]) and pd.notna(r["created_at"]) else None,
        axis=1,
    )
    df["is_sla_breached"] = df.apply(
        lambda r: bool(r["resolution_hours"] > r["sla_hours"])
        if pd.notna(r["resolution_hours"]) and pd.notna(r["sla_hours"]) else None,
        axis=1,
    )

    silver_cols = [
        "ticket_id", "created_at", "resolved_at", "category", "priority",
        "status", "building", "description", "submitted_by", "assigned_to",
        "resolution_notes", "cost", "sla_hours", "resolution_hours", "is_sla_breached",
    ]
    silver_df = df[silver_cols + ["_row_hash"]].rename(columns={"_row_hash": "_source_hash"})

    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE silver.tickets")

    rows = [tuple(None if pd.isna(v) else v for v in row) for row in silver_df.itertuples(index=False)]
    execute_values(cur, f"""
        INSERT INTO silver.tickets ({', '.join(silver_cols + ['_source_hash'])})
        VALUES %s
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[SILVER] Done! Wrote {len(silver_df)} rows to silver.tickets.")


if __name__ == "__main__":
    transform()
