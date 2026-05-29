"""
Gold Layer - Business-ready aggregations.

Models:
1. category_summary - Volume, avg resolution time, cost, SLA breach rate per category
2. monthly_trends  - Time-series for capacity planning
"""
import os
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


def build():
    conn = psycopg2.connect(**DB_CONFIG)
    print("[GOLD] Reading from silver.tickets...")
    df = pd.read_sql("SELECT * FROM silver.tickets", conn)
    print(f"[GOLD] Read {len(df)} rows")

    if df.empty:
        print("[GOLD] No silver data. Skipping.")
        conn.close()
        return

    cur = conn.cursor()

    # === Gold 1: Category Summary ===
    cat = df.groupby("category").agg(
        ticket_count=("ticket_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
        total_cost=("cost", "sum"),
    ).reset_index()

    sla = df[df["is_sla_breached"].notna()].groupby("category").apply(
        lambda g: g["is_sla_breached"].sum() / g["is_sla_breached"].count()
    ).reset_index(name="sla_breach_rate")

    cat = cat.merge(sla, on="category", how="left")
    cat["avg_resolution_hours"] = cat["avg_resolution_hours"].round(2)
    cat["total_cost"] = cat["total_cost"].round(2)
    cat["sla_breach_rate"] = cat["sla_breach_rate"].round(4)

    cur.execute("TRUNCATE TABLE gold.category_summary")
    execute_values(cur, """
        INSERT INTO gold.category_summary
            (category, ticket_count, avg_resolution_hours, total_cost, sla_breach_rate)
        VALUES %s
    """, [
        (r["category"], int(r["ticket_count"]),
         None if pd.isna(r["avg_resolution_hours"]) else r["avg_resolution_hours"],
         None if pd.isna(r["total_cost"]) else r["total_cost"],
         None if pd.isna(r["sla_breach_rate"]) else r["sla_breach_rate"])
        for _, r in cat.iterrows()
    ])
    print(f"[GOLD] category_summary: {len(cat)} rows")

    # === Gold 2: Monthly Trends ===
    monthly_df = df[df["created_at"].notna()].copy()
    monthly_df["month"] = pd.to_datetime(monthly_df["created_at"]).dt.strftime("%Y-%m")

    monthly = monthly_df.groupby("month").agg(
        ticket_count=("ticket_id", "count"),
        resolved_count=("status", lambda s: (s == "Resolved").sum()),
        avg_resolution_hours=("resolution_hours", "mean"),
        total_cost=("cost", "sum"),
    ).reset_index().sort_values("month")
    monthly["avg_resolution_hours"] = monthly["avg_resolution_hours"].round(2)
    monthly["total_cost"] = monthly["total_cost"].round(2)

    cur.execute("TRUNCATE TABLE gold.monthly_trends")
    execute_values(cur, """
        INSERT INTO gold.monthly_trends
            (month, ticket_count, resolved_count, avg_resolution_hours, total_cost)
        VALUES %s
    """, [
        (r["month"], int(r["ticket_count"]), int(r["resolved_count"]),
         None if pd.isna(r["avg_resolution_hours"]) else r["avg_resolution_hours"],
         None if pd.isna(r["total_cost"]) else r["total_cost"])
        for _, r in monthly.iterrows()
    ])
    print(f"[GOLD] monthly_trends: {len(monthly)} rows")

    conn.commit()
    cur.close()
    conn.close()
    print("[GOLD] Done! All 2 models built.")


if __name__ == "__main__":
    build()
