"""
Data Quality Agent - Uses Meta Llama 3.3 70B (via Groq) to profile and audit data.

This agent:
1. Profiles the bronze layer — distributions, null rates, outliers, cardinality
2. Proposes cleaning/validation rules in natural language
3. Generates the SQL AND Python/PySpark to implement each rule
4. Explains WHY each rule matters for downstream analytics

Uses Groq (Llama 3.3 70B) — free tier available at console.groq.com.
"""
import os
import json
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Groq uses OpenAI-compatible API
client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)
MODEL = "llama-3.3-70b-versatile"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "medallion"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}


def get_sample_data(n=50):
    """Pull a random sample of rows from bronze for the LLM to analyze."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM bronze.raw_tickets ORDER BY RANDOM() LIMIT {n}")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]


def get_profiling_stats():
    """Generate comprehensive column-level profiling stats via SQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM bronze.raw_tickets")
    total = cur.fetchone()[0]

    cols = ["ticket_id", "created_at", "resolved_at", "category", "priority",
            "status", "building", "description", "submitted_by", "assigned_to",
            "resolution_notes", "cost", "sla_hours"]

    stats = {"total_rows": total, "columns": {}}

    for c in cols:
        col_stats = {}
        # Null / empty rate
        cur.execute(f"SELECT COUNT(*) FROM bronze.raw_tickets WHERE {c} IS NULL OR TRIM({c}) = ''")
        null_count = cur.fetchone()[0]
        col_stats["null_count"] = null_count
        col_stats["null_pct"] = round(null_count / total * 100, 1)

        # Cardinality (distinct non-null values)
        cur.execute(f"SELECT COUNT(DISTINCT {c}) FROM bronze.raw_tickets WHERE {c} IS NOT NULL AND TRIM({c}) != ''")
        col_stats["distinct_values"] = cur.fetchone()[0]

        # Top 5 most frequent values (distribution)
        cur.execute(f"""
            SELECT {c}, COUNT(*) as cnt
            FROM bronze.raw_tickets
            WHERE {c} IS NOT NULL AND TRIM({c}) != ''
            GROUP BY {c} ORDER BY cnt DESC LIMIT 5
        """)
        col_stats["top_values"] = [{"value": str(r[0])[:80], "count": r[1]} for r in cur.fetchall()]

        # Min/max/avg length (detect outliers in text fields)
        cur.execute(f"""
            SELECT MIN(LENGTH({c})), MAX(LENGTH({c})), AVG(LENGTH({c}))::int
            FROM bronze.raw_tickets WHERE {c} IS NOT NULL AND TRIM({c}) != ''
        """)
        lengths = cur.fetchone()
        col_stats["min_length"] = lengths[0]
        col_stats["max_length"] = lengths[1]
        col_stats["avg_length"] = lengths[2]

        stats["columns"][c] = col_stats

    cur.close()
    conn.close()
    return stats


def run_agent():
    """Run the Data Quality Agent."""
    print("=" * 60)
    print("DATA QUALITY AGENT")
    print("=" * 60)

    # Step 1: Profile data
    print("\n[DQ Agent] Profiling bronze layer...")
    stats = get_profiling_stats()
    print(f"[DQ Agent] Total rows: {stats['total_rows']}")
    print(f"[DQ Agent] Columns profiled: {len(stats['columns'])}")

    # Step 2: Sample rows for LLM context
    print("[DQ Agent] Sampling 30 rows for LLM analysis...")
    sample = get_sample_data(30)

    # Step 3: Build prompt
    system_prompt = """You are a senior data engineer performing a data quality audit on raw operational support ticket data ingested into a bronze layer (PostgreSQL).

You will receive:
1. Column-level profiling stats (null rates, cardinality, value distributions, string lengths)
2. A sample of 30 raw rows

Produce a comprehensive DATA QUALITY REPORT with these exact sections:

## 1. Executive Summary
Brief overview of data health — what's good, what's critical.

## 2. Column-by-Column Analysis
For EACH problematic column:
- **Issue**: Describe the specific problem with examples
- **Business Impact**: WHY this matters (SLA tracking, cost reporting, trend analysis, etc.)
- **Examples**: Show 2-3 specific bad values from the data
- **Cleaning Rule** (natural language): What transformation to apply
- **SQL Implementation**: PostgreSQL SQL to fix it in the silver layer
- **PySpark Implementation**: PySpark code alternative

## 3. Data Integrity Issues
- Duplicate detection strategy
- Impossible/contradictory values
- Format inconsistencies within same column

## 4. Priority Ranking
Rank all issues: Critical > High > Medium > Low. Justify each ranking.

Be specific — use actual values from the sample. Every rule must explain WHY it matters for analytics."""

    user_prompt = f"""## Column Profiling Statistics
```json
{json.dumps(stats, indent=2, default=str)}
```

## Sample Data (30 rows)
```json
{json.dumps(sample[:20], indent=1, default=str)}
```

Produce the complete data quality report with SQL and PySpark implementations for each cleaning rule."""

    # Step 4: Call LLM
    print(f"[DQ Agent] Calling {MODEL} via Groq...")
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_tokens=4000,
    )

    report = response.choices[0].message.content
    tokens = response.usage.total_tokens

    # Step 5: Save outputs
    os.makedirs("output", exist_ok=True)
    with open("output/data_quality_report.md", "w", encoding="utf-8") as f:
        f.write(report)

    with open("output/profiling_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, default=str)

    print(f"\n[DQ Agent] Analysis complete! (tokens used: {tokens})")
    print(f"[DQ Agent] Report saved to: output/data_quality_report.md")
    print(f"[DQ Agent] Profiling stats: output/profiling_stats.json")
    print("\n" + "=" * 60)
    print("REPORT PREVIEW:")
    print("=" * 60)
    print(report[:2000])
    if len(report) > 2000:
        print(f"\n... ({len(report) - 2000} more chars in full report)")


if __name__ == "__main__":
    run_agent()
