# Medallion Pipeline with AI Agents

A **bronze → silver → gold** data pipeline over 10,280 facilities support tickets, with two LLM-powered agents for data quality analysis and semantic enrichment. Built with Python, pandas, PostgreSQL, and Llama 3.3 70B via Groq.

---

## How to Run

**Prerequisites:** Docker installed, Groq API key (free at [console.groq.com](https://console.groq.com))

```bash
cp .env.example .env        # add your GROQ_API_KEY
docker compose up --build   # starts PostgreSQL + runs full pipeline
```

That's it. Docker Compose starts PostgreSQL, waits for it to be healthy, then runs bronze → silver → gold → agents automatically. Output files appear in `./output/`.

---

## Architecture

```
  raw_tickets.csv
         │
         ▼
┌─────────────────┐
│     BRONZE      │  All columns TEXT, schema-on-read
│  raw_tickets    │  + _source_file, _ingested_at, _row_hash
└────────┬────────┘  Idempotent: skips rows by hash
         │
         ▼
┌─────────────────┐
│     SILVER      │  Type-cast, deduplicated, validated
│     tickets     │  Dates parsed, cost/SLA numeric,
│  enrichments    │  resolution_hours + is_sla_breached computed
└────────┬────────┘  + AI enrichment fields (Agent 2)
         │
         ▼
┌─────────────────┐
│      GOLD       │  Business-ready aggregations
│ category_summary│  — 49 categories: volume, cost, SLA breach rate
│ monthly_trends  │  — 18 months: volume + resolution trends
└─────────────────┘

         ┌──────────────────────────────────────────┐
         │             AI AGENTS                    │
         │  Bronze layer ──▶ Agent 1 (DQ Report)    │
         │  Silver layer ──▶ Agent 2 (Enrichment)   │
         │  Model: Llama 3.3 70B via Groq (free)    │
         └──────────────────────────────────────────┘
```

## Silver Cleaning Rules

The raw data is messy in predictable ways. Each rule targets a specific problem in the source:

| Column | Problem in raw data | Rule applied | Why it matters |
|--------|--------------------|--------------|----|
| `category` | 49+ free-text variants for ~12 actual types ("a/c", "Air Conditioning", "HVAC", "cooling") | Map to fixed enum via lookup + partial match | Without this, gold aggregations fragment the same issue type across dozens of rows — SLA breach rates become meaningless |
| `priority` | "crit", "CRITICAL", "urgent", "hi", "HIGH" for the same severity | Collapse to `LOW / MEDIUM / HIGH / CRITICAL / UNKNOWN` | Required for consistent SLA triage; UNKNOWN is explicit so it's queryable, not silently dropped |
| `status` | "open", "Open", "OPEN", "in-progress", "In Progress" | Lowercase + keyword match → canonical set | Aggregations like `resolved_count` in gold monthly trends need a stable value |
| `created_at` / `resolved_at` | 5+ date formats: ISO 8601, `dd-MMM-yyyy HH:mm`, plain date, invalid values ("asap", "???") | Try each format in order, NULL unparseable values | `resolution_hours` and `is_sla_breached` can only be computed on proper timestamps. NULL is preferred over dropping the row — the ticket is still useful for volume analytics |
| `cost` | "$1,200", "1200.00", "TBD", "error" | Strip non-numeric chars → float, NULL for non-numeric sentinels | Gold total_cost aggregation requires a numeric column |
| `sla_hours` | "8h", "8 hours", "8" | Strip non-numeric chars → int | Needed to compute `is_sla_breached = resolution_hours > sla_hours` |
| `ticket_id` duplicates | 21 duplicate ticket IDs in source | Keep first occurrence, drop subsequent | Duplicate rows inflate all aggregations; first-seen is chosen because the source has no versioning concept |

---

## Gold Model Justification

Two models were chosen because they answer the two most actionable questions for a facilities operations team:

**`gold.category_summary`** — answers *"where is time and money going?"*
Facilities managers prioritise by category (HVAC, Plumbing, Electrical, etc.). Aggregating ticket count, average resolution time, total cost, and SLA breach rate per category directly surfaces which problem types are overrunning budgets and missing SLAs — the inputs to headcount and vendor contract decisions. Without this rollup, answering "why is our SLA breach rate high?" requires a full table scan every time.

**`gold.monthly_trends`** — answers *"is the situation getting better or worse?"*
A point-in-time category summary is useful but not actionable on its own. Monthly trends expose seasonality (e.g. HVAC spikes in summer), workload growth, and whether resolution times are improving after process changes. This is the table you'd connect to a BI dashboard to track progress over time.

---

## AI Agent Assessment

### Agent 1 — Data Quality Agent

**What it does:** Computes column-level profiling stats (null rates, cardinality, top values, string length distributions) across the full bronze table, samples 30 raw rows, then sends both to the LLM. Returns a structured markdown report: per-column issue analysis, ready-to-run SQL and Python cleaning rules, and a priority ranking with business justifications.

**Sample input → output:**

```
Input stats:  created_at — 12.3% null, distinct formats: 7,
              examples: "2024-10-24 14:32:10", "asap", "00/00/0000"

LLM output:
  Issue:  Mixed date formats + invalid sentinel values
  Impact: SLA breach calculations fail silently for ~1,200 tickets
  SQL fix:
    UPDATE bronze.raw_tickets SET created_at = NULL
    WHERE created_at IN ('asap', '00/00/0000');
  Priority: CRITICAL
```

**My take:** This saved manual time to find the quality issues. The LLM is genuinely useful here because it connects column stats to *business impact* — it noticed that `resolved_at` being 48% null means SLA reporting is unreliable, not just that there are missing values. It also caught semantic issues like `"TBD"` and `"error"` in the `cost` column that pure null-rate stats would miss. 
Limitation: it hallucinates cleaning rules occasionally (e.g., suggesting regex patterns that don't match the actual data), so the SQL output needs review before running in production.

In production it can be added as a monitoring agend which check the data quality, notify human and if hum approves it can write SQL to fix the issue or can modify the pipeline code to include more DQE rules.
---

### Agent 2 — Semantic Classification Agent

**What it does:** Reads free-text ticket descriptions from `silver.tickets`, sends batches of 20 to the LLM, and extracts four structured fields per ticket. Writes results to `silver.ticket_enrichments`. Incremental — already-enriched tickets are skipped on re-run.

| Field | What it extracts |
|-------|-----------------|
| `urgency_signal` | True urgency from text cues, independent of the priority field |
| `affected_system` | Specific equipment/system named in the description |
| `location_detail` | Sub-building location (floor, wing, room) |
| `root_cause_category` | Inferred root cause (equipment_age, human_error, weather, etc.) |

**Sample input → output:**

```
Input:  ticket_id=TKT-3960, priority=<empty>,
        description="Breaker keeps tripping in server room 391.
                     Critical — affects production systems."

Output: urgency_signal=critical,
        affected_system=electrical breaker,
        location_detail=server room 391,
        root_cause_category=overload
```

**My take:** The urgency extraction is where this earns its keep. The priority field was empty or wrong on ~15% of tickets — the LLM correctly inferred `critical` from phrases like "affects production" that a regex would never catch. Location extraction is strong too. Root cause is hit-or-miss: `unknown` was returned for ~40% of tickets because descriptions just don't contain enough signal. If I were taking this further, I'd fine-tune on labelled examples rather than relying on zero-shot for root cause.

---

## What Changes at 100x Scale (1M+ rows, daily incremental loads)

| Aspect | Current | At 1M+ rows / daily incremental |
|--------|---------|----------------------------------|
| **Ingestion** | Full CSV read into memory | Chunked reads or streaming ingest; partition source files by date |
| **Deduplication** | Hash set in memory | Bloom filter or partition-pruned lookup — can't hold 1M hashes in RAM reliably |
| **Silver** | Full table overwrite | Merge/upsert on `ticket_id`; only process rows newer than watermark |
| **Gold** | Full rebuild each run | Incremental aggregation — update only affected partitions |
| **DQ Agent** | Full profile + 30-row sample | Stratified 0.1% sample; run on a schedule, not every pipeline run |
| **Semantic Agent** | Sequential batches of 20 | Async parallel batching; cache embeddings for near-duplicate descriptions |
| **Storage** | Single PostgreSQL instance | Partitioned tables by month; or move gold to a columnar store (Redshift, BigQuery) |
| **Orchestration** | Python script | Airflow or Dagster — DAG with retries, SLA alerts, and backfill support |
| **Schema evolution** | Manual | Schema registry + migration tooling (Flyway/Alembic) |

The biggest redesign would be the silver layer: full overwrites don't scale. At 1M rows you need a proper merge strategy with a watermark, and the gold aggregations need to be incremental or backed by materialized views.

---

## Project Structure

```
medallion-agent/
├── data/
│   └── raw_tickets.csv          # 10,280 raw support tickets (source)
├── pipeline/
│   ├── bronze.py                # Ingest CSV with SHA-256 dedup
│   ├── silver.py                # Clean, type-cast, validate, compute
│   └── gold.py                  # Two aggregation models
├── agents/
│   ├── data_quality_agent.py    # LLM-powered profiling report
│   └── semantic_agent.py        # LLM-powered ticket enrichment
├── output/
│   ├── data_quality_report.md   # Generated after running agents
│   └── profiling_stats.json     # Column-level stats snapshot
├── run.py                       # Entry point — all / pipeline / agents / stage
├── docker-compose.yml           # PostgreSQL + pipeline app
├── Dockerfile                   # python:3.11-slim, no JVM needed
├── init.sql                     # Schema DDL (bronze / silver / gold)
├── requirements.txt
└── .env.example
```

---

## Tech Stack

| Component | Choice |
|-----------|--------|
| Pipeline | Python + pandas |
| Database | PostgreSQL (bronze / silver / gold schemas) |
| LLM | Llama 3.3 70B Versatile via Groq |
| Containers | Docker Compose |
