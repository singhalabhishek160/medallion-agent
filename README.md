# Medallion Pipeline with AI Agents (POC)

A simple bronze → silver → gold data pipeline using **PySpark** and **PostgreSQL**, with two **LLM-powered agents** for data quality analysis and semantic classification.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│             │     │             │     │             │
│ Raw CSV     │     │ Cleaned     │     │ Aggregated  │
│ All TEXT    │     │ Typed       │     │ Models      │
│ + lineage   │     │ Deduped     │     │             │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │  AI AGENTS  │
                    │             │
                    │ 1. DQ Agent │──▶ output/data_quality_report.md
                    │ 2. Semantic │──▶ silver.ticket_enrichments
                    └─────────────┘

Storage: PostgreSQL (schemas: bronze / silver / gold)
Engine:  PySpark (local mode)
AI:      OpenAI GPT-4o-mini
```

---

## Quick Start

### 1. Start PostgreSQL

```bash
docker-compose up -d
```

### 2. Install Python deps

```bash
pip install -r requirements.txt
```

### 3. Run the full pipeline

```bash
python run.py
```

Or run stages individually:
```bash
python run.py bronze    # Ingest raw CSV
python run.py silver    # Clean and transform
python run.py gold      # Build aggregations
python run.py agents    # Run AI agents only
python run.py pipeline  # bronze + silver + gold (no agents)
```

---

## Pipeline Layers

### Bronze (`bronze.raw_tickets`)
- Ingests `data/raw_tickets.csv` as-is, all columns as TEXT
- Adds: `_source_file`, `_ingested_at`, `_row_hash`
- Idempotent: skips rows with existing hash

### Silver (`silver.tickets`)
- Parses dates (multiple formats → timestamps)
- Normalizes categories (114 messy values → ~15 canonical)
- Standardizes priority (LOW/MEDIUM/HIGH/CRITICAL/UNKNOWN)
- Parses cost and sla_hours to numeric
- Deduplicates by ticket_id
- Computes `resolution_hours` and `is_sla_breached`

### Gold (3 models)
| Table | Purpose |
|-------|---------|
| `gold.category_summary` | Ticket volume, avg resolution time, cost, SLA breach rate per category |
| `gold.building_summary` | Per-building scorecard with top issue category |
| `gold.monthly_trends` | Monthly time-series for capacity planning |

---

## AI Agents

### Agent 1: Data Quality Agent (`agents/data_quality_agent.py`)

**What it does:** Samples 50 rows from bronze + column stats, sends to GPT-4o-mini, gets back a structured quality report with specific cleaning rules and business justifications.

**Sample output:**
```
- Column: priority
  Issue: 10.5% null rate, 21 distinct values including "CRIT", "urgent", "med"
  Impact: Can't properly triage or track SLA compliance
  Rule: Map to enum {LOW, MEDIUM, HIGH, CRITICAL}, default UNKNOWN
```

**Honest take:** This saved ~20 minutes of manual EDA. The LLM spots semantic issues (e.g., "CRIT" = "CRITICAL") that pure stats miss. Cost: ~$0.01 per run.

### Agent 2: Semantic Classification Agent (`agents/semantic_agent.py`)

**What it does:** Reads ticket descriptions from silver, sends batches of 20 to GPT-4o-mini, extracts urgency signal, affected system, and specific location. Writes to `silver.ticket_enrichments`.

**Sample input/output:**
```
Input:  "Breaker keeps tripping in server room 391. Critical — affects production."
Output: urgency=critical, system=electrical breaker, location=server room 391
```

**Honest take:** The LLM is clearly better than regex for urgency detection ("affects production" → critical). Location extraction also strong. Processes 200 tickets for ~$0.03. At 10k rows, would cost ~$0.15 total.

---

## What Changes at 100x Scale

| Aspect | Current (10k) | At 1M+ rows |
|--------|---------------|-------------|
| Spark | Local mode | Cluster (EMR/Databricks) |
| Bronze | Full dedup check | Partition by date, incremental watermark |
| Silver | Full overwrite | Merge/upsert on new rows only |
| Gold | Full rebuild | Materialized views or incremental aggregation |
| DQ Agent | Sample 50 rows | Sample 0.1%, run on schedule |
| Semantic Agent | Process 200 | Batch async API, embed-cluster-then-classify |
| Storage | Single PostgreSQL | Partitioned tables or Delta Lake |
| Orchestration | Python script | Airflow/Dagster with retries |

---

## Project Structure

```
├── data/raw_tickets.csv       # Source data (don't modify)
├── pipeline/
│   ├── bronze.py              # Raw ingestion
│   ├── silver.py              # Cleaning & transformation
│   └── gold.py                # Aggregation models
├── agents/
│   ├── data_quality_agent.py  # LLM-powered profiling
│   └── semantic_agent.py      # LLM-powered enrichment
├── run.py                     # Main entry point
├── docker-compose.yml         # PostgreSQL
├── init.sql                   # Schema DDL
├── requirements.txt
├── .env                       # API keys & config
└── README.md
```

---

## Configuration (`.env`)

```
OPENAI_API_KEY=sk-...
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=medallion
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline123
```
