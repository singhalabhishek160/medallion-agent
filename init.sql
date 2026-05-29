-- Create schemas for medallion layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze: raw tickets (all TEXT, no transformation)
CREATE TABLE IF NOT EXISTS bronze.raw_tickets (
    _id SERIAL PRIMARY KEY,
    _source_file TEXT,
    _ingested_at TIMESTAMP DEFAULT NOW(),
    _row_hash TEXT,
    ticket_id TEXT,
    created_at TEXT,
    resolved_at TEXT,
    category TEXT,
    priority TEXT,
    status TEXT,
    building TEXT,
    description TEXT,
    submitted_by TEXT,
    assigned_to TEXT,
    resolution_notes TEXT,
    cost TEXT,
    sla_hours TEXT
);

CREATE INDEX IF NOT EXISTS idx_bronze_hash ON bronze.raw_tickets(_row_hash);

-- Silver: cleaned tickets (properly typed)
CREATE TABLE IF NOT EXISTS silver.tickets (
    ticket_id TEXT PRIMARY KEY,
    created_at TIMESTAMP,
    resolved_at TIMESTAMP,
    category TEXT,
    priority TEXT,
    status TEXT,
    building TEXT,
    description TEXT,
    submitted_by TEXT,
    assigned_to TEXT,
    resolution_notes TEXT,
    cost NUMERIC(12,2),
    sla_hours INTEGER,
    resolution_hours NUMERIC(10,2),
    is_sla_breached BOOLEAN,
    _source_hash TEXT,
    _cleaned_at TIMESTAMP DEFAULT NOW()
);

-- Gold: category summary
CREATE TABLE IF NOT EXISTS gold.category_summary (
    category TEXT PRIMARY KEY,
    ticket_count INTEGER,
    avg_resolution_hours NUMERIC(10,2),
    total_cost NUMERIC(14,2),
    sla_breach_rate NUMERIC(5,4),
    _refreshed_at TIMESTAMP DEFAULT NOW()
);

-- Gold: building summary
CREATE TABLE IF NOT EXISTS gold.building_summary (
    building TEXT PRIMARY KEY,
    ticket_count INTEGER,
    open_tickets INTEGER,
    resolved_tickets INTEGER,
    avg_cost NUMERIC(12,2),
    top_category TEXT,
    _refreshed_at TIMESTAMP DEFAULT NOW()
);

-- Gold: monthly trends
CREATE TABLE IF NOT EXISTS gold.monthly_trends (
    month TEXT PRIMARY KEY,
    ticket_count INTEGER,
    resolved_count INTEGER,
    avg_resolution_hours NUMERIC(10,2),
    total_cost NUMERIC(14,2),
    _refreshed_at TIMESTAMP DEFAULT NOW()
);
