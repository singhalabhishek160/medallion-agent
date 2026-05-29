"""
Silver Layer - Clean, deduplicate, type-cast, validate.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, upper, lower, to_timestamp,
    unix_timestamp, lit, coalesce, round as spark_round
)
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime, timezone


def get_spark():
    return (
        SparkSession.builder
        .appName("medallion-silver")
        .master("local[*]")
        .getOrCreate()
    )


JDBC_URL = "jdbc:postgresql://localhost:5432/medallion"
JDBC_PROPS = {"user": "pipeline", "password": "pipeline123", "driver": "org.postgresql.Driver"}


# Category normalization mapping, can be expanded with more examples or handled via LLM in future iterations.
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


def normalize_category_udf(cat):
    """Map messy category to canonical value."""
    if not cat or cat.strip() == "":
        return "Other"
    cat_lower = cat.strip().lower()
    if cat_lower in CATEGORY_MAP:
        return CATEGORY_MAP[cat_lower]
    # Partial match
    for key, val in CATEGORY_MAP.items():
        if key in cat_lower:
            return val
    # If it looks like a description (too long), classify by keywords
    if len(cat_lower) > 25:
        for key, val in CATEGORY_MAP.items():
            if len(key) > 3 and key in cat_lower:
                return val
        return "Other"
    return cat.strip().title()


def transform():
    """Read from bronze, clean, write to silver."""
    spark = get_spark()
    
    print("[SILVER] Reading from bronze.raw_tickets...")
    bronze_df = spark.read.jdbc(JDBC_URL, "bronze.raw_tickets", properties=JDBC_PROPS)
    print(f"[SILVER] Read {bronze_df.count()} rows from bronze")
    
    # Deduplicate by ticket_id (keep first by _id)
    df = bronze_df.dropDuplicates(["ticket_id"])
    print(f"[SILVER] After dedup: {df.count()} unique tickets")
    
    # Apply category normalization using native Spark SQL (avoids UDF serialization issues)
    cat_col = lower(trim(col("category")))
    df = df.withColumn("category",
        when(cat_col.isin("power issue", "power", "electrical", "electric"), "Power")
        .when(cat_col.isin("fire safety", "fire/safety", "fire"), "Fire Safety")
        .when(cat_col.isin("a/c", "hvac", "heating", "cooling", "air conditioning", "cold"), "HVAC")
        .when(cat_col.isin("plumbing", "water", "leak", "pipe burst"), "Plumbing")
        .when(cat_col.isin("elevator", "lift"), "Elevator")
        .when(cat_col.isin("pest", "pest control"), "Pest Control")
        .when(cat_col.isin("security", "access"), "Security")
        .when(cat_col.isin("cleaning", "janitorial"), "Cleaning")
        .when(cat_col.isin("structural", "roof", "roofing"), "Structural")
        .when(cat_col.isin("general", "maintenance"), "General Maintenance")
        .when(cat_col.isin("it support", "network"), "IT Support")
        .when(cat_col.isin("other", "???", "null", "unknown", "test", "asdf"), "Other")
        .when(cat_col.isNull() | (trim(col("category")) == ""), "Other")
        .when(cat_col.contains("power") | cat_col.contains("electric"), "Power")
        .when(cat_col.contains("fire") | cat_col.contains("safety"), "Fire Safety")
        .when(cat_col.contains("hvac") | cat_col.contains("cool") | cat_col.contains("heat"), "HVAC")
        .when(cat_col.contains("plumb") | cat_col.contains("water") | cat_col.contains("leak"), "Plumbing")
        .when(cat_col.contains("elev") | cat_col.contains("lift"), "Elevator")
        .when(cat_col.contains("pest"), "Pest Control")
        .when(cat_col.contains("secur") | cat_col.contains("access"), "Security")
        .when(cat_col.contains("clean") | cat_col.contains("janit"), "Cleaning")
        .when(cat_col.contains("struct") | cat_col.contains("roof"), "Structural")
        .when(cat_col.contains("maint"), "General Maintenance")
        .when(cat_col.contains("network") | cat_col.contains("it "), "IT Support")
        .otherwise("Other")
    )
    
    # Priority normalization
    df = df.withColumn("priority",
        when(lower(trim(col("priority"))).isin("low"), "LOW")
        .when(lower(trim(col("priority"))).isin("medium", "med"), "MEDIUM")
        .when(lower(trim(col("priority"))).isin("high"), "HIGH")
        .when(lower(trim(col("priority"))).isin("critical", "urgent", "crit"), "CRITICAL")
        .otherwise("UNKNOWN")
    )
    
    # Status normalization
    df = df.withColumn("status",
        when(lower(trim(col("status"))).contains("open"), "Open")
        .when(lower(trim(col("status"))).contains("resolved"), "Resolved")
        .when(lower(trim(col("status"))).contains("closed"), "Closed")
        .when(lower(trim(col("status"))).contains("progress"), "In Progress")
        .when(lower(trim(col("status"))).contains("pending"), "Pending")
        .when(lower(trim(col("status"))).contains("escalat"), "Escalated")
        .otherwise("Unknown")
    )
    
    # Parse dates - try multiple formats
    df = df.withColumn("created_at",
        coalesce(
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("created_at"), "dd-MMM-yyyy HH:mm"),
            to_timestamp(col("created_at"), "yyyy-MM-dd"),
        )
    )
    df = df.withColumn("resolved_at",
        coalesce(
            to_timestamp(col("resolved_at"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("resolved_at"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("resolved_at"), "dd-MMM-yyyy HH:mm"),
            to_timestamp(col("resolved_at"), "yyyy-MM-dd"),
        )
    )
    
    # Parse cost to double
    df = df.withColumn("cost",
        regexp_replace(col("cost"), "[^0-9.]", "").cast(DoubleType())
    )
    
    # Parse sla_hours to integer
    df = df.withColumn("sla_hours",
        regexp_replace(col("sla_hours"), "[^0-9]", "").cast(IntegerType())
    )
    
    # Calculate resolution hours
    df = df.withColumn("resolution_hours",
        when(col("resolved_at").isNotNull() & col("created_at").isNotNull(),
             spark_round(
                 (unix_timestamp(col("resolved_at")) - unix_timestamp(col("created_at"))) / 3600.0,
                 2
             )
        ).otherwise(lit(None))
    )
    
    # Flag SLA breaches (resolution_hours > sla_hours)
    df = df.withColumn("is_sla_breached",
        when(col("resolution_hours").isNotNull() & col("sla_hours").isNotNull(),
             col("resolution_hours") > col("sla_hours")
        ).otherwise(lit(None))
    )
    
    # Select final silver columns
    silver_df = df.select(
        "ticket_id", "created_at", "resolved_at", "category", "priority",
        "status", "building", "description", "submitted_by", "assigned_to",
        "resolution_notes", "cost", "sla_hours", "resolution_hours", "is_sla_breached",
        col("_row_hash").alias("_source_hash")
    )
    
    # Write to silver (overwrite for idempotency)
    print(f"[SILVER] Writing {silver_df.count()} rows to silver.tickets...")
    silver_df.write.jdbc(
        url=JDBC_URL,
        table="silver.tickets",
        mode="overwrite",
        properties=JDBC_PROPS
    )
    
    print("[SILVER] Done!")
    spark.stop()


if __name__ == "__main__":
    transform()
