"""
Gold Layer - Business-ready aggregations.

Models:
1. category_summary - Volume, avg resolution time, cost, SLA breach rate per category
2. building_summary - Per-building scorecard
3. monthly_trends - Time-series for capacity planning
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, lit,
    date_format, round as spark_round, first
)
from datetime import datetime, timezone


def get_spark():
    return (
        SparkSession.builder
        .appName("medallion-gold")
        .master("local[*]")
        .getOrCreate()
    )


JDBC_URL = "jdbc:postgresql://localhost:5432/medallion"
JDBC_PROPS = {"user": "pipeline", "password": "pipeline123", "driver": "org.postgresql.Driver"}


def build():
    """Build gold aggregations from silver layer."""
    spark = get_spark()
    
    print("[GOLD] Reading from silver.tickets...")
    df = spark.read.jdbc(JDBC_URL, "silver.tickets", properties=JDBC_PROPS)
    row_count = df.count()
    print(f"[GOLD] Read {row_count} rows")
    
    if row_count == 0:
        print("[GOLD] No silver data. Skipping.")
        spark.stop()
        return
    
    now = datetime.now(timezone.utc).isoformat()
    
    # === Gold 1: Category Summary ===
    cat_df = df.groupBy("category").agg(
        count("ticket_id").alias("ticket_count"),
        spark_round(avg("resolution_hours"), 2).alias("avg_resolution_hours"),
        spark_round(spark_sum("cost"), 2).alias("total_cost"),
        spark_round(
            spark_sum(when(col("is_sla_breached") == True, 1).otherwise(0)) /
            spark_sum(when(col("is_sla_breached").isNotNull(), 1).otherwise(0)),
            4
        ).alias("sla_breach_rate")
    ).withColumn("_refreshed_at", lit(now))
    
    print(f"[GOLD] category_summary: {cat_df.count()} rows")
    cat_df.write.jdbc(JDBC_URL, "gold.category_summary", mode="overwrite", properties=JDBC_PROPS)
    
    # === Gold 2: Building Summary ===
    # Get top category per building
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    bld_cat = df.groupBy("building", "category").agg(count("*").alias("cnt"))
    w = Window.partitionBy("building").orderBy(desc("cnt"))
    top_cat = bld_cat.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).select("building", col("category").alias("top_category"))
    
    bld_df = df.groupBy("building").agg(
        count("ticket_id").alias("ticket_count"),
        spark_sum(when(col("status") == "Open", 1).otherwise(0)).alias("open_tickets"),
        spark_sum(when(col("status") == "Resolved", 1).otherwise(0)).alias("resolved_tickets"),
        spark_round(avg("cost"), 2).alias("avg_cost"),
    ).join(top_cat, "building", "left").withColumn("_refreshed_at", lit(now))
    
    print(f"[GOLD] building_summary: {bld_df.count()} rows")
    bld_df.write.jdbc(JDBC_URL, "gold.building_summary", mode="overwrite", properties=JDBC_PROPS)
    
    # === Gold 3: Monthly Trends ===
    monthly_df = (
        df.filter(col("created_at").isNotNull())
        .withColumn("month", date_format(col("created_at"), "yyyy-MM"))
        .groupBy("month").agg(
            count("ticket_id").alias("ticket_count"),
            spark_sum(when(col("status") == "Resolved", 1).otherwise(0)).alias("resolved_count"),
            spark_round(avg("resolution_hours"), 2).alias("avg_resolution_hours"),
            spark_round(spark_sum("cost"), 2).alias("total_cost"),
        ).withColumn("_refreshed_at", lit(now))
        .orderBy("month")
    )
    
    print(f"[GOLD] monthly_trends: {monthly_df.count()} rows")
    monthly_df.write.jdbc(JDBC_URL, "gold.monthly_trends", mode="overwrite", properties=JDBC_PROPS)
    
    print("[GOLD] Done! All 3 models built.")
    spark.stop()


if __name__ == "__main__":
    build()
