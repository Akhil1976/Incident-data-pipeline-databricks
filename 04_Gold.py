# Databricks notebook source
# ============================================================
# NOTEBOOK 4 - GOLD LAYER
# Aggregates Silver clean data into summaries
# Creates Gold Delta table for reporting
# ============================================================

# ----- WIDGETS -----
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "realproject_schema")

# ----- GET WIDGET VALUES -----
catalog_name  = dbutils.widgets.get("catalog_name")
schema_name   = dbutils.widgets.get("schema_name")
silver_table  = f"{catalog_name}.{schema_name}.silver_incidents"
gold_table    = f"{catalog_name}.{schema_name}.gold_incidents"

print(f"Catalog       : {catalog_name}")
print(f"Schema        : {schema_name}")
print(f"Silver Table  : {silver_table}")
print(f"Gold Table    : {gold_table}")

# ----- STEP 1: SET CATALOG & SCHEMA -----
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"\n✅ Using: {catalog_name}.{schema_name}")

# ----- STEP 2: READ SILVER TABLE -----
from pyspark.sql.functions import (
    col, count, round, avg, 
    month, year, concat, lit,
    unix_timestamp, current_timestamp
)

df_silver = spark.table(silver_table)
total_silver_rows = df_silver.count()

print(f"\n✅ Silver table loaded!")
print(f"📊 Total Silver Rows : {total_silver_rows}")

# ----- STEP 3: AGGREGATION 1 — INCIDENTS BY PRIORITY -----
df_by_priority = df_silver \
    .groupBy("Priority") \
    .agg(count("*").alias("incident_count")) \
    .orderBy(col("incident_count").desc()) \
    .withColumn("aggregation_type", lit("by_priority")) \
    .withColumn("group_column", col("Priority")) \
    .withColumn("metric", lit("incident_count")) \
    .withColumn("value", col("incident_count").cast("string")) \
    .select("aggregation_type", "group_column", "metric", "value")

print(f"\n✅ Aggregation 1 — Incidents by Priority:")
df_by_priority.show(truncate=False)

# ----- STEP 4: AGGREGATION 2 — INCIDENTS BY CATEGORY -----
df_by_category = df_silver \
    .groupBy("Category") \
    .agg(count("*").alias("incident_count")) \
    .orderBy(col("incident_count").desc()) \
    .withColumn("aggregation_type", lit("by_category")) \
    .withColumn("group_column", col("Category")) \
    .withColumn("metric", lit("incident_count")) \
    .withColumn("value", col("incident_count").cast("string")) \
    .select("aggregation_type", "group_column", "metric", "value")

print(f"\n✅ Aggregation 2 — Incidents by Category:")
df_by_category.show(truncate=False)

# ----- STEP 5: AGGREGATION 3 — INCIDENTS BY STATE -----
df_by_state = df_silver \
    .groupBy("State") \
    .agg(count("*").alias("incident_count")) \
    .orderBy(col("incident_count").desc()) \
    .withColumn("aggregation_type", lit("by_state")) \
    .withColumn("group_column", col("State")) \
    .withColumn("metric", lit("incident_count")) \
    .withColumn("value", col("incident_count").cast("string")) \
    .select("aggregation_type", "group_column", "metric", "value")

print(f"\n✅ Aggregation 3 — Incidents by State:")
df_by_state.show(truncate=False)

# ----- STEP 6: AGGREGATION 4 — INCIDENTS BY MONTH -----
df_by_month = df_silver \
    .filter(col("Opened").isNotNull()) \
    .withColumn("year",  year(col("Opened"))) \
    .withColumn("month", month(col("Opened"))) \
    .withColumn("year_month", concat(col("year"), lit("-"), col("month"))) \
    .groupBy("year_month") \
    .agg(count("*").alias("incident_count")) \
    .orderBy("year_month") \
    .withColumn("aggregation_type", lit("by_month")) \
    .withColumn("group_column", col("year_month")) \
    .withColumn("metric", lit("incident_count")) \
    .withColumn("value", col("incident_count").cast("string")) \
    .select("aggregation_type", "group_column", "metric", "value")

print(f"\n✅ Aggregation 4 — Incidents by Month:")
df_by_month.show(truncate=False)

# ----- STEP 7: AGGREGATION 5 — AVG RESOLUTION TIME -----
df_resolution = df_silver \
    .filter(
        col("Opened").isNotNull() & 
        col("Updated").isNotNull()
    ) \
    .withColumn(
        "resolution_time_hours",
        round(
            (unix_timestamp(col("Updated")) - 
             unix_timestamp(col("Opened"))) / 3600, 2
        )
    ) \
    .filter(col("resolution_time_hours") >= 0) \
    .groupBy("Assignment_Group") \
    .agg(
        round(avg("resolution_time_hours"), 2)
        .alias("avg_resolution_hours")
    ) \
    .orderBy(col("avg_resolution_hours").asc()) \
    .withColumn("aggregation_type", lit("avg_resolution_time")) \
    .withColumn("group_column", col("Assignment_Group")) \
    .withColumn("metric", lit("avg_resolution_hours")) \
    .withColumn("value", col("avg_resolution_hours").cast("string")) \
    .select("aggregation_type", "group_column", "metric", "value")

print(f"\n✅ Aggregation 5 — Avg Resolution Time by Assignment Group:")
df_resolution.show(truncate=False)

# ----- STEP 8: COMBINE ALL AGGREGATIONS -----
df_gold = df_by_priority \
    .union(df_by_category) \
    .union(df_by_state) \
    .union(df_by_month) \
    .union(df_resolution) \
    .withColumn("created_timestamp", current_timestamp())

print(f"\n✅ All aggregations combined!")
print(f"📊 Total Gold Rows : {df_gold.count()}")

# ----- STEP 9: SAVE GOLD TABLE -----
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(gold_table)

print(f"\n✅ Gold table saved: {gold_table}")

# ----- STEP 10: VERIFY -----
df_verify = spark.table(gold_table)

print(f"\n{'='*50}")
print(f"=== GOLD LAYER VERIFICATION ===")
print(f"{'='*50}")
print(f"✅ Total Gold Rows : {df_verify.count()}")
print(f"\n=== SAMPLE DATA ===")
df_verify.show(10, truncate=False)

print(f"\n✅ GOLD LAYER COMPLETE!")