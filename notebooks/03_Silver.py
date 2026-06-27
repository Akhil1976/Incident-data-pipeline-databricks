# Databricks notebook source
# ============================================================
# NOTEBOOK 3 - SILVER LAYER (FIXED)
# ============================================================

# ----- WIDGETS -----
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "realproject_schema")

# ----- GET WIDGET VALUES -----
catalog_name     = dbutils.widgets.get("catalog_name")
schema_name      = dbutils.widgets.get("schema_name")
bronze_table     = f"{catalog_name}.{schema_name}.bronze_incidents"
silver_table     = f"{catalog_name}.{schema_name}.silver_incidents"
quarantine_table = f"{catalog_name}.{schema_name}.quarantine_incidents"

print(f"Catalog          : {catalog_name}")
print(f"Schema           : {schema_name}")
print(f"Bronze Table     : {bronze_table}")
print(f"Silver Table     : {silver_table}")
print(f"Quarantine Table : {quarantine_table}")

# ----- STEP 1: SET CATALOG & SCHEMA -----
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"\n✅ Using: {catalog_name}.{schema_name}")

# ----- STEP 2: READ BRONZE TABLE -----
from pyspark.sql.functions import col, when, lit, to_timestamp, row_number, current_timestamp
from pyspark.sql import Window
import pyspark.sql.functions as F

df_bronze = spark.table(bronze_table)
total_bronze_rows = df_bronze.count()

print(f"\n✅ Bronze table loaded!")
print(f"📊 Total Bronze Rows : {total_bronze_rows}")

# ----- STEP 3: SEPARATE DUPLICATES → QUARANTINE -----
# Use row_number to identify duplicate rows
window = Window.partitionBy(df_bronze.columns).orderBy(lit(1))
df_with_rownum = df_bronze.withColumn("row_num", row_number().over(window))

# First occurrence = keep, rest = duplicates
df_duplicates    = df_with_rownum.filter(col("row_num") > 1).drop("row_num")
df_no_duplicates = df_with_rownum.filter(col("row_num") == 1).drop("row_num")
duplicate_count  = df_duplicates.count()

# Add reject reason to duplicates
df_duplicates_quarantine = df_duplicates.withColumn(
    "reject_reason", lit("duplicate row")
)

print(f"\n✅ Duplicate Check:")
print(f"   Duplicate Rows Found : {duplicate_count}")

# ----- STEP 4: SEPARATE CRITICAL NULLS → QUARANTINE -----
null_condition = (
    col("Number").isNull()   |
    col("Category").isNull() |
    col("Priority").isNull() |
    col("State").isNull()
)

df_critical_nulls    = df_no_duplicates.filter(null_condition)
df_no_critical_nulls = df_no_duplicates.filter(~null_condition)
critical_null_count  = df_critical_nulls.count()

# Add reject reason to critical nulls
df_nulls_quarantine = df_critical_nulls.withColumn(
    "reject_reason",
    when(col("Number").isNull(),    lit("null in Number"))
    .when(col("Category").isNull(), lit("null in Category"))
    .when(col("Priority").isNull(), lit("null in Priority"))
    .when(col("State").isNull(),    lit("null in State"))
    .otherwise(lit("null in critical column"))
)

print(f"\n✅ Critical Null Check:")
print(f"   Critical Null Rows Found : {critical_null_count}")

# ----- STEP 5: COMBINE QUARANTINE -----
df_quarantine = df_duplicates_quarantine.union(df_nulls_quarantine) \
    .withColumn("quarantine_timestamp", current_timestamp())

total_quarantine = df_quarantine.count()
print(f"\n✅ Total Quarantine Rows : {total_quarantine}")

# ----- STEP 6: FILL NON-CRITICAL NULLS -----
df_clean = df_no_critical_nulls \
    .fillna({
        "Service"             : "Unknown",
        "Service_Family"      : "Unknown",
        "Service_Subcategory" : "Unknown",
        "Assigned_To"         : "Unassigned",
        "Assignment_Group"    : "Unassigned",
        "User_ID"             : "Unknown",
        "Channel"             : "Unknown",
        "Updated_By"          : "Unknown",
        "Caller"              : "Unknown",
        "short_description"   : "No Description"
    })

print(f"\n✅ Non-critical nulls filled with default values!")

# ----- STEP 7: FIX DATA TYPES -----
df_silver = df_clean \
    .withColumn("Opened",  to_timestamp(col("Opened"),  "dd-MM-yyyy HH:mm")) \
    .withColumn("Updated", to_timestamp(col("Updated"), "dd-MM-yyyy HH:mm"))

print(f"\n✅ Data types fixed!")
print(f"   Opened  → Timestamp")
print(f"   Updated → Timestamp")

# ----- STEP 8: SAVE QUARANTINE TABLE -----
df_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(quarantine_table)

print(f"\n✅ Quarantine table saved: {quarantine_table}")

# ----- STEP 9: SAVE SILVER TABLE -----
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(silver_table)

print(f"\n✅ Silver table saved: {silver_table}")

# ----- STEP 10: VERIFY -----
silver_count     = spark.table(silver_table).count()
quarantine_count = spark.table(quarantine_table).count()

print(f"\n{'='*50}")
print(f"=== SILVER LAYER VERIFICATION ===")
print(f"{'='*50}")
print(f"Bronze Rows         : {total_bronze_rows}")
print(f"Silver Rows         : {silver_count}")
print(f"Quarantine Rows     : {quarantine_count}")
print(f"Silver + Quarantine : {silver_count + quarantine_count}")
print(f"{'='*50}")

if silver_count + quarantine_count == total_bronze_rows:
    print(f"✅ Row count MATCHED! No data lost!")
else:
    print(f"⚠️  Row count MISMATCH! Check the pipeline!")

print(f"\n✅ SILVER LAYER COMPLETE!")
