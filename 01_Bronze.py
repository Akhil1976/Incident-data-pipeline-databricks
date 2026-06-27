# Databricks notebook source
# ============================================================
# NOTEBOOK 1 - BRONZE LAYER
# ============================================================

# ----- WIDGETS -----
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "realproject_schema")
dbutils.widgets.text("file_name", "incident_with_duplicates.csv")

# ----- GET WIDGET VALUES -----
catalog_name = dbutils.widgets.get("catalog_name")
schema_name  = dbutils.widgets.get("schema_name")
file_name    = dbutils.widgets.get("file_name")

# ----- BUILD PATHS -----
volume_path  = f"/Volumes/{catalog_name}/{schema_name}/realproject_volume"
file_path    = f"{volume_path}/{file_name}"
bronze_table = f"{catalog_name}.{schema_name}.bronze_incidents"

print(f"Catalog      : {catalog_name}")
print(f"Schema       : {schema_name}")
print(f"File Path    : {file_path}")
print(f"Bronze Table : {bronze_table}")

# ----- STEP 1: SET CATALOG & SCHEMA -----
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"\n✅ Using: {catalog_name}.{schema_name}")

# ----- STEP 2: READ RAW CSV -----
from pyspark.sql.functions import current_timestamp, lit

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(file_path)

print(f"\n✅ CSV Loaded Successfully!")
print(f"📊 Total Rows    : {df_raw.count()}")
print(f"📊 Total Columns : {len(df_raw.columns)}")

# ----- STEP 2.5: FIX COLUMN NAMES -----
df_raw = df_raw \
    .withColumnRenamed("User ID", "User_ID") \
    .withColumnRenamed("Assignment group", "Assignment_Group") \
    .withColumnRenamed("Assigned to", "Assigned_To") \
    .withColumnRenamed("Service Family", "Service_Family") \
    .withColumnRenamed("Service Subcategory", "Service_Subcategory") \
    .withColumnRenamed("Updated by", "Updated_By")

print(f"\n✅ Column names fixed!")
print(f"📊 Columns: {df_raw.columns}")

# ----- STEP 3: ADD METADATA COLUMNS -----
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file_name", lit(file_name))

print(f"\n✅ Added metadata columns:")
print(f"   • ingestion_timestamp")
print(f"   • source_file_name")
print(f"📊 Final Columns : {len(df_bronze.columns)}")

# ----- STEP 4: SAVE AS DELTA TABLE -----
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(bronze_table)

print(f"\n✅ Bronze table created: {bronze_table}")

# ----- STEP 5: VERIFY -----
df_verify = spark.table(bronze_table)

print(f"\n=== BRONZE TABLE VERIFICATION ===")
print(f"✅ Total Rows    : {df_verify.count()}")
print(f"✅ Total Columns : {len(df_verify.columns)}")
print(f"\n=== SAMPLE DATA (3 rows) ===")
df_verify.show(3, truncate=True)

print(f"\n✅ BRONZE LAYER COMPLETE!")