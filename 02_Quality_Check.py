# Databricks notebook source
# ============================================================
# NOTEBOOK 2 - QUALITY CHECK
# Reads Bronze table and generates quality report
# Saves quality report as Delta table
# ============================================================

# ----- WIDGETS -----
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "realproject_schema")

# ----- GET WIDGET VALUES -----
catalog_name    = dbutils.widgets.get("catalog_name")
schema_name     = dbutils.widgets.get("schema_name")
bronze_table    = f"{catalog_name}.{schema_name}.bronze_incidents"
quality_table   = f"{catalog_name}.{schema_name}.quality_report"

print(f"Catalog        : {catalog_name}")
print(f"Schema         : {schema_name}")
print(f"Bronze Table   : {bronze_table}")
print(f"Quality Table  : {quality_table}")

# ----- STEP 1: SET CATALOG & SCHEMA -----
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"\n✅ Using: {catalog_name}.{schema_name}")

# ----- STEP 2: READ BRONZE TABLE -----
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

df_bronze = spark.table(bronze_table)
total_rows = df_bronze.count()

print(f"\n✅ Bronze table loaded!")

# ----- STEP 3: RECORD COUNT -----
print(f"\n{'='*50}")
print(f"📊 RECORD COUNT")
print(f"{'='*50}")
print(f"Total Rows in Bronze : {total_rows}")

# ----- STEP 4: NULL CHECK -----
print(f"\n{'='*50}")
print(f"🔍 NULL CHECK PER COLUMN")
print(f"{'='*50}")

# Store null results for report table
null_results = []

for col_name in df_bronze.columns:
    null_count  = df_bronze.filter(col(col_name).isNull()).count()
    percentage  = round((null_count / total_rows) * 100, 2)
    status      = "WARNING" if null_count > 0 else "PASS"

    # Store for report table
    null_results.append((
        "NULL_CHECK",
        col_name,
        str(null_count),
        f"{percentage}%",
        status
    ))

    if null_count > 0:
        print(f"⚠️  {col_name:30s} : {null_count} nulls ({percentage}%)")
    else:
        print(f"✅ {col_name:30s} : No nulls")

# ----- STEP 5: DUPLICATE CHECK -----
print(f"\n{'='*50}")
print(f"👥 DUPLICATE CHECK")
print(f"{'='*50}")

unique_rows    = df_bronze.dropDuplicates().count()
duplicate_rows = total_rows - unique_rows
dup_status     = "WARNING" if duplicate_rows > 0 else "PASS"

print(f"Total Rows     : {total_rows}")
print(f"Unique Rows    : {unique_rows}")
print(f"Duplicate Rows : {duplicate_rows}")

# ----- STEP 6: CRITICAL COLUMNS CHECK -----
print(f"\n{'='*50}")
print(f"🚨 CRITICAL COLUMNS CHECK")
print(f"{'='*50}")

critical_columns = ["Number", "Category", "Priority", "State"]
critical_issues  = 0

for col_name in critical_columns:
    null_count  = df_bronze.filter(col(col_name).isNull()).count()
    status      = "FAIL" if null_count > 0 else "PASS"

    if null_count > 0:
        print(f"❌ {col_name:20s} : {null_count} nulls — WILL BE QUARANTINED")
        critical_issues += 1
    else:
        print(f"✅ {col_name:20s} : No nulls — CLEAN")

# ----- STEP 7: BUILD QUALITY REPORT -----
print(f"\n{'='*50}")
print(f"📋 BUILDING QUALITY REPORT TABLE")
print(f"{'='*50}")

# Create report rows
report_rows = []

# Record count row
report_rows.append((
    "RECORD_COUNT",
    "total_rows",
    str(total_rows),
    "100%",
    "INFO"
))

# Duplicate row
report_rows.append((
    "DUPLICATE_CHECK",
    "duplicate_rows",
    str(duplicate_rows),
    f"{round((duplicate_rows/total_rows)*100, 2)}%",
    dup_status
))

# Critical columns summary
report_rows.append((
    "CRITICAL_NULL_CHECK",
    "critical_columns_with_nulls",
    str(critical_issues),
    f"{critical_issues}/{len(critical_columns)} columns affected",
    "FAIL" if critical_issues > 0 else "PASS"
))

# Add all null check results
report_rows.extend(null_results)

# Create DataFrame
schema = StructType([
    StructField("check_type",   StringType(), True),
    StructField("column_name",  StringType(), True),
    StructField("value",        StringType(), True),
    StructField("percentage",   StringType(), True),
    StructField("status",       StringType(), True),
])

df_quality_report = spark.createDataFrame(report_rows, schema) \
    .withColumn("report_timestamp", current_timestamp())

# ----- STEP 8: SAVE QUALITY REPORT TABLE -----
df_quality_report.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(quality_table)

print(f"✅ Quality report saved: {quality_table}")

# ----- STEP 9: FINAL DECISION -----
print(f"\n{'='*50}")
print(f"📋 QUALITY REPORT SUMMARY")
print(f"{'='*50}")
print(f"Total Rows           : {total_rows}")
print(f"Duplicate Rows       : {duplicate_rows}")
print(f"Critical Null Issues : {critical_issues}")
print(f"Non-Critical Nulls   : Will be filled with defaults in Silver")

print(f"\n{'='*50}")
print(f"=== DECISION ===")
print(f"{'='*50}")

if critical_issues == 0:
    print(f"✅ Critical columns CLEAN!")
    print(f"✅ Quality Check PASSED — Safe to proceed to Silver!")
else:
    print(f"⚠️  Critical null issues found!")
    print(f"✅ Proceed to Silver — bad rows will be quarantined!")

if duplicate_rows > 0:
    print(f"⚠️  {duplicate_rows} duplicates found — will be quarantined in Silver!")

print(f"\n✅ QUALITY CHECK COMPLETE!")
print(f"✅ Quality report saved to: {quality_table}")