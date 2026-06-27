# Databricks notebook source
# ============================================================
# NOTEBOOK 5 - DELTA LAKE FEATURES
# Demonstrates:
# 1. Time Travel
# 2. Optimize
# 3. Vacuum
# 4. Change Data Feed (CDF)
# ============================================================

# ----- WIDGETS -----
dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("schema_name", "realproject_schema")

# ----- GET WIDGET VALUES -----
catalog_name    = dbutils.widgets.get("catalog_name")
schema_name     = dbutils.widgets.get("schema_name")
silver_table    = f"{catalog_name}.{schema_name}.silver_incidents"
gold_table      = f"{catalog_name}.{schema_name}.gold_incidents"
bronze_table    = f"{catalog_name}.{schema_name}.bronze_incidents"

print(f"Catalog      : {catalog_name}")
print(f"Schema       : {schema_name}")
print(f"Silver Table : {silver_table}")
print(f"Gold Table   : {gold_table}")

# ----- SET CATALOG & SCHEMA -----
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")
print(f"\n✅ Using: {catalog_name}.{schema_name}")

# ============================================================
# FEATURE 1 - TIME TRAVEL
# See previous versions of your Delta table
# Like Ctrl+Z for your data!
# ============================================================

print("="*50)
print("⏰ FEATURE 1 — TIME TRAVEL")
print("="*50)

# Step 1: Check version history
print("\n📋 Version History of Silver Table:")
df_history = spark.sql(f"DESCRIBE HISTORY {silver_table}")
df_history.select("version", "timestamp", "operation") \
    .show(5, truncate=False)

# Step 2: Get the earliest AVAILABLE version (not necessarily 0)
earliest_version = df_history.selectExpr("min(version)").collect()[0][0]
latest_version    = df_history.selectExpr("max(version)").collect()[0][0]

print(f"\n✅ Earliest available version : {earliest_version}")
print(f"✅ Latest version              : {latest_version}")

# Step 3: Try reading the earliest available version safely
try:
    df_old_version = spark.read.format("delta") \
        .option("versionAsOf", earliest_version) \
        .table(silver_table)
    print(f"\n📖 Row count at version {earliest_version} : {df_old_version.count()}")
except Exception as e:
    print(f"\n⚠️  Could not time travel to version {earliest_version}")
    print(f"   Reason: Older file versions were removed by VACUUM (expected Delta behavior)")

# Step 4: Read latest version
df_latest = spark.table(silver_table)
print(f"\n📖 Row count at latest version : {df_latest.count()}")

print(f"\n✅ TIME TRAVEL COMPLETE!")

# ============================================================
# FEATURE 2 - OPTIMIZE
# Compacts small files into larger ones
# Makes queries faster!
# ============================================================

print("="*50)
print("⚡ FEATURE 2 — OPTIMIZE")
print("="*50)

# Step 1: Check table details before optimize
print("\n📋 Silver Table Details BEFORE Optimize:")
spark.sql(f"DESCRIBE DETAIL {silver_table}") \
    .select("numFiles", "sizeInBytes") \
    .show(truncate=False)

# Step 2: Run Optimize on Silver table
print("\n⚡ Running OPTIMIZE on Silver table...")
spark.sql(f"OPTIMIZE {silver_table}")
print(f"✅ Silver table optimized!")

# Step 3: Run Optimize on Gold table
print("\n⚡ Running OPTIMIZE on Gold table...")
spark.sql(f"OPTIMIZE {gold_table}")
print(f"✅ Gold table optimized!")

# Step 4: Check table details after optimize
print("\n📋 Silver Table Details AFTER Optimize:")
spark.sql(f"DESCRIBE DETAIL {silver_table}") \
    .select("numFiles", "sizeInBytes") \
    .show(truncate=False)

print(f"\n✅ OPTIMIZE COMPLETE!")
print(f"   Fewer files = Faster queries! ✅")

# ============================================================
# FEATURE 3 - VACUUM
# Cleans up old unused files
# Saves storage space!
# ============================================================

print("="*50)
print("🧹 FEATURE 3 — VACUUM")
print("="*50)

# Step 1: Check what vacuum will delete (dry run)
print("\n📋 Files to be cleaned (DRY RUN — no actual deletion):")
spark.sql(f"VACUUM {silver_table} RETAIN 168 HOURS DRY RUN") \
    .show(truncate=False)

# Step 2: Run Vacuum on Silver table
# 168 hours = 7 days retention
print("\n🧹 Running VACUUM on Silver table...")
spark.sql(f"VACUUM {silver_table} RETAIN 168 HOURS")
print(f"✅ Silver table vacuumed!")

# Step 3: Run Vacuum on Bronze table
print("\n🧹 Running VACUUM on Bronze table...")
spark.sql(f"VACUUM {bronze_table} RETAIN 168 HOURS")
print(f"✅ Bronze table vacuumed!")

# Step 4: Run Vacuum on Gold table
print("\n🧹 Running VACUUM on Gold table...")
spark.sql(f"VACUUM {gold_table} RETAIN 168 HOURS")
print(f"✅ Gold table vacuumed!")

print(f"\n✅ VACUUM COMPLETE!")
print(f"   Old files cleaned! Storage saved! ✅")

# ============================================================
# FEATURE 4 - CHANGE DATA FEED (CDF) - FIXED
# ============================================================

print("="*50)
print("📡 FEATURE 4 — CHANGE DATA FEED (CDF)")
print("="*50)

# Step 1: Enable CDF on Silver table
print("\n📡 Enabling Change Data Feed on Silver table...")
spark.sql(f"""
    ALTER TABLE {silver_table}
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print(f"✅ CDF enabled on Silver table!")

# Step 2: Check history to find CDF enable version
print("\n📋 Silver Table Version History:")
df_history = spark.sql(f"DESCRIBE HISTORY {silver_table}")
df_history.select("version", "timestamp", "operation") \
    .show(10, truncate=False)

# Step 3: Get latest version number
latest_version = df_history.selectExpr("max(version)") \
    .collect()[0][0]

print(f"\n✅ Latest Version : {latest_version}")
print(f"✅ CDF enabled from version : {latest_version}")

# Step 4: Make a small change to trigger CDF
print("\n📝 Inserting 5 test rows to trigger CDF...")
spark.sql(f"""
    INSERT INTO {silver_table}
    SELECT * FROM {silver_table} LIMIT 5
""")
print(f"✅ 5 test rows inserted!")

# Step 5: Read CDF from the version AFTER it was enabled
print(f"\n📋 Reading Changes from version {latest_version}:")
df_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", latest_version) \
    .table(silver_table)

# Step 6: Show changes
df_changes.select(
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
    "Number",
    "Category",
    "Priority"
).show(10, truncate=False)

# Step 7: Summary of changes
print(f"\n📊 Changes Summary:")
df_changes.groupBy("_change_type") \
    .count() \
    .show(truncate=False)

print(f"\n✅ CHANGE DATA FEED COMPLETE!")
print(f"   Tracking all changes from version {latest_version}! ✅")