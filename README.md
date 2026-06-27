# 🔥 Incident Data Pipeline — Databricks Medallion Architecture

An end-to-end data engineering pipeline built on **Databricks**, implementing the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, validate, clean, and aggregate IT incident ticket data using **PySpark** and **Delta Lake**.

This project simulates a real-world data engineering workflow — from raw ingestion to a production-style quality framework, orchestrated through **Databricks Workflows**.

---

## 📌 Project Overview

| | |
|---|---|
| **Dataset** | IT Incident Tickets (32,100 records, 16 columns) |
| **Architecture** | Medallion (Bronze / Silver / Gold) |
| **Platform** | Databricks (Unity Catalog enabled) |
| **Orchestration** | Databricks Workflows (Job with task dependencies) |
| **Storage Format** | Delta Lake |
| **Language** | PySpark, SQL, Python |

---

## 🏗️ Architecture

```
                ┌─────────────────────┐
                │   incident.csv      │
                │  (Unity Catalog     │
                │      Volume)        │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   🥉 BRONZE LAYER    │
                │  Raw ingestion as-is │
                │  + ingestion metadata│
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  📋 QUALITY CHECK    │
                │  Nulls / Duplicates  │
                │  / Record Counts     │
                └──────────┬──────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
   ┌─────────────────────┐   ┌─────────────────────┐
   │   🥈 SILVER LAYER    │   │  🗑️ QUARANTINE TABLE │
   │  Cleaned, validated  │   │  Bad records stored  │
   │  data                │   │  with reject reason  │
   └──────────┬──────────┘   └─────────────────────┘
              │
              ▼
   ┌─────────────────────┐
   │   🥇 GOLD LAYER      │
   │  Business aggregates │
   │  for reporting       │
   └─────────────────────┘
```

---

## 🗂️ Pipeline Stages

### 1️⃣ Bronze Layer — Raw Ingestion
- Reads the raw CSV from a Unity Catalog Volume
- Sanitizes invalid column names (e.g. `"User ID"` → `User_ID`)
- Adds metadata columns: `ingestion_timestamp`, `source_file_name`
- No transformations — preserves data exactly as received

### 2️⃣ Quality Check — Data Validation Framework
- Computes total record count
- Calculates null counts/percentages per column
- Detects duplicate rows
- Validates critical business columns (`Number`, `Category`, `Priority`, `State`)
- Persists results as a queryable **`quality_report`** Delta table

### 3️⃣ Silver Layer — Cleansing & Quarantine
- Identifies and isolates **duplicate rows** and **critical-null rows**
- Routes bad records to a separate **`quarantine_incidents`** table with a `reject_reason`
- Fills non-critical nulls with sensible defaults (`Unknown`, `Unassigned`)
- Casts string date columns to proper `timestamp` types
- **Zero data loss guarantee:** `Bronze rows = Silver rows + Quarantine rows`

### 4️⃣ Gold Layer — Business Aggregations
Aggregates the clean Silver data into reporting-ready metrics:
- Incident count by **Priority**
- Incident count by **Category**
- Incident count by **State**
- Incident count by **Month**
- Average **resolution time** by **Assignment Group**

### 5️⃣ Delta Lake Features
Demonstrates core Delta Lake capabilities applied to the pipeline:
- ⏰ **Time Travel** — query historical table versions
- ⚡ **OPTIMIZE** — compact small files for faster reads
- 🧹 **VACUUM** — clean up stale data files
- 📡 **Change Data Feed (CDF)** — track row-level inserts/updates/deletes

---

## ⚙️ Key Engineering Practices

| Practice | Implementation |
|---|---|
| **Metadata-driven design** | Catalog, schema, and file paths passed as widget parameters — no hardcoding |
| **Reusability** | Same notebooks can ingest any new dataset by changing parameter values |
| **Parameterization** | `dbutils.widgets` used across every notebook |
| **Data Quality** | Dedicated validation stage with a persisted quality report |
| **Quarantine Pattern** | Bad records preserved with reasons instead of being deleted |
| **Governance** | Built on Unity Catalog (catalog → schema → tables/volumes hierarchy) |
| **Orchestration** | Databricks Workflow job with task-level dependencies |

---

## 🔄 Databricks Workflow

All five notebooks are orchestrated as a single Databricks Job with sequential task dependencies:

```
01_bronze → 02_quality_check → 03_silver → 04_gold → 05_delta_lake_features
```

Each task runs only if the preceding task succeeds, ensuring no invalid data propagates downstream.

---

## 🧱 Tech Stack

- **Databricks** (Free Edition / Unity Catalog)
- **Apache Spark / PySpark**
- **Delta Lake**
- **SQL**
- **Databricks Workflows** (Job Orchestration)
- **Python**

---

## 📂 Repository Structure

```
Incident-data-pipeline-databricks/
│
├── notebooks/
│   ├── 01_bronze.py
│   ├── 02_quality_check.py
│   ├── 03_silver.py
│   ├── 04_gold.py
│   └── 05_delta_lake_features.py
│
├── sample_data/
│   └── sample_incidents.csv
│
├── screenshots/
│   ├── workflow_graph.png
│   ├── catalog_tables.png
│   └── gold_output.png
│
└── README.md
```

---

## 📊 Sample Results

| Layer | Table | Row Count |
|---|---|---|
| Bronze | `bronze_incidents` | 32,100 |
| Quality | `quality_report` | ~20 metrics |
| Silver | `silver_incidents` | 32,000 |
| Quarantine | `quarantine_incidents` | 100 |
| Gold | `gold_incidents` | 129 aggregated rows |

**Validation:** `32,100 (Bronze) = 32,000 (Silver) + 100 (Quarantine)` ✅

---

## 🚀 How to Run

1. Upload the source CSV to a Unity Catalog Volume
2. Run notebooks in sequence (or trigger the Databricks Workflow job):
   ```
   01_bronze → 02_quality_check → 03_silver → 04_gold → 05_delta_lake_features
   ```
3. Pass parameters via notebook widgets:
   - `catalog_name`
   - `schema_name`
   - `file_name`
4. Query results directly from Unity Catalog tables, or view via SQL:
   ```sql
   SELECT * FROM workspace.realproject_schema.gold_incidents;
   ```

---

## 🎯 What This Project Demonstrates

- Building production-style ETL pipelines using the Medallion Architecture
- Implementing a custom data quality and quarantine framework (without Delta Live Tables)
- Working with Unity Catalog governance (catalogs, schemas, tables, volumes)
- Orchestrating multi-stage pipelines with Databricks Workflows
- Applying core Delta Lake features: Time Travel, Optimize, Vacuum, and CDF
- Writing reusable, parameterized PySpark notebooks

---

## 👤 Author

**Akhil Perakam**
Data Engineering | Databricks | PySpark | SQL
[Portfolio](https://portfolio-akhil-two.vercel.app) · [LinkedIn](https://www.linkedin.com/in/perakam-akhil-0204b12a0/) · [GitHub](https://github.com/Akhil1976)
