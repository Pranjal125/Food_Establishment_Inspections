# 🍽️ Food Inspection Data Analytics Pipeline

An end-to-end data engineering project that integrates, cleanses, and models food inspection data from **Chicago** and **Dallas** using Alteryx, Databricks Delta Live Tables, and a medallion architecture.

---

## 📌 Project Overview

This pipeline processes multi-year food inspection records from two cities with fundamentally different data formats, unifies them into a standardized schema, and builds a dimensional model (star schema) optimized for analytics and reporting.

| Attribute | Details |
|---|---|
| **Data Sources** | Chicago Food Inspections, Dallas Food Inspections |
| **Years Covered** | 2021–2025 (5 files per city) |
| **Total Layers** | 3 (Bronze, Silver, Gold) |
| **Total Tables** | 7 (1 Bronze + 1 Silver + 5 Dimensions + 1 Fact) |
| **Tools Used** | Alteryx, Databricks DLT, Navicat, PySpark |

---

## 🗂️ Data Sources

### Chicago
- **Format:** Pre-normalized — violations already split into individual rows
- **Files:** 5 CSV files (2021–2022 through 2025 partial year)
- **Columns:** 17 source columns

### Dallas
- **Format:** Wide format — up to 25 violation columns per inspection record requiring transformation to long format
- **Files:** 5 TSV files (tab-delimited)
- **Columns:** 75+ columns including violation description, detail, and memo fields

---

## ⚙️ Data Processing — Alteryx Workflows

### Chicago Pipeline

1. **Ingestion** — Loaded 5 CSV files with auto-detect settings
2. **Union** — Combined all files into a single dataset using Auto Config by Name
3. **Profiling** — Field Summary and Field Info to identify nulls, data types, and distributions
4. **Cleansing** — Removed whitespace, standardized text, corrected type mismatches
5. **Null Validation** — Created four flags (`Is_Valid_DBA`, `Is_Valid_Date`, `Is_Valid_Type`, `Is_Valid_Zip`) and dropped records failing any check
6. **Violation Parsing** — Split violations by `|` delimiter, then by `- Comments:` delimiter; used RegEx to extract violation codes and clean descriptions
7. **Standardization:**
   - Zip codes: removed `.0` decimal suffix → `60651.0` → `60651`
   - License numbers: padded to 7 digits with `_chicago` suffix
   - Risk field: stripped redundant "Risk" word → `"Medium Risk"` → `"Medium"`
   - Null coordinates replaced with `-999`
8. **Deduplication** — Composite key: `Inspection_ID + Violation_Code`
9. **Output** — 23 standardized columns

### Dallas Pipeline

1. **Ingestion** — Loaded 5 TSV files with tab delimiter
2. **Union** — Combined into single dataset (~28,224 initial records)
3. **Profiling** — Field Summary across 75+ columns (sampled 5,000 records)
4. **ID Generation** — Sequential `Inspection_ID` created with `_dallas` suffix for uniqueness
5. **Derived Fields** — City (`"Dallas"`), Facility Type (`"Unknown"`), License Number (`-9999`)
6. **Risk & Results Derivation** from Inspection Score:
   - Score ≥ 90 → Low Risk / Pass
   - Score ≥ 80 → Medium Risk / Pass w/ Conditions
   - Score < 80 → High Risk / Fail
7. **Violation Transformation (Most Complex):**
   - Transposed 3 sets of 25 violation columns (Description, Detail, Memo) into long format
   - Extracted sequence numbers to align components
   - Joined all three via `Temp_ID + Seq_Num`
   - Combined Detail + Memo into unified `violation_comments`
8. **Violation Parsing** — RegEx to extract numeric codes and clean description text
9. **Standardization** — Zip codes, null coordinates, valid 5-digit zip filter
10. **Deduplication** — Composite key: `Inspection_ID + Violation_Code + Violation_Description`
11. **Output** — 23 standardized columns matching Chicago schema

---

## 🏗️ Medallion Architecture (Databricks Delta Live Tables)

```
BRONZE  →  Raw ingestion, no transformations, CDC enabled
   ↓
SILVER  →  8 data quality rules enforced, bad records dropped
   ↓
GOLD    →  Star schema — 5 dimensions + 1 fact table
```

###  Bronze Layer
- **Table:** `bronze_table`
- Reads unified Chicago + Dallas data from `midterm.source1_layer.raw_table`
- No transformations — full fidelity preserved for audit and reprocessing
- Change Data Capture (CDC) enabled for lineage tracking

```python
@dlt.table(name="bronze_table", comment="Bronze layer - Raw food inspection data")
def bronze_table():
    return spark.read.table("midterm.source1_layer.raw_table")
```

###  Silver Layer
- **Table:** `silver_table`
- Applies 8 data quality rules using `@dlt.expect_all_or_drop()` — records failing any rule are dropped

```python
@dlt.table(name="silver_table", comment="Silver layer - Cleansed data with quality rules")
@dlt.expect_all_or_drop(dataset_rules)
def silver_table():
    return dlt.read("bronze_table")
```

| Rule ID | Rule Name | Validation Logic |
|---|---|---|
| R1 | valid_dba_name | `DBA_Name IS NOT NULL AND DBA_Name != ''` |
| R2 | valid_inspection_date | `Inspection_Date IS NOT NULL` |
| R3 | valid_inspection_type | `Inspection_Type IS NOT NULL AND != ''` |
| R4 | valid_zip_code | `Zip_Code IS NOT NULL` |
| R5 | valid_inspection_result | `Inspection_Results IS NOT NULL AND != ''` |
| R6 | valid_score_range | `Inspection_Score BETWEEN 0 AND 100` |
| R7 | valid_high_score_violations | High scores (≥90) cannot have more than 3 violations |
| R8 | no_critical_on_pass | PASS results cannot have critical violations |

###  Gold Layer — Star Schema

Dimensional model built in Navicat and implemented via Databricks DLT.

#### Dimension Tables

| Table | Description | Key Feature |
|---|---|---|
| `dim_restaurant` | Restaurant attributes | **SCD Type 2** — tracks historical changes |
| `dim_location` | Geographic data | Address, city, state, zip, lat/lon |
| `dim_date` | Time dimension | Year, month, quarter, day of week |
| `dim_violation` | Violation master data | Code, description, category, severity |
| `dim_inspection_type` | Inspection classification | Type and category |

#### Fact Table

**`fact_inspections`**
- **Grain:** One row per inspection–violation combination
- **Foreign Keys:** Links to all 5 dimension tables
- **Measures:** `inspection_score`, `violation_points`, `violation_count`

---

## 📐 Final Unified Schema (23 columns)

`inspection_id`, `business_name`, `aka_name`, `license_number`, `facility_type`, `risk_category`, `address`, `city`, `state`, `zipcode`, `inspection_date`, `inspection_type`, `inspection_results`, `inspection_score`, `violation_code`, `violation_description`, `violation_comments`, `violation_points`, `latitude`, `longitude`, `location`, `source_city`, `source_file`

---

##  Key Data Quality Challenges

- **Chicago violations** were stored in a concatenated, pipe-delimited string requiring multi-step parsing
- **Dallas violations** were spread across 75 columns requiring three separate transpose operations and sequence-based joining
- **Zip code inconsistencies** — decimal format (`60651.0`) standardized to 5-digit strings
- **Missing fields in Dallas** — risk category, inspection results, license numbers, and city all derived or defaulted
- **Duplicate records** handled via composite key deduplication at both city levels

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Alteryx** | Data profiling, cleansing, transformation, violation parsing |
| **Databricks DLT** | Medallion architecture, data quality enforcement |
| **PySpark** | Silver and Gold layer transformations |
| **Navicat** | Dimensional model design |
| **Delta Lake** | Storage format with CDC support |
