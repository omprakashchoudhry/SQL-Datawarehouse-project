# 🏢 SQL Data Warehouse & Analytics Project

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![T-SQL](https://img.shields.io/badge/T--SQL-004880?style=flat&logo=microsoftsqlserver&logoColor=white)
![ETL](https://img.shields.io/badge/ETL-Pipeline-orange?style=flat)
![Star Schema](https://img.shields.io/badge/Data%20Model-Star%20Schema-blue?style=flat)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

> A complete data warehousing and analytics solution built on **SQL Server** — implementing Medallion Architecture (Bronze → Silver → Gold), ETL pipelines, star schema data modelling, and business analytics across ERP and CRM source systems.

---

## 📌 Project Summary

This project builds a modern data warehouse from scratch using SQL Server, consolidating data from two source systems — **ERP** and **CRM** — into a unified, analytics-ready data model. The pipeline ingests raw CSV files, applies data quality transformations layer by layer, and produces a clean star schema that powers business reporting on customer behaviour, product performance, and sales trends.


**What this demonstrates:**
- End-to-end data warehouse design and build on SQL Server
- Medallion architecture (Bronze / Silver / Gold)
- ETL pipeline development using T-SQL stored procedures
- Star schema dimensional modelling
- Data quality engineering across two source systems
- Advanced SQL analytics — window functions, CTEs, segmentation, ranking
- Report views

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Database | Microsoft SQL Server |
| Language | T-SQL |
| Architecture | Medallion (Bronze / Silver / Gold) |
| Data Model | Star Schema (Fact + 2 Dimensions) |
| ETL | T-SQL Stored Procedures |
| Source Format | CSV Files (ERP + CRM) |
| Diagramming | Draw.io |

---

## 🏗️ Data Architecture

```
CSV Source Files
(ERP System + CRM System)
        │
        ▼
┌─────────────────────────────────────────────┐
│              BRONZE LAYER                   │
│  Raw data ingested as-is from CSV files     │
│  No transformations — exact source copy     │
│  BULK INSERT into staging tables            │
└─────────────────────────────────────────────┘
        │
        ▼  (cleanse · standardise · normalise)
┌─────────────────────────────────────────────┐
│              SILVER LAYER                   │
│  • Data type casting and normalisation      │
│  • Duplicate removal (ROW_NUMBER)           │
│  • NULL handling with default values        │
│  • Date format standardisation              │
│  • ERP + CRM data integration               │
│  • Whitespace trimming                      │
└─────────────────────────────────────────────┘
        │
        ▼  (model · enrich · aggregate)
┌─────────────────────────────────────────────┐
│              GOLD LAYER                     │
│  gold.dim_customers  — customer dimension   │
│  gold.dim_products   — product dimension    │
│  gold.fact_sales     — sales fact table     │
│  gold.vw_customer_report  — report view     │
│  gold.vw_product_report   — report view     │
└──────────────────┬──────────────────────────┘
                   │
        ▼
┌─────────────────────────────────────────────┐
│            ANALYTICS LAYER                  │
│  Sales trends · Customer segmentation       │
│  Product rankings · Part-to-whole analysis  │
│  Running totals · YoY growth                │
└─────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
SQL-Datawarehouse-project/
│
├── README.md
├── LICENSE
│
├── Datasets/                          # Raw source CSV files (ERP + CRM)
│
├── Scripts/                           # All T-SQL scripts
│   ├── Bronze/                        # Raw ingestion (BULK INSERT)
│   ├── Silver/                        # Cleansing and transformation
│   └── Gold/                          # Dimensional model + analytics
│       ├── ddl_gold.sql               # Dimension and fact table creation
│       ├── analytics_queries.sql      # All business analytics queries
│       └── report_views.sql           # Customer and product report views
│
├── Docs/                              # Architecture and design docs
│   ├── data_architecture.drawio
│   ├── data_models.drawio
│   ├── data_flow.drawio
│   ├── etl.drawio
│   ├── data_catalog.md
│   └── naming-conventions.md
│
└── Tests/                             # Data quality test scripts
```

---

## ⚙️ Pipeline Execution Order

```
1. Scripts/Bronze/    → Load raw CSV data into staging tables
2. Scripts/Silver/    → Clean, transform and integrate data
3. Scripts/Gold/      → Build star schema + create analytics views
4. Analytics queries  → Run reporting queries on Gold layer
```

---

## 📐 Data Model (Star Schema)

```
                   ┌──────────────────┐
                   │  dim_customers   │
                   │──────────────────│
                   │ customer_key  PK │
                   │ customer_id      │
                   │ first_name       │
                   │ last_name        │
                   │ country          │
                   │ gender           │
                   │ birthdate        │
                   └────────┬─────────┘
                            │
┌──────────────────┐  ┌─────┴────────────┐
│  dim_products    │  │   fact_sales     │
│──────────────────│  │──────────────────│
│ product_key   PK ├──┤ order_number  PK │
│ product_id       │  │ product_key   FK │
│ product_name     │  │ customer_key  FK │
│ category         │  │ order_date       │
│ subcategory      │  │ sales_amount     │
│ cost             │  │ quantity         │
│ price            │  │ price            │
└──────────────────┘  └──────────────────┘
```

---

## 🧹 Data Quality Issues Handled

| Issue | Source | Fix Applied |
|---|---|---|
| Inconsistent date formats | ERP + CRM | `CAST` and `CONVERT` with explicit formats |
| Duplicate customer records | CRM | `ROW_NUMBER()` deduplication |
| NULL values in key columns | ERP | `ISNULL()` with business default values |
| Mismatched product IDs across systems | ERP + CRM | Lookup join to resolve keys |
| Inconsistent gender / category codes | CRM | `CASE WHEN` standardisation |
| Leading and trailing whitespace | Both | `TRIM()` on all string columns |

---

## 📊 Analytics Queries Covered

All analytics queries are in `Scripts/Gold/analytics_queries.sql`:


| Analysis Type | Description |
|---|---|
| Key Metrics | Total revenue, customers, orders, avg order value |
| Sales Trends | Monthly and yearly revenue, YoY growth % |
| Cumulative Analytics | Running totals, 7-day moving average |
| Product Performance | Top/bottom 10 products, revenue by category |
| Part-to-Whole | Each category's % share of total revenue |
| Customer Analysis | Top customers, revenue by country |
| Customer Segmentation | VIP / Regular / New based on spend |
| Product Ranking | `RANK()` within each category |
| Report Views | `vw_customer_report`, `vw_product_report` for BI tools |

---
📊 [View Interactive Dashboard](Docs/sql_dw_dashboard.html)

## 🚀 How to Run

### Prerequisites
- [SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) — free
- [SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms) — free

### Steps

```sql
-- 1. Clone this repo
git clone https://github.com/omprakashchoudhry/SQL-Datawarehouse-project.git

-- 2. Open SSMS and connect to your SQL Server instance

-- 3. Create the database
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

-- 4. Run Bronze scripts → load raw CSV data
-- 5. Run Silver scripts → clean and transform
-- 6. Run Gold scripts  → build star schema
-- 7. Run analytics_queries.sql → explore insights
```

---

## 🌱 What I Learned

- Designing a **multi-layer data warehouse** from raw CSV to analytics-ready tables
- Writing **T-SQL stored procedures** for repeatable, maintainable ETL
- Handling **real-world data quality** issues — duplicates, NULLs, type mismatches, format inconsistencies
- Building a **star schema** with proper surrogate keys and relationships
- Using **advanced SQL** — window functions (`ROW_NUMBER`, `RANK`, `LAG`, `SUM OVER`), CTEs, subqueries
- Integrating **two source systems** with different schemas into one unified model
- Creating **report views** to simplify BI tool connections

---

## 👤 Author

**Omprakash Choudhary**
Aspiring Data Engineer | SQL Server · T-SQL · ETL · Data Warehousing

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://www.linkedin.com/in/omprakash-choudhary-a95361155/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/omprakashchoudhry)

---

*Built as a portfolio project demonstrating end-to-end data warehouse development and analytics on SQL Server.*
