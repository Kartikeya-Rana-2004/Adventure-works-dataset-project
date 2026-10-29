# Azure Cloud Lakehouse: End-to-End Medallion Pipeline

[![Live Executive Dashboard](https://img.shields.io/badge/Live%20Dashboard-GitHub%20Pages-0ea5e9?style=for-the-badge&logo=google-chrome)](https://kartikeya-rana-2004.github.io/Adventure-works-dataset-project/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark%20Delta-orange?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Orchestration](https://img.shields.io/badge/Azure%20Data%20Factory-ADLS%20Gen2-blue?style=for-the-badge&logo=microsoftazure)](https://azure.microsoft.com)
[![dbt Tested](https://img.shields.io/badge/dbt-28%2F28%20Tests%20Passing-FF694B?style=for-the-badge&logo=dbt)](https://www.getdbt.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%E2%86%92%20Silver%20%E2%86%92%20Gold)-purple?style=for-the-badge)](https://github.com/Kartikeya-Rana-2004/Adventure-works-dataset-project)

An enterprise-grade, end-to-end cloud lakehouse architecture processing multi-year retail transactions from the AdventureWorks enterprise dataset ($24.9M net revenue across 25,166 orders). 

Engineered with a **3-tier Medallion architecture** utilizing **Azure Data Factory, Azure Databricks (PySpark), Delta Lake, ADLS Gen2, dbt, and Synapse Analytics**, featuring Slowly Changing Dimensions Type 2 (SCD2) and 28 automated dbt schema tests.

---

## 🚀 Live Interactive Executive Dashboard

👉 **Explore the live interactive executive dashboard:** [**https://kartikeya-rana-2004.github.io/Adventure-works-dataset-project/**](https://kartikeya-rana-2004.github.io/Adventure-works-dataset-project/)

- **Executive KPI Tracking:** Real-time visibility into Gross Revenue ($24.9M), Order Volume (25,166), Return Rate (2.14%), and Average Order Value ($989.98).
- **Temporal & Dimensional Slicing:** Dynamic filtering by Fiscal Year (2015, 2016, 2017), Territory Country, and Product Category.
- **Dimensional Performance Analysis:** Category margin breakdowns and top-selling SKU profitability analysis.
- **Medallion Inspector:** Interactive architecture breakdown showing Bronze landing, Silver SCD2 transformations, and Gold Star Schema marts.

---

## 🏗️ End-to-End Lakehouse Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               AZURE DATA FACTORY (ADF)                                 │
│                   Scheduled Triggers • Webhooks • Batch Orchestration                  │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        ADLS Gen2: BRONZE LAYER (Raw Landing Zone)                      │
│      • Raw CSV Ingestion: Sales (2015-2017), Customers, Products, Returns, Calendar     │
│      • Metadata Injection: _ingested_at, _source_file, _batch_id                       │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                   AZURE DATABRICKS / PYSPARK: SILVER LAYER (Conformed Delta)            │
│      • Deduplication & Schema Enforcement (dropDuplicates, strict typing)              │
│      • Customer Dimension: Slowly Changing Dimension Type 2 (SCD2)                     │
│        (customer_sk, effective_start_date, effective_end_date, is_current)            │
│      • Product Dimension: Hierarchical joins across Categories & Subcategories         │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                     dbt & SYNAPSE ANALYTICS: GOLD LAYER (Star Schema)                  │
│      • Fact Table: fct_sales (partitioned by OrderYear, OrderMonth)                    │
│      • Dimension Tables: dim_customers, dim_products, dim_territories                  │
│      • Automated Data Quality Gates: 28 dbt tests (uniqueness, referential integrity)  │
│      • Executive Serving & Direct Query for Power BI Dashboards                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏛️ Dimensional Model (Star Schema)

- **Fact Table:** `fct_sales`
  - Granularity: One row per order line item.
  - Metrics: `OrderQuantity`, `GrossRevenue`, `TotalCost`, `NetMargin`.
  - Keys: `OrderNumber`, `CustomerKey`, `ProductKey`, `TerritoryKey`, `OrderDate`.
  - Partitioning: Partitioned by `(OrderYear, OrderMonth)` in Delta format for optimal partition pruning.

- **Dimension Tables:**
  - `dim_customers`: Slowly Changing Dimension Type 2 (SCD2) tracking customer demographic shifts over time.
  - `dim_products`: Denormalized product hierarchy containing product attributes, unit costs, list prices, and category classifications.
  - `dim_territories`: Regional geographic hierarchy (Country, Group, Region).
  - `dim_calendar`: Corporate calendar lookup for fiscal quarter and holiday attribution.

---

## 🛡️ Data Quality & dbt Test Suite

The pipeline enforces 28 automated schema validation rules before promoting data to the Gold analytical layer:

| Model | Column Tested | Test Type | Target Invariant | Result |
| :--- | :--- | :--- | :--- | :--- |
| `dim_customers` | `customer_sk` | `unique` & `not_null` | Surrogate key uniqueness | **PASS** |
| `dim_customers` | `is_current` | `accepted_values` | Boolean flag `[true, false]` | **PASS** |
| `dim_products` | `ProductKey` | `unique` & `not_null` | Primary key integrity | **PASS** |
| `fct_sales` | `ProductKey` | `relationships` | Foreign key references `dim_products` | **PASS** |
| `fct_sales` | `CustomerKey` | `relationships` | Foreign key references `dim_customers` | **PASS** |
| `fct_sales` | `GrossRevenue` | `not_null` & `>= 0` | Non-negative sales transactions | **PASS** |

---

## 📁 Repository Structure

```
├── azure_data_factory/
│   └── pipeline_adventureworks_etl.json  # Complete ADF Pipeline definition
├── databricks/
│   └── pyspark_medallion_pipeline.py     # Production PySpark Bronze/Silver/Gold ETL
├── dbt_project/
│   ├── dbt_project.yml                   # dbt configuration profile
│   └── models/
│       ├── schema.yml                    # Automated tests & column contracts
│       └── marts/
│           ├── fct_sales.sql             # Partitioned Sales Fact model
│           ├── dim_customers.sql         # SCD Type 2 Customer model
│           └── dim_products.sql          # Conformed Product Dimension model
├── data/                                 # Raw AdventureWorks enterprise CSV fixtures
├── index.html                            # Live Interactive Executive Analytics Web App
└── .github/workflows/deploy.yml          # GitHub Actions deployment to GitHub Pages
```

---

## 💻 Local Setup & Execution

### 1. Run the PySpark Medallion Pipeline

```bash
# Clone the repository
git clone https://github.com/Kartikeya-Rana-2004/Adventure-works-dataset-project.git
cd Adventure-works-dataset-project

# Install dependencies
pip install pyspark delta-spark

# Execute the local Spark medallion pipeline
python databricks/pyspark_medallion_pipeline.py
```

### 2. Run dbt Data Quality Tests

```bash
cd dbt_project
dbt deps
dbt test
```

---

## 👤 Author & Contact

**Kartikeya Rana**  
*Data Engineer & Distributed Systems Specialist*  
Penn State University — B.S. in Applied Data Science (Dec 2026)  
- **Email:** [kartikeyarana2004@gmail.com](mailto:kartikeyarana2004@gmail.com)  
- **LinkedIn:** [linkedin.com/in/kartikeyarana2004](https://linkedin.com/in/kartikeyarana2004)  
- **GitHub:** [github.com/Kartikeya-Rana-2004](https://github.com/Kartikeya-Rana-2004)  
- **Core Engineering Portfolio:** [Live Interactive Showcase](https://kartikeya-rana-2004.github.io/engineering-portfolio/)
