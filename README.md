# 🌍 CO₂ Emissions Monitoring & Environmental Impact Analytics System

## 📌 Project Overview
This capstone project implements an **end-to-end data analytics pipeline** to analyze **global CO₂ emissions**, identify high-emission regions, and support **environmental planning and sustainability goals**.

The system follows an **industry-standard Medallion Architecture (Bronze → Silver → Gold)** using **Databricks Lakeflow (Delta Live Tables)** and delivers **interactive Power BI dashboards** for policymakers and stakeholders.

Databricks | Delta Live Tables | Power BI | Data Engineering | Apache Airflow

---

## 🎯 Project Objectives
- Automate CO₂ emissions data ingestion and processing  
- Analyze emission trends and regional comparisons  
- Identify high-emission regions and countries  
- Enable policy-driven emission reduction simulations  
- Provide clear, data-driven visual insights for decision-makers  

---

## 🏗️ Architecture Overview
```
Raw CSV / Delta Data
↓
Bronze Layer (DLT Streaming Table)
↓
Silver Layer (Cleaning, Standardization, Enrichment)
↓
Gold Layer (Analytics & Business Tables)
↓
Power BI Dashboards
```

---

## 🛠️ Technology Stack
- **Databricks Lakehouse**
- **Delta Live Tables (DLT)**
- **Apache Spark (SQL)**
- **Power BI**
- **Apache Airflow (Databricks Job Trigger)**

---

## 🗂️ Dataset Description
- Source: Global CO₂ emissions dataset (Kaggle reference)
- Records: 100,000+  
- Granularity: Country–Year  
- Key Attributes:
  - Country
  - Region
  - Income Level
  - Year
  - CO₂ Emissions (Metric Tons)
  - Population  

Missing and inconsistent values were intentionally included to demonstrate real-world data cleaning and validation.

---

## 🟤 Bronze Layer — Raw Ingestion

**Purpose**
- Preserve raw data in its original form  
- Act as the single source of truth  

**DLT Object**
- `dlt_bronze_co2`

**Key Characteristics**
- Streaming ingestion using Delta  
- No transformations applied  

---

## 🥈 Silver Layer — Data Cleaning & Enrichment

### 1️⃣ `dlt_silver_co2_cleaned`
- Trim and standardize textual fields  
- Validate numeric columns  
- Remove invalid and inconsistent records  

### 2️⃣ `dlt_silver_co2_enriched`
- Handle missing values (`Unknown`)  
- Correct country–region mappings  
- Derive **CO₂ per capita** metric  

co2_per_capita = co2_emissions_mt / population

### 3️⃣ `dlt_silver_country_region_fixed`
- Ensures one-to-one mapping between country and region  
- Prevents duplication in regional analysis  

---

## 🥇 Gold Layer — Analytics Tables

### Gold Tables Implemented

1. **`dlt_gold_global_yoy_emissions`**  
   Global year-over-year CO₂ emissions trend  

2. **`dlt_gold_country_yearly_emissions`**  
   Country-wise yearly emissions and per-capita values  

3. **`dlt_gold_regional_emissions_summary`**  
   Regional emissions totals and averages  

4. **`dlt_gold_high_emission_regions`**  
   Ranked high-emission regions  

5. **`dlt_gold_population_emissions_correlation`**  
   Correlation between population and CO₂ emissions  

6. **`dlt_gold_income_level_emissions`**  
   Emissions analysis by income level  

7. **`dlt_gold_country_cluster_summary`**  
   Country-level sustainability clusters for map visualization  

**Clustering Approach**
- Percentile-based clustering  
- Categories:
  - High Polluter
  - Moderate
  - Eco-Friendly  

---

## 📊 Power BI Dashboards

### 📍 Dashboard 1: Global Emissions Overview
- Line chart: Global CO₂ trend  
- KPI cards:
  - Total CO₂ emissions  
  - Highest emitting region  
  - Global average CO₂ per capita  
- Map visualization:
  - Country-level sustainability clusters  

### 📍 Dashboard 2: Regional & Country Comparison
- Region-wise emission bar charts  
- Country ranking tables  
- Income-level comparison visuals  

### 📍 Dashboard 3: Policy Simulation (Interactive)
- Year range slicer  
- Target reduction percentage slider (0%–50%)  
- Line chart:
  - Actual emissions (solid line)  
  - Target reduction path (dotted line)  

---

## 🧠 Advanced Analytics & DAX
- Percentile-based clustering logic  
- Disconnected parameter table for simulation  
- Context-aware KPI measures  
- Dynamic text KPIs using multiline DAX expressions  

---

## 🔄 Workflow Automation
- ETL implemented using Databricks Delta Live Tables  
- Databricks Job created for pipeline execution  
- Apache Airflow triggers the Databricks job  
- Enables scheduled and monitored pipeline runs  

---

## 🧪 Testing & Validation
- Schema validation at Bronze and Silver layers  
- Data quality checks for nulls and invalid records  
- Cross-verification between Gold tables and dashboards  
- Manual sanity checks on trends and aggregates  

---

## 🚧 Challenges & Solutions

| Challenge | Solution |
|--------|--------|
| Invalid CSV headers | Converted dataset to Delta format |
| Country–region duplication | Normalized in Silver layer |
| Skewed emissions distribution | Percentile-based clustering |
| DLT SQL limitations | Declarative materialized views |
| Visual slicer issues | Configured Edit Interactions |

---

## 🎓 Key Learnings
- Designing scalable Medallion architectures  
- Working with Delta Live Tables constraints  
- Implementing policy-oriented analytics  
- Handling real-world, messy datasets  
- Building interactive Power BI dashboards  

---

## 🚀 Future Enhancements
- Real-time emissions ingestion  
- Machine learning-based emission forecasting  
- Scenario comparison dashboards  
- Carbon neutrality goal tracking  

---

## 🧾 Conclusion
This project demonstrates a **real-world climate analytics use case**, combining **data engineering**, **advanced analytics**, and **interactive visualization** to support **sustainability-focused decision-making**.

---

## 👤 Author
**NIKHITHA**  

