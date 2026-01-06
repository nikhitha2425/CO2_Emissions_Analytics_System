# 🌍 CO₂ Emissions Monitoring & Environmental Impact Analytics System
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 📌 Project Overview
This capstone project implements a **production-style climate analytics and monitoring platform** to analyze global CO₂ emissions, detect abnormal emission spikes, and support **data-driven environmental decision-making**.

The system integrates:
- Scalable **Databricks Lakehouse** architecture  
- **Delta Live Tables (DLT)** for declarative data pipelines  
- **Apache Airflow** for orchestration and alerting  
- **Power BI** for interactive analytics and policy simulations  

Unlike traditional dashboards that require manual monitoring, this project introduces **automated anomaly detection and proactive alerting**, transforming static analytics into an **active monitoring system**.

---

## 🎯 Project Objectives
- Automate ingestion and processing of global CO₂ emissions data  
- Design a **Bronze → Silver → Gold** Medallion Architecture  
- Identify high-emission countries and regions  
- Detect abnormal emission spikes using dynamic thresholds  
- Orchestrate pipelines and alerts using Apache Airflow  
- Deliver policy-oriented and interactive insights via Power BI  

---

## 🏗️ Architecture Overview
```
Raw CSV / Delta Data
↓
Databricks Bronze Layer (DLT)
↓
Databricks Silver Layer (Cleaning & Enrichment)
↓
Databricks Gold Layer (Analytics + Alerts)
↓
Apache Airflow (Orchestration & Conditional Logic)
↓
Email Alerts + Power BI Dashboards
```

---

## 🛠️ Technology Stack
- **Databricks Lakehouse (Azure)**
- **Delta Live Tables (DLT)**
- **Apache Spark (SQL / PySpark)**
- **Apache Airflow**
- **Power BI (Desktop)**
- **Python**
- **Docker (Airflow deployment)**

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
## 🚨 Emission Alerting Mechanism

### 🔍 Alert Design Logic
- Alerts are generated **only from Gold-layer data**  
- Annual country-level emissions are evaluated  
- A **95th percentile** threshold defines normal behavior  
- Countries exceeding this threshold are flagged as **ALERT**

**Why percentile-based alerts?**
- No hardcoded limits  
- Automatically adapts as data changes  
- Reduces false positives in skewed datasets  

---

## ⚙️ Workflow Orchestration with Apache Airflow

Apache Airflow acts as the **control plane** for the system.
### 🔄 Airflow Execution Flow
```
Airflow DAG Trigger
↓
Databricks Serverless Job Execution
↓
Gold Tables Generated
↓
Emission Alert Check
↓
(Alert Exists?)
┌──── Yes ────┐
↓             ↓
Prepare Alert Stop Workflow
Email Content
↓
Send Email Notification
```
### 🔧 Airflow Responsibilities
- Triggers Databricks pipelines using `DatabricksRunNowOperator`  
- Performs post-ETL alert checks using `ShortCircuitOperator`  
- Sends notifications only when anomalies are detected  
- Prevents redundant or unnecessary alerts  

---

## 📧 Automated Alert Notifications

When emission anomalies are detected:
- Airflow generates a **dynamic HTML email**
- Email includes:
  - Total number of affected countries  
  - Country-wise emission vs threshold comparison  
  - Embedded **Power BI dashboard snapshot**
This ensures stakeholders receive **immediate, contextual insights** directly in their inbox.

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
- Active Emission Alerts by country & year
- Analysis based on population & income level

---

## 🧠 Advanced Analytics & DAX
- Percentile-based clustering logic  
- Disconnected parameter table for simulation  
- Context-aware KPI measures  
- Dynamic text KPIs using multiline DAX expressions  

---

## 🧪 Testing & Validation
- Schema validation at Bronze and Silver layers  
- Data quality checks for nulls and invalid records  
- Cross-verification between Gold tables and dashboards  
- Manual sanity checks on trends and aggregates  

---

## 🚧 Challenges & Solutions

| Challenge | Solution |
|--------|---------|
| Messy real-world CSV data | Converted to Delta with validation |
| Country–region duplication | Normalized in Silver layer |
| Skewed emission distribution | Percentile-based clustering |
| Delta Live Tables constraints | Declarative materialized views |
| Alert noise | Conditional Airflow execution |
---

## 🎓 Key Learnings
- Designing scalable **Lakehouse architectures**  
- Working within **Delta Live Tables constraints**  
- Handling real-world data quality issues  
- Building **proactive alerting systems**  
- Orchestrating pipelines using **Apache Airflow**  
- Translating analytics into policy-driven insights  
---

## 🚀 Future Enhancements
- Real-time emissions ingestion  
- Machine learning-based emission forecasting  
- Scenario comparison dashboards  
- Carbon neutrality goal tracking  

---

## 🧾 Conclusion
This project demonstrates how modern data platforms can evolve from **static reporting** to **proactive climate monitoring systems**.

By integrating Databricks, Airflow, alerting mechanisms, and Power BI, the solution enables early detection of emission anomalies and supports **sustainability-focused decision-making**.

> *From visualizing climate data to actively monitoring emission risks.*
---

## 👤 Author
**NIKHITHA**  

