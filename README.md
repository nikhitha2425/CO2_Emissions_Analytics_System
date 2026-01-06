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
## 🏗️ Data Processing & ETL
<img width="1220" height="719" alt="ETL Pipeline" src="https://github.com/user-attachments/assets/3a5f141f-502d-4e04-abca-b4456cc037b9" />
The ETL pipeline follows a Bronze–Silver–Gold medallion architecture implemented using Delta Live Tables on Databricks. Raw CO₂ emissions data is ingested into the Bronze layer, cleaned and enriched in the Silver layer, and transformed into multiple analytics-ready Gold tables. The pipeline is fully automated, scalable, and maintains clear data lineage with built-in dependency tracking.

---
### 🛠 Airflow Setup (Docker-based)

The following Docker Compose commands were used to initialize and manage Airflow locally:

```bash
# Initialize Airflow metadata database
docker compose run --rm airflow-webserver airflow db init

# Create Airflow admin user
docker compose run --rm airflow-webserver airflow users create \
  --username admin \
  --firstname Nikhitha \
  --lastname Bandela \
  --role Admin \
  --email nikki@example.com \
  --password admin

# Start all Airflow services (webserver, scheduler, etc.)
docker compose up -d

# Stop and clean up Airflow services
docker compose down
```

### 🔐 Airflow Access

* **URL:** `http://localhost:8080`
* **Username:** `admin`
* **Password:** `admin`

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
<img width="1876" height="578" alt="Airflow-Trigger Result" src="https://github.com/user-attachments/assets/a6b6a219-4de8-4e7d-9250-549ee3868340" />

---

## 📧 Automated Alert Notifications

When emission anomalies are detected:
- Airflow generates a **dynamic HTML email**
- Email includes:
  - Total number of affected countries  
  - Country-wise emission vs threshold comparison  
  - Embedded **Power BI dashboard snapshot**
This ensures stakeholders receive **immediate, contextual insights** directly in their inbox.
<img width="1386" height="688" alt="image" src="https://github.com/user-attachments/assets/6d8b7f7b-85c9-43f1-ab78-ab1da1c90277" />


---

## 📊 Power BI Dashboards

### 📍 Dashboard 1: Global Emissions Overview
**Purpose:**
Analyze long-term global CO₂ emission trends and country-wise contributions.

**Key Insights:**

- Year-wise global CO₂ emissions trends (1990–2020)
- Country-wise contribution to total emissions
- Year-over-year emission growth and decline
- Interactive year-based analysis

**Business Value:**
Supports data-driven climate policy decisions by highlighting emission patterns, major contributors, and abnormal changes over time.
    
<img width="575" height="324" alt="Co2 Emission Trend Analysis" src="https://github.com/user-attachments/assets/d308b6a1-5e6f-40d9-91df-1f1d7321aeb1" />


### 📍 Dashboard 2: Regional & Country Comparison
**Purpose:**
Compare CO₂ emissions across regions to identify high-emission zones and per-capita impact.

**Key Insights:**

- Total CO₂ emissions by region
- Highest emitting region identification
- Average CO₂ emissions per capita across regions
- Sustainability clusters mapped geographically

**Business Value:**
Helps policymakers prioritize regions for targeted emission reduction strategies and sustainability planning.

<img width="575" height="324" alt="Regional Emission Comparisions" src="https://github.com/user-attachments/assets/05dd3b23-1bb6-4d6c-b5a5-2cc07003a345" />


### 📍 Dashboard 3: Policy Simulation (Interactive)
**Purpose:**
Monitor key environmental KPIs and evaluate emissions against policy-driven targets.

**Key Insights:**

- Total global CO₂ emissions and average emissions per capita
- Correlation between population and CO₂ emissions by country
- CO₂ emissions distribution across income levels
- Active emission alerts by region and year
- Actual vs target emissions comparison with reduction simulation

**Business Value:**
Supports policymakers in tracking sustainability targets, identifying high-risk regions, and assessing the effectiveness of emission reduction strategies.
   
<img width="575" height="324" alt="Environmental   Policy Insights" src="https://github.com/user-attachments/assets/e331410e-d80c-4025-8cc9-e659c1527608" />



### 📍 Dashboard 4: Emission Alert Analysis
**Purpose:**
Monitor abnormal CO₂ emission spikes and track alert patterns across time, regions, and countries.

**Key Insights:**

- Year-wise distribution of active emission alerts
- Geographic visualization of alert locations
- Country-level alert counts and trends
- Region-wise alert contribution analysis
- Total active emission alerts overview

**Business Value:**
Enables proactive environmental monitoring by identifying emission anomalies early and supporting timely, targeted intervention strategies.
    
<img width="575" height="324" alt="Emission Alert Analysis" src="https://github.com/user-attachments/assets/ab653cd3-143b-4540-8623-cd9acd6175a3" />


## 🧠 Advanced Analytics & DAX
- Percentile-based clustering logic  
- Disconnected parameter table for simulation  
- Context-aware KPI measures  
- Dynamic text KPIs using multiline DAX expressions  

---
## 🔹 Logging, Monitoring & Observability

Execution-level logging and monitoring are handled by Apache Airflow and Azure Databricks, providing end-to-end visibility into the CO₂ emissions ETL pipeline without requiring custom logging logic.

Apache Airflow captures detailed logs for DAG scheduling, task execution, retries, and alert-trigger conditions, while Databricks records job execution details, Delta Live Tables lineage, Spark driver logs, and executor metrics through its managed runtime environment. This ensures reliable observability, faster troubleshooting, and consistent monitoring across all pipeline layers.

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

