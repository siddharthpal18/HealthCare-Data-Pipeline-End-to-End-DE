# HealthCare-Data-Pipeline-End-to-End-DE

## Project Overview

This project demonstrates a full-scale End-to-End Data Engineering Pipeline using the Azure ecosystem. The solution implements a Medallion Architecture to process healthcare data—ingesting raw streams from an HTTP source and transforming them into structured, high-performance analytics tables in Azure SQL and Power BI.

# Architecture Diagram
<img width="1096" height="839" alt="image" src="https://github.com/user-attachments/assets/b49d7b78-0058-4645-a329-b5cd75f5612e" />

# Tech Stack

Orchestration: Azure Data Factory (ADF)

Storage: Azure Data Lake Storage (ADLS) Gen2

Compute: Azure Databricks (PySpark)

Database: Azure SQL Database

Security: Azure Key Vault (Secret Scopes)

Visualization: Power BI


# Data Flow (Medallion Architecture)

1. Bronze Layer (Ingestion)

Source: HTTP Endpoint (Kaggle Dataset)

Process: ADF Copy Activity using ZipDeflateReadSettings.

Format: Raw CSV storage in adlstoragesiddharth/bronze

2. Silver Layer (Standardization)

Process: Databricks Notebook (minor_transformation.ipynb).

Operations:

initcap and trim applied to Name, Gender, Condition, and Hospital columns.

Schema enforcement: Casted Age and Billing Amount to Numeric types.

Data Cleaning: Filtered out invalid records (e.g., Age outside 0-120 range).

Format: Optimized Parquet files in adlstoragesiddharth/silver.

3. Gold Layer & Azure SQL (Analytics)

Process: Databricks Notebook (major_transformation_sql_and_gold.ipynb).

Aggregation: Created 12 specific reporting tables (e.g., hospital_performance, patient_demographics).

Output: * ADLS Gold: Partitioned Parquet files for Big Data workloads.

Azure SQL: JDBC-write to relational tables for high-performance Power BI consumption.

# Project Structure
├── Documentation/
│   ├── Step_1_ADF_Ingestion.md
│   ├── Step_2_Minor_Transformation.md
│   ├── Step_3_Major_Transformation_Gold.md
│   └── Step_4_PowerBI_Visualisation.md
├── Code/
│   ├── minor_transformation.ipynb
│   ├── major_transformation_sql_and_gold.ipynb
│   └── health_dataset_pipeline.json
├── Screenshots/
│   └── (Architecture, ADF Config, & SQL Output screenshots)
└── README.md


# Setup Instructions

Storage: Create ADLS Gen2 account with bronze, silver, and gold containers.

Database: Provision an Azure SQL Database.

Secrets: Configure a Databricks Secret Scope (DBSecrets) to securely access ADLS keys and SQL passwords via dbutils.secrets.

Linked Services: Connect ADF to HTTP Source, ADLS Gen2, and Databricks.

Execution: Run the ADF Pipeline health_dataset_pipeline.

# Business Insights (Power BI)

The final dashboard connects to Azure SQL via Import Mode to provide:

KPI Cards: Total Patients, Average Billing, and Mortality Trends.

Demographics: Hospital usage categorized by age groups.

Performance: Efficiency of hospitals per medical condition.

Financials: Average billing distribution across insurance providers.

# Contributor

Siddharth Pal

