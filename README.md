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


