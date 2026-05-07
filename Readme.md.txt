Healthcare Data Pipeline: End-to-End Azure Data Engineering Project
Project Overview
This project implements a comprehensive end-to-end Azure Data Engineering pipeline for a healthcare dataset. Raw patient data is ingested from an HTTP source into the Bronze layer, cleaned and standardized in the Silver layer, and aggregated into 12 Power BI-ready tables stored in both the Gold layer (ADLS Parquet) and Azure SQL Database. 
The pipeline follows the Medallion Architecture (Bronze ? Silver ? Gold) and is orchestrated entirely by Azure Data Factory. 
Architecture
Tech Stack
* Orchestration: Azure Data Factory 
* Compute: Azure Databricks (PySpark) 
* Storage: Azure Data Lake Storage (ADLS) Gen2 
* Database: Azure SQL Database 
* Visualization: Power BI 
* Security: Azure Key Vault 

Data Pipeline Layers
LayerDescriptionBronzeRaw CSV data copied from Kaggle HTTP source via ADF Copy Activity into ADLS Gen2. SilverCleaned & typed Parquet data—MinorTransformation Databricks notebook. Gold12 aggregated Parquet tables partitioned for analytics—MajorTransformation notebook. Azure SQLSame 12 tables loaded via JDBC for direct Power BI Import / DirectQuery. 
Setup & Implementation
1. Infrastructure Setup
* Storage: Create ADLS Gen2 account adlstoragesiddharth with containers: bronze, silver, and gold. 
* Database: Provision Azure SQL Database AzureSQLDB on server sqldbc37.database.windows.net. 
* Security: Store ADLS keys and SQL passwords in Databricks secret scope DBSecrets using Azure Key Vault integration. 
2. Data Ingestion (HTTP ? Bronze)
* Activity: Copy Activity in ADF. 
* Source: DelimitedTextSource (HTTP). 
* Compression: Handled ZipDeflate on the source HTTP endpoint. 
* Sink: DelimitedTextSink in ADLS Gen2 Bronze container. 
3. Transformations (Silver & Gold)
* Minor Transformation: Standardizes casing using initcap, trims whitespace, casts data types (Age, Billing, Dates), and filters invalid records. 
* Major Transformation: Generates 12 specific aggregate tables (e.g., Hospital Performance, Patient Demographics, Age Group Summaries). 
* Dual-Write Strategy: Data is written simultaneously to ADLS Gen2 Gold layer as partitioned Parquet and to Azure SQL Database via JDBC. 
4. Visualization
* Power BI connects to Azure SQL Database in Import mode. 
* Key Visuals: Hospital Usage by Age Group, Medical Conditions by Gender, and Average Billing by Insurance Provider. 

Folder Structure
Plaintext
Customer_Account_Analysis_Pipeline_TeamName/
??? Documentation/
?   ??? Step_1_ADF_Ingestion.md
?   ??? Step_2_Minor_Transformation.md
?   ??? Step_3_Major_Transformation_Gold.md
?   ??? Step_4_PowerBI_Visualisation.md
??? Code/
?   ??? minor_transformation.ipynb
?   ??? major_transformation_sql_and_gold.ipynb
?   ??? health_dataset_pipeline.json
??? Screenshots/
?   ??? Step_1_Screenshot_1_ADF_Pipeline_Overview.png
?   ??? ... (Other process screenshots)
??? README.md

http://googleusercontent.com/immersive_entry_chip/0

