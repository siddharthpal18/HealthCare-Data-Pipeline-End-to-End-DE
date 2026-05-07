# Step 1 — ADF Data Ingestion (HTTP → Bronze)







## Overview

Azure Data Factory copies the raw healthcare CSV from a Kaggle HTTP endpoint into the Bronze container of ADLS Gen2. This is the first activity in the `HealthDataset\_Pipeline` and acts as the entry point for all downstream processing.









## Configuration

|Setting|Value|
|-|-|
|Activity type|Copy Activity|
|Source type|DelimitedTextSource (HTTP)|
|Request method|GET|
|Compression|ZipDeflate|
|Sink type|DelimitedTextSink (ADLS Gen2)|
|Copy behaviour|FlattenHierarchy|
|File extension|.csv|
|Quote all text|true|
|Staging enabled|false|
|Timeout|12 hours|
|Retry|0|
|Sink container|bronze|
|Sink folder|Healthcare\_Dataset|
|Input dataset|HealthcareData|
|Output dataset|HeathCareData|







## ADF Pipeline JSON — Key Extract







```json
{
  "name": "CopyHealthData",
  "type": "Copy",
  "dependsOn": \[],
  "typeProperties": {
    "source": {
      "type": "DelimitedTextSource",
      "storeSettings": {
        "type": "HttpReadSettings",
        "requestMethod": "GET"
      },
      "formatSettings": {
        "type": "DelimitedTextReadSettings",
        "compressionProperties": {
          "type": "ZipDeflateReadSettings"
        }
      }
    },
    "sink": {
      "type": "DelimitedTextSink",
      "storeSettings": {
        "type": "AzureBlobFSWriteSettings",
        "copyBehavior": "FlattenHierarchy"
      },
      "formatSettings": {
        "type": "DelimitedTextWriteSettings",
        "quoteAllText": true,
        "fileExtension": ".csv"
      }
    },
    "enableStaging": false,
    "translator": {
      "type": "TabularTranslator",
      "typeConversion": true,
      "typeConversionSettings": {
        "allowDataTruncation": true,
        "treatBooleanAsNumber": false
      }
    }
  },
  "inputs":  \[{ "referenceName": "HealthcareData", "type": "DatasetReference" }],
  "outputs": \[{ "referenceName": "HeathCareData",  "type": "DatasetReference" }]
}




Linked Services Used

|Linked Service|Type|Purpose|
|-|-|-|
|HTTP Linked Service|HTTP|Connect to Kaggle dataset URL|
|AzureDatabricksLS|Azure Databricks|Trigger downstream notebooks|
|ADLS Gen2 Linked Service|Azure Data Lake Storage Gen2|Write CSV to Bronze container|

\---

## Output Path

```
abfss://bronze@adlstoragesiddharth.dfs.core.windows.net/Healthcare\_Dataset/
```

\---

## Pipeline Dependency Chain

```
CopyHealthData (Copy Activity)
        ↓ Succeeded
Notebook\_DataBricks\_MinTransfm
        ↓ Succeeded
Notebook\_Databricks\_MajorTransfm
```

\---

## Challenges \& Resolutions

|Challenge|Resolution|
|-|-|
|ZIP compression on HTTP source|Added `ZipDeflateReadSettings` to `formatSettings` so ADF auto-decompresses on download|
|Special characters in CSV|Enabled `quoteAllText` on sink and configured `quote`/`escape` chars in the downstream Spark reader|
|Pipeline dependency ordering|Set `CopyHealthData` as predecessor — Databricks notebooks only trigger after copy Succeeds|
|Dataset naming typo (`HeathCareData`)|Retained as-is to match existing ADF dataset reference; rename in ADF UI if re-deploying|



