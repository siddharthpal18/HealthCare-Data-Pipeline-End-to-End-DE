# Step 3 — Major Transformation (Silver → Gold + Azure SQL)

## Overview
The `Major_Transformation_SQLandGold` Databricks notebook reads cleaned Parquet from the Silver layer, derives an `age_group` column, and produces 12 aggregated DataFrames. Each table is written simultaneously to two destinations:

- **Azure SQL Database** — via JDBC for direct Power BI Import / DirectQuery
- **ADLS Gold Layer** — as partitioned Parquet for downstream analytics and reuse

---

## Configuration

| Setting | Value |
|---|---|
| Notebook path | `/Users/siddharthpal1990@gmail.com/Transformation_PySpark/Major_Transformation_SQLandGold` |
| Linked service | AzureDatabricksLS |
| Activity type | DatabricksNotebook |
| Depends on | Notebook_DataBricks_MinTransfm — Succeeded |
| Timeout | 12 hours |
| Spark app name | `SilverToSQLAndGold_PowerBI` |
| Silver source | `abfss://silver@adlstoragesiddharth.dfs.core.windows.net/healthcare/patient_data` |
| Gold base path | `abfss://gold@adlstoragesiddharth.dfs.core.windows.net/healthcare` |
| SQL server | `sqldbc37.database.windows.net` |
| SQL database | `AzureSQLDB` |
| SQL username | `siddharthdb` |
| SQL driver | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| Write mode | overwrite (both destinations) |
| Secret scope | `DBSecrets` → keys `adls`, `azsqldb` |

---

## Age Group Derivation

Applied once on `df_silver` and reused across all aggregations:

```python
df = df_silver.withColumn(
    "age_group",
    when(col("Age") < 18,  "Child")
   .when(col("Age") < 60,  "Adult")
   .otherwise("Senior")
)
```

| Age Range | Group |
|---|---|
| < 18 | Child |
| 18 – 59 | Adult |
| 60+ | Senior |

---

## Dual-Write Helper

```python
def write_to_sql(df, table_name, mode="overwrite"):
    df.write \
        .format("jdbc") \
        .mode(mode) \
        .option("url",      SQL_URL) \
        .option("dbtable",  table_name) \
        .option("user",     SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

def write_to_gold(df, folder_name, partition_by=None, mode="overwrite"):
    path   = f"{ADLS_GOLD_BASE}/{folder_name}"
    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)

def write_both(df, table_name, folder_name, partition_by=None):
    write_to_sql(df, table_name)
    write_to_gold(df, folder_name, partition_by)
```

---

## 12 Aggregated Tables

| # | SQL / Gold Table | Partition By | Power BI Visual |
|---|---|---|---|
| 1 | `patient_age_group_summary` | — | Hospital Usage by Age Group (bar) |
| 2 | `age_group_condition_summary` | age_group | Medical Conditions Across Age Groups (stacked bar) |
| 3 | `insurance_avg_billing_summary` | — | Average Billing by Insurance Provider (bar) |
| 4 | `gender_condition_summary` | gender | Medical Condition by Gender (stacked bar) |
| 5 | `blood_type_condition_summary` | blood_type | Blood Type vs Medical Condition (stacked bar) |
| 6 | `condition_billing_summary` | — | Average Billing by Medical Condition (bar) |
| 7 | `patient_demographics` | gender | Patient dimension table |
| 8 | `medical_conditions_summary` | — | Condition-level KPIs |
| 9 | `hospital_performance_metrics` | — | Hospital-level KPIs |
| 10 | `doctor_performance_summary` | — | Doctor-level KPIs |
| 11 | `admission_type_analysis` | admission_type | Admission type KPIs |
| 12 | `insurance_provider_analysis` | — | Insurer-level KPIs |

---

## Aggregation Code — All 12 Tables

### Table 1 — patient_age_group_summary
```python
df_age_group = (
    df
    .groupBy("age_group")
    .agg(
        count("*")                                   .alias("patient_count"),
        spark_round(avg("Age"), 2)                   .alias("avg_age"),
        min("Age")                                   .alias("min_age"),
        max("Age")                                   .alias("max_age"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_billing_amount")
    )
    .orderBy(desc("patient_count"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_age_group, "patient_age_group_summary", "patient_age_group_summary")
```

### Table 2 — age_group_condition_summary
```python
df_age_condition = (
    df
    .groupBy("age_group", "Medical Condition")
    .agg(
        count("*")                             .alias("patient_count"),
        spark_round(avg("Billing Amount"), 2)  .alias("avg_billing_amount")
    )
    .withColumnRenamed("Medical Condition", "medical_condition")
    .orderBy("age_group", desc("patient_count"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_age_condition, "age_group_condition_summary",
           "age_group_condition_summary", partition_by="age_group")
```

### Table 3 — insurance_avg_billing_summary
```python
df_insurance_billing = (
    df
    .groupBy("Insurance Provider")
    .agg(
        count("*")                                   .alias("covered_patients"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_billing_amount"),
        spark_round(min("Billing Amount"), 2)        .alias("min_billing_amount"),
        spark_round(max("Billing Amount"), 2)        .alias("max_billing_amount")
    )
    .withColumnRenamed("Insurance Provider", "insurance_provider")
    .orderBy(desc("avg_billing_amount"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_insurance_billing, "insurance_avg_billing_summary",
           "insurance_avg_billing_summary")
```

### Table 4 — gender_condition_summary
```python
df_gender_condition = (
    df
    .groupBy("Gender", "Medical Condition")
    .agg(
        count("*")                             .alias("patient_count"),
        spark_round(avg("Age"), 2)             .alias("avg_age"),
        spark_round(avg("Billing Amount"), 2)  .alias("avg_billing_amount")
    )
    .withColumnRenamed("Gender",            "gender")
    .withColumnRenamed("Medical Condition", "medical_condition")
    .orderBy("gender", desc("patient_count"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_gender_condition, "gender_condition_summary",
           "gender_condition_summary", partition_by="gender")
```

### Table 5 — blood_type_condition_summary
```python
df_blood_condition = (
    df
    .groupBy("Blood Type", "Medical Condition")
    .agg(
        count("*")                             .alias("patient_count"),
        spark_round(avg("Billing Amount"), 2)  .alias("avg_billing_amount")
    )
    .withColumnRenamed("Blood Type",        "blood_type")
    .withColumnRenamed("Medical Condition", "medical_condition")
    .orderBy("blood_type", desc("patient_count"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_blood_condition, "blood_type_condition_summary",
           "blood_type_condition_summary", partition_by="blood_type")
```

### Table 6 — condition_billing_summary
```python
df_condition_billing = (
    df
    .groupBy("Medical Condition")
    .agg(
        count("*")                                   .alias("patient_count"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_billing_amount"),
        spark_round(min("Billing Amount"), 2)        .alias("min_billing_amount"),
        spark_round(max("Billing Amount"), 2)        .alias("max_billing_amount"),
        spark_round(avg("Age"), 2)                   .alias("avg_age")
    )
    .withColumnRenamed("Medical Condition", "medical_condition")
    .orderBy(desc("avg_billing_amount"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_condition_billing, "condition_billing_summary", "condition_billing_summary")
```

### Table 7 — patient_demographics
```python
df_patient_demographics = (
    df_silver
    .select(
        col("Name")               .alias("patient_name"),
        col("Age")                .alias("age"),
        col("Gender")             .alias("gender"),
        col("Blood Type")         .alias("blood_type"),
        col("Insurance Provider") .alias("insurance_provider"),
        col("Date of Admission")  .alias("admission_date"),
        col("Discharge Date")     .alias("discharge_date"),
        col("Medical Condition")  .alias("medical_condition"),
        col("Admission Type")     .alias("admission_type"),
        col("Hospital")           .alias("hospital"),
        col("Doctor")             .alias("doctor"),
        col("Billing Amount")     .alias("billing_amount"),
        col("Room Number")        .alias("room_number"),
        col("Medication")         .alias("medication"),
        col("Test Results")       .alias("test_results")
    )
    .dropDuplicates(["patient_name", "admission_date"])
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_patient_demographics, "patient_demographics",
           "patient_demographics", partition_by="gender")
```

### Table 8 — medical_conditions_summary
```python
df_conditions = (
    df_silver
    .groupBy("Medical Condition")
    .agg(
        count("*")                                   .alias("patient_count"),
        spark_round(avg("Age"), 2)                   .alias("avg_age"),
        min("Age")                                   .alias("min_age"),
        max("Age")                                   .alias("max_age"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_amount"),
        spark_round(max("Billing Amount"), 2)        .alias("max_billing_amount"),
        spark_round(min("Billing Amount"), 2)        .alias("min_billing_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_billing_amount")
    )
    .withColumnRenamed("Medical Condition", "condition_name")
    .orderBy(desc("patient_count"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_conditions, "medical_conditions_summary", "medical_conditions_summary")
```

### Table 9 — hospital_performance_metrics
```python
df_hospital_metrics = (
    df_silver
    .groupBy("Hospital")
    .agg(
        count("*")                                   .alias("total_patients"),
        spark_round(avg("Age"), 2)                   .alias("avg_patient_age"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_revenue"),
        spark_round(
            count(when(col("Test Results") == "Normal", 1)) / count("*") * 100, 2
        )                                            .alias("normal_test_pct"),
        count(when(col("Admission Type") == "Emergency", 1)) .alias("emergency_admissions"),
        count(when(col("Admission Type") == "Urgent",    1)) .alias("urgent_admissions"),
        count(when(col("Admission Type") == "Elective",  1)) .alias("elective_admissions")
    )
    .withColumnRenamed("Hospital", "hospital_name")
    .orderBy(desc("total_patients"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_hospital_metrics, "hospital_performance_metrics",
           "hospital_performance_metrics")
```

### Table 10 — doctor_performance_summary
```python
df_doctor_metrics = (
    df_silver
    .groupBy("Doctor")
    .agg(
        count("*")                                   .alias("patients_treated"),
        spark_round(avg("Age"), 2)                   .alias("avg_patient_age"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_billing_per_patient"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_revenue"),
        count(when(col("Test Results") == "Normal",       1)) .alias("normal_count"),
        count(when(col("Test Results") == "Abnormal",     1)) .alias("abnormal_count"),
        count(when(col("Test Results") == "Inconclusive", 1)) .alias("inconclusive_count")
    )
    .withColumnRenamed("Doctor", "doctor_name")
    .orderBy(desc("patients_treated"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_doctor_metrics, "doctor_performance_summary", "doctor_performance_summary")
```

### Table 11 — admission_type_analysis
```python
df_admission_analysis = (
    df_silver
    .groupBy("Admission Type")
    .agg(
        count("*")                                   .alias("total_admissions"),
        spark_round(avg("Age"), 2)                   .alias("avg_patient_age"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_cost"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_cost"),
        spark_round(
            avg(datediff(col("Discharge Date"), col("Date of Admission"))), 2
        )                                            .alias("avg_length_of_stay_days")
    )
    .withColumnRenamed("Admission Type", "admission_type")
    .orderBy(desc("total_admissions"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_admission_analysis, "admission_type_analysis",
           "admission_type_analysis", partition_by="admission_type")
```

### Table 12 — insurance_provider_analysis
```python
df_insurance_analysis = (
    df_silver
    .groupBy("Insurance Provider")
    .agg(
        count("*")                                   .alias("covered_patients"),
        spark_round(avg("Billing Amount"), 2)        .alias("avg_claim_amount"),
        spark_round(spark_sum("Billing Amount"), 2)  .alias("total_claims"),
        count(when(col("Medical Condition") == "Cancer",       1)) .alias("cancer_patients"),
        count(when(col("Medical Condition") == "Diabetes",     1)) .alias("diabetes_patients"),
        count(when(col("Medical Condition") == "Hypertension", 1)) .alias("hypertension_patients"),
        count(when(col("Medical Condition") == "Arthritis",    1)) .alias("arthritis_patients"),
        count(when(col("Medical Condition") == "Asthma",       1)) .alias("asthma_patients"),
        count(when(col("Medical Condition") == "Obesity",      1)) .alias("obesity_patients")
    )
    .withColumnRenamed("Insurance Provider", "insurance_provider")
    .orderBy(desc("covered_patients"))
    .withColumn("load_timestamp", current_timestamp())
)
write_both(df_insurance_analysis, "insurance_provider_analysis",
           "insurance_provider_analysis")
```

---

## Gold Layer Folder Structure

```
abfss://gold@adlstoragesiddharth.dfs.core.windows.net/healthcare/
  ├── patient_age_group_summary/
  ├── age_group_condition_summary/
  │   ├── age_group=Adult/
  │   ├── age_group=Senior/
  │   └── age_group=Child/
  ├── insurance_avg_billing_summary/
  ├── gender_condition_summary/
  │   ├── gender=Female/
  │   └── gender=Male/
  ├── blood_type_condition_summary/
  │   ├── blood_type=A+/
  │   ├── blood_type=A-/
  │   ├── blood_type=AB+/
  │   ├── blood_type=AB-/
  │   ├── blood_type=B+/
  │   ├── blood_type=B-/
  │   ├── blood_type=O+/
  │   └── blood_type=O-/
  ├── condition_billing_summary/
  ├── patient_demographics/
  │   ├── gender=Female/
  │   └── gender=Male/
  ├── medical_conditions_summary/
  ├── hospital_performance_metrics/
  ├── doctor_performance_summary/
  ├── admission_type_analysis/
  │   ├── admission_type=Emergency/
  │   ├── admission_type=Urgent/
  │   └── admission_type=Elective/
  └── insurance_provider_analysis/
```

---

## Challenges & Resolutions

| Challenge | Resolution |
|---|---|
| `NameError: df_patient_demographics not defined` | Old notebook defined all DataFrames in one cell — a mid-block failure left all variables undefined. Fixed by giving each table its own isolated cell |
| SQL JDBC timeout on large writes | Connection timeout set to `30s` in JDBC URL; `overwrite` mode avoids any merge/upsert overhead |
| Gold path conflict with Silver | Used separate `gold` container with `healthcare/` subfolder, fully independent from `silver` container |
| Too many small Parquet files from partitioning | Only low-cardinality columns used as partition keys (gender=2, age_group=3, blood_type=8, admission_type=3) to keep file count manageable |
| Column names with spaces (e.g. "Blood Type") | Used `.withColumnRenamed()` immediately after groupBy to produce clean snake_case SQL column names |
