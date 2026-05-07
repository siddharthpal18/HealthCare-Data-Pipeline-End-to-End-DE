# Step 2 — Minor Transformation (Bronze → Silver)

## Overview
The `MinorTransformation` Databricks notebook reads raw CSV from the Bronze layer, applies string cleaning, type casting, date parsing, and basic row filtering, then writes the result as Parquet to the Silver layer. It is the second activity in `HealthDataset_Pipeline` and only runs after the Copy Activity succeeds.

---

## Configuration

| Setting | Value |
|---|---|
| Notebook path | `/Users/siddharthpal1990@gmail.com/Transformation_PySpark/MinorTransformation` |
| Linked service | AzureDatabricksLS |
| Activity type | DatabricksNotebook |
| Depends on | CopyHealthData — Succeeded |
| Timeout | 12 hours |
| Retry | 0 |
| Spark app name | `Healthcare_Bronze_to_Silver_Parquet` |
| Source path | `abfss://bronze@adlstoragesiddharth.dfs.core.windows.net/Healthcare_Dataset` |
| Sink path | `abfss://silver@adlstoragesiddharth.dfs.core.windows.net/healthcare/patient_data` |
| Output format | Parquet — overwrite mode |
| Secret scope | `DBSecrets` → key `adls` |

---

## Transformations Applied

| Column | Operation | Detail |
|---|---|---|
| Name | `initcap(trim(...))` | Standardises casing, removes leading/trailing whitespace |
| Gender | `initcap(trim(...))` | e.g. `" female"` → `"Female"` |
| Blood Type | `upper(trim(...))` | Ensures e.g. `ab-` → `AB-` |
| Medical Condition | `initcap(trim(...))` | Standardises casing |
| Doctor | `initcap(trim(...))` | Standardises casing |
| Insurance Provider | `initcap(trim(...))` | Standardises casing |
| Medication | `initcap(trim(...))` | Standardises casing |
| Test Results | `initcap(trim(...))` | Standardises casing |
| Hospital | `regexp_replace` + `initcap(trim(...))` | Strips embedded `"` and `,` characters first, then standardises casing |
| Age | `cast(IntegerType())` | Ensures integer; filtered: `Age > 0 AND Age < 120` |
| Billing Amount | `cast(DoubleType())` | Ensures double; filtered: `Billing Amount >= 0` |
| Room Number | `cast(IntegerType())` | Ensures integer |
| Date of Admission | `to_date(...)` | Parses string to `DateType` |
| Discharge Date | `to_date(...)` | Parses string to `DateType` |
| processed_at | `current_timestamp()` | Audit column — records when the row was written to Silver |

---

## Full Code

### Cell 1 — Imports
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, upper, to_date,
    regexp_replace, current_timestamp, lit
)
from pyspark.sql.types import DoubleType, IntegerType
import logging
```

### Cell 2 — Setup & Authentication
```python
spark = SparkSession.builder \
    .appName("Healthcare_Bronze_to_Silver_Parquet") \
    .getOrCreate()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE_ACCOUNT  = "adlstoragesiddharth"
STORAGE_KEY      = dbutils.secrets.get("DBSecrets", "adls")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"

ADLS_BRONZE_PATH = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Healthcare_Dataset"
ADLS_SILVER_PATH = f"abfss://{CONTAINER_SILVER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthcare/patient_data"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)
```

### Cell 3 — Read from Bronze
```python
logger.info("Reading raw CSV data from Bronze...")

df_raw = spark.read.format("csv") \
    .option("header",      "true") \
    .option("inferSchema", "true") \
    .option("quote",       "\"") \
    .option("escape",      "\"") \
    .load(ADLS_BRONZE_PATH)
```

### Cell 4 — Clean & Transform
```python
logger.info("Applying cleaning logic...")

df_cleaned = (df_raw
    .withColumn("Name",               initcap(trim(col("Name"))))
    .withColumn("Gender",             initcap(trim(col("Gender"))))
    .withColumn("Blood Type",         upper(trim(col("Blood Type"))))
    .withColumn("Medical Condition",  initcap(trim(col("Medical Condition"))))
    .withColumn("Doctor",             initcap(trim(col("Doctor"))))
    .withColumn("Insurance Provider", initcap(trim(col("Insurance Provider"))))
    .withColumn("Medication",         initcap(trim(col("Medication"))))
    .withColumn("Test Results",       initcap(trim(col("Test Results"))))

    # Hospital — strip bad characters then standardise
    .withColumn("Hospital", regexp_replace(col("Hospital"), r'[",]', ""))
    .withColumn("Hospital", initcap(trim(col("Hospital"))))

    # Type casting
    .withColumn("Age",            col("Age").cast(IntegerType()))
    .withColumn("Billing Amount", col("Billing Amount").cast(DoubleType()))
    .withColumn("Room Number",    col("Room Number").cast(IntegerType()))

    # Date parsing
    .withColumn("Date of Admission", to_date(col("Date of Admission")))
    .withColumn("Discharge Date",    to_date(col("Discharge Date")))

    # Row filters
    .filter((col("Age") > 0) & (col("Age") < 120))
    .filter(col("Billing Amount") >= 0)
    .dropDuplicates()

    # Audit column
    .withColumn("processed_at", current_timestamp())
)
```

### Cell 5 — Write to Silver
```python
logger.info(f"Writing to Silver: {ADLS_SILVER_PATH}")

df_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save(ADLS_SILVER_PATH)

logger.info("Pipeline Execution Successful!")
```

### Cell 6 — Verify
```python
final_df = spark.read.format("parquet").load(ADLS_SILVER_PATH)
final_df.select("Name", "Age", "Blood Type", "Hospital").show(5)
```

---

## Sample Output (Verified)

```
+---------------+---+----------+--------------------+
|           Name|Age|Blood Type|            Hospital|
+---------------+---+----------+--------------------+
|Brandon Collins| 77|        O+|           Lopez Plc|
|   Jeffrey Wood| 81|       AB-|         Brown-yoder|
| Michael Jordan| 30|       AB-|     Hernandez-green|
| Melinda Tanner| 38|       AB-|            Ball Llc|
|  Brian Osborne| 30|        B-|Parker Turner Hou...|
+---------------+---+----------+--------------------+
```

---

## Challenges & Resolutions

| Challenge | Resolution |
|---|---|
| Hospital names with embedded commas and quotes | Used `regexp_replace(col("Hospital"), r'[",]', "")` before `initcap` to strip corrupt characters |
| `inferSchema` reads every column as string | Explicit `cast()` calls applied after read to enforce correct types |
| Duplicate records in source CSV | `.dropDuplicates()` added before the write step |
| `NameError` on downstream variable in Major Transformation | Fixed by isolating each table into its own cell in the Major Transformation notebook |
