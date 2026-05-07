# Step 4 — Power BI Visualisation

## Overview

Power BI Desktop connects to Azure SQL Database in Import mode and renders 6 chart visuals matching the project dashboard. All 12 tables are loaded. Slicers applied to any visual automatically cross-filter the entire dashboard through Power BI's relationship model.

\---

## Connection Setup

|Setting|Value|
|-|-|
|Data source|Azure SQL Database|
|Server|`sqldbc37.database.windows.net`|
|Database|`AzureSQLDB`|
|Authentication|Database — SQL credentials|
|Username|`siddharthdb`|
|Connectivity mode|Import|
|Tables loaded|All 12 (see table list below)|

### Steps

1. Open Power BI Desktop → **Home** → **Get Data** → search **Azure SQL Database** → Connect
2. Enter server `sqldbc37.database.windows.net` and database `AzureSQLDB` → OK
3. Choose **Database** authentication → enter username and password → Connect
4. In the Navigator, check all 12 tables → click **Load**

\---

## Tables Loaded

|SQL Table|Purpose|
|-|-|
|`patient\_age\_group\_summary`|Hospital Usage by Age Group chart|
|`age\_group\_condition\_summary`|Medical Conditions Across Age Groups chart|
|`insurance\_avg\_billing\_summary`|Average Billing by Insurance Provider chart|
|`gender\_condition\_summary`|Medical Condition by Gender chart|
|`blood\_type\_condition\_summary`|Blood Type vs Medical Condition chart|
|`condition\_billing\_summary`|Average Billing by Medical Condition chart|
|`patient\_demographics`|Patient dimension table — used for slicers|
|`medical\_conditions\_summary`|Condition-level KPI cards|
|`hospital\_performance\_metrics`|Hospital-level KPI cards|
|`doctor\_performance\_summary`|Doctor-level KPI cards|
|`admission\_type\_analysis`|Admission type KPI cards|
|`insurance\_provider\_analysis`|Insurer-level KPI cards|

\---

## Chart Mapping

### Chart 1 — Hospital Usage by Age Group

* **Visual type:** Clustered Bar Chart
* **Table:** `patient\_age\_group\_summary`
* **X-axis:** `age\_group`
* **Y-axis:** `patient\_count`
* **Sort:** `patient\_count` Descending

### Chart 2 — Medical Conditions Across Age Groups

* **Visual type:** Stacked Bar Chart
* **Table:** `age\_group\_condition\_summary`
* **X-axis:** `age\_group`
* **Y-axis:** `patient\_count`
* **Legend:** `medical\_condition`

### Chart 3 — Average Billing by Insurance Provider

* **Visual type:** Clustered Bar Chart
* **Table:** `insurance\_avg\_billing\_summary`
* **X-axis:** `insurance\_provider`
* **Y-axis:** `avg\_billing\_amount`
* **Sort:** `avg\_billing\_amount` Descending

### Chart 4 — Medical Condition by Gender

* **Visual type:** Stacked Bar Chart
* **Table:** `gender\_condition\_summary`
* **X-axis:** `gender`
* **Y-axis:** `patient\_count`
* **Legend:** `medical\_condition`

### Chart 5 — Blood Type vs Medical Condition

* **Visual type:** Stacked Bar Chart
* **Table:** `blood\_type\_condition\_summary`
* **X-axis:** `blood\_type`
* **Y-axis:** `patient\_count`
* **Legend:** `medical\_condition`

### Chart 6 — Average Billing by Medical Condition

* **Visual type:** Clustered Bar Chart
* **Table:** `condition\_billing\_summary`
* **X-axis:** `medical\_condition`
* **Y-axis:** `avg\_billing\_amount`
* **Sort:** `avg\_billing\_amount` Descending

\---

\---

\---

## Model View — Relationships

Go to **Model view** and verify these relationships exist (Power BI usually auto-detects them):

|From Table|Column|To Table|Column|Cardinality|
|-|-|-|-|-|
|`patient\_demographics`|`medical\_condition`|`medical\_conditions\_summary`|`condition\_name`|Many → One|
|`patient\_demographics`|`medical\_condition`|`gender\_condition\_summary`|`medical\_condition`|Many → Many|
|`patient\_demographics`|`medical\_condition`|`condition\_billing\_summary`|`medical\_condition`|Many → One|
|`patient\_demographics`|`admission\_type`|`admission\_type\_analysis`|`admission\_type`|Many → One|
|`patient\_demographics`|`insurance\_provider`|`insurance\_provider\_analysis`|`insurance\_provider`|Many → One|

\---

## Polish Checklist

* \[ ] Sort bars: click visual → **…** menu → **Sort axis** → choose field → **Sort descending**
* \[ ] Y-axis number format: Format pane → Y-axis → Display units → **Thousands**
* \[ ] Billing fields: Format pane → value format → **$ English (US)** fixed decimal
* \[ ] Legend colours: Format pane → Legend → manually assign consistent colours per `medical\_condition`
* \[ ] Chart titles: Format pane → Title → enter descriptive title for each visual
* \[ ] Gridlines: Format pane → Gridlines → reduce opacity to 20–30% for cleaner look

\---

## Refresh After Pipeline Re-run

When the ADF pipeline re-runs and overwrites the SQL tables:

1. Open the `.pbix` file in Power BI Desktop
2. Click **Home** → **Refresh**
3. All 12 tables re-import and all visuals update automatically

For scheduled refresh via Power BI Service:

1. Publish the `.pbix` to a Power BI workspace
2. Go to dataset **Settings** → **Scheduled refresh**
3. Add Azure SQL credentials under **Data source credentials**
4. Set refresh frequency (e.g. Daily at 6 AM)

\---

## Challenges \& Resolutions

|Challenge|Resolution|
|-|-|
|Cross-filter not working between visuals|Go to Model view → verify relationships on shared columns; set cross-filter direction to **Both** if needed|
|Bars not sorting as expected|Use the **Sort axis** menu on the visual directly — not column sort in the Fields pane|
|Billing values displaying as whole numbers|Change value format to **Fixed decimal number** → set decimal places to 2 in Format pane|
|All insurance provider billing bars same height|Expected — billing is uniformly distributed across insurers in this dataset; reflects data distribution, not a bug|
|Legend colours inconsistent across pages|Manually pin colours per `medical\_condition` value in Format → Legend → Customize series|



