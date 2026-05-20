## Azure Data Factory

A medallion-architecture data pipeline built with Azure Data Factory (ADF), ingesting data from multiple sources into a Data Lake and transforming it through Bronze → Silver → Gold layers.

---

## Architecture Overview

```

┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCES                        │
│  On-Premises Files │ REST API (GitHub) │ Azure SQL DB   │
└────────────┬───────────────┬──────────────────┬─────────┘
             │               │                  │
             ▼               ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│              BRONZE LAYER  (Raw Ingestion)              │
│   CSV files │ JSON files │ Parquet files                │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│           SILVER LAYER  (Cleaned & Transformed)         │
│   Delta format │ Uppercased fields │ Derived columns    │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              GOLD LAYER  (Analytics)                    │
│         Car Sales Aggregates │ Student Grades           │
└─────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Azure Data Factory instance
- Azure Data Lake Storage Gen2 account with containers: `bronze`, `silver`, `gold`
    - bronze container contains `sqlmonitor` folder for incremental load tracking
    - `sqlmonitor/lastload/lastload.json` stores latest watermark timestamps
    - `sqlmonitor/empty/empty.json` used as template JSON file during watermark updates
- Azure SQL Database with tables containing an `Updated_at` watermark column
- Self-Hosted Integration Runtime (`local-self-host`) installed on the on-premises machine
- Azure Logic App endpoint configured for email alerts

## Repository Structure

```
├── linkedServices/
│   ├── ls_AzureSql          # Azure SQL Database linked service
│   ├── ls_datalake          # Azure Data Lake Storage Gen2
│   ├── ls_github            # HTTP linked service (GitHub raw content)
│   └── ls_onprem_file       # On-premises file server (Self-hosted IR)
│
|── datasets/
|   ├── ds_onprem                        # On-prem root folder
|   ├── ds_onprem_dynamic                # On-prem dynamic file
|   ├── ds_api_scr                       # GitHub HTTP API source
|   ├── ds_sql_scr                       # Azure SQL source (query driven)
|   ├── ds_datalake                      # Bronze CSV sink (on-prem files)
|   ├── ds_datalake_api                  # Bronze JSON sink (API)
|   ├── ds_datalake_bronze_sql_Parquet   # Bronze Parquet sink (SQL tables)
|   ├── ds_lastload_json                 # Watermark store (last load timestamp)
|   ├── ds_empty_json                    # Empty JSON (watermark update source)
|   ├── ds_student_names                 # Silver input: Student Names CSV
|   ├── ds_student_marks                 # Silver input: Student Marks CSV
|   ├── ds_student_grades                # Silver input: Grade table JSON
|   ├── ds_car_details                   # Silver input: Car Details Parquet
|   └── ds_car_sales_details             # Silver input: Car Sales Parquet 
|
├── pipelines/
│   ├── onprem_ingestion        # On-prem CSV ingestion
│   ├── API_Ingestion           # REST API ingestion
│   ├── SQL_to_Datalake         # Watermark-based SQL ingestion
│   ├── parent_pipeline         # Orchestrator pipeline
│   ├── Silver_Transformations  # Silver layer pipeline
│   └── Gold_Transformations    # Gold layer pipeline
│
└── dataflows/
    ├── Silver_DataTransformation  # Silver mapping data flow
    └── Gold_DataServing           # Gold mapping data flow
```

---
## Linked Services


| Name              | Type                         | Details                                                    |
|-------------------|------------------------------|------------------------------------------------------------|
| `ls_AzureSql`     | Azure SQL Database           | Server: `adfprojectservers.database.windows.net`, SQL Auth |
| `ls_datalake`     | Azure Blob FS (ADLS Gen2)    | Account: `adfprojectaccount`                               |
| `ls_github`       | HTTP Server                  | Base URL: `https://raw.githubusercontent.com/`, Anonymous  |
| `ls_onprem_file`  | File Server                  | Host: `C:\source`, Self-Hosted IR: `local-self-host`       |

---

## Pipelines

### onprem_ingestion
Reads CSV files from a local file server via a **Self-Hosted Integration Runtime** and copies them to the Bronze layer of the Data Lake.

- Uses `GetMetadata` to list all child items dynamically
- Iterates with `ForEach` and copies each file individually
- Sink: Azure Data Lake Storage Gen2 (CSV format)

---

### API_Ingestion
Pulls JSON data from a GitHub raw URL and lands it in the Bronze layer.

- Source: HTTP linked service (Anonymous auth)
- Mapped fields: `Marks`, `Grade`
- Sink: Azure Data Lake Storage Gen2 (JSON format)

---

### SQL_to_Datalake — Watermark-Based SQL Ingestion
Incrementally copies tables from Azure SQL Database to the Bronze layer using a watermark column (`Updated_at`).

`Lookup - GetSQLTables` — queries `INFORMATION_SCHEMA` for tables containing `Updated_at`
`ForEachTable` — for each table:
- `LastLoad` — reads last watermark timestamp from a JSON file in the lake
- `Copy_SQL_Data` — copies only new/changed rows as Parquet
- `CurrentTime` — captures current UTC timestamp
- `Update_LastLoad` — writes updated watermark back to the lake

---

### parent_pipeline — Orchestrator
The top-level pipeline that chains all ingestion pipelines in sequence and sends an email alert on completion.

---

### Email Alerting
The `Mail_Alert` activity in `parent_pipeline` sends a POST request to an Azure Logic App with the following payload:

```json
{
  "pipeline_name": "<pipeline name>",
  "run_id": "<run ID>",
  "status": "<Succeeded | Failed | Skipped>"
}
```

Configure the Logic App to forward this to your email of choice.

---

**Execution Order:**
1. `onprem_ingestion` 
2. `API_Ingestion` 
3. `SQL_to_Datalake` 
4. `Mail_Alert` — POST to Azure Logic App with pipeline name, run ID, and status

---

### Silver_Transformations — Silver Layer Pipeline
Executes the `Silver_DataTransformation` mapping data flow.

---

### Gold_Transformations — Gold Layer Pipeline
Executes the `Gold_DataServing` mapping data flow.

---

## Data Flows

## Silver_DataTransformation
There are **5 independent streams** — one per source — each ending in an upsert sink.

### Stream 1 — Student Names

```
[studentnames]  ──►  [derivedUpperCase]              ──►  [derivedFullname]  ──►            [alterRow1]    ──►  [sinkstudentnames]
 (Bronze CSV)         First_Name = upper(First_Name)      Full_Name =                       upsertIf(1==1)        silver/student_names
                      Last_Name  = upper(Last_Name)       concat(First_Name," ",Last_Name)                        Delta · key: Id 
```

### Stream 2 — Student Marks

```
[studentmarks]  ──►  [sortMarks]          ──►  [derivedUpper]              ──►  [alterRow2]        ──►  [sinkstudentmarks]
(Bronze CSV)         order by Marks DESC       Subject = upper(Subject)         upsertIf(1==1)          silver/student_marks
                                                                                                        Delta · key: Student_Id,
```

### Stream 3 — Grade Table

```
[studentgrades]  ──►  [derivedGrade]                     ──►  [alterRow3]        ──►  [sinkgrades]
(Bronze JSON)         Grade = replace(Grade,'F','FAIL')        upsertIf(1==1)          silver/grades_table
                                                                                       Delta · key: Marks
```

### Stream 4 — Car Details

```
[cardetails]     ──►  [castPrice]     ──►  [derivedUpperCases]      ──►  [alterRow4]     ──►  [sinkcardetails]
(Bronze Parquet)      Price → integer      All columns = upper(…)        upsertIf(1==1)        silver/car_details
                                                                                               Delta · key: VIN
```

### Stream 5 — Car Sales Details

```
[carsalesdetails]  ──►  [derivedColumnUpper]      ──►  [select]             ──►  [alterRow5]    ──►  [sinkcarsalesdetails]
(Bronze Parquet)        All columns = upper(…)         Buyer_Name   → Name       upsertIf(1==1)      silver/car_sales_details
                                                       Buyer_Gender →                                Delta · key: VIN
                                                       Buyer_City   → City
                                                       Buyer_State  → State
                                                       Loan_Taken   → Loan
                                                       drops: Dealer_Name
```

## `Gold_DataServing`
Produces two analytics-ready Gold Delta tables by joining and aggregating Silver data. Both streams run independently in parallel.

### Stream 1 — Car Sales Analytics

```
[CarDetails]-----──┐
                   ├──►[Innerjoin] ──►  [select] ──► [derivedSellDate] ──► [aggregate1] ──► [sort1] ──► [alterRow2]  ──►  [sink2]
[CarSalesDetails]──┘                  
```

### Stream 2 — Student Grades       

```
[StudentNames]──┐
                ├──►[Leftjoin] ─────────►[Crossjoin] ──►  [window1] ──►  [filter1] ──►  [select1] ──►  [alterRow1]  ──►  [sink1]
[StudentMarks]──┘
                                           
                                           │
[GradeTable] ─► [selectGradeTable] ------──┘
```
