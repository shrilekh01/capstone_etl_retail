

```markdown
# 📘 Retail ETL Platform

## 📌 Project Overview

This project is an **end-to-end ETL (Extract, Transform, Load) pipeline** built using **Python and MySQL**, designed to simulate a **real-world data warehouse ingestion system**.

It demonstrates:
- Incremental data loading
- Audit logging
- Data validation
- Layer-wise testing using Pytest
- Production-style ETL architecture
- Clean Git workflow

This project is suitable for **ETL / Data Engineer / Data QA roles**.

---

## 🏗️ High-Level Architecture

```

Source Tables (MySQL)
│
▼
Staging Layer
│
▼
Transformation Layer
│
▼
Fact Tables
│
▼
Audit Layer
│
▼
Pytest Validation

```

---

## 📂 Project Structure

```

retail-etl-platform/
│
├── etl_core/
│   ├── extraction/        # Source extraction logic
│   ├── transformations/   # Incremental & transformation logic
│   ├── loading/           # Load into warehouse
│   ├── audit/             # Audit logging
│   └── orchestration/     # Pipeline controller
│
├── etl_tests/
│   ├── layer1_source_to_staging/
│   ├── layer2_staging_to_transform/
│   ├── layer3_transform_to_fact/
│   ├── layer4_e2e/
│   └── layer5_audit/
│
├── pytest.ini
├── requirements.txt
└── README.md

````

---

## 🔁 ETL Flow Explanation

### 1️⃣ Extraction
- Reads data from MySQL source tables
- Uses `pandas.read_sql()`
- Supports table-level extraction

### 2️⃣ Incremental Loading
- Uses last processed date from `fact_sales`
- Prevents duplicate loading
- Restart-safe logic

### 3️⃣ Transformation
- Aggregation
- Filtering
- Monthly summaries
- Inventory calculations

### 4️⃣ Loading
Data is loaded into:
- `fact_sales`
- `inventory_levels_by_store`
- `monthly_sales_summary`

---

## 🧾 Audit Framework

Two audit tables are maintained:

### 🔹 etl_run_audit
Tracks overall pipeline execution

| Column | Description |
|------|-------------|
| run_id | ETL run identifier |
| pipeline_name | Name of pipeline |
| status | SUCCESS / FAILED |
| start_time | Execution start |
| end_time | Execution end |
| total_tables_loaded | Count |
| total_rows_loaded | Count |
| error_message | Failure reason |

### 🔹 table_load_audit
Tracks table-level execution

| Column | Description |
|------|-------------|
| run_id | FK to etl_run_audit |
| table_name | Table name |
| rows_loaded | Rows processed |
| status | SUCCESS / FAILED |
| error_message | Optional |

---

## 🧪 Testing Strategy

### ✔ Layer-wise Tests
| Layer | Purpose |
|------|--------|
| Layer 1 | Source validation |
| Layer 2 | Transformation validation |
| Layer 3 | Fact table validation |
| Layer 4 | End-to-end testing |
| Layer 5 | Audit verification |

### ✔ Audit Tests Validate
- ETL execution success
- Table-level audit entries
- Row counts
- Run-to-table mapping
- Failure handling

---

## ▶️ How to Run the Project

### 1️⃣ Install dependencies
```bash
pip install -r requirements.txt
````

### 2️⃣ Run ETL Pipeline

```bash
python -m etl_core.orchestration.pipeline
```

### 3️⃣ Run All Tests

```bash
pytest -v
```

### 4️⃣ Run Only Audit Tests

```bash
pytest etl_tests/layer5_audit -v
```

---

## 🧠 Key Features

✔ Incremental data loading
✔ Audit-driven ETL design
✔ Restart-safe execution
✔ Pytest-based validation
✔ Production-ready architecture
✔ Clean Git workflow

---

## 🚀 Future Enhancements

* GitHub Actions CI/CD
* SQLAlchemy-based connection layer
* Data quality checks
* Airflow orchestration
* Dockerized deployment

---

## 👤 Author

**Shrilekh Shrikhande**
ETL / Data Engineering Enthusiast
📍 India

---

