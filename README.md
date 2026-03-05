# Python ETL Toolkit

Reusable ETL pipeline components for NbS and pharma BI data workflows.

## Features
- **Transform pipeline**: configurable step chain (normalize → deduplicate → hash)
- **Type coercion**: schema-driven column type enforcement
- **Deduplication**: key-based with removed record tracking
- **Row hashing**: MD5 hash for CDC (Change Data Capture) incremental loads
- **Load summary**: post-run report with null counts and dedup stats

## Quick Start

```python
from src.main import ETLToolkit

etl = ETLToolkit(config={"dedup_keys": ["record_id", "period"]})

# Extract
df = etl.extract("sample_data/nbs_raw_data.csv")

# Transform
df = etl.transform_pipeline(df, steps=["normalize", "deduplicate", "hash"])

# Coerce types
df = etl.coerce_types(df, {"area_ha": "float", "carbon_credits_tco2": "float"})

# Load summary
summary = etl.load_summary(df)
print(f"Loaded: {summary['records_loaded']} records")
print(f"Dedup removed: {summary['dedup_removed']}")
```

## Running Tests
```bash
pytest tests/ -v
```

---

## [v1.3.0] Incremental Load (CDC) & Data Quality Rules

```python
# Detect inserts and updates between pipeline runs (CDC pattern)
result = etl.incremental_load(df_new, df_existing, key_cols=["record_id"])
print(f"Inserts: {len(result['inserts'])}, Unchanged: {result['unchanged']}")

# Run data quality checks with rules
rules = [
    {"column": "record_id", "rule": "not_null"},
    {"column": "record_id", "rule": "unique"},
    {"column": "value_usd", "rule": "min", "value": 0},
]
dq = etl.data_quality_report(df, rules=rules)
print(f"Quality Score: {dq['quality_score']}% ({dq['passed']}/{dq['total_rules']} rules passed)")
# Quality Score: 66.7% (2/3 rules passed)
```
