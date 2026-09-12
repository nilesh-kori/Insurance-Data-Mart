# Multi-Source Insurance Data Mart Build

A PySpark pipeline that ingests three intentionally messy, inconsistent
source extracts — policy records, claims records, and CRM data — and builds
a single, clean, unified data mart, the way a real multi-source data
integration project would.

## Why the source data is "messy" on purpose

Real source systems rarely agree on formatting. This project simulates that
directly:

- **Inconsistent casing**: policy status values arrive as `"Active"`,
  `"active"`, `"ACTIVE"` — sometimes even blank
- **Inconsistent customer ID casing**: the CRM extract contains true
  duplicate customers differing only by case (`CUST0001` vs `cust0001`)
- **Inconsistent date formats**: the claims extract uses `DD/MM/YYYY` while
  everything else uses `YYYY-MM-DD`
- **Missing values**: some policy records have no premium recorded at all

## What the pipeline does

1. Loads all three raw CSV sources
2. Standardizes status, product, and customer ID casing
3. **Removes duplicate CRM records** that differ only by case
4. **Documents (rather than silently drops)** policy records with missing
   premium data — the exclusion is counted and reported, not hidden
5. Parses the claims date field from its inconsistent `DD/MM/YYYY` format
6. Joins policy, CRM, and aggregated claims data into one unified table
7. Writes the final unified mart back out to `unified_data_mart.csv`

## Real results from this run

| Metric | Value |
|---|---|
| Raw policy records | 500 |
| Raw claims records | 220 |
| Raw CRM records | 319 |
| Duplicate CRM records removed | 19 |
| Policy records excluded (missing premium) | 13 |
| **Final unified data mart rows** | **487** |
| Active policies | 233 |
| Total active monthly premium | $215,613.65 |
| Policies with at least one claim | 178 of 487 |

## Files

- `build_data_mart.py` — the full PySpark pipeline: load, clean, deduplicate,
  join, and export
- `source_policy_export.csv`, `source_claims_export.csv`,
  `source_customer_crm.csv` — the three raw, messy source extracts
- `unified_data_mart.csv` — the final cleaned, joined output

## Running it

```bash
pip install pyspark
python3 build_data_mart.py
```

Requires a Java runtime (PySpark runs on the JVM under the hood).

## Note on scope

This is a self-directed learning project using synthetic data designed to
mirror real-world data mart challenges — inconsistent source systems,
duplicate detection, and documented data quality decisions — not data from
any real company or production system.
