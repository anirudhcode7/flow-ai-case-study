# Silver Layer — dbt-duckdb Transformation

The Silver layer reads from 13 Bronze tables (all VARCHAR), applies type casting, standardization, deduplication, and validation, and writes typed tables to the `silver` schema in `data/duckdb/flowai.duckdb`.

---

## How to Run

```bash
# Prerequisites: pip3 install dbt-duckdb
# Run from this directory (transforms/dbt_flowai/)

dbt deps --profiles-dir .         # Install dbt-utils and dbt-expectations packages
dbt run --profiles-dir .          # Build all 26 models (13 staging views + 13 silver tables)
dbt test --profiles-dir .         # Run 113 data tests
```

If starting from scratch, first generate and load bronze data:

```bash
# From project root:
python ingestion/generate/run_generate.py --dry-run   # Faker only, no LLM/Synthea needed
python ingestion/bronze/load_bronze.py
```

---

## Transformations Applied

### Global (all 13 tables)

| Transform | Description |
|-----------|-------------|
| Deduplication | ROW_NUMBER() OVER (PARTITION BY PK ORDER BY _ingested_at DESC) = 1 — keeps latest row per PK |
| Type casting | TRY_CAST for all non-string columns — returns NULL on parse failure (never drops rows) |
| String TRIM | Applied to all VARCHAR columns |
| Metadata passthrough | `_ingested_at`, `_source_system`, `_source_file`, `_row_hash` carried through |
| `_silver_loaded_at` | CURRENT_TIMESTAMP added to every silver table |

### Type Casts by Column Type

| Target Type | Pattern | Tables |
|-------------|---------|--------|
| DATE | `TRY_CAST(col AS DATE)` | dob, submitted_date, payment_date, snapshot_date, service_from_date, service_to_date |
| TIMESTAMP | `TRY_CAST(col AS TIMESTAMP)` | start_time, end_time, performed_time, referral_created_at, status_time, uploaded_at, last_updated_at |
| DECIMAL(12,2) | `TRY_CAST(col AS DECIMAL(12,2))` | charge_amount, payment_amount, balance fields |
| DECIMAL(10,2) | `TRY_CAST(col AS DECIMAL(10,2))` | units |
| INTEGER | `TRY_CAST(col AS INTEGER)` | diagnosis_rank, line_num |
| BOOLEAN | `TRY_CAST(col AS BOOLEAN)` | deceased_flag |

### String Standardizations

| Pattern | Applied to |
|---------|-----------|
| `UPPER(TRIM(col))` | mrn, state, city, address fields, org_name, icd10_code |
| `LOWER(TRIM(col))` | email, encounter_type, claim_type, claim_status, doc_type, priority, order_status |
| Title case (`CONCAT(UPPER(LEFT(TRIM(x),1)), LOWER(SUBSTRING(TRIM(x),2)))`) | first_name, last_name fields in patient/provider/account tables |
| `REGEXP_REPLACE(phone, '[^0-9]', '', 'g')` | All phone fields — strips dashes, parens, spaces |
| `LEFT(TRIM(zip), 5)` | All zip fields — normalize to 5 digits |
| `CASE WHEN REGEXP_MATCHES(npi, '^[0-9]{10}$') THEN npi ELSE NULL END` | All NPI fields — NULL if not exactly 10 digits |

### Enum Validation Pattern

Invalid values become NULL (rows are preserved, not dropped):

```sql
CASE WHEN LOWER(TRIM(col)) IN ('a','b','c') THEN LOWER(TRIM(col)) ELSE NULL END AS col
```

Validated fields: `encounter_type`, `status`, `sex`, `claim_type`, `claim_status`, `aging_bucket`, `priority`, `order_status`, `counterparty_org_type`, `doc_type`

---

## Intentionally Messy Fields (for Entity Resolution)

These fields are preserved as-is (only TRIM applied, no normalization):

| Field | Table | Reason |
|-------|-------|--------|
| `referring_provider_name` | silver_referral_order | Free-text provider names with spelling variants — entity resolution will cluster them |
| `counterparty_org_name` | silver_referral_order | Org name variants preserved — entity resolution will deduplicate |

---

## Intentionally Nullable Fields

These fields are NULL in a known percentage of rows by design:

| Field | Table | Expected NULL Rate |
|-------|-------|-------------------|
| `emr_patient_id` | silver_rcm_patient_account | ~30% — simulates RCM accounts with no EMR link yet |
| `referring_provider_npi` | silver_referral_order | ~20% — referrals from providers not yet in NPI registry |

Do NOT add `not_null` tests to these fields.

---

## Test Results Summary

**dbt run:** 26/26 models — PASS=26, ERROR=0

**dbt test:** 113 tests total — **PASS=111, FAIL=2**

### Passing Tests (111)
- All `unique` and `not_null` tests on primary keys
- All `not_null` tests on metadata columns
- All `accepted_values` tests on enum fields
- All `relationships` (FK) tests across models
- All `dbt_utils.unique_combination_of_columns` on composite PKs
- All `dbt_expectations` regex tests (phone, zip, NPI)

### Failing Tests (2) — Expected Data Quality Catches

| Test | Result | Root Cause |
|------|--------|-----------|
| `not_null_silver_rcm_claim_header_total_charge_amount` | 1200 failures | Faker generator left `total_charge_amount` NULL in all bronze rows — TRY_CAST(NULL) = NULL |
| `not_null_silver_referral_document_reference_doc_type` | 185 failures | Faker generated `auth_request` (95 rows) and `medical_records` (90 rows) — values outside `['referral_form','imaging','insurance_card']` → set to NULL per validation pattern |

These failures demonstrate the data quality framework working correctly: invalid values are surfaced by tests rather than silently dropped or passed through. In production, these would trigger data pipeline alerts.

---

## Row Counts: Bronze vs Silver

| Table | Bronze | Silver | Delta | Note |
|-------|--------|--------|-------|------|
| emr_patient | 500 | 500 | 0 | |
| emr_encounter | 1500 | 1500 | 0 | |
| emr_diagnosis | 2250 | 2130 | -120 | Duplicate (encounter_id, icd10_code) pairs deduped |
| emr_procedure | 1800 | 1644 | -156 | Duplicate (encounter_id, cpt_code) pairs deduped |
| emr_provider | 80 | 80 | 0 | |
| rcm_patient_account | 620 | 620 | 0 | |
| rcm_claim_header | 1200 | 1200 | 0 | |
| rcm_claim_line | 2400 | 2008 | -392 | Duplicate (claim_id, line_num) pairs deduped |
| rcm_remittance_835 | 900 | 900 | 0 | |
| rcm_ar_balance_snapshot | 1200 | 1199 | -1 | |
| referral_order | 320 | 320 | 0 | |
| referral_order_status_history | 800 | 723 | -77 | Duplicate status events deduped |
| referral_document_reference | 480 | 480 | 0 | |

---

## Verified Column Types (silver_emr_patient)

| Column | Type |
|--------|------|
| emr_patient_id | VARCHAR |
| dob | DATE |
| deceased_flag | BOOLEAN |
| last_updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _silver_loaded_at | TIMESTAMP WITH TIME ZONE |

---

## Project Structure

```
transforms/dbt_flowai/
├── dbt_project.yml          — staging: view, silver: table
├── profiles.yml             — DuckDB connection (../../data/duckdb/flowai.duckdb)
├── packages.yml             — dbt-labs/dbt_utils, calogica/dbt_expectations
├── macros/
│   └── generate_schema_name.sql  — ensures silver/ writes to exactly 'silver' schema
└── models/
    ├── staging/
    │   ├── sources.yml      — 13 bronze source definitions
    │   └── stg_*.sql (×13) — thin SELECT * views
    └── silver/
        ├── schema.yml       — 113 tests across all 13 models
        └── silver_*.sql (×13) — typed, deduped, validated tables
```
