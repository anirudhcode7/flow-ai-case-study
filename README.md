# FlowAI Healthcare Data Platform

End-to-end healthcare data engineering platform: multi-source ingestion (EMR, RCM, Referral Orders), patient entity resolution across conflicting identifiers, and a BI-ready star schema for AR analytics. Built with Python, dbt-duckdb, and SQL.

---

## Quick Start

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Generate data + load Bronze
python ingestion/generate/run_generate.py --dry-run
python ingestion/bronze/load_bronze.py

# Build Silver + Gold + Analytics, run all 174 tests
cd transform/dbt_flowai
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11+ | `python3 --version` |
| Java | 17+ | Required to run Synthea (optional with `--dry-run`) |
| Claude Code CLI | Latest | Optional — used for LLM gap-filling; `--dry-run` skips it |
| dbt-duckdb | 1.10+ | Installed via `requirements.txt` |

---

## Setup

```bash
# 1. Clone / enter the project
cd /path/to/flow-ai

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy environment template
cp .env.example .env
```

---

## Pipeline Steps

### Step 1: Run Synthea (optional)

Synthea generates realistic synthetic patient records as CSV. Skip this step if using `--dry-run` mode.

```bash
wget https://github.com/synthetichealth/synthea/releases/latest/download/synthea-with-dependencies.jar

java -jar synthea-with-dependencies.jar -s 42 -p 600 \
  --exporter.csv.export=true \
  --exporter.fhir.export=false \
  --exporter.ccda.export=false \
  California

cp -r output/csv/* data/raw/synthea_output/
```

### Step 2: Generate Synthetic Data

```bash
python ingestion/generate/run_generate.py        # Full mode (Synthea + Claude Haiku + Faker)
python ingestion/generate/run_generate.py --dry-run  # Faker-only, no LLM or Synthea needed
```

Runs 3 generators in dependency order (EMR first, then RCM and Referral which depend on EMR patient IDs). Uses a 3-tier generation model:
- **Tier 1 (Synthea):** Clinically realistic base data
- **Tier 2 (Claude Haiku):** Contextually coherent gap-filling via CLI subprocess
- **Tier 3 (Faker):** Mechanical fallback; used exclusively in `--dry-run` mode

Output: 13 CSV files in `data/raw/{emr,rcm,referral}/`.

### Step 3: Load Bronze Layer

```bash
python ingestion/bronze/load_bronze.py
```

Reads all CSVs, adds metadata columns (`_ingested_at`, `_source_system`, `_source_file`, `_row_hash`), and bulk-loads into DuckDB. All columns stored as VARCHAR (raw landing zone).

Output: 13 tables in `bronze` schema of `data/duckdb/flowai.duckdb`.

### Step 4: Build Silver + Gold Layers (dbt)

```bash
cd transform/dbt_flowai
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

This single command builds the entire transformation pipeline:

- **Silver (13 tables):** Type casting via `TRY_CAST`, name/phone/address standardization, deduplication by primary key, enum validation. Contract enforcement on all models.
- **Gold Entity Resolution (6 models):** Deterministic + probabilistic patient matching, label propagation clustering, survivorship-applied canonical patient dimension + crosswalk.
- **Gold Analytics (9 models):** Star schema with 6 dimensions, 1 bridge table, and 3 fact tables.

**174 dbt tests** validate uniqueness, not_null constraints, referential integrity, enum values, composite keys, and match confidence bounds.

---

## What This Implements

### A) Ingestion + Storage Architecture

Medallion architecture across 3 layers. Bronze preserves raw data as all-VARCHAR for auditability. Silver enforces typed schemas with `contract: {enforced: true}`, applies standardization (INITCAP names, UPPER addresses, digits-only phones), and deduplicates by primary key. Gold produces business entities and analytics models. Every row carries 4 metadata columns for lineage tracking.

### B) Patient Entity Resolution

Resolves 1,440 source records (500 EMR + 620 RCM + 320 Referral) into **738 canonical patients**. Six deterministic rules (ID link, MRN, SSN+DOB, phone+DOB, email+DOB, phone+lastname) produce 690 high-confidence pair-links. Probabilistic matching via Jaro-Winkler similarity with weighted scoring (name, DOB, phone, address, email) adds 627 fuzzy matches above a 0.70 threshold. Transitive links resolved via 4-pass label propagation. Output: `gold_dim_patient` (canonical records with survivorship) + `gold_bridge_patient_source_xref` (crosswalk with confidence and match method).

### C) Analytics Star Schema

Six dimensions (date, patient, provider, facility, counterparty_org, payer), one bridge table (patient-to-counterparty via referral crosswalk), and three fact tables: `fact_receivable` (claim + snapshot grain, 1,199 rows), `fact_patient_balance` (patient-level AR aggregation), `fact_claim_line` (line-level denial analysis, 31.1% denial rate). Answers 4 BI questions: top counterparties by pending receivables, patients with highest balances, AR aging by payer, and denial rate by payer.

---

## Project Structure

```
flow-ai/
├── ingestion/
│   ├── generate/
│   │   ├── config.py              # All constants, paths, messiness rates
│   │   ├── llm_filler.py          # Tier 2: Claude Haiku via CLI subprocess
│   │   ├── helpers.py             # Tier 3: Faker utilities + name variants
│   │   ├── generators/
│   │   │   ├── emr_from_synthea.py
│   │   │   ├── rcm.py
│   │   │   └── referral.py
│   │   └── run_generate.py        # Master entry point
│   └── bronze/
│       ├── schema.sql             # 13 bronze table DDL (all VARCHAR)
│       └── load_bronze.py         # CSV -> DuckDB loader
├── transform/dbt_flowai/
│   ├── dbt_project.yml
│   ├── profiles.yml               # DuckDB connection
│   ├── packages.yml               # dbt_utils, dbt_expectations
│   ├── models/
│   │   ├── staging/               # 13 views over bronze sources
│   │   ├── silver/                # 13 typed, deduped, validated tables
│   │   └── gold/
│   │       ├── entity_resolution/ # Patient spine, matching, dim_patient, crosswalk
│   │       └── analytics/         # Star schema: dims, bridge, facts
│   └── target/
├── data/
│   ├── raw/{emr,rcm,referral}/    # Generated CSVs (gitignored)
│   └── duckdb/flowai.duckdb       # Single DuckDB file (gitignored)
└── docs/
    ├── SOLUTION_BRIEF.md          # Case study submission document
    ├── ENTITY_RESOLUTION.md       # ER algorithm documentation
    ├── ANALYTICS_FRAMEWORK.md     # Star schema + BI query docs
    └── SILVER_LAYER.md            # Silver transformation docs
```

---

## Data Architecture

### Intentional Messiness (for entity resolution training)

| Issue | Rate | Tables |
|-------|------|--------|
| RCM-EMR patient link gaps | 30% | `rcm_patient_account` |
| Null referring NPI | 20% | `referral_order` |
| DOB year off by +/-1 | 10% | Cross-system |
| DOB month/day swapped | 5% | Cross-system |
| Name variants (case, suffix) | ~50% | RCM, Referral |

### Layer Summary

| Layer | Tables | Rows | Key Property |
|-------|-------:|-----:|-------------|
| Bronze | 13 | 13,050 | All VARCHAR, raw fidelity |
| Silver | 13 | 12,304 | Typed, deduped, contract-enforced |
| Gold (ER) | 6 | 5,575 | 738 canonical patients |
| Gold (Analytics) | 9 | 12,771 | Star schema, 100% dim coverage |

