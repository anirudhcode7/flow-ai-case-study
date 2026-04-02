# FlowAI Healthcare Data Platform

A synthetic healthcare data engineering platform demonstrating a multi-source Bronze/Silver/Gold DuckDB pipeline with realistic cross-system patient identity challenges.

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11+ | `python3 --version` |
| Java | 17+ | Required to run Synthea |
| Claude Code CLI | Latest | `claude --version` — used for LLM gap-filling |

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

## Step 1: Run Synthea

Synthea generates realistic synthetic patient records as CSV.

```bash
# Download Synthea (if not already present)
wget https://github.com/synthetichealth/synthea/releases/latest/download/synthea-with-dependencies.jar

# Generate 600 patients in California (seed 42 for reproducibility)
java -jar synthea-with-dependencies.jar -s 42 -p 600 \
  --exporter.csv.export=true \
  --exporter.fhir.export=false \
  --exporter.ccda.export=false \
  California

# Copy output to the expected location
cp -r output/csv/* data/raw/synthea_output/
```

Expected files in `data/raw/synthea_output/`:
- `patients.csv`, `encounters.csv`, `conditions.csv`, `procedures.csv`, `providers.csv`, `organizations.csv`

---

## Step 2: Generate Synthetic Data

```bash
python ingestion/generate/run_generate.py
```

This runs all 3 generators in order:
1. **EMR** — transforms Synthea CSVs into 5 EMR tables
2. **RCM** — generates 5 Revenue Cycle Management tables
3. **Referral** — generates 3 Personal Injury referral tables

Output CSVs land in `data/raw/{emr,rcm,referral}/`.

### Dry-Run Mode (no LLM calls)

```bash
python ingestion/generate/run_generate.py --dry-run
```

Skips all Claude Code CLI calls and uses Faker for every field. Use this to test the pipeline without Claude Pro.

---

## Step 3: Load Bronze Layer

```bash
python ingestion/bronze/load_bronze.py
```

Reads all CSVs, adds metadata columns (`_source_system`, `_source_file`, `_row_hash`, `_ingested_at`), creates the DuckDB `bronze` schema, and bulk-loads all 13 tables.

DuckDB file: `data/duckdb/flowai.duckdb`

---

## Project Structure

```
flow-ai/
├── ingestion/
│   ├── generate/
│   │   ├── config.py          # All constants and paths
│   │   ├── llm_filler.py      # Tier 2: Claude Haiku via CLI subprocess
│   │   ├── helpers.py         # Tier 3: Faker utilities
│   │   ├── reference/
│   │   │   └── snomed_to_cpt_crosswalk.csv
│   │   ├── generators/
│   │   │   ├── emr_from_synthea.py
│   │   │   ├── rcm.py
│   │   │   └── referral.py
│   │   └── run_generate.py    # Master entry point
│   └── bronze/
│       ├── schema.sql         # 13 bronze table DDL
│       └── load_bronze.py     # CSV → DuckDB loader
├── transform/
│   └── dbt_flowai/            # dbt-duckdb project (Silver layer — Phase 2)
│       ├── models/staging/    # 13 thin views over bronze sources
│       ├── models/silver/     # 13 typed, deduped Silver tables
│       └── SILVER_LAYER.md    # Transformation docs and test results
├── entity_resolution/         # (Phase 4)
├── analytics/                 # (Phase 5)
├── data/
│   ├── raw/
│   │   ├── synthea_output/    # Synthea CSVs (gitignored)
│   │   ├── emr/               # Generated EMR CSVs
│   │   ├── rcm/               # Generated RCM CSVs
│   │   └── referral/          # Generated Referral CSVs
│   └── duckdb/
│       └── flowai.duckdb      # Single DuckDB file (gitignored)
└── docs/
```

---

## Data Architecture

### Tiered Generation
- **Tier 1**: Synthea (Java) — clinically realistic base patient/encounter data
- **Tier 2**: Claude Haiku via `claude` CLI subprocess — contextually coherent field filling
- **Tier 3**: Faker — mechanical fallback when LLM unavailable

### Intentional Messiness (for entity resolution training)
| Issue | Rate | Tables |
|-------|------|--------|
| RCM–EMR patient link gaps | 30% | `rcm_patient_account` |
| Null referring NPI | 20% | `referral_order` |
| DOB year off by ±1 | 10% | Cross-system |
| DOB month/day swapped | 5% | Cross-system |
| Name variants (case, suffix) | ~50% | RCM, Referral |

### Bronze Layer
All 13 source tables land in the `bronze` schema of `flowai.duckdb`. All columns are `VARCHAR` (raw landing zone). Metadata columns added on ingest: `_ingested_at`, `_source_system`, `_source_file`, `_row_hash`.

---

## LLM Gap-Filling Notes

The pipeline uses the **Claude Code CLI** (`claude` binary) via Python `subprocess`, not the Anthropic Python SDK. This requires:
- Claude Code CLI installed and authenticated (`claude --version` works)
- An active Claude Pro subscription

The `--dry-run` flag bypasses all LLM calls for offline / CI use.
