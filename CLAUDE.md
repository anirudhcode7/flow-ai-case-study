# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### Run the Full Pipeline
```bash
# Step 1: Generate synthetic data (requires Synthea output + Claude Code CLI)
python ingestion/generate/run_generate.py

# Step 1 (alternative): Faker-only mode — no Synthea or LLM required
python ingestion/generate/run_generate.py --dry-run

# Step 2: Load all CSVs into the Bronze DuckDB layer
python ingestion/bronze/load_bronze.py

# Step 3: Verify (requires duckdb CLI)
duckdb data/duckdb/flowai.duckdb "SELECT COUNT(*) FROM bronze.emr_patient;"
```

### Synthea Setup (one-time, required for non-dry-run)
```bash
wget https://github.com/synthetichealth/synthea/releases/latest/download/synthea-with-dependencies.jar
java -jar synthea-with-dependencies.jar -s 42 -p 600 \
  --exporter.csv.export=true --exporter.fhir.export=false --exporter.ccda.export=false California
cp -r output/csv/* data/raw/synthea_output/
```

## Architecture

### Data Flow
```
Synthea CSVs (data/raw/synthea_output/)
    └─> emr_from_synthea.py   → data/raw/emr/      (5 tables)
    └─> rcm.py                → data/raw/rcm/       (5 tables)
    └─> referral.py           → data/raw/referral/  (3 tables)
         └─> load_bronze.py   → data/duckdb/flowai.duckdb (bronze schema, all VARCHAR)
```

**Generation order matters:** RCM and Referral generators depend on the EMR patient CSV being written first. `run_generate.py` enforces this order.

### 3-Tier Generation Model
- **Tier 1 (Synthea):** Clinically realistic base patient/encounter data
- **Tier 2 (Claude Haiku via CLI subprocess):** Contextually coherent gap-filling for fields like email, MRN, chief_complaint, payer names — runs in batches of 50 rows
- **Tier 3 (Faker):** Mechanical fallback when Tier 2 is unavailable or fails; used exclusively in `--dry-run` mode

### Bronze Layer
All 13 tables land in DuckDB (`data/duckdb/flowai.duckdb`) under the `bronze` schema with every column stored as `VARCHAR`. Four metadata columns are appended per row: `_ingested_at`, `_source_system`, `_source_file`, `_row_hash`.

**Tables:** `emr_patient`, `emr_encounter`, `emr_diagnosis`, `emr_procedure`, `emr_provider`, `rcm_patient_account`, `rcm_claim_header`, `rcm_claim_line`, `rcm_remittance_835`, `rcm_ar_balance_snapshot`, `referral_order`, `referral_order_status_history`, `referral_document_reference`

### Intentional Cross-System Messiness
These are deliberate design choices for entity resolution training — do not "fix" them:

| Type | Rate | Location |
|------|------|----------|
| RCM accounts with NULL `emr_patient_id` | 30% | `RCM_NULL_EMR_LINK_RATE` in config.py |
| Referral orders with NULL `referring_provider_npi` | 20% | `REFERRAL_NULL_NPI_RATE` in config.py |
| DOB off by ±1 year across systems | 10% | `DOB_YEAR_SKEW_RATE` in config.py |
| DOB month/day swapped across systems | 5% | `DOB_MONTH_DAY_SWAP_RATE` in config.py |
| Name format variants (UPPER, Last-First, middle initial) | varies | `helpers.apply_name_variant()` |

## Key Configuration (`ingestion/generate/config.py`)

All paths, record counts, batch sizes, and messiness rates live here. Key settings:
- `RANDOM_SEED = 42` — reproducibility
- `DRY_RUN = False` — set `True` to skip LLM calls (same as `--dry-run` CLI flag)
- Record counts: `EMR_PATIENT_COUNT=500`, `EMR_ENCOUNTER_COUNT=1500`
- Paths: `SYNTHEA_OUTPUT`, `EMR_OUTPUT`, `RCM_OUTPUT`, `REFERRAL_OUTPUT`, `DUCKDB_PATH`, `CROSSWALK_PATH`

## LLM Integration (`ingestion/generate/llm_filler.py`)

Uses `claude` CLI subprocess — **not the Anthropic SDK**. Requires Claude Code with a Pro subscription.

```python
fill_missing_fields(
    table_name="emr_patient",          # maps to FILL_RULES dict for table-specific prompt
    df=df,
    columns_to_fill=["mrn", "phone"],
    context_columns=["first_name", "last_name"],
    batch_size=50
)
```

- If `claude --version` fails, the function silently falls back to `FAKER_FALLBACKS` lambdas
- On JSON parse error or response length mismatch, the affected batch falls back to Faker
- Model used: `claude-haiku-4-5-20251001`
