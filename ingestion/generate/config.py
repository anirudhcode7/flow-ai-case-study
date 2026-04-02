"""
FlowAI generation configuration.
All constants and paths used across the pipeline are defined here.
"""

import os

# ── Reproducibility ───────────────────────────────────────────────────────────
RANDOM_SEED = 42

# ── Run mode ──────────────────────────────────────────────────────────────────
DRY_RUN = False          # if True, skip all LLM calls, use Faker fallback

# ── Batch sizes for LLM calls ─────────────────────────────────────────────────
BATCH_SIZE_DEFAULT = 50
BATCH_SIZE_LARGE = 100   # for single-field fills
BATCH_SIZE_SMALL = 25    # for many-field fills

# ── Record counts ─────────────────────────────────────────────────────────────
EMR_PATIENT_COUNT = 500
EMR_PROVIDER_COUNT = 80
EMR_ENCOUNTER_COUNT = 1500
EMR_DIAGNOSIS_COUNT = 2250
EMR_PROCEDURE_COUNT = 1800

RCM_ACCOUNT_COUNT = 620
RCM_CLAIM_HEADER_COUNT = 1200
RCM_CLAIM_LINE_COUNT = 2400
RCM_REMITTANCE_COUNT = 900
RCM_AR_SNAPSHOT_COUNT = 1200

REFERRAL_ORDER_COUNT = 320
REFERRAL_STATUS_HISTORY_COUNT = 800
REFERRAL_DOC_REF_COUNT = 480

# ── Messiness rates ───────────────────────────────────────────────────────────
# 30% of RCM accounts have null emr_patient_id (cross-system linkage gap)
RCM_NULL_EMR_LINK_RATE = 0.30
# 20% of referral orders have null referring NPI
REFERRAL_NULL_NPI_RATE = 0.20
# 10% of cross-system DOBs are off by ±1 year
DOB_YEAR_SKEW_RATE = 0.10
# 5% have month/day swapped
DOB_MONTH_DAY_SWAP_RATE = 0.05

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
SYNTHEA_OUTPUT = os.path.join(DATA_RAW, "synthea_output")
EMR_OUTPUT = os.path.join(DATA_RAW, "emr")
RCM_OUTPUT = os.path.join(DATA_RAW, "rcm")
REFERRAL_OUTPUT = os.path.join(DATA_RAW, "referral")
DUCKDB_PATH = os.path.join(PROJECT_ROOT, "data", "duckdb", "flowai.duckdb")
CROSSWALK_PATH = os.path.join(
    PROJECT_ROOT, "ingestion", "generate", "reference", "snomed_to_cpt_crosswalk.csv"
)
