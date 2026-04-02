"""
RCM generator: creates 5 Revenue Cycle Management tables.

Sources from emr_patient.csv (EMR_OUTPUT) to build a realistic cross-system
patient population with intentional linkage gaps (RCM_NULL_EMR_LINK_RATE).

Output tables written to RCM_OUTPUT:
  - rcm_patient_account.csv
  - rcm_claim_header.csv
  - rcm_claim_line.csv
  - rcm_remittance_835.csv
  - rcm_ar_balance_snapshot.csv
"""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from ingestion.generate import config
from ingestion.generate import helpers
from ingestion.generate.llm_filler import fill_missing_fields

logger = logging.getLogger(__name__)
_fake = Faker()
_fake.seed_instance(config.RANDOM_SEED)
random.seed(config.RANDOM_SEED)
np.random.seed(config.RANDOM_SEED)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_date(start: str = "2022-01-01", end: str = "2024-12-31") -> str:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    delta = (e - s).days
    return (s + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def _load_emr_patients() -> pd.DataFrame:
    path = os.path.join(config.EMR_OUTPUT, "emr_patient.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"EMR patient file not found at {path}. Run EMR generation first."
        )
    return pd.read_csv(path, dtype=str)


# ── Patient Accounts ──────────────────────────────────────────────────────────

def generate_patient_accounts(emr_patients: pd.DataFrame) -> pd.DataFrame:
    """
    Build rcm_patient_account.csv.
    70% linked to real EMR patients (with name/DOB variants),
    30% net-new fictional patients.
    """
    n = config.RCM_ACCOUNT_COUNT
    n_linked = int(n * (1 - config.RCM_NULL_EMR_LINK_RATE))
    n_unlinked = n - n_linked

    records = []

    # Detect patient ID column (emr_patient_id or patient_id)
    pid_col = "emr_patient_id" if "emr_patient_id" in emr_patients.columns else "patient_id"
    first_col = "first_name" if "first_name" in emr_patients.columns else "FIRST"
    last_col = "last_name" if "last_name" in emr_patients.columns else "LAST"
    dob_col = "dob" if "dob" in emr_patients.columns else "date_of_birth"

    # Linked pool (sample with replacement if needed)
    sample = emr_patients.sample(
        n=min(n_linked, len(emr_patients)), replace=(n_linked > len(emr_patients)),
        random_state=config.RANDOM_SEED,
    )

    for _, row in sample.iterrows():
        first, last = helpers.apply_name_variant(
            str(row.get(first_col, "")), str(row.get(last_col, ""))
        )
        dob = helpers.apply_dob_skew(str(row.get(dob_col, "")))
        records.append({
            "rcm_account_id": helpers.generate_uuid(),
            "emr_patient_id": row.get(pid_col),
            "patient_external_id": f"EXT-{_fake.random_number(digits=8, fix_len=True)}",
            "guarantor_name": None,  # filled by LLM/Faker below
            "patient_first_name": first,
            "patient_last_name": last,
            "dob": dob,
            "phone": None,  # filled by LLM/Faker below
            "address_line1": row.get("address_line1"),
            "city": row.get("city"),
            "state": row.get("state"),
            "zip": row.get("zip"),
        })

    # Unlinked pool
    for _ in range(n_unlinked):
        records.append({
            "rcm_account_id": helpers.generate_uuid(),
            "emr_patient_id": None,
            "patient_external_id": f"EXT-{_fake.random_number(digits=8, fix_len=True)}",
            "guarantor_name": None,
            "patient_first_name": _fake.first_name(),
            "patient_last_name": _fake.last_name(),
            "dob": _fake.date_of_birth(minimum_age=18, maximum_age=85).strftime("%Y-%m-%d"),
            "phone": None,
            "address_line1": _fake.street_address(),
            "city": _fake.city(),
            "state": _fake.state_abbr(),
            "zip": _fake.zipcode(),
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="rcm_patient_account",
        df=df,
        columns_to_fill=["phone", "guarantor_name"],
        context_columns=["patient_first_name", "patient_last_name", "dob", "state"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    out_path = os.path.join(config.RCM_OUTPUT, "rcm_patient_account.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote rcm_patient_account.csv: %d rows", len(df))
    return df


# ── Claim Headers ─────────────────────────────────────────────────────────────

def generate_claim_headers(accounts: pd.DataFrame) -> pd.DataFrame:
    """Build rcm_claim_header.csv linked to rcm_patient_account."""
    n = config.RCM_CLAIM_HEADER_COUNT
    account_ids = accounts["rcm_account_id"].tolist()

    records = []
    for _ in range(n):
        claim_id = helpers.generate_uuid()
        svc_from = _rand_date()
        records.append({
            "claim_id": claim_id,
            "rcm_account_id": random.choice(account_ids),
            "facility_id": helpers.generate_uuid(),
            "billing_provider_npi": helpers.generate_npi(),
            "rendering_provider_npi": helpers.generate_npi(),
            "payer_id": None,  # filled below
            "payer_name": None,  # filled below
            "claim_type": random.choice(["professional", "institutional", "dental"]),
            "total_charge_amount": None,  # filled below
            "claim_status": None,  # filled below
            "submitted_date": _rand_date(),
            "service_from_date": svc_from,
            "service_to_date": svc_from,
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="rcm_claim_header",
        df=df,
        columns_to_fill=["payer_name", "payer_id", "total_charge_amount", "claim_status"],
        context_columns=["service_from_date", "claim_type"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    out_path = os.path.join(config.RCM_OUTPUT, "rcm_claim_header.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote rcm_claim_header.csv: %d rows", len(df))
    return df


# ── Claim Lines ───────────────────────────────────────────────────────────────

_CPT_CODES = [
    "99213", "99214", "99215", "85025", "80053", "93000",
    "71046", "99395", "82947", "87086", "90471", "97001",
]


def generate_claim_lines(claim_headers: pd.DataFrame) -> pd.DataFrame:
    """Build rcm_claim_line.csv with internally consistent financial fields."""
    n = config.RCM_CLAIM_LINE_COUNT
    claim_ids = claim_headers["claim_id"].tolist()

    records = []
    for i in range(n):
        charge = round(random.uniform(50, 3000), 2)
        allowed = round(charge * random.uniform(0.40, 0.95), 2)
        paid = round(allowed * random.uniform(0.60, 1.0), 2)
        records.append({
            "claim_id": random.choice(claim_ids),
            "line_num": (i % 5) + 1,
            "cpt_code": random.choice(_CPT_CODES),
            "units": random.randint(1, 3),
            "charge_amount": str(charge),
            "allowed_amount": str(allowed),
            "paid_amount": str(paid),
            "denial_code": None,  # filled below
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="rcm_claim_line",
        df=df,
        columns_to_fill=["denial_code"],
        context_columns=["cpt_code", "charge_amount", "paid_amount"],
        batch_size=config.BATCH_SIZE_LARGE,
    )

    out_path = os.path.join(config.RCM_OUTPUT, "rcm_claim_line.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote rcm_claim_line.csv: %d rows", len(df))
    return df


# ── Remittances ───────────────────────────────────────────────────────────────

def generate_remittances(claim_headers: pd.DataFrame) -> pd.DataFrame:
    """Build rcm_remittance_835.csv (ERA/835 records)."""
    n = config.RCM_REMITTANCE_COUNT

    # Collect payer_id values from claim headers for remit records
    payer_ids = claim_headers["payer_id"].dropna().tolist()
    if not payer_ids:
        payer_ids = [f"PAYER{random.randint(100, 999)}" for _ in range(10)]

    records = []
    for _ in range(n):
        paid_date = _rand_date("2022-01-01", "2024-12-31")
        records.append({
            "remit_id": helpers.generate_uuid(),
            "payer_id": random.choice(payer_ids),
            "payment_date": paid_date,
            "trace_number": None,  # filled below
            "total_payment_amount": str(round(random.uniform(50, 5000), 2)),
            "raw_835_document_ref": None,  # filled below
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="rcm_remittance_835",
        df=df,
        columns_to_fill=["trace_number", "raw_835_document_ref"],
        context_columns=["payment_date", "total_payment_amount"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    out_path = os.path.join(config.RCM_OUTPUT, "rcm_remittance_835.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote rcm_remittance_835.csv: %d rows", len(df))
    return df


# ── AR Balance Snapshots ──────────────────────────────────────────────────────

def generate_ar_snapshots(claim_headers: pd.DataFrame) -> pd.DataFrame:
    """Build rcm_ar_balance_snapshot.csv."""
    n = config.RCM_AR_SNAPSHOT_COUNT
    claim_ids = claim_headers["claim_id"].tolist()
    account_ids = claim_headers["rcm_account_id"].tolist()

    # Build a claim_id -> rcm_account_id mapping
    claim_to_account = dict(zip(claim_headers["claim_id"], claim_headers["rcm_account_id"]))

    records = []
    for _ in range(n):
        total = round(random.uniform(0, 4000), 2)
        payer_resp = round(total * random.uniform(0.0, 1.0), 2)
        patient_resp = round(total - payer_resp, 2)
        bucket = helpers.aging_bucket_weighted()
        cid = random.choice(claim_ids)
        records.append({
            "snapshot_date": _rand_date(),
            "claim_id": cid,
            "rcm_account_id": claim_to_account.get(cid, random.choice(account_ids)),
            "payer_responsibility_balance": str(payer_resp),
            "patient_responsibility_balance": str(patient_resp),
            "aging_bucket": bucket,
        })

    df = pd.DataFrame(records)

    out_path = os.path.join(config.RCM_OUTPUT, "rcm_ar_balance_snapshot.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote rcm_ar_balance_snapshot.csv: %d rows", len(df))
    return df


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
    """Run the full RCM generation pipeline. Returns table_name -> DataFrame."""
    os.makedirs(config.RCM_OUTPUT, exist_ok=True)

    print("  [RCM] Loading EMR patients...")
    emr_patients = _load_emr_patients()

    print("  [RCM] Generating patient accounts...")
    accounts = generate_patient_accounts(emr_patients)

    print("  [RCM] Generating claim headers...")
    claim_headers = generate_claim_headers(accounts)

    print("  [RCM] Generating claim lines...")
    claim_lines = generate_claim_lines(claim_headers)

    print("  [RCM] Generating remittances (835)...")
    remittances = generate_remittances(claim_headers)

    print("  [RCM] Generating AR balance snapshots...")
    ar_snapshots = generate_ar_snapshots(claim_headers)

    return {
        "rcm_patient_account": accounts,
        "rcm_claim_header": claim_headers,
        "rcm_claim_line": claim_lines,
        "rcm_remittance_835": remittances,
        "rcm_ar_balance_snapshot": ar_snapshots,
    }
