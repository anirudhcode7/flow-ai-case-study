"""
Referral generator: creates 3 Personal Injury / specialist referral tables.

80% of referral patients draw from the EMR patient pool (with messiness),
20% are net-new fictional patients.

Output tables written to REFERRAL_OUTPUT:
  - referral_order.csv
  - referral_order_status_history.csv
  - referral_document_reference.csv
"""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

from ingestion.generate import config
from ingestion.generate import helpers
from ingestion.generate.llm_filler import fill_missing_fields

logger = logging.getLogger(__name__)
_fake = Faker()
_fake.seed_instance(config.RANDOM_SEED)
random.seed(config.RANDOM_SEED)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_date(start: str = "2022-01-01", end: str = "2024-12-31") -> str:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (s + timedelta(days=random.randint(0, (e - s).days))).strftime("%Y-%m-%d")


def _load_emr_patients() -> pd.DataFrame:
    path = os.path.join(config.EMR_OUTPUT, "emr_patient.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"EMR patient file not found at {path}. Run EMR generation first."
        )
    return pd.read_csv(path, dtype=str)


# ── Referral Orders ───────────────────────────────────────────────────────────

_REFERRAL_SOURCES = [
    "emr_system", "fax_intake", "patient_portal", "provider_direct", "third_party_platform",
]

_ORDER_STATUSES = ["pending", "scheduled", "completed", "cancelled", "in_review"]

_COUNTERPARTY_ORG_TYPES = ["law_firm", "injury_management", "specialist_group", "insurance"]

_ICD10_SAMPLES = [
    "S93.401A", "M54.5", "G43.909", "S09.90XA", "M79.3",
    "Z23", "I10", "E11.9", "F32.9", "J45.909",
]

_CPT_SAMPLES = [
    "99213", "99214", "97001", "72148", "70553",
    "93000", "85025", "80053", "27447", "20610",
]


def generate_referral_orders(emr_patients: pd.DataFrame) -> pd.DataFrame:
    """Build referral_order.csv."""
    n = config.REFERRAL_ORDER_COUNT

    # Detect patient column names
    pid_col = "emr_patient_id" if "emr_patient_id" in emr_patients.columns else "patient_id"
    first_col = "first_name" if "first_name" in emr_patients.columns else "FIRST"
    last_col = "last_name" if "last_name" in emr_patients.columns else "LAST"
    dob_col = "dob" if "dob" in emr_patients.columns else "date_of_birth"
    phone_col = "phone" if "phone" in emr_patients.columns else None
    email_col = "email" if "email" in emr_patients.columns else None
    addr_col = "address_line1" if "address_line1" in emr_patients.columns else None
    city_col = "city" if "city" in emr_patients.columns else None
    state_col = "state" if "state" in emr_patients.columns else None
    zip_col = "zip" if "zip" in emr_patients.columns else None

    n_linked = int(n * (1 - config.REFERRAL_NULL_NPI_RATE))
    n_unlinked = n - n_linked

    sample = emr_patients.sample(
        n=min(n_linked, len(emr_patients)), replace=(n_linked > len(emr_patients)),
        random_state=config.RANDOM_SEED,
    )

    records = []

    for _, row in sample.iterrows():
        first, last = helpers.apply_name_variant(
            str(row.get(first_col, "")), str(row.get(last_col, ""))
        )
        dob = helpers.apply_dob_skew(str(row.get(dob_col, "")))
        npi = helpers.generate_npi() if random.random() > config.REFERRAL_NULL_NPI_RATE else None
        created_at = _rand_date()
        records.append({
            "referral_order_id": helpers.generate_uuid(),
            "referral_source_system": random.choice(_REFERRAL_SOURCES),
            "referral_created_at": created_at + " " + _fake.time(),
            "patient_first_name": first,
            "patient_last_name": last,
            "dob": dob,
            "phone": str(row.get(phone_col, "")) if phone_col else helpers.random_us_phone(),
            "email": str(row.get(email_col, "")) if email_col else _fake.email(),
            "address_line1": str(row.get(addr_col, "")) if addr_col else _fake.street_address(),
            "city": str(row.get(city_col, "")) if city_col else _fake.city(),
            "state": str(row.get(state_col, "")) if state_col else _fake.state_abbr(),
            "zip": str(row.get(zip_col, "")) if zip_col else _fake.zipcode(),
            "referring_provider_name": None,  # filled below
            "referring_provider_npi": npi,
            "receiving_facility_id": helpers.generate_uuid(),
            "primary_diagnosis_icd10": random.choice(_ICD10_SAMPLES),
            "requested_service_cpt": random.choice(_CPT_SAMPLES),
            "priority": random.choice(["routine", "urgent", "stat", "routine", "routine"]),
            "order_status": random.choice(_ORDER_STATUSES),
            "counterparty_org_name": None,  # filled below
            "counterparty_org_type": random.choice(_COUNTERPARTY_ORG_TYPES),
            "case_reference_id": None,  # filled below
        })

    for _ in range(n_unlinked):
        npi = helpers.generate_npi() if random.random() > config.REFERRAL_NULL_NPI_RATE else None
        created_at = _rand_date()
        first = _fake.first_name()
        last = _fake.last_name()
        records.append({
            "referral_order_id": helpers.generate_uuid(),
            "referral_source_system": random.choice(_REFERRAL_SOURCES),
            "referral_created_at": created_at + " " + _fake.time(),
            "patient_first_name": first,
            "patient_last_name": last,
            "dob": _fake.date_of_birth(minimum_age=18, maximum_age=75).strftime("%Y-%m-%d"),
            "phone": helpers.random_us_phone(),
            "email": helpers.random_email(first, last),
            "address_line1": _fake.street_address(),
            "city": _fake.city(),
            "state": _fake.state_abbr(),
            "zip": _fake.zipcode(),
            "referring_provider_name": None,
            "referring_provider_npi": npi,
            "receiving_facility_id": helpers.generate_uuid(),
            "primary_diagnosis_icd10": random.choice(_ICD10_SAMPLES),
            "requested_service_cpt": random.choice(_CPT_SAMPLES),
            "priority": random.choice(["routine", "urgent", "stat", "routine", "routine"]),
            "order_status": random.choice(_ORDER_STATUSES),
            "counterparty_org_name": None,
            "counterparty_org_type": random.choice(_COUNTERPARTY_ORG_TYPES),
            "case_reference_id": None,
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="referral_order",
        df=df,
        columns_to_fill=["referring_provider_name", "counterparty_org_name", "case_reference_id"],
        context_columns=["order_status", "primary_diagnosis_icd10", "priority"],
        batch_size=config.BATCH_SIZE_SMALL,
    )

    out_path = os.path.join(config.REFERRAL_OUTPUT, "referral_order.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote referral_order.csv: %d rows", len(df))
    return df


# ── Status History ────────────────────────────────────────────────────────────

_STATUS_PROGRESSIONS = [
    ["received", "scheduled", "completed"],
    ["received", "scheduled", "completed", "completed"],
    ["received", "cancelled"],
    ["received", "scheduled", "cancelled"],
    ["received", "pending_auth", "scheduled", "completed"],
]


def generate_status_history(referral_orders: pd.DataFrame) -> pd.DataFrame:
    """Build referral_order_status_history.csv."""
    n_target = config.REFERRAL_STATUS_HISTORY_COUNT
    referral_ids = referral_orders["referral_order_id"].tolist()
    referral_dates = dict(
        zip(referral_orders["referral_order_id"], referral_orders["referral_created_at"])
    )

    records = []
    while len(records) < n_target and referral_ids:
        ref_id = random.choice(referral_ids)
        progression = random.choice(_STATUS_PROGRESSIONS)
        try:
            base_date = datetime.strptime(
                str(referral_dates.get(ref_id, "2023-01-01"))[:10], "%Y-%m-%d"
            )
        except ValueError:
            base_date = datetime(2023, 1, 1)

        offset_days = 0
        for status in progression:
            records.append({
                "referral_order_id": ref_id,
                "status": status,
                "status_time": (base_date + timedelta(days=offset_days)).strftime("%Y-%m-%d %H:%M:%S"),
                "changed_by": None,  # filled below
            })
            offset_days += random.randint(1, 14)

        if len(records) >= n_target:
            break

    df = pd.DataFrame(records[:n_target])

    df = fill_missing_fields(
        table_name="referral_order_status_history",
        df=df,
        columns_to_fill=["changed_by"],
        context_columns=["status", "status_time"],
        batch_size=config.BATCH_SIZE_LARGE,
    )

    out_path = os.path.join(config.REFERRAL_OUTPUT, "referral_order_status_history.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote referral_order_status_history.csv: %d rows", len(df))
    return df


# ── Document References ───────────────────────────────────────────────────────

def generate_document_references(referral_orders: pd.DataFrame) -> pd.DataFrame:
    """Build referral_document_reference.csv."""
    n = config.REFERRAL_DOC_REF_COUNT
    referral_ids = referral_orders["referral_order_id"].tolist()

    records = []
    for _ in range(n):
        records.append({
            "referral_order_id": random.choice(referral_ids),
            "doc_type": None,  # filled below
            "doc_uri": None,   # filled below
            "uploaded_at": _rand_date() + " " + _fake.time(),
        })

    df = pd.DataFrame(records)

    df = fill_missing_fields(
        table_name="referral_document_reference",
        df=df,
        columns_to_fill=["doc_type", "doc_uri"],
        context_columns=["referral_order_id", "uploaded_at"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    out_path = os.path.join(config.REFERRAL_OUTPUT, "referral_document_reference.csv")
    df.to_csv(out_path, index=False)
    logger.info("Wrote referral_document_reference.csv: %d rows", len(df))
    return df


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
    """Run the full Referral generation pipeline. Returns table_name -> DataFrame."""
    os.makedirs(config.REFERRAL_OUTPUT, exist_ok=True)

    print("  [Referral] Loading EMR patients...")
    emr_patients = _load_emr_patients()

    print("  [Referral] Generating referral orders...")
    orders = generate_referral_orders(emr_patients)

    print("  [Referral] Generating status history...")
    history = generate_status_history(orders)

    print("  [Referral] Generating document references...")
    doc_refs = generate_document_references(orders)

    return {
        "referral_order": orders,
        "referral_order_status_history": history,
        "referral_document_reference": doc_refs,
    }
