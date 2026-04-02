"""
EMR generator: transforms Synthea CSV output into Bronze EMR source files.

Reads from SYNTHEA_OUTPUT and writes to EMR_OUTPUT:
  - emr_provider.csv   (from providers.csv + organizations.csv)
  - emr_patient.csv    (from patients.csv, capped at EMR_PATIENT_COUNT)
  - emr_encounter.csv  (from encounters.csv)
  - emr_diagnosis.csv  (from conditions.csv)
  - emr_procedure.csv  (from procedures.csv, SNOMED -> CPT via crosswalk)

In --dry-run mode (or when Synthea output is missing), falls back to
generating fully synthetic EMR data using Faker.
"""

from __future__ import annotations

import logging
import os
import random
import sys
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


# ── Synthea presence check ────────────────────────────────────────────────────

SYNTHEA_REQUIRED_FILES = [
    "patients.csv",
    "encounters.csv",
    "conditions.csv",
    "procedures.csv",
    "providers.csv",
]


def _check_synthea() -> bool:
    """Return True if Synthea CSVs exist; print actionable message and return False otherwise."""
    if not os.path.isdir(config.SYNTHEA_OUTPUT):
        _print_synthea_instructions()
        return False
    missing = [
        f for f in SYNTHEA_REQUIRED_FILES
        if not os.path.isfile(os.path.join(config.SYNTHEA_OUTPUT, f))
    ]
    if missing:
        print(f"ERROR: Synthea output missing files: {missing}")
        _print_synthea_instructions()
        return False
    return True


def _print_synthea_instructions():
    print(
        "\nERROR: Synthea output not found at data/raw/synthea_output/\n"
        "Please run Synthea first:\n"
        "  java -jar synthea-with-dependencies.jar -s 42 -p 600 \\\n"
        "    --exporter.csv.export=true --exporter.fhir.export=false \\\n"
        "    --exporter.ccda.export=false California\n"
        "Then copy output/csv/* to data/raw/synthea_output/\n"
        "Tip: use --dry-run to generate fake EMR data without Synthea.\n"
    )


# ── Loaders ───────────────────────────────────────────────────────────────────

def _read(filename: str) -> pd.DataFrame:
    path = os.path.join(config.SYNTHEA_OUTPUT, filename)
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    logger.info("Loaded %s: %d rows", filename, len(df))
    return df


# ── Crosswalk ─────────────────────────────────────────────────────────────────

def _load_crosswalk() -> dict[str, dict]:
    """Load SNOMED->CPT crosswalk CSV into a dict keyed by snomed_code."""
    try:
        xwalk = pd.read_csv(config.CROSSWALK_PATH, dtype=str)
        return {
            row["snomed_code"]: {
                "cpt_code": row["cpt_code"],
                "cpt_description": row["cpt_description"],
            }
            for _, row in xwalk.iterrows()
            if pd.notna(row.get("snomed_code"))
        }
    except Exception as exc:
        logger.warning("Could not load crosswalk: %s — CPT mapping will use Faker fallback.", exc)
        return {}


_CPT_FALLBACK_CODES = [
    ("99213", "Office visit"), ("99214", "Office visit high"),
    ("85025", "CBC"), ("80053", "CMP"), ("71046", "Chest X-ray"),
    ("93000", "ECG"), ("99395", "Preventive visit"),
]


def _rand_date(start: str = "2022-01-01", end: str = "2024-12-31") -> str:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (s + timedelta(days=random.randint(0, (e - s).days))).strftime("%Y-%m-%d")


# ── Faker-based EMR generation (dry-run / no Synthea) ─────────────────────────

_SPECIALTIES = [
    "Internal Medicine", "Family Medicine", "Cardiology", "Orthopedics",
    "Neurology", "Oncology", "Pediatrics", "Psychiatry", "Radiology",
    "Emergency Medicine", "Obstetrics and Gynecology", "Dermatology",
]

_ORG_NAMES = [
    "Valley Medical Center", "Riverside Health System", "Northside Clinic",
    "Central Hospital", "Westlake Medical Group", "Summit Health Partners",
    "Pacific Care Associates", "Lakeside Family Practice",
]

_ENCOUNTER_TYPES = ["outpatient", "inpatient", "emergency", "urgent_care", "wellness", "telehealth"]

_ICD10_CODES = [
    ("Z00.00", "Encounter for general adult medical examination without abnormal findings"),
    ("I10", "Essential (primary) hypertension"),
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("J06.9", "Acute upper respiratory infection, unspecified"),
    ("M54.5", "Low back pain"),
    ("F32.9", "Major depressive disorder, single episode, unspecified"),
    ("J45.909", "Unspecified asthma, uncomplicated"),
    ("E78.5", "Hyperlipidemia, unspecified"),
    ("K21.0", "Gastro-esophageal reflux disease with esophagitis"),
    ("N39.0", "Urinary tract infection, site not specified"),
    ("M79.3", "Panniculitis, unspecified"),
    ("R05.9", "Cough, unspecified"),
    ("Z23", "Encounter for immunization"),
    ("S93.401A", "Sprain of unspecified ligament of right ankle"),
    ("G43.909", "Migraine, unspecified, not intractable, without status migrainosus"),
]


def _generate_fake_providers() -> pd.DataFrame:
    """Generate fully synthetic emr_provider records using Faker."""
    n = config.EMR_PROVIDER_COUNT
    records = []
    for _ in range(n):
        first = _fake.first_name()
        last = _fake.last_name()
        records.append({
            "emr_provider_id": helpers.generate_uuid(),
            "npi": helpers.generate_npi(),
            "first_name": first,
            "last_name": last,
            "specialty": random.choice(_SPECIALTIES),
            "org_name": random.choice(_ORG_NAMES),
            "phone": helpers.random_us_phone(),
            "address_line1": _fake.street_address(),
            "city": _fake.city(),
            "state": _fake.state_abbr(),
            "zip": _fake.zipcode(),
        })
    return pd.DataFrame(records)


def _generate_fake_patients() -> pd.DataFrame:
    """Generate fully synthetic emr_patient records using Faker."""
    n = config.EMR_PATIENT_COUNT
    records = []
    for _ in range(n):
        first = _fake.first_name()
        last = _fake.last_name()
        dob = _fake.date_of_birth(minimum_age=18, maximum_age=85).strftime("%Y-%m-%d")
        records.append({
            "emr_patient_id": helpers.generate_uuid(),
            "mrn": helpers.generate_mrn(),
            "first_name": first,
            "last_name": last,
            "dob": dob,
            "sex": random.choice(["M", "F"]),
            "phone": helpers.random_us_phone(),
            "email": helpers.random_email(first, last),
            "address_line1": _fake.street_address(),
            "city": _fake.city(),
            "state": _fake.state_abbr(),
            "zip": _fake.zipcode(),
            "ssn_last4": str(random.randint(1000, 9999)),
            "deceased_flag": "N",
            "last_updated_at": _rand_date() + " " + _fake.time(),
        })
    return pd.DataFrame(records)


def _generate_fake_encounters(patient_ids: list[str]) -> pd.DataFrame:
    """Generate fully synthetic emr_encounter records using Faker."""
    n = config.EMR_ENCOUNTER_COUNT
    records = []
    for _ in range(n):
        start_dt = _rand_date()
        records.append({
            "emr_encounter_id": helpers.generate_uuid(),
            "emr_patient_id": random.choice(patient_ids),
            "encounter_type": random.choice(_ENCOUNTER_TYPES),
            "status": "completed",
            "start_time": start_dt + " " + _fake.time(),
            "end_time": start_dt + " " + _fake.time(),
            "facility_id": helpers.generate_uuid(),
            "attending_provider_id": helpers.generate_uuid(),
            "chief_complaint": _fake.sentence(nb_words=8) if random.random() > 0.15 else None,
        })
    return pd.DataFrame(records)


def _generate_fake_diagnoses(encounter_ids: list[str]) -> pd.DataFrame:
    """Generate fully synthetic emr_diagnosis records using Faker."""
    n = config.EMR_DIAGNOSIS_COUNT
    records = []
    for _ in range(n):
        icd, desc = random.choice(_ICD10_CODES)
        records.append({
            "emr_encounter_id": random.choice(encounter_ids),
            "icd10_code": icd,
            "diagnosis_desc": desc,
            "diagnosis_rank": random.randint(1, 4),
        })
    return pd.DataFrame(records)


def _generate_fake_procedures(encounter_ids: list[str]) -> pd.DataFrame:
    """Generate fully synthetic emr_procedure records using Faker."""
    n = config.EMR_PROCEDURE_COUNT
    records = []
    for _ in range(n):
        cpt, desc = random.choice(_CPT_FALLBACK_CODES)
        records.append({
            "emr_encounter_id": random.choice(encounter_ids),
            "cpt_code": cpt,
            "procedure_desc": desc,
            "performed_time": _rand_date() + " " + _fake.time(),
        })
    return pd.DataFrame(records)


def run_fake() -> dict[str, pd.DataFrame]:
    """
    Generate all EMR tables using Faker (no Synthea required).
    Used in --dry-run mode or when Synthea output is missing.
    """
    os.makedirs(config.EMR_OUTPUT, exist_ok=True)

    print("  [EMR] Generating fake providers (Faker fallback)...")
    providers = _generate_fake_providers()
    out_path = os.path.join(config.EMR_OUTPUT, "emr_provider.csv")
    providers.to_csv(out_path, index=False)
    logger.info("Wrote emr_provider.csv: %d rows", len(providers))

    print("  [EMR] Generating fake patients (Faker fallback)...")
    patients = _generate_fake_patients()
    out_path = os.path.join(config.EMR_OUTPUT, "emr_patient.csv")
    patients.to_csv(out_path, index=False)
    logger.info("Wrote emr_patient.csv: %d rows", len(patients))

    patient_ids = patients["emr_patient_id"].tolist()

    print("  [EMR] Generating fake encounters (Faker fallback)...")
    encounters = _generate_fake_encounters(patient_ids)
    out_path = os.path.join(config.EMR_OUTPUT, "emr_encounter.csv")
    encounters.to_csv(out_path, index=False)
    logger.info("Wrote emr_encounter.csv: %d rows", len(encounters))

    encounter_ids = encounters["emr_encounter_id"].tolist()

    print("  [EMR] Generating fake diagnoses (Faker fallback)...")
    diagnoses = _generate_fake_diagnoses(encounter_ids)
    out_path = os.path.join(config.EMR_OUTPUT, "emr_diagnosis.csv")
    diagnoses.to_csv(out_path, index=False)
    logger.info("Wrote emr_diagnosis.csv: %d rows", len(diagnoses))

    print("  [EMR] Generating fake procedures (Faker fallback)...")
    procedures = _generate_fake_procedures(encounter_ids)
    out_path = os.path.join(config.EMR_OUTPUT, "emr_procedure.csv")
    procedures.to_csv(out_path, index=False)
    logger.info("Wrote emr_procedure.csv: %d rows", len(procedures))

    return {
        "emr_provider": providers,
        "emr_patient": patients,
        "emr_encounter": encounters,
        "emr_diagnosis": diagnoses,
        "emr_procedure": procedures,
    }


# ── Synthea-based providers ───────────────────────────────────────────────────

def generate_providers() -> pd.DataFrame:
    """Parse providers.csv (+ optional organizations.csv) -> emr_provider.csv."""
    providers = _read("providers.csv")
    providers.rename(
        columns={
            "Id": "provider_id",
            "ORGANIZATION": "organization_id",
            "NAME": "full_name",
            "GENDER": "gender",
            "SPECIALTY": "specialty",
            "ADDRESS": "address_line1",
            "CITY": "city",
            "STATE": "state",
            "ZIP": "zip",
        },
        inplace=True,
    )

    # Try to load org names
    org_name_map: dict[str, str] = {}
    org_path = os.path.join(config.SYNTHEA_OUTPUT, "organizations.csv")
    if os.path.isfile(org_path):
        orgs = pd.read_csv(org_path, dtype=str)
        orgs.columns = orgs.columns.str.strip()
        id_col = next((c for c in orgs.columns if c.upper() in ("ID", "Id")), None)
        name_col = next((c for c in orgs.columns if c.upper() == "NAME"), None)
        if id_col and name_col:
            org_name_map = dict(zip(orgs[id_col], orgs[name_col]))

    providers["org_name"] = providers.get("organization_id", pd.Series()).map(org_name_map)

    # Cap to config
    providers = providers.head(config.EMR_PROVIDER_COUNT).copy()

    # Generate split first/last names from full_name
    if "full_name" in providers.columns:
        split = providers["full_name"].str.split(" ", n=1, expand=True)
        providers["first_name"] = split[0].fillna("")
        providers["last_name"] = split[1].fillna("") if 1 in split.columns else ""
    else:
        providers["first_name"] = [_fake.first_name() for _ in range(len(providers))]
        providers["last_name"] = [_fake.last_name() for _ in range(len(providers))]

    providers["emr_provider_id"] = providers.get("provider_id", pd.Series(dtype=str)).fillna(
        pd.Series([helpers.generate_uuid() for _ in range(len(providers))])
    )

    # LLM fill: npi + phone
    providers = fill_missing_fields(
        table_name="emr_provider",
        df=providers,
        columns_to_fill=["npi", "phone"],
        context_columns=["full_name", "specialty", "state", "city"],
        batch_size=config.BATCH_SIZE_LARGE,
    )

    keep = ["emr_provider_id", "npi", "first_name", "last_name", "specialty",
            "org_name", "phone", "address_line1", "city", "state", "zip"]
    out = providers[[c for c in keep if c in providers.columns]]

    os.makedirs(config.EMR_OUTPUT, exist_ok=True)
    out_path = os.path.join(config.EMR_OUTPUT, "emr_provider.csv")
    out.to_csv(out_path, index=False)
    logger.info("Wrote emr_provider.csv: %d rows", len(out))
    return out


# ── Synthea-based patients ────────────────────────────────────────────────────

def generate_patients() -> pd.DataFrame:
    """Parse patients.csv -> emr_patient.csv (living only, capped at EMR_PATIENT_COUNT)."""
    patients = _read("patients.csv")
    patients.rename(
        columns={
            "Id": "patient_id",
            "BIRTHDATE": "dob",
            "DEATHDATE": "death_date",
            "SSN": "ssn",
            "FIRST": "first_name",
            "LAST": "last_name",
            "GENDER": "sex",
            "RACE": "race",
            "ETHNICITY": "ethnicity",
            "ADDRESS": "address_line1",
            "CITY": "city",
            "STATE": "state",
            "ZIP": "zip",
            "COUNTY": "county",
        },
        inplace=True,
    )

    # Filter deceased
    if "death_date" in patients.columns:
        patients = patients[patients["death_date"].isna()].copy()

    patients = patients.head(config.EMR_PATIENT_COUNT).copy()

    # SSN last 4
    if "ssn" in patients.columns:
        patients["ssn_last4"] = patients["ssn"].str[-4:].where(
            patients["ssn"].notna(), other=None
        )
    else:
        patients["ssn_last4"] = None

    patients["deceased_flag"] = "N"
    patients["last_updated_at"] = _rand_date() + " " + _fake.time()

    patients["emr_patient_id"] = patients.get("patient_id", pd.Series(dtype=str)).fillna(
        pd.Series([helpers.generate_uuid() for _ in range(len(patients))])
    )
    if "patient_id" not in patients.columns:
        patients["patient_id"] = patients["emr_patient_id"]

    # LLM fill: mrn, phone, email
    patients = fill_missing_fields(
        table_name="emr_patient",
        df=patients,
        columns_to_fill=["mrn", "phone", "email"],
        context_columns=["first_name", "last_name", "dob", "city", "state"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    keep = [
        "emr_patient_id", "mrn", "first_name", "last_name", "dob",
        "sex", "phone", "email",
        "address_line1", "city", "state", "zip",
        "ssn_last4", "deceased_flag", "last_updated_at",
    ]
    out = patients[[c for c in keep if c in patients.columns]]

    out_path = os.path.join(config.EMR_OUTPUT, "emr_patient.csv")
    out.to_csv(out_path, index=False)
    logger.info("Wrote emr_patient.csv: %d rows", len(out))
    return out


# ── Synthea-based encounters ──────────────────────────────────────────────────

_ENCOUNTER_CLASS_MAP = {
    "ambulatory": "outpatient",
    "outpatient": "outpatient",
    "inpatient": "inpatient",
    "emergency": "emergency",
    "urgentcare": "urgent_care",
    "wellness": "wellness",
    "office": "outpatient",
    "virtual": "telehealth",
}


def generate_encounters(patient_ids: set[str]) -> pd.DataFrame:
    """Parse encounters.csv -> emr_encounter.csv for the filtered patient set."""
    encounters = _read("encounters.csv")
    encounters.rename(
        columns={
            "Id": "encounter_id",
            "START": "start_time",
            "STOP": "end_time",
            "PATIENT": "patient_id",
            "PROVIDER": "provider_id",
            "ENCOUNTERCLASS": "encounter_class_raw",
            "CODE": "encounter_code",
            "DESCRIPTION": "encounter_description",
            "REASONCODE": "reason_code",
            "REASONDESCRIPTION": "reason_description",
        },
        inplace=True,
    )

    encounters = encounters[encounters["patient_id"].isin(patient_ids)].copy()
    encounters = encounters.head(config.EMR_ENCOUNTER_COUNT).copy()

    encounters["encounter_type"] = (
        encounters["encounter_class_raw"]
        .str.lower()
        .map(_ENCOUNTER_CLASS_MAP)
        .fillna("outpatient")
    )
    encounters["status"] = "completed"
    encounters["emr_encounter_id"] = encounters.get("encounter_id", pd.Series(dtype=str)).fillna(
        pd.Series([helpers.generate_uuid() for _ in range(len(encounters))])
    )
    encounters["emr_patient_id"] = encounters["patient_id"]
    encounters["attending_provider_id"] = encounters.get("provider_id", pd.Series(dtype=str))
    encounters["facility_id"] = encounters.get("organization_id", pd.Series(dtype=str)).fillna(
        pd.Series([helpers.generate_uuid() for _ in range(len(encounters))])
    )

    # LLM fill: chief_complaint
    encounters = fill_missing_fields(
        table_name="emr_encounter",
        df=encounters,
        columns_to_fill=["chief_complaint"],
        context_columns=["reason_description", "encounter_type", "encounter_description"],
        batch_size=config.BATCH_SIZE_DEFAULT,
    )

    keep = [
        "emr_encounter_id", "emr_patient_id", "encounter_type", "status",
        "start_time", "end_time", "facility_id", "attending_provider_id",
        "chief_complaint",
    ]
    out = encounters[[c for c in keep if c in encounters.columns]]

    out_path = os.path.join(config.EMR_OUTPUT, "emr_encounter.csv")
    out.to_csv(out_path, index=False)
    logger.info("Wrote emr_encounter.csv: %d rows", len(out))
    return out


# ── Synthea-based diagnoses ───────────────────────────────────────────────────

def generate_diagnoses(patient_ids: set[str], encounter_ids: set[str]) -> pd.DataFrame:
    """Parse conditions.csv -> emr_diagnosis.csv."""
    conditions = _read("conditions.csv")
    conditions.rename(
        columns={
            "START": "onset_date",
            "STOP": "resolution_date",
            "PATIENT": "patient_id",
            "ENCOUNTER": "encounter_id",
            "CODE": "icd10_code",
            "DESCRIPTION": "diagnosis_desc",
        },
        inplace=True,
    )

    conditions = conditions[conditions["patient_id"].isin(patient_ids)].copy()
    conditions = conditions.head(config.EMR_DIAGNOSIS_COUNT).copy()

    conditions["emr_encounter_id"] = conditions["encounter_id"]

    # Rank diagnoses per encounter
    conditions["diagnosis_rank"] = (
        conditions.sort_values("onset_date")
        .groupby("encounter_id")
        .cumcount()
        + 1
    )

    keep = ["emr_encounter_id", "icd10_code", "diagnosis_desc", "diagnosis_rank"]
    out = conditions[[c for c in keep if c in conditions.columns]]

    out_path = os.path.join(config.EMR_OUTPUT, "emr_diagnosis.csv")
    out.to_csv(out_path, index=False)
    logger.info("Wrote emr_diagnosis.csv: %d rows", len(out))
    return out


# ── Synthea-based procedures ──────────────────────────────────────────────────

def generate_procedures(patient_ids: set[str]) -> pd.DataFrame:
    """Parse procedures.csv -> emr_procedure.csv with SNOMED->CPT mapping."""
    procedures = _read("procedures.csv")
    procedures.rename(
        columns={
            "DATE": "performed_time",
            "PATIENT": "patient_id",
            "ENCOUNTER": "encounter_id",
            "CODE": "snomed_code",
            "DESCRIPTION": "snomed_description",
            "REASONCODE": "reason_code",
            "REASONDESCRIPTION": "reason_description",
            "BASE_COST": "base_cost",
        },
        inplace=True,
    )

    procedures = procedures[procedures["patient_id"].isin(patient_ids)].copy()
    procedures = procedures.head(config.EMR_PROCEDURE_COUNT).copy()

    procedures["emr_encounter_id"] = procedures["encounter_id"]

    crosswalk = _load_crosswalk()

    def map_cpt(row):
        entry = crosswalk.get(str(row.get("snomed_code", "")))
        if entry:
            return entry["cpt_code"], entry["cpt_description"]
        fallback = random.choice(_CPT_FALLBACK_CODES)
        return fallback[0], fallback[1]

    mapped = procedures.apply(map_cpt, axis=1, result_type="expand")
    procedures["cpt_code"] = mapped[0]
    procedures["procedure_desc"] = mapped[1]

    keep = ["emr_encounter_id", "cpt_code", "procedure_desc", "performed_time"]
    out = procedures[[c for c in keep if c in procedures.columns]]

    out_path = os.path.join(config.EMR_OUTPUT, "emr_procedure.csv")
    out.to_csv(out_path, index=False)
    logger.info("Wrote emr_procedure.csv: %d rows", len(out))
    return out


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run() -> dict[str, pd.DataFrame]:
    """
    Run the full EMR generation pipeline.
    Returns dict of table_name -> DataFrame.
    Falls back to Faker-based generation if Synthea output is missing (dry-run mode).
    """
    if not _check_synthea():
        if config.DRY_RUN:
            logger.info("Synthea not found; using Faker fallback for EMR generation.")
            return run_fake()
        else:
            sys.exit(1)

    os.makedirs(config.EMR_OUTPUT, exist_ok=True)

    print("  [EMR] Generating providers...")
    providers = generate_providers()

    print("  [EMR] Generating patients...")
    patients = generate_patients()
    patient_ids = set(patients["emr_patient_id"].dropna())

    print("  [EMR] Generating encounters...")
    encounters = generate_encounters(patient_ids)
    encounter_ids = set(encounters["emr_encounter_id"].dropna())

    print("  [EMR] Generating diagnoses...")
    diagnoses = generate_diagnoses(patient_ids, encounter_ids)

    print("  [EMR] Generating procedures...")
    procedures = generate_procedures(patient_ids)

    return {
        "emr_provider": providers,
        "emr_patient": patients,
        "emr_encounter": encounters,
        "emr_diagnosis": diagnoses,
        "emr_procedure": procedures,
    }
