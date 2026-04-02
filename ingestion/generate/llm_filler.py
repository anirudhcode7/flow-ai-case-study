"""
FlowAI Tier 2 LLM gap-filler.

Uses the Claude Code CLI (`claude` binary via subprocess) with claude-haiku-4-5
to fill missing DataFrame fields with contextually-aware synthetic values.

Falls back to Faker (Tier 3) on any failure, or when DRY_RUN=True.

Invocation model:
  - Sends a batch of rows as a JSON prompt to Claude Haiku
  - Expects a JSON array response of length == batch size
  - Each element is a dict with keys == columns_to_fill
  - Invalid/short responses trigger row-level Faker fallback
"""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Any

import pandas as pd
from faker import Faker

from ingestion.generate import config
from ingestion.generate import helpers

logger = logging.getLogger(__name__)
_fake = Faker()
_fake.seed_instance(config.RANDOM_SEED)

# ── Table-specific fill instructions sent to Haiku ────────────────────────────
FILL_RULES: dict[str, str] = {
    "emr_patient": (
        "email must look derived from patient first_name + last_name "
        "(e.g. jsmith84@gmail.com). phone must be realistic US format. "
        "mrn is an 8-digit zero-padded integer with prefix MRN (e.g. MRN00043217). "
        "address_line2 should be NULL ~75% of the time."
    ),
    "emr_encounter": (
        "chief_complaint should be in patient-language (casual, first-person), "
        "not clinical terminology. ~15% should be null (procedure-only encounters)."
    ),
    "emr_provider": (
        "npi must be a valid-format 10-digit number starting with 1 or 2. "
        "phone should be an office line in realistic US format."
    ),
    "rcm_patient_account": (
        "Generate slight variations of the patient name (e.g. uppercase, middle initial, suffix). "
        "DOB may be skewed slightly. Phone format should differ from EMR record. "
        "guarantor_name may be a spouse/parent name."
    ),
    "rcm_claim_header": (
        "payer_name must match a real US insurance company. "
        "total_charge_amount should be realistic for medical claims ($50-$15000). "
        "claim_status weighted: 50% paid, 20% submitted, 20% denied, 10% adjusted."
    ),
    "rcm_claim_line": (
        "charge_amount > allowed_amount >= paid_amount always. "
        "denial_code should be a real CARC code (e.g. CO-97, PR-1) when present, "
        "NULL ~70% of the time."
    ),
    "rcm_remittance_835": (
        "trace_number looks like an EFT trace number (18 digits). "
        "raw_835_document_ref looks like a file path: /archive/835/2024/01/remit_XXXXXX.835"
    ),
    "rcm_ar_balance_snapshot": (
        "payer_responsibility_balance + patient_responsibility_balance should sum to a realistic total. "
        "Values must be internally consistent with claim data."
    ),
    "referral_order": (
        "referring_provider_name is free-text, messy (Dr. X / X, MD / Last, First). "
        "counterparty_org_name sounds like a PI law firm or injury management company. "
        "case_reference_id format: PI-YYYY-ST-NNNNN."
    ),
    "referral_order_status_history": (
        "statuses should progress logically: received -> scheduled -> completed "
        "OR received -> cancelled. "
        "changed_by is a staff name or system identifier like 'system_auto'."
    ),
    "referral_document_reference": (
        "doc_type one of: referral_form, imaging, insurance_card, medical_records, auth_request. "
        "doc_uri is a realistic file path or S3-like URL."
    ),
}

# ── Faker fallback lambdas keyed by column name ───────────────────────────────
FAKER_FALLBACKS: dict[str, Any] = {
    "email":                  lambda row: helpers.random_email(
                                  row.get("first_name", ""), row.get("last_name", "")
                              ),
    "phone":                  lambda row: helpers.random_us_phone(),
    "mrn":                    lambda row: helpers.generate_mrn(),
    "address_line2":          lambda row: None if _fake.boolean(chance_of_getting_true=75) else _fake.secondary_address(),
    "chief_complaint":        lambda row: None if _fake.boolean(chance_of_getting_true=15) else _fake.sentence(nb_words=8),
    "npi":                    lambda row: helpers.generate_npi(),
    "guarantor_name":         lambda row: _fake.name(),
    "payer_name":             lambda row: _fake.random_element([
                                  "UnitedHealthcare", "Aetna", "Cigna", "BlueCross BlueShield",
                                  "Humana", "Anthem", "Centene", "Molina Healthcare", "Kaiser Permanente"
                              ]),
    "claim_status":           lambda row: _fake.random_element(
                                  elements=("paid", "paid", "paid", "submitted", "submitted",
                                            "denied", "denied", "adjusted")
                              ),
    "denial_code":            lambda row: None if _fake.boolean(chance_of_getting_true=70) else _fake.random_element([
                                  "CO-97", "CO-4", "CO-16", "PR-1", "PR-2",
                                  "CO-45", "CO-50", "PR-204", "CO-96", "OA-23"
                              ]),
    "trace_number":           lambda row: str(_fake.random_number(digits=18, fix_len=True)),
    "raw_835_document_ref":   lambda row: f"/archive/835/{_fake.year()}/{_fake.month():0>2}/remit_{_fake.random_number(digits=6, fix_len=True)}.835",
    "doc_type":               lambda row: _fake.random_element([
                                  "referral_form", "imaging", "insurance_card",
                                  "medical_records", "auth_request"
                              ]),
    "doc_uri":                lambda row: f"s3://flowai-docs/{_fake.uuid4()}/{_fake.file_name(extension='pdf')}",
    "case_reference_id":      lambda row: f"PI-{_fake.year()}-{_fake.state_abbr()}-{_fake.random_number(digits=5, fix_len=True)}",
    "status":                 lambda row: _fake.random_element(["received", "scheduled", "completed", "cancelled"]),
    "changed_by":             lambda row: _fake.random_element([_fake.name(), "system_auto", "admin_portal"]),
    "referring_provider_name": lambda row: _fake.random_element([
                                  f"Dr. {_fake.last_name()}",
                                  f"{_fake.last_name()}, {_fake.first_name()} MD",
                                  f"{_fake.name()}, MD",
                              ]),
    "counterparty_org_name":  lambda row: _fake.random_element([
                                  f"{_fake.last_name()} & {_fake.last_name()} Law Group",
                                  f"{_fake.last_name()} Injury Management",
                                  f"{_fake.city()} PI Associates",
                              ]),
}


# ── Claude CLI wrapper ────────────────────────────────────────────────────────

def _call_claude_haiku(prompt: str) -> str:
    """
    Invoke the local `claude` CLI with the given prompt.
    Returns raw stdout text.
    Raises RuntimeError on non-zero exit code.
    """
    result = subprocess.run(
        [
            "claude",
            "-p", prompt,
            "--model", "claude-haiku-4-5-20251001",
            "--output-format", "text",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Claude CLI returned exit code {result.returncode}. "
            f"stderr: {result.stderr[:500]}"
        )
    return result.stdout.strip()


def _build_prompt(
    table_name: str,
    chunk: list[dict],
    columns_to_fill: list[str],
    context_columns: list[str],
) -> str:
    """Build a concise JSON-output prompt for Haiku."""
    rules = FILL_RULES.get(table_name, "Generate realistic synthetic healthcare data.")
    context_data = json.dumps(chunk, default=str)
    cols_str = ", ".join(f'"{c}"' for c in columns_to_fill)

    return (
        f"You are a synthetic healthcare data generator.\n"
        f"Table: {table_name}\n"
        f"Rules: {rules}\n\n"
        f"For each row in the following JSON array, generate realistic values for these columns: [{cols_str}].\n"
        f"Context columns (already filled — use them for coherence): {context_columns}\n\n"
        f"Input rows:\n{context_data}\n\n"
        f"Return ONLY a valid JSON array of {len(chunk)} objects. "
        f"Each object must have exactly these keys: {columns_to_fill}. "
        f"Use null (JSON null, not the string 'null') for intentionally missing values. "
        f"No markdown, no explanation — raw JSON array only."
    )


def _parse_response(raw: str, expected_len: int, columns_to_fill: list[str]) -> list[dict] | None:
    """
    Parse and validate Haiku's JSON response.
    Returns None if invalid so caller can fall back to Faker.
    """
    # Strip accidental markdown code fences
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(
            line for line in lines if not line.startswith("```")
        ).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("JSON parse error from Haiku: %s", exc)
        return None

    if not isinstance(parsed, list):
        logger.warning("Haiku response is not a list (got %s)", type(parsed))
        return None

    if len(parsed) != expected_len:
        logger.warning(
            "Haiku returned %d rows, expected %d", len(parsed), expected_len
        )
        return None

    for item in parsed:
        if not isinstance(item, dict):
            logger.warning("Non-dict item in Haiku response")
            return None
        for col in columns_to_fill:
            if col not in item:
                logger.warning("Missing column '%s' in Haiku response item", col)
                return None

    return parsed


def _faker_fill_row(row: dict, columns_to_fill: list[str]) -> dict:
    """Fill a single row's missing columns using Faker fallbacks."""
    filled = dict(row)
    for col in columns_to_fill:
        fallback = FAKER_FALLBACKS.get(col)
        if fallback is not None:
            try:
                filled[col] = fallback(row)
            except Exception:
                filled[col] = None
        else:
            filled[col] = None
    return filled


# ── Public interface ──────────────────────────────────────────────────────────

def fill_missing_fields(
    table_name: str,
    df: pd.DataFrame,
    columns_to_fill: list[str],
    context_columns: list[str],
    batch_size: int = config.BATCH_SIZE_DEFAULT,
) -> pd.DataFrame:
    """
    Fill `columns_to_fill` in `df` using LLM (Tier 2) or Faker (Tier 3).

    Parameters
    ----------
    table_name:       Name key into FILL_RULES (e.g. 'emr_patient').
    df:               Source DataFrame; rows to fill.
    columns_to_fill:  Column names that need values generated.
    context_columns:  Existing columns passed to the LLM for coherence.
    batch_size:       Rows per LLM call. Smaller = more reliable JSON parsing.

    Returns
    -------
    DataFrame with `columns_to_fill` populated.
    """
    df = df.copy()

    # Pre-create target columns as object dtype to hold mixed types
    for col in columns_to_fill:
        if col not in df.columns:
            df[col] = None

    use_faker = config.DRY_RUN

    if not use_faker:
        # Quick sanity-check: is the claude CLI available?
        try:
            probe = subprocess.run(
                ["claude", "--version"],
                capture_output=True, text=True, timeout=5
            )
            if probe.returncode != 0:
                logger.warning("Claude CLI not available; switching to Faker fallback.")
                use_faker = True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            logger.warning("Claude CLI not found; switching to Faker fallback.")
            use_faker = True

    rows = df.to_dict(orient="records")

    for batch_start in range(0, len(rows), batch_size):
        chunk = rows[batch_start : batch_start + batch_size]
        context_chunk = [
            {k: v for k, v in row.items() if k in context_columns}
            for row in chunk
        ]

        filled_chunk: list[dict] | None = None

        if not use_faker:
            try:
                prompt = _build_prompt(table_name, context_chunk, columns_to_fill, context_columns)
                raw = _call_claude_haiku(prompt)
                filled_chunk = _parse_response(raw, len(chunk), columns_to_fill)
            except Exception as exc:
                logger.warning(
                    "LLM fill failed for table=%s batch=%d: %s — using Faker fallback.",
                    table_name, batch_start, exc,
                )

        if filled_chunk is None:
            # Tier 3: row-level Faker fallback
            filled_chunk = [_faker_fill_row(row, columns_to_fill) for row in chunk]

        # Write generated values back into the main rows list
        for i, generated in enumerate(filled_chunk):
            for col in columns_to_fill:
                rows[batch_start + i][col] = generated.get(col)

        logger.info(
            "fill_missing_fields: table=%s batch=%d/%d done (%s).",
            table_name,
            min(batch_start + batch_size, len(rows)),
            len(rows),
            "faker" if filled_chunk and use_faker else "llm",
        )

    return pd.DataFrame(rows)
