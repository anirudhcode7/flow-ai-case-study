"""
FlowAI helper utilities — Tier 3 Faker-based fallbacks and mechanical operations.
Used both as standalone generators and as LLM fallback functions.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

from ingestion.generate import config

_fake = Faker()
_fake.seed_instance(config.RANDOM_SEED)
random.seed(config.RANDOM_SEED)


# ── Identity ──────────────────────────────────────────────────────────────────

def generate_uuid() -> str:
    """Return a new random UUID string."""
    return str(uuid.uuid4())


def generate_mrn() -> str:
    """Return a zero-padded 8-digit MRN string, e.g. 'MRN00043217'."""
    number = random.randint(1, 99_999_999)
    return f"MRN{number:08d}"


# ── Contact ───────────────────────────────────────────────────────────────────

def random_us_phone() -> str:
    """Return a random realistic US phone number string."""
    formats = [
        "({}) {}-{}",
        "{}-{}-{}",
        "1-{}-{}-{}",
    ]
    area = random.randint(200, 999)
    exchange = random.randint(200, 999)
    subscriber = random.randint(1000, 9999)
    fmt = random.choice(formats)
    return fmt.format(area, exchange, subscriber)


def random_email(first: str, last: str) -> str:
    """
    Generate a realistic email derived from first and last name.
    E.g. 'jsmith84@gmail.com', 'jane.doe@yahoo.com'.
    """
    first = (first or "user").lower().strip()
    last = (last or "unknown").lower().strip()
    # strip non-alpha
    first = "".join(c for c in first if c.isalpha())
    last = "".join(c for c in last if c.isalpha())

    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
    domain = random.choice(domains)
    suffix = random.randint(10, 99)

    patterns = [
        f"{first[0]}{last}{suffix}@{domain}",
        f"{first}.{last}@{domain}",
        f"{first}{last[0]}{suffix}@{domain}",
        f"{last}.{first[0]}@{domain}",
    ]
    return random.choice(patterns)


# ── RCM utilities ─────────────────────────────────────────────────────────────

_AGING_BUCKETS = ["0-30", "31-60", "61-90", "90+"]
_AGING_WEIGHTS = [0.40, 0.25, 0.20, 0.15]


def aging_bucket_weighted() -> str:
    """
    Return a weighted random AR aging bucket.
    Distribution: 40% 0-30, 25% 31-60, 20% 61-90, 15% 90+.
    """
    return random.choices(_AGING_BUCKETS, weights=_AGING_WEIGHTS, k=1)[0]


# ── Name variants ─────────────────────────────────────────────────────────────

def apply_name_variant(first: str, last: str) -> tuple[str, str]:
    """
    Return a slightly-varied (first, last) tuple to simulate cross-system name drift.
    Variants: exact, UPPERCASE, 'Last, First', 'First M. Last' (middle initial injected).
    Returns a (first, last) tuple in all cases.
    """
    variant = random.choices(
        ["exact", "upper", "last_first", "middle_initial"],
        weights=[0.50, 0.20, 0.15, 0.15],
        k=1,
    )[0]

    if variant == "exact":
        return first, last
    elif variant == "upper":
        return first.upper(), last.upper()
    elif variant == "last_first":
        # Encode the "Last, First" convention by swapping fields
        return last, first
    else:  # middle_initial
        middle_initial = random.choice("ABCDEFGHJKLMNPRSTW")
        return f"{first} {middle_initial}.", last


# ── DOB skewing ───────────────────────────────────────────────────────────────

def apply_dob_skew(dob_str: str) -> str:
    """
    Introduce realistic DOB messiness for cross-system records.
    - DOB_YEAR_SKEW_RATE chance of ±1 year offset
    - DOB_MONTH_DAY_SWAP_RATE chance of month/day swap (only if day ≤ 12)
    Returns the (possibly modified) date as 'YYYY-MM-DD' string.
    Falls back to the original string on any parse error.
    """
    if not dob_str:
        return dob_str

    try:
        dob = datetime.strptime(str(dob_str)[:10], "%Y-%m-%d")
    except ValueError:
        return dob_str

    roll = random.random()
    if roll < config.DOB_YEAR_SKEW_RATE:
        delta_years = random.choice([-1, 1])
        try:
            dob = dob.replace(year=dob.year + delta_years)
        except ValueError:
            pass  # e.g. Feb 29 on non-leap year — skip
    elif roll < config.DOB_YEAR_SKEW_RATE + config.DOB_MONTH_DAY_SWAP_RATE:
        if dob.day <= 12:  # only swap when day is ambiguous
            try:
                dob = dob.replace(month=dob.day, day=dob.month)
            except ValueError:
                pass

    return dob.strftime("%Y-%m-%d")


# ── NPI ───────────────────────────────────────────────────────────────────────

def generate_npi() -> str:
    """
    Return a realistic-format 10-digit NPI starting with 1 or 2.
    Not Luhn-validated — sufficient for synthetic data.
    """
    prefix = random.choice(["1", "2"])
    rest = "".join([str(random.randint(0, 9)) for _ in range(9)])
    return prefix + rest
