# Patient Entity Resolution

## Overview

Two-pass patient matching across EMR, RCM, and Referral source systems:
1. **Deterministic** — exact identifier matching (6 rules, confidence 0.90-1.00)
2. **Probabilistic** — fuzzy similarity scoring (jaro_winkler + DOB + contact info, threshold >= 0.70)

Transitive links resolved via 4-pass iterative label propagation. Canonical patient records built with survivorship rules (EMR > RCM > Referral).

## Deterministic Rules

| # | Rule | Criteria | Confidence |
|---|------|----------|------------|
| 1 | Direct ID Link | RCM.emr_patient_id = EMR.emr_patient_id | 1.000 |
| 2 | MRN Match | Same MRN | 1.000 |
| 3 | SSN-last4 + DOB | Same ssn_last4 AND exact DOB | 1.000 |
| 4 | Phone + DOB | Same phone AND exact DOB (cross-system) | 0.950 |
| 5 | Email + DOB | Same email AND exact DOB (cross-system) | 0.950 |
| 6 | Phone + Last Name | Same phone AND last name (cross-system) | 0.900 |

## Probabilistic Scoring

| Component | Weight | Method |
|-----------|--------|--------|
| Last name | 0.20 | jaro_winkler_similarity |
| First name | 0.15 | jaro_winkler_similarity |
| DOB | 0.25 | Custom: exact=1.0, +/-2 days=0.9, year off=0.6, month/day swap=0.5 |
| Phone | 0.20 | Exact match = 1.0 |
| Address | 0.10 | zip=0.5, +city=0.8, +address=1.0 |
| Email | 0.10 | Exact match = 1.0 |

Threshold: weighted_score >= 0.70

Blocking criteria (candidate pairs must share at least one):
- Same first 3 characters of last_name
- Same DOB year (+/-1 tolerance)
- Same phone (if both non-NULL)
- Same zip code

## Survivorship Rules

| Field | Priority |
|-------|----------|
| Name (first, last) | EMR > RCM > Referral |
| DOB | EMR > RCM > Referral |
| Phone | EMR > RCM > Referral |
| Email | EMR > Referral (RCM has no email) |
| Address | EMR > RCM > Referral |

## How to Run

```bash
cd transform/dbt_flowai
dbt deps --profiles-dir .
dbt run --profiles-dir . --select gold.entity_resolution
dbt test --profiles-dir . --select gold.entity_resolution
```

## Output Tables (gold schema)

- **gold_dim_patient** — Canonical patient record (one row per unique patient)
  - patient_id (PK), best_first_name, best_last_name, best_dob, best_phone, best_email, best_address_line1, best_city, best_state, best_zip, created_at, updated_at

- **gold_bridge_patient_source_xref** — Maps every source record to its canonical patient_id
  - patient_id, source_system, source_patient_key, match_confidence, match_method, effective_from, effective_to

## Intermediate Tables (gold schema)

- **int_patient_spine** — Union of all patient records in common shape (1440 records)
- **int_patient_match_deterministic** — High-confidence exact matches
- **int_patient_match_probabilistic** — Fuzzy similarity matches (>= 0.70)
- **int_patient_match_final** — Merged matches with canonical patient IDs assigned

## Verification

After building, run verification queries to check:
1. Spine counts: EMR=500, RCM=620, Referral=320
2. Deterministic match distribution (id_link should be largest)
3. Canonical patient count should be < 1440 and >= 500
4. Every source record maps to exactly one canonical patient
5. No EMR patient appears in multiple canonical records

## Known Limitations

- Synthetic Faker data produces lower match rates than real healthcare data
- Survivorship uses system priority (EMR > RCM > Referral) rather than recency-based (_ingested_at)
- 4-pass label propagation handles chains up to length 5; sufficient for this dataset
- Provider/counterparty identity graph is out of scope for this phase
