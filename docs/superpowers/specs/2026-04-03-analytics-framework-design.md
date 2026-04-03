# Analytics Framework & BI-Ready Star Schema — Design Spec

**Date:** 2026-04-03
**Phase:** 5 of healthcare data platform
**Status:** Draft

## Context

The platform has bronze (raw), silver (typed/cleaned), and gold (entity resolution) layers built. The gold layer contains 738 canonical patients (`gold_dim_patient`) and a crosswalk (`gold_bridge_patient_source_xref`) mapping 1,440 source records across EMR, RCM, and referral systems.

**This phase** creates the star schema (5 dimensions + 1 bridge + 3 fact tables) that powers business reporting. The case study requires answering:
1. Provider counterparty with most pending receivables
2. Patients with highest outstanding balances
3. AR aging by payer / counterparty / facility
4. Denial rate and turnaround time

## Architecture

### Star Schema Overview

```
                         dim_date (2022-2027)
                              |
dim_payer ──── fact_receivable ──── dim_facility
                    |
               gold_dim_patient (existing)
                    |                \
  fact_patient_balance        bridge_patient_counterparty ──── dim_counterparty_org
                    |
dim_payer ──── fact_claim_line ──── dim_facility
                    |
               dim_provider (via NPI on claim header)
```

**Key design decision:** Counterparty is linked to patients via a bridge table (many-to-many), not directly on fact_receivable. This preserves the clean fact grain while allowing full counterparty analysis through: `fact_receivable → patient_id → bridge → dim_counterparty_org`.

**Dimensions (5 new + 1 existing):**
| Dimension | PK | Source | Expected Rows |
|---|---|---|---|
| `dim_date` | date_key (DATE) | generate_series | ~2,192 (6 years) |
| `dim_provider` | provider_key (VARCHAR) | EMR providers + external NPIs | ~80-100 |
| `dim_facility` | facility_key (VARCHAR) | Encounters + claims + referrals | varies |
| `dim_counterparty_org` | counterparty_org_key (VARCHAR) | Referral orders | ~83 (law firms) |
| `dim_payer` | payer_key (VARCHAR) | Claim headers | varies |
| `gold_dim_patient` | patient_id (VARCHAR) | **Already exists** | 738 |

**Bridge (1 new):**
| Bridge | Grain | Source | Purpose |
|---|---|---|---|
| `bridge_patient_counterparty` | patient_id + counterparty_org_key | Crosswalk + referrals + dim_counterparty_org | Many-to-many: patient ↔ counterparty |

**Facts (3 new):**
| Fact | Grain | Source | Expected Rows |
|---|---|---|---|
| `fact_receivable` | claim_id + snapshot_date | AR snapshots + claims + crosswalk | ~1,199 |
| `fact_patient_balance` | patient_id + snapshot_date | Aggregated from fact_receivable | varies |
| `fact_claim_line` | claim_id + line_num | Claim lines + headers + crosswalk | ~2,008 |

### File Structure

All new files under `transform/dbt_flowai/models/gold/analytics/`:
```
models/gold/analytics/
├── _analytics__models.yml          # schema + tests
├── dim_date.sql
├── dim_provider.sql
├── dim_facility.sql
├── dim_counterparty_org.sql
├── dim_payer.sql
├── bridge_patient_counterparty.sql  # many-to-many patient ↔ counterparty
├── fact_receivable.sql
├── fact_patient_balance.sql
└── fact_claim_line.sql
```

Plus update `dbt_project.yml` to add `analytics` subfolder config.

## Model Designs

### dim_date

Standard date dimension via DuckDB `generate_series`. Range: **2022-01-01 to 2027-12-31** (covers all existing data back to 2022).

Columns: date_key (PK), year, quarter, month, month_name, day_of_month, day_of_week, day_name, week_of_year, is_weekend, fiscal_year (Oct 1 start), fiscal_quarter.

### dim_provider

Canonical provider dimension. Strategy: EMR providers as base, stub records for NPIs found in claims or referrals but missing from EMR.

Sources: `silver_emr_provider` (full details), `silver_rcm_claim_header` (billing/rendering NPIs), `silver_referral_order` (referring NPI + free-text name).

For stub records, parse first/last name from referral free-text using `SPLIT_PART`.

### dim_facility

Union of distinct facility IDs from encounters, claims, and referrals. Facility names are generated as "Facility {id}" since source IDs are opaque UUIDs.

### dim_counterparty_org

Counterparty organizations from referral orders. Normalization: UPPER + strip common suffixes (LLC, INC, CORP, etc.) via `REGEXP_REPLACE`. Group by normalized name, keep canonical spelling.

**Note:** Current data only contains `law_firm` type (~83 records). Other types (pi_firm, clinic, employer) may appear with different data seeds.

### dim_payer

Distinct payer_id + payer_name from claim headers. Simple dimension with surrogate key.

### bridge_patient_counterparty

**Grain:** patient_id + counterparty_org_key (one row per patient-counterparty relationship).

Join path: crosswalk (`source_system='referral'`) → `silver_referral_order` → normalize counterparty name → `dim_counterparty_org`.

Columns: patient_id, counterparty_org_key, referral_order_id, referral_created_at, counterparty_org_name (denormalized for convenience).

This is a standard star schema bridge table for a many-to-many relationship. BI queries for counterparty analysis join: `fact_receivable → patient_id → bridge_patient_counterparty → dim_counterparty_org`.

### fact_receivable

**Grain:** claim_id + snapshot_date (from AR balance snapshots).

Join path:
- `ar_balance_snapshot.rcm_account_id` → crosswalk (`source_system='rcm'`) → `patient_id`
- `claim_header.payer_id` → `dim_payer`
- `claim_header.facility_id` → `dim_facility`

**No counterparty key on this table.** Counterparty analysis uses the bridge table via patient_id.

Measures: total_ar_balance, payer_balance, patient_balance, aging_bucket.
Denormalized context: claim_status, total_charge_amount, submitted_date, service_from_date.

### fact_patient_balance

**Grain:** patient_id + snapshot_date. Aggregated from `fact_receivable`.

Measures: total_outstanding, overdue_amount (61-90 + 90+), total_payer_balance, total_patient_balance, open_claim_count.

`last_payment_date` left as NULL placeholder (clean remittance→patient join path doesn't exist in current schema).

### fact_claim_line

**Grain:** claim_id + line_num. Enables denial analysis by CPT code.

Join path same as fact_receivable for patient_id, payer_key, facility_key. Adds: cpt_code, units, charge_amount, allowed_amount, paid_amount, denial_code, is_denied flag.

Includes billing_provider_npi and rendering_provider_npi for provider-level analysis (join to dim_provider by NPI).

## Schema Tests

- All dimension PKs: unique + not_null
- Bridge composite key: `dbt_utils.unique_combination_of_columns` on (patient_id, counterparty_org_key)
- Bridge `patient_id`: relationships to `gold_dim_patient`
- Bridge `counterparty_org_key`: relationships to `dim_counterparty_org`
- Fact composite keys: `dbt_utils.unique_combination_of_columns`
- `dim_counterparty_org.counterparty_org_type`: accepted_values (law_firm, pi_firm, clinic, employer)
- `fact_patient_balance.patient_id`: relationships to `gold_dim_patient`
- All measure columns: not_null where appropriate

## Verification Plan

After `dbt run` + `dbt test`:
1. All 9 analytics tables exist in gold schema (5 dims + 1 bridge + 3 facts)
2. Dimension row counts > 0 (dim_date ~2,192)
3. Fact row counts > 0
4. BI Query 1: Top counterparties by pending receivables (via bridge: fact_receivable → patient_id → bridge_patient_counterparty → dim_counterparty_org)
5. BI Query 2: Top patients by outstanding balances
6. BI Query 3: AR aging by payer
7. BI Query 4: Denial rate by payer
8. AR aging by facility
9. Referential integrity: no orphan patient_ids in facts
10. Coverage: % of fact_receivable rows with each dimension key populated

## Documentation Deliverable

`docs/ANALYTICS_FRAMEWORK.md` covering: star schema description, grain definitions, dimension-to-fact joins, BI question SQL, known limitations, and how to run.

## Known Limitations

- Counterparty linkage depends on entity resolution quality; not all claims resolve to patients with referrals
- Dollar amounts are Faker-generated and may not reflect realistic healthcare billing patterns
- Facility names are synthetic labels (no real facility metadata in source)
- `last_payment_date` in fact_patient_balance is a placeholder (NULL)
- Counterparty dimension currently sparse (only law_firm type in current data seed)

## Constraints

- Do NOT modify existing models in entity_resolution/, silver/, or staging/
- Do NOT modify bronze or ingestion code
- All new files in `models/gold/analytics/`
- Follow existing CTE-based style from entity_resolution models
- All models materialized as tables in `gold` schema
