# Analytics Framework & BI-Ready Star Schema

## Overview

Phase 5 of the FlowAI data platform: a star schema in the `gold` DuckDB schema that powers business reporting. Built as dbt models under `transform/dbt_flowai/models/gold/analytics/`.

**5 dimensions + 1 bridge + 3 fact tables**, all materialized as tables in the `gold` schema.

## Star Schema

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

## Dimensions

| Table | PK | Description | Rows |
|---|---|---|---|
| `dim_date` | date_key (DATE) | Calendar + fiscal year (Oct 1 start), 2022-2027 | ~2,191 |
| `dim_provider` | provider_key | EMR providers + stub records for external NPIs | ~2,719 |
| `dim_facility` | facility_key | Facilities from encounters, claims, referrals | ~3,020 |
| `dim_counterparty_org` | counterparty_org_key | Law firms/PI firms/clinics from referrals | ~307 |
| `dim_payer` | payer_key | Insurance payers from claim headers | 9 |
| `gold_dim_patient` | patient_id | **Pre-existing** canonical patient dimension | 738 |

## Bridge Table

| Table | Grain | Description |
|---|---|---|
| `bridge_patient_counterparty` | patient_id + counterparty_org_key | Many-to-many: patients linked to counterparty orgs via referrals |

This bridge table enables counterparty analysis without duplicating fact rows. Join path: `fact_receivable.patient_id` -> `bridge_patient_counterparty` -> `dim_counterparty_org`.

## Fact Tables

| Table | Grain | Description |
|---|---|---|
| `fact_receivable` | claim_id + snapshot_date | AR balances per claim per snapshot. Core receivables fact. |
| `fact_patient_balance` | patient_id + snapshot_date | Aggregated patient-level AR (from fact_receivable). |
| `fact_claim_line` | claim_id + line_num | Line-level claim detail for denial/CPT analysis. |

### fact_receivable
- **Dimension keys:** patient_id, facility_key, payer_key
- **Measures:** total_ar_balance, payer_balance, patient_balance
- **Context:** aging_bucket, claim_status, total_charge_amount, submitted_date, service_from_date

### fact_patient_balance
- **Dimension keys:** patient_id (FK to gold_dim_patient)
- **Measures:** total_outstanding, overdue_amount (61-90 + 90+ buckets), total_payer_balance, total_patient_balance, open_claim_count

### fact_claim_line
- **Dimension keys:** patient_id, facility_key, payer_key
- **Measures:** charge_amount, allowed_amount, paid_amount, is_denied
- **Context:** cpt_code, denial_code, claim_status, billing/rendering NPI

## BI Questions

### 1. Provider counterparty with most pending receivables
```sql
SELECT co.counterparty_org_name, co.counterparty_org_type,
       SUM(fr.total_ar_balance) AS total_pending_ar,
       COUNT(DISTINCT fr.claim_id) AS claim_count
FROM gold.fact_receivable fr
JOIN gold.bridge_patient_counterparty bpc ON fr.patient_id = bpc.patient_id
JOIN gold.dim_counterparty_org co ON bpc.counterparty_org_key = co.counterparty_org_key
WHERE fr.aging_bucket IN ('61-90', '90+')
GROUP BY co.counterparty_org_name, co.counterparty_org_type
ORDER BY total_pending_ar DESC;
```

### 2. Patients with highest outstanding balances
```sql
SELECT dp.patient_id,
       dp.best_first_name || ' ' || dp.best_last_name AS patient_name,
       pb.total_outstanding, pb.overdue_amount, pb.open_claim_count
FROM gold.fact_patient_balance pb
JOIN gold.gold_dim_patient dp ON pb.patient_id = dp.patient_id
ORDER BY pb.total_outstanding DESC;
```

### 3. AR aging by payer
```sql
SELECT p.payer_name, fr.aging_bucket,
       SUM(fr.total_ar_balance) AS total_ar,
       COUNT(DISTINCT fr.claim_id) AS claims
FROM gold.fact_receivable fr
LEFT JOIN gold.dim_payer p ON fr.payer_key = p.payer_key
GROUP BY p.payer_name, fr.aging_bucket
ORDER BY p.payer_name, fr.aging_bucket;
```

### 4. Denial rate by payer
```sql
SELECT p.payer_name, COUNT(*) AS total_lines,
       SUM(CASE WHEN fcl.is_denied THEN 1 ELSE 0 END) AS denied_lines,
       ROUND(100.0 * SUM(CASE WHEN fcl.is_denied THEN 1 ELSE 0 END) / COUNT(*), 2) AS denial_rate_pct
FROM gold.fact_claim_line fcl
LEFT JOIN gold.dim_payer p ON fcl.payer_key = p.payer_key
GROUP BY p.payer_name
ORDER BY denial_rate_pct DESC;
```

## How to Run

```bash
cd transform/dbt_flowai
dbt run --select gold.analytics --profiles-dir .
dbt test --select gold.analytics --profiles-dir .
```

## Known Limitations

- Dollar amounts are Faker-generated and may not reflect realistic healthcare billing
- Facility names are synthetic labels ("Facility {uuid}") since source IDs are opaque
- Provider dimension includes ~2,639 stub records with external NPIs from Faker-generated claims (each claim gets a unique NPI)
- Counterparty dimension is sparse: current data seed produces only `law_firm` type organizations
- `last_payment_date` in fact_patient_balance is a NULL placeholder (clean remittance-to-patient join path not available)
- `payer_id` is NULL in source data; dim_payer is keyed on `payer_name` instead
