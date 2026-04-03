# FlowAI Data Engineering Case Study - Quick Reference Cheat Sheet

**One-page summary of all requirements & schemas**

---

## WHAT YOU MUST BUILD

```
EMR + RCM + Referral Orders → Data Ingestion → Patient 360 + Identity Graph → BI Analytics
```

### Three Core Components:

1. **Ingestion Architecture** (multi-source, quality controls, PHI security)
2. **Entity Resolution** (patient matching + provider/counterparty graphs)
3. **Analytics Framework** (star schema + BI-ready model)

---

## SOURCE SYSTEMS & SCHEMAS

### EMR (Clinical/Admin)
```
emr_patient           (emr_patient_id, mrn, name, dob, phone, email, address, ssn_last4)
emr_encounter         (encounter_id, patient_id, type, status, start/end, facility, provider)
emr_diagnosis         (encounter_id, icd10_code, description, rank)
emr_procedure         (encounter_id, cpt_code, description, performed_time)
emr_provider          (provider_id, npi, name, specialty, org_name, contact, address)
```

### RCM (Billing/Claims)
```
rcm_patient_account   (account_id, patient_id, guarantor, demographics)
rcm_claim_header      (claim_id, account_id, facility, billing_provider_npi, payer, status, amount, dates)
rcm_claim_line        (claim_id, cpt_code, units, charge, allowed, paid, denial_code)
rcm_remittance_835    (remit_id, payer_id, payment_date, trace_number, amount)
rcm_ar_balance_snapshot (snapshot_date, claim_id, payer_balance, patient_balance, aging_bucket)
```

### Referral Orders
```
referral_order        (order_id, patient_demo, referring_provider_npi, facility, diagnosis, cpt, status)
referral_order_status_history (order_id, status, status_time, changed_by)
referral_document_reference (order_id, doc_type, doc_uri)
+ counterparty_org_name, counterparty_org_type (law_firm, pi_firm, clinic, employer)
```

### Global Metadata (Add to ALL raw tables)
```
source_system         (e.g., "emr_epic", "rcm_waystar")
source_record_id      (string)
ingested_at           (timestamp)
batch_id              (string)
record_hash           (string for dedup)
payload_json          (optional raw fidelity)
```

---

## TARGET TABLES (GOLD LAYER)

### Patient 360
```
dim_patient
├─ patient_id (canonical)
├─ best_first_name, best_last_name (survivorship)
├─ best_dob, best_phone, best_email, best_address
└─ created_at, updated_at

bridge_patient_source_xref
├─ patient_id (canonical)
├─ source_system (emr/rcm/referral)
├─ source_patient_key (emr_patient_id, rcm_external_id, etc.)
├─ match_confidence (0.0-1.0)
├─ match_method (deterministic/probabilistic/manual)
└─ effective_from/to (timestamp)
```

### Provider/Counterparty Identity Graph
```
identity_entity
├─ entity_id
├─ entity_type (person/org)
├─ canonical_name, canonical_phone, canonical_email, canonical_address
└─ created_at, updated_at

identity_identifier
├─ entity_id
├─ id_type (NPI/EIN/TIN/MRN/external_id/case_id)
├─ id_value
├─ issuing_authority (CMS NPPES, etc.)
├─ source_system, first_seen_at

identity_edge
├─ from_entity_id, to_entity_id
├─ edge_type (refers_to / employed_by / represents / billed_by)
├─ confidence (0.0-1.0)
├─ evidence (e.g., "same NPI", "same address+phone")
└─ source_system, created_at
```

### Star Schema (Analytics)
```
DIMENSIONS:
  dim_date              (standard date dimension)
  dim_patient           (from Patient 360)
  dim_provider          (canonical provider person)
  dim_facility          (facility/org)
  dim_counterparty_org  (law firm / PI firm / referral org)
  dim_payer             (insurance payer)

FACTS:
  fact_receivable
  ├─ grain: claim_id + snapshot_date
  ├─ snapshot_date, claim_id, patient_id, counterparty_org_id, payer_id
  ├─ total_ar_balance, patient_balance, payer_balance, aging_bucket

  fact_patient_balance
  ├─ grain: patient_id + snapshot_date
  ├─ snapshot_date, patient_id
  ├─ total_outstanding, overdue_amount, last_payment_date
```

---

## ENTITY RESOLUTION STRATEGY

### Patient Matching: TWO-STEP APPROACH

**Step 1: Deterministic (Exact Matches)**
```sql
-- Match across emr + rcm on:
WHERE mrn = mrn           -- Medical Record Number (high confidence)
  AND dob = dob           -- Date of Birth

OR    ssn_last4 = ssn_last4 AND dob = dob  -- SSN last 4 + DOB
OR    phone = phone AND dob = dob         -- Phone + DOB (if in both)
OR    email = email AND dob = dob         -- Email + DOB (if in both)
```
→ Result: **match_confidence = 0.95-1.0**, **match_method = 'deterministic'**

**Step 2: Probabilistic (Similarity Scoring)**
```
For remaining unmatched records:
  score = 0.3 * levenshtein(first_name)
        + 0.3 * levenshtein(last_name)
        + 0.2 * (1 - |dob_diff_in_years| / 100)
        + 0.2 * address_similarity

IF score >= 0.75:
  match_confidence = score
  match_method = 'probabilistic'
ELSE:
  → Manual review queue
```

### Provider/Counterparty Matching

**Deterministic:**
- NPI exact match (for providers)
- EIN exact match (for organizations)
- Legal business name (case-insensitive, no punctuation)

**Probabilistic:**
- name similarity + address match
- phone + address match
- Case reference ID + org type + name

---

## BI QUESTIONS YOUR MODEL MUST SUPPORT

```sql
-- Top counterparties by pending receivables
SELECT counterparty_org_id, SUM(total_ar_balance)
FROM fact_receivable
WHERE aging_bucket IN ('61-90', '90+')
GROUP BY counterparty_org_id
ORDER BY 2 DESC

-- Patients with highest outstanding balances
SELECT patient_id, SUM(total_outstanding)
FROM fact_patient_balance
GROUP BY patient_id
ORDER BY 2 DESC
LIMIT 20

-- AR aging analysis
SELECT aging_bucket, COUNT(*), SUM(total_ar_balance), AVG(total_ar_balance)
FROM fact_receivable
WHERE snapshot_date = CURRENT_DATE
GROUP BY aging_bucket

-- Denial rate by payer/CPT/facility
SELECT payer_id, cpt_code, facility_id,
       COUNT(*) AS total_claims,
       SUM(CASE WHEN status = 'denied' THEN 1 ELSE 0 END) AS denied_claims,
       ROUND(100.0 * SUM(CASE WHEN status = 'denied' THEN 1 ELSE 0 END) / COUNT(*), 2) AS denial_rate
FROM fact_claim_line
GROUP BY payer_id, cpt_code, facility_id
ORDER BY denial_rate DESC
```

---

## DATA QUALITY REQUIREMENTS

### Ingestion Layer (Bronze) Checks:
- Schema validation (column count, types, nullability)
- Freshness (data no older than SLA)
- Completeness (% non-null by critical column)
- Duplication (hash-based, row_number() partitioning)
- Referential integrity (FK checks)

### Transformation Layer (Silver) Checks:
- Null handling (expected or error?)
- Domain validation (valid values, ranges)
- Consistency (same entity IDs consistent across tables)

### Curated Layer (Gold) Checks:
- Dimension cardinality (no unexpected spikes)
- Fact volume (row counts reasonable)
- Aggregation reconciliation (sum fact vs source)

---

## PHI/HIPAA SAFEGUARDS

```
Data at Rest:     AES-256 encryption for all PHI columns
Data in Transit:  TLS 1.2+ for API/database connections
Access Control:   Role-based access (RBAC) by job function
Audit Logging:    Log user + timestamp for PHI access
Masking:          Hash SSN, truncate phone in logs/UI
Data Retention:   Define per compliance policy (usually 7 years for claims)
Encryption Keys:  Rotate quarterly; manage in Vault/KMS
```

---

## DELIVERABLES CHECKLIST

```
✅ SOLUTION BRIEF (Written Document)
  ├─ Executive summary
  ├─ Architecture diagrams (ingestion, entity resolution, BI model)
  ├─ Data contracts & schema evolution strategy
  ├─ Data quality framework & tests
  ├─ Entity resolution methodology (deterministic + probabilistic)
  ├─ HIPAA compliance approach
  └─ Cost/performance trade-offs & scaling paths

✅ CODE & ARTIFACTS
  ├─ DDL for Bronze/Silver/Gold tables
  ├─ Data quality validation queries
  ├─ Entity resolution logic (Python/SQL)
  ├─ dbt models (if using dbt)
  ├─ BI dashboard definitions
  └─ Sample data generation scripts

✅ TESTING & VALIDATION
  ├─ Unit tests for entity resolution
  ├─ Data quality test suite
  ├─ Lineage verification
  ├─ Sample query outputs
  └─ Documentation of assumptions & limitations
```

---

## RECOMMENDED TECH STACK

| Layer | Tools |
|-------|-------|
| **Data Generation** | Synthea (EMR) + CMS NPPES (providers) + custom scripts (claims/referrals) |
| **Ingestion** | Python scripts, dbt, Apache Airflow |
| **Storage** | Snowflake, BigQuery, or Redshift |
| **Transformation** | dbt, SQL |
| **Entity Resolution** | Python (pandas, fuzzywuzzy, recordlinkage) |
| **Quality** | dbt tests, Great Expectations |
| **BI** | Tableau, Looker, Power BI |
| **Security** | AWS KMS, Vault, database-native encryption |

---

## EVALUATION RUBRIC (How You'll Be Scored)

| Criteria | Weight | What Matters |
|----------|--------|-------------|
| **Architecture** | 30% | Reliable ingestion, lineage, replayability, schema evolution |
| **Data Modeling** | 20% | Clear keys, normalization vs analytics modeling, extensibility |
| **Entity Resolution** | 20% | Crosswalks, confidence scoring, explainability, error handling |
| **Quality & Governance** | 20% | Tests, monitoring, PHI controls, HIPAA compliance |
| **Pragmatism** | 10% | MVP path, scaling path, cost/performance awareness |

---

## RECOMMENDED MVP DATASET COMBINATION

1. **EMR:** Synthea (generates 1,000-5,000 synthetic patients)
2. **Providers:** CMS NPPES (free, public, real data)
3. **Claims:** Synthetic script mapping Synthea encounters → claims
4. **Referrals:** Synthetic script with counterparty firms
5. **Optional:** MIMIC-IV for clinical realism (if DUA obtained)

---

## KEY DECISION POINTS

| Decision | MVP | Production |
|----------|-----|------------|
| **EMR Source** | Synthea (CSV) | Real EMR API (Epic, Cerner) |
| **Data Volume** | 1K-5K patients | 100K+ patients |
| **Ingestion Freq** | Daily batch | Real-time streams |
| **Storage** | Single warehouse | Multi-region, replicated |
| **Entity Resolution** | Rule-based | ML-based (optional) |
| **Monitoring** | Manual dashboards | Automated alerting |
| **Archiving** | Keep all | Purge after retention |

---

## TIME ESTIMATE (Per Phase)

| Phase | Effort | Days |
|-------|--------|------|
| 1: Research & Design | ✅ DONE | 1 |
| 2: Data Preparation | Synthea setup, script generation | 2-3 |
| 3: Ingestion Architecture | Schema, quality rules, encryption | 3-4 |
| 4: Entity Resolution | Deterministic + probabilistic matching | 4-5 |
| 5: Analytics & BI | Star schema, dashboards | 2-3 |
| 6: Documentation & Polish | Write-ups, test suite | 2-3 |
| **Total** | | **14-18 days** |

---

## QUICK LINKS

- **Case Study PDF:** `/Downloads/DE Case Study.pdf`
- **Full Planning Document:** `/Downloads/FlowAI_DE_Case_Study_Research_Plan.md`
- **Synthea:** https://synthetichealth.github.io/synthea/
- **CMS NPPES:** https://download.cms.gov/nppes/NPI_Files.html
- **MIMIC-IV:** https://physionet.org/content/mimiciv/3.1/

---

**Status:** Ready for Phase 2 (Data Preparation)
**Created:** April 2, 2026
