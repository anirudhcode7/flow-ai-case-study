# FlowAI Case Study: Data Sources Reference & Download Guide

**Complete reference for all recommended data sources with direct links**

---

## TIER 1: ESSENTIAL (For MVP)

### 1. Synthea - Synthetic Patient Population Generator
- **Type:** Synthetic EMR data generation tool
- **Primary URL:** https://synthetichealth.github.io/synthea/
- **GitHub:** https://github.com/synthetichealth/synthea
- **License:** Open Source (Apache 2.0)
- **Language:** Java
- **What it generates:**
  - Patient demographics (name, DOB, contact, address, SSN)
  - Encounters (outpatient, inpatient, ED)
  - Diagnoses (ICD-10)
  - Procedures (CPT)
  - Medications & allergies
  - Lab results
- **Export formats:** FHIR (R4, STU3, DSTU2), C-CDA, CSV
- **Setup time:** 15-30 minutes
- **Documentation:** https://synthetichealth.github.io/synthea/
- **Use case:** Generate emr_patient, emr_encounter, emr_diagnosis, emr_procedure tables
- **MVP Scale:** Generate 1,000-5,000 patients
- **Cost:** Free

**How to use for FlowAI:**
```bash
git clone https://github.com/synthetichealth/synthea.git
cd synthea
./build.sh
./run_synthea.sh -p 1000 --exporter.csv.export=true

# Exports to: output/csv/*.csv
# Key files: patients.csv, encounters.csv, conditions.csv, procedures.csv, providers.csv
```

---

### 2. CMS NPPES NPI Provider Registry
- **Type:** Real provider master data (10-digit NPI identifiers)
- **Official Download:** https://download.cms.gov/nppes/NPI_Files.html
- **Public API:** https://npiregistry.cms.hhs.gov/api-page
- **AWS Marketplace:** https://aws.marketplace.com/pp/prodview-6hrha5c7pe3am
- **License:** Public domain (US Government)
- **What it contains:**
  - NPI (10-digit unique provider ID)
  - Provider name (first, last, middle)
  - Specialty
  - Credentials
  - Practice location(s)
  - Business name (for organizations)
  - Phone, fax
  - Address
- **Update frequency:** Weekly (individual NPI updates), Monthly (full file)
- **Version:** 2.0 (as of 03/03/2026)
- **File format:** CSV (comma-separated)
- **File size:** ~1.5 GB (compressed)
- **Setup time:** 5-10 minutes (download + decompress)
- **Use case:** Build dim_provider, identity_entity tables
- **Cost:** Free

**How to use:**
```bash
# Option 1: Download from CMS
curl -O https://download.cms.gov/nppes/NPPES_Data_Dissemination_<MONTH>_<YEAR>.zip
unzip NPPES_Data_Dissemination_<MONTH>_<YEAR>.zip

# Contains: npidata_pfile_<date>.csv (main file)
# Plus reference files for other names, practice locations, endpoints

# Option 2: Use API for single provider lookup
curl "https://npiregistry.cms.hhs.gov/api?version=2.0&lastname=Smith&firstname=John"
```

---

### 3. Synthetic Claims Data (Self-Generated)
- **Type:** Custom-generated RCM data (you create this)
- **Source:** Map Synthea encounters → claims
- **What to generate:**
  - rcm_claim_header (claim IDs, facility, provider NPI, payer, status, amounts)
  - rcm_claim_line (CPT codes, units, charges, paid amounts, denial codes)
  - rcm_remittance_835 (payment records from payers)
  - rcm_ar_balance_snapshot (aging buckets)
- **Setup time:** 1-2 hours (build generator script in Python)
- **Use case:** Prototype RCM ingestion and claims processing
- **Cost:** Free (your effort)

**Example approach:**
```python
import pandas as pd
import random
from datetime import datetime, timedelta

# Load Synthea encounters
encounters = pd.read_csv('synthea_output/encounters.csv')

# For each encounter, generate claims
claims = []
for idx, enc in encounters.iterrows():
    claim_id = f"CLM_{enc['encounter']}"
    claim = {
        'claim_id': claim_id,
        'encounter_id': enc['encounter'],
        'patient_id': enc['patient'],
        'facility_id': enc['provider'],  # Use provider as facility
        'billing_provider_npi': random.choice(provider_npis),  # From NPPES
        'payer_id': random.choice(['PAYER_AETNA', 'PAYER_BCBS', 'PAYER_CIGNA']),
        'total_charge_amount': random.uniform(500, 5000),
        'claim_status': random.choice(['submitted', 'accepted', 'paid', 'denied']),
        'submitted_date': enc['start'].split('T')[0],
        'service_from_date': enc['start'].split('T')[0],
    }
    claims.append(claim)

claims_df = pd.DataFrame(claims)
claims_df.to_csv('rcm_claim_header.csv', index=False)
```

---

### 4. Synthetic Referral Orders (Self-Generated)
- **Type:** Custom-generated referral data (you create this)
- **What to generate:**
  - referral_order (patient, referring provider, receiving facility, diagnosis, CPT, status)
  - referral_order_status_history (status updates over time)
  - counterparty_org_type (law_firm, pi_firm, clinic, employer)
- **Setup time:** 1-2 hours (build generator script in Python)
- **Use case:** Prototype referral ingestion and counterparty matching
- **Cost:** Free (your effort)

---

## TIER 2: VALIDATION (For Enhanced Testing)

### 5. MIMIC-IV: Freely Accessible Electronic Health Record Dataset
- **Type:** Real, de-identified ICU and ED clinical data
- **Primary URL:** https://physionet.org/content/mimiciv/3.1/
- **Paper:** https://www.nature.com/articles/s41597-022-01899-x (Nature Scientific Data)
- **AWS Registry:** https://registry.opendata.aws/mimic-iv-demo/
- **License:** PhysioNet Credentialed Health Data License Agreement
- **What it contains:**
  - Patient demographics
  - ICU stay records (64K+ patients)
  - ED visit records (200K+ patients)
  - Diagnoses (ICD-10)
  - Procedures
  - Medications
  - Lab results
  - Vital signs
  - Clinical notes
- **Time period:** 2008-2019
- **Organization:** Beth Israel Deaconess Medical Center, Boston
- **File format:** CSV, Parquet
- **Setup time:** 2-4 hours (training + DUA)
- **Cost:** Free (requires DUA)

**How to access:**
```
1. Visit https://physionet.org/content/mimiciv/3.1/
2. Create PhysioNet account
3. Complete "CITI Data or Specimens Only Research" training
4. Sign Data Use Agreement (online)
5. Download CSV files or access via AWS registry
```

**Use case for FlowAI:**
- Validate emr_patient, emr_encounter, emr_diagnosis schemas against real clinical data
- Test entity resolution on real patient duplicates
- Benchmark data quality rules

---

### 6. X12 835 Remittance Advice Format (Reference)
- **Type:** EDI transaction standard specification
- **CMS Reference PDF:** https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf
- **Educational Guides:**
  - https://www.stedi.com/edi/x12/transaction-set/835
  - https://saplingdata.com/x12-837-and-835/
  - https://www.clinii.com/healthcare-abbreviation-list/what-is-era/
- **What it specifies:** Structure of Electronic Remittance Advice (ERA) files
- **Content:** Claim payments, adjustments, denials, insurance responsibility
- **Use case for FlowAI:** Understand rcm_remittance_835 format; parse live payer feeds
- **Cost:** Free

---

## TIER 3: EXPLORATION (Optional, For Deep Dives)

### 7. Kaggle Healthcare Datasets
- **General Healthcare Dataset:** https://www.kaggle.com/datasets/prasad22/healthcare-dataset
- **Healthcare Datasets (Sample):** https://www.kaggle.com/datasets/benitoitelewuver/healthcare-datasets-sample
- **License:** Varies (check each dataset)
- **Content:** Patient demographics, diagnoses, lab results, treatments
- **Use case:** Schema exploration, quick prototyping
- **Cost:** Free (with Kaggle account)

### 8. GitHub Awesome Lists
- **Awesome Medical Datasets:** https://github.com/openmedlab/Awesome-Medical-Dataset
- **Awesome Healthcare Datasets:** https://github.com/geniusrise/awesome-healthcare-datasets
- **Content:** Curated lists of 50+ healthcare datasets (imaging, EHR, genomics, etc.)
- **Use case:** Discover domain-specific datasets
- **Cost:** Free

### 9. Health Data Analytics Open Datasets Guide
- **URL:** https://guides.library.unt.edu/health-data-analytics/open-datasets
- **Content:** Curated guide to public health datasets
- **Use case:** Resource for additional datasets
- **Cost:** Free

### 10. Synthea FHIR for Research
- **URL:** https://mitre.github.io/fhir-for-research/modules/synthea-overview/
- **Content:** Detailed guide on Synthea FHIR export structure
- **Use case:** Understand FHIR resource mapping to EMR tables
- **Cost:** Free

---

## QUICK REFERENCE TABLE

| Resource | Type | Access | Cost | Setup | Use Case |
|----------|------|--------|------|-------|----------|
| **Synthea** | Synthetic EMR | GitHub | Free | 15 min | EMR source data |
| **CMS NPPES** | Real providers | Web DL / API | Free | 5 min | Provider master, identity graph |
| **Synthetic Claims** | Generated | Custom script | Free | 2 hrs | RCM source data |
| **Synthetic Referrals** | Generated | Custom script | Free | 2 hrs | Referral source data |
| **MIMIC-IV** | Real ICU/ED | PhysioNet DUA | Free | 4 hrs | Validation, testing |
| **X12 835** | Standard spec | CMS PDF | Free | 1 hr | Format reference |
| **Kaggle** | Various | Web | Free | 10 min | Exploration |
| **GitHub Lists** | Curated | GitHub | Free | 5 min | Resource discovery |

---

## RECOMMENDED DOWNLOAD CHECKLIST (MVP)

```
✅ Synthea
   □ Clone: git clone https://github.com/synthetichealth/synthea.git
   □ Build: ./build.sh
   □ Generate: ./run_synthea.sh -p 1000 --exporter.csv.export=true
   □ Output: output/csv/*.csv

✅ CMS NPPES
   □ Download: https://download.cms.gov/nppes/NPI_Files.html
   □ Extract: Unzip the monthly file
   □ Key file: npidata_pfile_<date>.csv
   □ Size: ~1.5 GB compressed

✅ Create Claims Generator
   □ Script: Python script mapping Synthea encounters → claims
   □ Output: rcm_claim_header.csv, rcm_claim_line.csv, rcm_remittance_835.csv

✅ Create Referral Generator
   □ Script: Python script generating referral orders
   □ Output: referral_order.csv, referral_order_status_history.csv

✅ Optional: MIMIC-IV (if pursuing enhanced validation)
   □ Get training: CITI Data course (~4 hours)
   □ Sign DUA: PhysioNet agreement (~30 minutes)
   □ Access: Download from https://physionet.org/content/mimiciv/3.1/
```

---

## IMPLEMENTATION SEQUENCE (Recommended)

### Week 1: Foundation
1. Clone Synthea, run once to generate 1,000 patients
2. Download CMS NPPES (monthly file ~1.5 GB)
3. Familiarize with Synthea CSV output structure

### Week 2: Generate RCM & Referral Data
1. Write Python script to parse Synthea encounters
2. Generate synthetic claims (rcm_claim_header, rcm_claim_line)
3. Generate synthetic remittance records (rcm_remittance_835)
4. Generate synthetic referral orders + counterparty data

### Week 3: Build Ingestion Architecture
1. Create Bronze layer table DDL
2. Build data quality validation rules
3. Implement entity resolution logic

### Week 4+: Analytics & BI
1. Build Silver/Gold layers
2. Create star schema
3. Design dashboards

---

## FILE STRUCTURE (Recommended Local Setup)

```
~/flowai-case-study/
├── data/
│   ├── synthetic/
│   │   ├── synthea/
│   │   │   └── output/csv/
│   │   │       ├── patients.csv
│   │   │       ├── encounters.csv
│   │   │       ├── conditions.csv
│   │   │       ├── procedures.csv
│   │   │       └── providers.csv
│   │   ├── rcm/
│   │   │   ├── rcm_claim_header.csv
│   │   │   ├── rcm_claim_line.csv
│   │   │   └── rcm_remittance_835.csv
│   │   └── referrals/
│   │       └── referral_order.csv
│   │
│   └── real/
│       └── nppes/
│           └── npidata_pfile_*.csv
│
├── scripts/
│   ├── generate_claims.py
│   ├── generate_referrals.py
│   ├── entity_resolution.py
│   └── quality_validation.sql
│
├── sql/
│   ├── 01_bronze_schema.sql
│   ├── 02_silver_schema.sql
│   └── 03_gold_schema.sql
│
└── docs/
    ├── data_dictionary.md
    └── architecture.md
```

---

## TROUBLESHOOTING & FAQ

### Q: Do I need real EMR data?
**A:** No. Synthea is perfectly valid for MVP. Real data (MIMIC-IV) is optional for validation.

### Q: Can I use MIMIC-IV without a DUA?
**A:** Yes, the demo version is available on AWS without DUA. Full MIMIC-IV requires DUA.

### Q: How large should my synthetic dataset be?
**A:** 1,000-5,000 patients for MVP. Anything larger gets you beyond MVP.

### Q: Should I include real claims data?
**A:** Create synthetic claims mapping Synthea encounters. Real claims data is harder to access and less necessary for demonstrating your architecture.

### Q: Can I use other EMR generators?
**A:** Yes, alternatives include:
- Michigan PatientGen (FHIR-compatible)
- Synthea alternatives listed in GitHub Awesome lists
- But Synthea is most mature and documented

### Q: Where should I store this data?
**A:** For MVP: Local filesystem or SQLite/PostgreSQL. For scaling: Snowflake, BigQuery, or S3.

---

## NEXT ACTIONS

1. **Download Synthea** (15 min)
   ```bash
   git clone https://github.com/synthetichealth/synthea.git
   cd synthea && ./build.sh
   ```

2. **Download CMS NPPES** (5 min)
   - Visit https://download.cms.gov/nppes/NPI_Files.html
   - Download latest monthly file
   - Decompress to `~/flowai-case-study/data/real/nppes/`

3. **Generate 1,000 synthetic patients** (10 min)
   ```bash
   ./run_synthea.sh -p 1000 --exporter.csv.export=true
   ```

4. **Review Synthea CSV structure** (30 min)
   - Map patients.csv → emr_patient
   - Map encounters.csv → emr_encounter
   - Map conditions.csv → emr_diagnosis
   - etc.

5. **Start Phase 2: Data Preparation** (in parallel with case study review)

---

**Document Version:** 1.0
**Last Updated:** April 2, 2026
**Status:** Ready for download and setup
