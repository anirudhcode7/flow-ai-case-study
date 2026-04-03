# FlowAI Data Engineering Case Study: Research & Planning Document

**Date:** April 2, 2026
**Status:** Research & Planning Phase (No Implementation Yet)

---

## 1. CASE STUDY SUMMARY

### 1.1 Project Context
FlowAI builds AI automation for healthcare processes and operations workflows. The company ingests operational and clinical data from multiple systems (EMR, RCM, Referral Order sources) and unifies them into:
- Patient 360 (canonical patient records)
- Provider/Counterparty Identity Graph
- Analytics and AI agent capabilities

### 1.2 Core Requirements & Deliverables

You must propose an **end-to-end solution** covering:

#### A) Data Ingestion Architecture (Multi-Source)
- **Sources:** EMR (clinical/admin), RCM (claims/charges/remits), Referral Orders (law firms, PI firms, clinics)
- **Patterns:** API, batch files, webhooks/CDC where applicable
- **Landing Zone:** Medallion approach (Bronze/Silver/Gold) or equivalent
- **Data Contracts:** Schema evolution strategy
- **Quality:** Freshness, completeness, deduplication, referential integrity checks
- **Security:** PHI encryption, access controls, auditability per HIPAA safeguards

#### B) Patient 360 & Entity Resolution
- **Patient Matching:**
  - Deterministic rules (MRN, SSN-last4, phone, email exact matches)
  - Probabilistic scoring (name + DOB + address similarity)
- **Deliverables:**
  - Canonical patient record
  - Source ID to canonical ID crosswalks
  - Survivorship rules (which system "wins" for each field)
- **Provider/Counterparty Identity Graph:** Same approach extended to law firms, PI firms, referring orgs, facilities

#### C) Analytics Framework & BI-Ready Data Model
- **Semantic Layer:** Star schema or metric layer approach
- **Example Reports:**
  - Provider counterparty with most pending receivables
  - Patients with highest outstanding balances
  - AR aging by payer/counterparty/facility
  - Denial rate and turnaround time (optional)

### 1.3 Evaluation Criteria
1. **Architecture Correctness:** Reliable ingestion, replayability, lineage, schema evolution
2. **Data Modeling Quality:** Clear keys, normalization vs analytics modeling, extensibility
3. **Entity Resolution Rigor:** Crosswalks, explainability, confidence scoring, error handling
4. **Data Quality & Governance:** Tests, monitoring, PHI controls
5. **Pragmatism:** MVP path + scaling path, cost/performance trade-offs

---

## 2. PROVIDED DATA SCHEMAS (FROM CASE STUDY)

### 2.1 Raw Ingestion Tables (Bronze Layer)

#### EMR Tables
- **emr_patient:** Patient demographics (emr_patient_id, mrn, name, DOB, phone, email, address, SSN-last4, deceased flag)
- **emr_encounter:** Visit records (encounter_id, patient_id, type, status, start/end times, facility, provider)
- **emr_diagnosis:** ICD-10 codes and descriptions by encounter
- **emr_procedure:** CPT codes and procedure details
- **emr_provider:** Provider info (NPI, specialty, org affiliation, contact)

#### RCM Tables
- **rcm_patient_account:** Billing account (account_id, external_id, guarantor, patient demographics)
- **rcm_claim_header:** Claim metadata (claim_id, facility, billing/rendering providers, payer, status, charges, dates)
- **rcm_claim_line:** Line-level charges (CPT, units, charge amount, allowed amount, paid amount, denial code)
- **rcm_remittance_835:** Remittance payments from payers (remit_id, payer, payment date, trace number, amount)
- **rcm_ar_balance_snapshot:** AR snapshots (date, claim_id, balances by payer/patient, aging buckets)

#### Referral Order Tables
- **referral_order:** Inbound referrals (order_id, patient demographics, referring provider NPI, facility, diagnosis, CPT, status)
- **referral_order_status_history:** Status tracking over time
- **referral_document_reference:** Attached docs (forms, imaging, insurance cards)
- Includes counterparty metadata (law firm, PI firm, clinic, employer) with case reference IDs

### 2.2 Target Gold Layer Tables

#### Patient 360
- **dim_patient:** Canonical patient (patient_id, best_first_name, best_last_name, best_dob, best_contact, best_address, survivorship metadata)
- **bridge_patient_source_xref:** Crosswalk mapping (canonical ID → source system → source ID, with confidence score and match method)

#### Identity Graph (Providers & Counterparties)
- **identity_entity:** Entity records (entity_id, type, canonical name/phone/email/address)
- **identity_identifier:** Associated IDs (NPI, EIN, TIN, MRN, external IDs, case IDs)
- **identity_edge:** Relationships (refers_to, employed_by, represents, billed_by) with confidence and evidence

#### Analytics Star Schema
**Dimensions:**
- dim_date, dim_patient, dim_provider, dim_facility, dim_counterparty_org, dim_payer

**Facts:**
- **fact_receivable:** Grain = claim_id + snapshot_date (total_ar_balance, patient_balance, payer_balance, aging_bucket)
- **fact_patient_balance:** Grain = patient_id + snapshot_date (total_outstanding, overdue_amount, last_payment_date)

### 2.3 Global Conventions (Applied to All Raw Tables)
- source_system (e.g., "emr_epic", "rcm_waystar", "referral_portal")
- source_record_id
- ingested_at (timestamp)
- batch_id
- record_hash
- payload_json (optional for raw fidelity)

---

## 3. FLOWAI PROJECT NOTES & CHATS

**Finding:** No local FlowAI project notes, chat exports, or supplementary documentation found in:
- ~/Downloads
- ~/Documents
- ~/Desktop

**Implication:** The PDF case study is the authoritative source. You may want to:
- Reach out to FlowAI for any clarifications on requirements
- Check if they have a Slack/Notion workspace with additional context
- Ask about real vs synthetic data preference

---

## 4. RECOMMENDED DATA SOURCES

### 4.1 Synthetic Healthcare Data (Recommended for Development/Demo)

#### **Synthea - HL7 FHIR Patient Generator**
- **URL:** https://synthetichealth.github.io/synthea/
- **GitHub:** https://github.com/synthetichealth/synthea
- **Format:** FHIR (R4, STU3, DSTU2), C-CDA, CSV
- **Coverage:** Complete medical histories with medications, allergies, encounters, social determinants
- **Advantages:**
  - Exports in FHIR and CSV (direct alignment with case study requirements)
  - Configurable patient populations and disease modules
  - No PHI/privacy concerns
  - Good for MVP and demos
  - Actively maintained
- **Use Case:** Generate synthetic EMR patient encounters, diagnoses, procedures

---

### 4.2 Real EMR/ICU Data (Academic/Research)

#### **MIMIC-IV Dataset**
- **Official:** https://physionet.org/content/mimiciv/3.1/
- **DOI/Publication:** https://www.nature.com/articles/s41597-022-01899-x
- **AWS Registry:** https://registry.opendata.aws/mimic-iv-demo/
- **Size:** 65,000+ ICU patients, 200,000+ ED patients, 364,627 individuals
- **Coverage:** 2008-2019, ICU and ED admissions at Beth Israel Deaconess Medical Center (Boston)
- **Content:** Vital signs, labs, diagnoses, procedures, medications, clinical notes
- **Access:** Requires DUA (Data Use Agreement) and human subjects training
- **Advantages:**
  - Real, de-identified clinical data
  - Rich encounter and diagnosis information
  - Modular structure (hosp + icu modules)
  - Well-documented
- **Use Case:** Prototype EMR ingestion pipeline and entity resolution logic

---

### 4.3 Healthcare Claims & RCM Data

#### **X12 835 Remittance Advice Format** (Conceptual)
- **CMS Reference:** https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf
- **Overview Articles:**
  - https://www.stedi.com/edi/x12/transaction-set/835
  - https://saplingdata.com/x12-837-and-835/
- **Content:** Claim payments, adjustments, denials, insurance responsibility
- **Approach:** Create synthetic 835 samples or use sample EDI tools
- **Use Case:** Parse remittance data, build RCM claim reconciliation logic

#### **CMS Medicare Claims (Limited Public Access)**
- Generally requires data use agreements and institutional review
- Alternative: Use synthetic claim data from claim data generators or simulators

---

### 4.4 Provider/NPI Data

#### **CMS NPPES NPI Registry**
- **Official Download:** https://download.cms.gov/nppes/NPI_Files.html
- **Public API:** https://npiregistry.cms.hhs.gov/api-page
- **Content:** Provider names, specialties, practice locations, NPIs (10-digit identifiers)
- **Update Frequency:** Weekly and monthly releases (Version 2 as of 03/03/2026)
- **Reference Files:**
  - Main provider data (CSV)
  - Other Name Reference File (Type 2 NPIs)
  - Practice Location Reference File
  - Endpoint Reference File
- **Advantages:**
  - Free and publicly available
  - Standardized NPI identifiers
  - Complete provider directory
- **Use Case:** Build provider master data and NPI-based entity graphs

#### **AWS Marketplace NPPES Data**
- https://aws.marketplace.com/pp/prodview-6hrha5c7pe3am
- Pre-loaded in AWS for easier querying

---

### 4.5 General Healthcare Datasets (Kaggle)

#### **Kaggle Healthcare Datasets**
- **General Healthcare Dataset:** https://www.kaggle.com/datasets/prasad22/healthcare-dataset
- **Healthcare Datasets (Sample):** https://www.kaggle.com/datasets/benitoitelewuver/healthcare-datasets-sample
- **Curated Resources:**
  - https://github.com/openmedlab/Awesome-Medical-Dataset
  - https://github.com/geniusrise/awesome-healthcare-datasets
- **Coverage:** Patient demographics, diagnoses, lab results, treatments
- **Advantages:** Quick prototyping, varied domains
- **Caution:** Verify provenance and licensing before use

---

### 4.6 Data.gov & Government Sources

#### **Health Data Analytics Open Datasets**
- **UNT Guide:** https://guides.library.unt.edu/health-data-analytics/open-datasets
- **Coverage:** Public health statistics, epidemiological data, outcomes

---

## 5. RECOMMENDED DATASET COMBINATION FOR MVP

For a realistic but manageable MVP, combine:

1. **EMR Source:** Synthea (generates patient encounters in FHIR/CSV format)
   - Covers emr_patient, emr_encounter, emr_diagnosis, emr_procedure
   - Includes provider records with NPI

2. **Provider Master:** CMS NPPES NPI Registry (https://download.cms.gov/nppes/NPI_Files.html)
   - Builds identity graph and provider dimension
   - Used for NPI-based entity resolution

3. **RCM/Claims Source:** Synthetic claims data (generated via script using Synthea encounters)
   - Maps Synthea encounters to synthetic claims
   - Create sample rcm_claim_header, rcm_claim_line rows

4. **Referral Orders:** Synthetic referral data (generated via script)
   - Incorporates counterparty firms (law, PI, clinic)
   - Links to Synthea patient and provider data

5. **Optional:** MIMIC-IV for enhanced clinical realism (if DUA obtained)

---

## 6. HIGH-LEVEL IMPLEMENTATION PLAN

### Phase 1: Research & Design (Current)
- [x] Extract case study requirements
- [x] Identify suitable data sources
- [x] Review sample schemas
- [ ] *Next:* Design data contracts and schema evolutions

### Phase 2: Data Preparation
1. **EMR Source Setup**
   - Download and configure Synthea
   - Generate synthetic patient population (1,000-5,000 patients)
   - Export in CSV format
   - Map Synthea FHIR output to emr_* tables

2. **Provider Master Data**
   - Download CMS NPPES NPI file
   - Extract and normalize to dim_provider / identity_entity tables
   - Build sample provider-to-facility mappings

3. **RCM/Claims Source Setup**
   - Create synthetic claims generator script
   - Map Synthea encounters → claims
   - Generate rcm_claim_header, rcm_claim_line, rcm_remittance_835 samples
   - Create AR balance snapshots

4. **Referral Orders Setup**
   - Generate synthetic referral orders
   - Assign to counterparty firms (law, PI, clinic types)
   - Link to patients and providers

### Phase 3: Ingestion Architecture
1. **Landing Zone (Bronze Layer)**
   - Design table structures for raw ingestion
   - Add metadata columns (source_system, ingested_at, batch_id, record_hash, payload_json)
   - Define data quality rules (freshness, completeness, dedup, referential integrity)
   - Implement PHI encryption and access controls (HIPAA safeguards)

2. **Data Quality & Validation**
   - Schema validation
   - Null/missing value checks
   - Duplicate detection (hash-based)
   - Referential integrity checks (FK validation)
   - Freshness monitoring (lag detection)

3. **Medallion/Layering Strategy**
   - Bronze: Raw ingested data as-is
   - Silver: Cleaned, deduplicated, validated data
   - Gold: Curated, business-ready tables

### Phase 4: Entity Resolution
1. **Patient 360 Build**
   - Deterministic matching (MRN, SSN-last4, phone, email across emr and rcm sources)
   - Probabilistic scoring (Levenshtein distance on name + DOB + address similarity)
   - Generate bridge_patient_source_xref (crosswalks with confidence scores)
   - Implement survivorship rules (define which source "wins" for each field)

2. **Provider/Counterparty Identity Graph**
   - Deterministic matching on NPI (for providers)
   - Matching on legal business name + address for counterparties
   - Build identity_entity, identity_identifier, identity_edge tables
   - Confidence scoring and evidence trails

### Phase 5: Analytics & BI Data Model
1. **Star Schema Design**
   - Dimensions: dim_date, dim_patient, dim_provider, dim_facility, dim_counterparty_org, dim_payer
   - Facts: fact_receivable, fact_patient_balance

2. **Metric Layer / Semantic Layer**
   - Define key metrics (AR balance, denial rate, aging, collection metrics)
   - Build dbt models or semantic layer (dbt metrics / cube.dev style)

3. **Sample Reports**
   - Top counterparties by pending receivables (aging 61-90, 90+)
   - Patients with highest outstanding balances
   - Denial rate by payer/CPT/facility
   - AR aging analysis

4. **BI Tool Integration**
   - Design dashboards in Tableau, Looker, or Power BI
   - Connect to fact/dimension tables

### Phase 6: Documentation & Submission
1. **Solution Brief** (Written)
   - Executive summary
   - Architecture diagrams (ingestion, entity resolution, BI model)
   - Data contracts and quality framework
   - Entity resolution methodology with confidence scoring
   - HIPAA compliance approach

2. **Code & Artifacts**
   - Data generation scripts (Synthea, synthetic claims/referrals)
   - SQL schema definitions (Bronze, Silver, Gold layers)
   - Data quality validation queries
   - Entity resolution matching logic (Python/SQL)
   - dbt models (if using dbt for transformation)
   - BI dashboard definitions

3. **Testing & Validation**
   - Unit tests for entity resolution
   - Data quality test suite
   - Lineage verification
   - Sample query outputs

---

## 7. TECHNICAL STACK RECOMMENDATIONS

### Data Storage & Processing
- **Data Lake:** AWS S3 (for raw/landing zone) or Snowflake, BigQuery
- **Data Warehouse:** Snowflake, BigQuery, or Redshift (for curated layers)
- **Transformation:** dbt (data transformation), SQL
- **Processing:** Python (for entity resolution, data generation)

### Languages & Libraries
- **Python:** pandas, pydantic (schema validation), fuzzywuzzy/difflib (name matching), hashlib (dedup hashing)
- **SQL:** Window functions, CTEs for entity resolution joins and scoring
- **Entity Resolution:** Record linkage libraries (recordlinkage, dedupe, py_entity_matching)

### Data Quality & Validation
- **dbt tests:** Schema, uniqueness, not null, referential integrity
- **Great Expectations:** Custom data quality rules
- **Custom validation:** Python scripts for business logic checks

### Security & Compliance
- **Encryption:** AES-256 for PHI at rest, TLS for in-transit
- **Access Control:** Role-based access (RBAC) in warehouse
- **Audit Logging:** Data lineage tracking, access logs
- **Tools:** Vault, AWS KMS, or database-native encryption

### BI & Visualization
- **Dashboards:** Tableau, Looker, or Power BI
- **Semantic Layer:** dbt metrics or Cube.dev
- **API:** REST API for data access if needed

---

## 8. KEY CONSIDERATIONS & BEST PRACTICES

### Entity Resolution
- **Confidence Scoring:** Store confidence (0.0-1.0) with every match
- **Explainability:** Record match evidence (e.g., "same NPI", "name + DOB + address within threshold")
- **Fallback Strategy:** Manual review queue for low-confidence matches
- **Incremental Updates:** Re-run matching on new data; preserve historical decisions

### Data Quality
- **Ownership:** Assign data quality rules to each source (EMR SLA, RCM SLA, Referral SLA)
- **Monitoring:** Dashboard for freshness, volume anomalies, null rate trends
- **Error Handling:** Log and alert on failures; implement retry logic for flaky sources

### Scalability
- **MVP Path:** Single-machine Python + SQL on Synthea data (minimal scale)
- **Growth Path:** Distributed dbt + data warehouse (Snowflake, BigQuery), incremental loads
- **Cost:** Optimize for query patterns; partition by date; archive old data

### Privacy & Compliance
- **PHI Masking:** Hash SSN, mask last digits of phone in logs
- **Audit Trail:** Log who accessed what, when
- **Encryption:** All PHI encrypted at rest; encrypted in transit (TLS)
- **Data Retention:** Define retention policies per compliance requirements

---

## 9. SUMMARY TABLE: DATA SOURCES & USE CASES

| Source | Type | Format | Access | MVP Use | Production Use |
|--------|------|--------|--------|---------|-----------------|
| **Synthea** | Synthetic EMR | FHIR, CSV | Free, GitHub | Generate emr_patient, encounter, diagnosis, procedure | Prototype only |
| **MIMIC-IV** | Real ICU/ED Data | CSV, Tables | DUA Required | Validate against real clinical data | Research, development |
| **CMS NPPES** | Provider Master | CSV | Free, public | Build dim_provider, identity_entity | Production master data |
| **X12 835 Format** | Claims Remittance | EDI Text | Specification only | Understand claim payment format | Parse live payer feeds |
| **Synthetic Claims** | Generated Claims | CSV | Custom script | Build rcm_claim_header, line, remittance | Prototype only |
| **Synthetic Referrals** | Generated Referrals | CSV | Custom script | Build referral_order, counterparty tables | Prototype only |
| **Kaggle/Public Data** | Various | CSV, Parquet | Free/Licensed | Exploration, schema validation | Depends on domain |

---

## 10. NEXT STEPS (ACTIONABLE CHECKLIST)

- [ ] **Confirm Data Sources with FlowAI**
  - Verify whether to use Synthea or real MIMIC-IV
  - Ask about expected data volumes
  - Clarify PHI handling preferences

- [ ] **Set Up Development Environment**
  - Clone Synthea repository
  - Download CMS NPPES file
  - Prepare Python environment (pandas, pydantic, entity matching libs)

- [ ] **Design Data Contracts**
  - Define expected column names, types, constraints for each source
  - Specify SLAs (freshness, volume, quality)
  - Document expected transformations

- [ ] **Build Schema Definitions**
  - Create DDL for Bronze (raw), Silver (cleaned), Gold (curated) layers
  - Define metadata columns globally
  - Plan for schema evolution

- [ ] **Prototype Entity Resolution**
  - Test deterministic matching on small Synthea sample
  - Build probabilistic scoring logic
  - Validate on known duplicates

- [ ] **Design BI Schema**
  - Finalize dimension and fact table definitions
  - Plan grain and aggregations
  - Define example queries and dashboards

- [ ] **Write Solution Brief**
  - Create architecture diagrams
  - Document design decisions and trade-offs
  - Include data quality and governance framework
  - Showcase sample queries and reports

---

## References & Sources

### Official Documentation
- [MIMIC-IV Dataset - PhysioNet](https://physionet.org/content/mimiciv/3.1/)
- [MIMIC-IV Paper - Nature Scientific Data](https://www.nature.com/articles/s41597-022-01899-x)
- [Synthea - Synthetic Health](https://synthetichealth.github.io/synthea/)
- [Synthea GitHub](https://github.com/synthetichealth/synthea)
- [CMS NPPES NPI Files](https://download.cms.gov/nppes/NPI_Files.html)
- [NPPES NPI Registry API](https://npiregistry.cms.hhs.gov/api-page)

### X12 & EDI References
- [Stedi X12 835 Guide](https://www.stedi.com/edi/x12/transaction-set/835)
- [Sapling Data X12 Formats](https://saplingdata.com/x12-837-and-835/)
- [CMS 835 Flat File PDF](https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf)

### Healthcare Datasets
- [22 Free and Open Medical Datasets 2025](https://www.shaip.com/blog/healthcare-datasets-for-machine-learning-projects/)
- [Health Data Analytics Open Datasets - UNT](https://guides.library.unt.edu/health-data-analytics/open-datasets)
- [Awesome Medical Datasets - GitHub](https://github.com/openmedlab/Awesome-Medical-Dataset)
- [Awesome Healthcare Datasets - GitHub](https://github.com/geniusrise/awesome-healthcare-datasets)
- [Kaggle Healthcare Dataset](https://www.kaggle.com/datasets/prasad22/healthcare-dataset)

### FHIR & Standards
- [FHIR for Research - Synthea Overview](https://mitre.github.io/fhir-for-research/modules/synthea-overview/)
- [Synthetic Data Generation (HHS)](https://www.healthit.gov/topic/scientific-initiatives/pcor/synthetic-health-data-generation-accelerate-patient-centered-outcomes)

---

**Document Version:** 1.0
**Last Updated:** April 2, 2026
**Status:** Ready for Implementation Planning
