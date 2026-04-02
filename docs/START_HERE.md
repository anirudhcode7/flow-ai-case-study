# FlowAI Data Engineering Case Study - START HERE

**Research & Planning Phase Complete**
**April 2, 2026**

---

## Quick Navigation

### For Quick Understanding (15 minutes)
1. Read this file (you're doing it!)
2. Scan: **FlowAI_Case_Study_Cheat_Sheet.md** (quick reference, one-pager)
3. Skim: **RESEARCH_SUMMARY.txt** (executive summary)

### For Planning Implementation (1 hour)
4. Read: **FlowAI_DE_Case_Study_Research_Plan.md** (comprehensive, 7000+ words)
5. Reference: **Data_Sources_Reference.md** (all download links and setup guides)

### For Deep Dives
6. Original: **DE Case Study.pdf** (17-page case study document)
7. Code/schemas: See the markdown documents for detailed table definitions

---

## What You Need to Know (60 Seconds)

**Project:** Design a healthcare data platform for FlowAI that:
- Ingests from EMR (clinical), RCM (billing), and Referral Order sources
- Resolves patients across systems (entity resolution)
- Creates Provider/Counterparty identity graphs
- Powers BI dashboards for AR aging, denial analysis

**What You'll Build:**
1. Multi-source ingestion architecture (Bronze → Silver → Gold layers)
2. Patient 360 + identity graphs with deterministic + probabilistic matching
3. Analytics star schema with example BI dashboards

**Timeline:** 14-18 days for MVP, 3-4 weeks with full BI

**Data Sources:**
- **Synthea** (synthetic EMR) - https://synthetichealth.github.io/synthea/
- **CMS NPPES** (real provider data) - https://download.cms.gov/nppes/NPI_Files.html
- **Synthetic claims & referrals** (you generate these)
- **Optional:** MIMIC-IV (real validation data) - https://physionet.org/content/mimiciv/3.1/

---

## Document Overview

### 1. FlowAI_Case_Study_Cheat_Sheet.md
**Read this first for quick orientation (5-10 min)**
- All source & target schemas on one page
- Entity resolution strategy (deterministic + probabilistic)
- Example BI queries
- Data quality checklist
- Tech stack overview
- Evaluation rubric

### 2. RESEARCH_SUMMARY.txt
**Executive summary of everything (10-15 min)**
- Case study requirements at a glance
- All data sources with URLs
- 6-phase roadmap overview
- Key decisions documented
- Recommended MVP combination
- Next actions checklist

### 3. FlowAI_DE_Case_Study_Research_Plan.md
**Comprehensive planning guide (30-45 min)**
- Detailed case study breakdown
- Complete sample schemas (EMR, RCM, Referral Orders)
- Target data models (dim_patient, identity_entity, star schema)
- 6-phase implementation roadmap with deliverables
- Technical stack recommendations
- Data source comparisons
- Best practices (entity resolution, quality, compliance)
- Full references and links

### 4. Data_Sources_Reference.md
**Complete download and setup guide (20-30 min)**
- Every data source with direct links
- Setup instructions (time required, file sizes)
- Licensing and cost info
- Recommended MVP combination with rationale
- Quick reference table (sources vs use cases)
- Troubleshooting FAQ
- Local directory structure template
- Step-by-step next actions

### 5. DE Case Study.pdf
**Original case study document (read as needed)**
- Official requirements (sections A/B/C)
- Sample schemas provided by FlowAI
- Target data models
- Evaluation criteria
- Reference for exact language

---

## Getting Started (Step by Step)

### Step 1: Understand the Requirements (30 min)
Read in this order:
1. START_HERE.md (this file)
2. FlowAI_Case_Study_Cheat_Sheet.md
3. RESEARCH_SUMMARY.txt

### Step 2: Plan Your Approach (45 min)
Read:
1. FlowAI_DE_Case_Study_Research_Plan.md (sections 1-3 for requirements)
2. Data_Sources_Reference.md (understand what to download)

### Step 3: Gather Data (1-2 hours)
Follow Data_Sources_Reference.md:
1. Clone Synthea
2. Generate 1,000 synthetic patients
3. Download CMS NPPES provider file
4. Create synthetic claims generator
5. Create synthetic referral generator

### Step 4: Design Your Schema (1-2 hours)
Use the planning documents to:
1. Create Bronze layer (raw ingestion)
2. Design Silver layer (cleaned data)
3. Design Gold layer (curated tables)

### Step 5: Implement (14+ hours)
Follow the 6-phase roadmap in FlowAI_DE_Case_Study_Research_Plan.md:
- Phase 2: Data Preparation
- Phase 3: Ingestion Architecture
- Phase 4: Entity Resolution
- Phase 5: Analytics & BI
- Phase 6: Documentation

---

## Key Concepts (Glossary)

**EMR:** Electronic Medical Record - clinical data (encounters, diagnoses, procedures)
**RCM:** Revenue Cycle Management - billing data (claims, payments, AR)
**Patient 360:** Canonical patient record unified across all sources
**Entity Resolution:** Matching records for the same patient/provider across systems
**Deterministic Matching:** Exact match on keys (MRN, SSN, NPI)
**Probabilistic Matching:** Similarity scoring on names, addresses, dates
**Identity Graph:** Network of entities (patients, providers, organizations) and their relationships
**Bronze/Silver/Gold:** Medallion architecture layers (raw → cleaned → curated)
**HIPAA:** Health Insurance Portability & Accountability Act (privacy/security law)
**PHI:** Protected Health Information (patient data requiring encryption)
**NPI:** National Provider Identifier (10-digit unique provider ID)
**X12 835:** EDI transaction standard for remittance advice (claim payments)
**Star Schema:** Analytics data model (dimensions + facts) for BI

---

## Files in This Directory

```
~/Downloads/
├── START_HERE.md ◄─── You are here
├── RESEARCH_SUMMARY.txt
├── FlowAI_Case_Study_Cheat_Sheet.md
├── FlowAI_DE_Case_Study_Research_Plan.md
├── Data_Sources_Reference.md
└── DE Case Study.pdf (original, 17 pages)
```

---

## Technology Recommendations

**For MVP:**
- Language: Python + SQL
- Storage: Snowflake / BigQuery / PostgreSQL
- Transformation: dbt or custom Python
- Entity Resolution: pandas + fuzzywuzzy
- BI: Tableau / Looker / Power BI

**Key Libraries:**
- pandas (data manipulation)
- pydantic (schema validation)
- fuzzywuzzy (name matching)
- recordlinkage (entity resolution)
- dbt (SQL transformation framework)

---

## Checklist Before You Start Coding

- [ ] Read FlowAI_Case_Study_Cheat_Sheet.md
- [ ] Review Data_Sources_Reference.md
- [ ] Understand the 3 main deliverables (ingestion, entity resolution, analytics)
- [ ] Plan your MVP scope (1,000-5,000 synthetic patients)
- [ ] Set up your development environment (Python 3.9+, git, SQL client)
- [ ] Clone Synthea repository
- [ ] Download CMS NPPES file
- [ ] Clarify any questions with FlowAI (real vs synthetic data preference, timeline, scale)

---

## Common Questions

**Q: Where do I start implementing?**
A: Phase 2: Data Preparation. See Data_Sources_Reference.md for step-by-step setup.

**Q: Should I use Synthea or MIMIC-IV?**
A: Start with Synthea (faster MVP). Use MIMIC-IV later to validate your architecture against real data.

**Q: How many synthetic patients should I generate?**
A: 1,000-5,000 for MVP. Enough to prove the concepts without overcomplicating.

**Q: Do I need all 6 phases?**
A: Yes, but MVP scope means lean implementations. Focus on: core ingestion, entity resolution, and basic analytics.

**Q: What's the hardest part?**
A: Entity resolution. Start with deterministic matching, add probabilistic scoring gradually.

**Q: Can I use dbt?**
A: Yes, highly recommended for Phase 3+. Great for reproducibility and testing.

**Q: What about HIPAA compliance?**
A: Use encryption (AES-256), access controls (RBAC), audit logging, and masked logs. See FlowAI_DE_Case_Study_Research_Plan.md for details.

---

## Scoring / Evaluation

You'll be evaluated on:
1. **Architecture (30%)** - Correct patterns, lineage, replayability
2. **Data Modeling (20%)** - Clear keys, normalization, extensibility
3. **Entity Resolution (20%)** - Accurate matching, confidence scoring, explainability
4. **Quality & Governance (20%)** - Tests, monitoring, PHI security
5. **Pragmatism (10%)** - MVP path clear, scaling plan articulated

Focus on getting these 5 areas right rather than building extras.

---

## Resources & Links

**Official Documentation:**
- Synthea: https://synthetichealth.github.io/synthea/
- CMS NPPES: https://download.cms.gov/nppes/NPI_Files.html
- MIMIC-IV: https://physionet.org/content/mimiciv/3.1/

**GitHub:**
- Synthea: https://github.com/synthetichealth/synthea
- Awesome Datasets: https://github.com/openmedlab/Awesome-Medical-Dataset

**References:**
- X12 835 Format: https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf
- Case Study PDF: DE Case Study.pdf (in this directory)

---

## Next Action

**Read FlowAI_Case_Study_Cheat_Sheet.md next (5-10 min)**

It's a one-pager with all the schemas, entity resolution strategy, and key concepts you need to understand before diving into implementation.

---

**Status:** ✅ Research Complete | Ready for Implementation
**Timeline:** 14-18 days for MVP
**No code written yet** - This is purely research and planning
