-- FlowAI Bronze Layer Schema
-- All 13 source tables stored as VARCHAR (raw landing zone).
-- Metadata columns appended by load_bronze.py.
-- Run via: duckdb data/duckdb/flowai.duckdb < ingestion/bronze/schema.sql

CREATE SCHEMA IF NOT EXISTS bronze;

-- ────────────────────────────────────────────────────────────────────────────
-- EMR Tables
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.emr_patient (
    -- Source columns
    emr_patient_id      VARCHAR,
    mrn                 VARCHAR,
    first_name          VARCHAR,
    last_name           VARCHAR,
    dob                 VARCHAR,
    sex                 VARCHAR,
    phone               VARCHAR,
    email               VARCHAR,
    address_line1       VARCHAR,
    city                VARCHAR,
    state               VARCHAR,
    zip                 VARCHAR,
    ssn_last4           VARCHAR,
    deceased_flag       VARCHAR,
    last_updated_at     VARCHAR,
    -- Metadata
    _ingested_at        TIMESTAMP DEFAULT current_timestamp,
    _source_system      VARCHAR,
    _source_file        VARCHAR,
    _row_hash           VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.emr_encounter (
    emr_encounter_id        VARCHAR,
    emr_patient_id          VARCHAR,
    encounter_type          VARCHAR,
    status                  VARCHAR,
    start_time              VARCHAR,
    end_time                VARCHAR,
    facility_id             VARCHAR,
    attending_provider_id   VARCHAR,
    chief_complaint         VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.emr_diagnosis (
    emr_encounter_id        VARCHAR,
    icd10_code              VARCHAR,
    diagnosis_desc          VARCHAR,
    diagnosis_rank          VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.emr_procedure (
    emr_encounter_id        VARCHAR,
    cpt_code                VARCHAR,
    procedure_desc          VARCHAR,
    performed_time          VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.emr_provider (
    emr_provider_id         VARCHAR,
    npi                     VARCHAR,
    first_name              VARCHAR,
    last_name               VARCHAR,
    specialty               VARCHAR,
    org_name                VARCHAR,
    phone                   VARCHAR,
    address_line1           VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    zip                     VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

-- ────────────────────────────────────────────────────────────────────────────
-- RCM Tables
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.rcm_patient_account (
    rcm_account_id          VARCHAR,
    emr_patient_id          VARCHAR,   -- nullable: RCM_NULL_EMR_LINK_RATE % will be NULL
    patient_external_id     VARCHAR,
    guarantor_name          VARCHAR,
    patient_first_name      VARCHAR,
    patient_last_name       VARCHAR,
    dob                     VARCHAR,
    phone                   VARCHAR,
    address_line1           VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    zip                     VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.rcm_claim_header (
    claim_id                VARCHAR,
    rcm_account_id          VARCHAR,
    facility_id             VARCHAR,
    billing_provider_npi    VARCHAR,
    rendering_provider_npi  VARCHAR,
    payer_id                VARCHAR,
    payer_name              VARCHAR,
    claim_type              VARCHAR,
    total_charge_amount     VARCHAR,
    claim_status            VARCHAR,
    submitted_date          VARCHAR,
    service_from_date       VARCHAR,
    service_to_date         VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.rcm_claim_line (
    claim_id                VARCHAR,
    line_num                VARCHAR,
    cpt_code                VARCHAR,
    units                   VARCHAR,
    charge_amount           VARCHAR,
    allowed_amount          VARCHAR,
    paid_amount             VARCHAR,
    denial_code             VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.rcm_remittance_835 (
    remit_id                VARCHAR,
    payer_id                VARCHAR,
    payment_date            VARCHAR,
    trace_number            VARCHAR,
    total_payment_amount    VARCHAR,
    raw_835_document_ref    VARCHAR,
    -- Metadata
    _ingested_at            TIMESTAMP DEFAULT current_timestamp,
    _source_system          VARCHAR,
    _source_file            VARCHAR,
    _row_hash               VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.rcm_ar_balance_snapshot (
    snapshot_date                   VARCHAR,
    claim_id                        VARCHAR,
    rcm_account_id                  VARCHAR,
    payer_responsibility_balance    VARCHAR,
    patient_responsibility_balance  VARCHAR,
    aging_bucket                    VARCHAR,
    -- Metadata
    _ingested_at                    TIMESTAMP DEFAULT current_timestamp,
    _source_system                  VARCHAR,
    _source_file                    VARCHAR,
    _row_hash                       VARCHAR
);

-- ────────────────────────────────────────────────────────────────────────────
-- Referral Tables
-- ────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.referral_order (
    referral_order_id           VARCHAR,
    referral_source_system      VARCHAR,
    referral_created_at         VARCHAR,
    patient_first_name          VARCHAR,
    patient_last_name           VARCHAR,
    dob                         VARCHAR,
    phone                       VARCHAR,
    email                       VARCHAR,
    address_line1               VARCHAR,
    city                        VARCHAR,
    state                       VARCHAR,
    zip                         VARCHAR,
    referring_provider_name     VARCHAR,
    referring_provider_npi      VARCHAR,   -- nullable: REFERRAL_NULL_NPI_RATE % NULL
    receiving_facility_id       VARCHAR,
    primary_diagnosis_icd10     VARCHAR,
    requested_service_cpt       VARCHAR,
    priority                    VARCHAR,
    order_status                VARCHAR,
    counterparty_org_name       VARCHAR,
    counterparty_org_type       VARCHAR,
    case_reference_id           VARCHAR,
    -- Metadata
    _ingested_at                TIMESTAMP DEFAULT current_timestamp,
    _source_system              VARCHAR,
    _source_file                VARCHAR,
    _row_hash                   VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.referral_order_status_history (
    referral_order_id           VARCHAR,
    status                      VARCHAR,
    status_time                 VARCHAR,
    changed_by                  VARCHAR,
    -- Metadata
    _ingested_at                TIMESTAMP DEFAULT current_timestamp,
    _source_system              VARCHAR,
    _source_file                VARCHAR,
    _row_hash                   VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.referral_document_reference (
    referral_order_id           VARCHAR,
    doc_type                    VARCHAR,
    doc_uri                     VARCHAR,
    uploaded_at                 VARCHAR,
    -- Metadata
    _ingested_at                TIMESTAMP DEFAULT current_timestamp,
    _source_system              VARCHAR,
    _source_file                VARCHAR,
    _row_hash                   VARCHAR
);
