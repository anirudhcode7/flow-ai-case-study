-- fact_receivable: AR fact table at claim + snapshot_date grain
-- No counterparty key — use bridge_patient_counterparty for counterparty analysis

WITH ar_snapshots AS (
    SELECT
        snapshot_date,
        claim_id,
        rcm_account_id,
        payer_responsibility_balance,
        patient_responsibility_balance,
        COALESCE(payer_responsibility_balance, 0)
            + COALESCE(patient_responsibility_balance, 0) AS total_ar_balance,
        aging_bucket
    FROM {{ ref('silver_rcm_ar_balance_snapshot') }}
),

claim_details AS (
    SELECT
        claim_id,
        rcm_account_id,
        facility_id,
        billing_provider_npi,
        rendering_provider_npi,
        payer_name,
        claim_status,
        total_charge_amount,
        submitted_date,
        service_from_date
    FROM {{ ref('silver_rcm_claim_header') }}
),

-- rcm_account_id → canonical patient_id via crosswalk
patient_map AS (
    SELECT
        source_patient_key AS rcm_account_id,
        patient_id
    FROM {{ ref('gold_bridge_patient_source_xref') }}
    WHERE source_system = 'rcm'
),

payer_dim AS (
    SELECT payer_key, payer_name
    FROM {{ ref('dim_payer') }}
),

facility_dim AS (
    SELECT facility_key, source_facility_id
    FROM {{ ref('dim_facility') }}
)

SELECT
    ar.snapshot_date,
    ar.claim_id,
    pm.patient_id,

    -- Dimension keys
    fd.facility_key,
    pd.payer_key,

    -- Measures
    ar.total_ar_balance,
    ar.payer_responsibility_balance  AS payer_balance,
    ar.patient_responsibility_balance AS patient_balance,
    ar.aging_bucket,

    -- Claim context (denormalized for query convenience)
    cl.claim_status,
    cl.total_charge_amount,
    cl.submitted_date,
    cl.service_from_date

FROM ar_snapshots ar

LEFT JOIN claim_details cl
    ON ar.claim_id = cl.claim_id

LEFT JOIN patient_map pm
    ON ar.rcm_account_id = pm.rcm_account_id

LEFT JOIN payer_dim pd
    ON cl.payer_name = pd.payer_name

LEFT JOIN facility_dim fd
    ON cl.facility_id = fd.source_facility_id
