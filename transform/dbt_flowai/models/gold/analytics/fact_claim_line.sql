-- fact_claim_line: Claim line-level fact for denial and CPT analysis
-- Grain: claim_id + line_num

WITH claim_lines AS (
    SELECT
        claim_id,
        line_num,
        cpt_code,
        units,
        charge_amount,
        allowed_amount,
        paid_amount,
        denial_code,
        CASE WHEN denial_code IS NOT NULL
             THEN TRUE ELSE FALSE
        END AS is_denied
    FROM {{ ref('silver_rcm_claim_line') }}
),

claim_header AS (
    SELECT
        claim_id,
        rcm_account_id,
        facility_id,
        billing_provider_npi,
        rendering_provider_npi,
        payer_name,
        claim_status,
        submitted_date,
        service_from_date,
        service_to_date
    FROM {{ ref('silver_rcm_claim_header') }}
),

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
    cl.claim_id,
    cl.line_num,
    cl.cpt_code,
    cl.units,
    cl.charge_amount,
    cl.allowed_amount,
    cl.paid_amount,
    cl.denial_code,
    cl.is_denied,

    -- Dimension keys
    pm.patient_id,
    fd.facility_key,
    pd.payer_key,

    -- Claim context
    ch.claim_status,
    ch.submitted_date,
    ch.service_from_date,
    ch.service_to_date,
    ch.billing_provider_npi,
    ch.rendering_provider_npi

FROM claim_lines cl

JOIN claim_header ch
    ON cl.claim_id = ch.claim_id

LEFT JOIN patient_map pm
    ON ch.rcm_account_id = pm.rcm_account_id

LEFT JOIN payer_dim pd
    ON ch.payer_name = pd.payer_name

LEFT JOIN facility_dim fd
    ON ch.facility_id = fd.source_facility_id
