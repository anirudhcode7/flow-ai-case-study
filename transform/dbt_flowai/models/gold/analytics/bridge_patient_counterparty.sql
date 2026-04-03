-- bridge_patient_counterparty: Many-to-many relationship between patients and counterparty orgs
-- Grain: patient_id + counterparty_org_key
-- Join path: crosswalk (referral) → referral_order → normalize name → dim_counterparty_org

WITH referral_patients AS (
    SELECT
        xref.patient_id,
        ro.referral_order_id,
        ro.referral_created_at,
        ro.counterparty_org_name
    FROM {{ ref('gold_bridge_patient_source_xref') }} xref
    JOIN {{ ref('silver_referral_order') }} ro
        ON xref.source_patient_key = ro.referral_order_id
    WHERE xref.source_system = 'referral'
      AND ro.counterparty_org_name IS NOT NULL
),

-- Normalize counterparty name to match dim_counterparty_org
with_normalized AS (
    SELECT
        patient_id,
        referral_order_id,
        referral_created_at,
        counterparty_org_name,
        TRIM(REGEXP_REPLACE(
            UPPER(counterparty_org_name),
            '\s*(LLC|INC|CORP|LLP|PLLC|PA|PC|P\.A\.|P\.C\.)\s*$',
            '',
            'gi'
        )) AS normalized_name
    FROM referral_patients
),

-- Join to dim to get the surrogate key
joined AS (
    SELECT
        rp.patient_id,
        cd.counterparty_org_key,
        rp.referral_order_id,
        rp.referral_created_at,
        rp.counterparty_org_name
    FROM with_normalized rp
    JOIN {{ ref('dim_counterparty_org') }} cd
        ON rp.normalized_name = cd.normalized_name
)

SELECT DISTINCT
    patient_id,
    counterparty_org_key,
    referral_order_id,
    referral_created_at,
    counterparty_org_name
FROM joined
