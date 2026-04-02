WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_rcm_claim_header') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(claim_id)                                                     AS claim_id,
    TRIM(rcm_account_id)                                               AS rcm_account_id,
    TRIM(facility_id)                                                  AS facility_id,
    CASE
        WHEN REGEXP_MATCHES(TRIM(billing_provider_npi), '^[0-9]{10}$')
            THEN TRIM(billing_provider_npi)
        ELSE NULL
    END                                                                AS billing_provider_npi,
    CASE
        WHEN REGEXP_MATCHES(TRIM(rendering_provider_npi), '^[0-9]{10}$')
            THEN TRIM(rendering_provider_npi)
        ELSE NULL
    END                                                                AS rendering_provider_npi,
    TRIM(payer_id)                                                     AS payer_id,
    TRIM(payer_name)                                                   AS payer_name,
    CASE
        WHEN LOWER(TRIM(claim_type)) IN ('institutional', 'professional')
            THEN LOWER(TRIM(claim_type))
        ELSE NULL
    END                                                                AS claim_type,
    TRY_CAST(total_charge_amount AS DECIMAL(12, 2))                   AS total_charge_amount,
    CASE
        WHEN LOWER(TRIM(claim_status)) IN ('submitted', 'accepted', 'denied', 'paid', 'void')
            THEN LOWER(TRIM(claim_status))
        ELSE NULL
    END                                                                AS claim_status,
    TRY_CAST(submitted_date AS DATE)                                   AS submitted_date,
    TRY_CAST(service_from_date AS DATE)                                AS service_from_date,
    TRY_CAST(service_to_date AS DATE)                                  AS service_to_date,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
