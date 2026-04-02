WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY referral_order_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_referral_order') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(referral_order_id)                                            AS referral_order_id,
    TRIM(referral_source_system)                                       AS referral_source_system,
    TRY_CAST(referral_created_at AS TIMESTAMP)                         AS referral_created_at,
    CONCAT(UPPER(LEFT(TRIM(patient_first_name), 1)), LOWER(SUBSTRING(TRIM(patient_first_name), 2))) AS patient_first_name,
    CONCAT(UPPER(LEFT(TRIM(patient_last_name), 1)), LOWER(SUBSTRING(TRIM(patient_last_name), 2)))  AS patient_last_name,
    TRY_CAST(dob AS DATE)                                              AS dob,
    REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g')                    AS phone,
    LOWER(TRIM(email))                                                 AS email,
    UPPER(TRIM(address_line1))                                         AS address_line1,
    UPPER(TRIM(city))                                                  AS city,
    UPPER(TRIM(state))                                                 AS state,
    LEFT(TRIM(zip), 5)                                                 AS zip,
    -- intentionally messy — kept as-is for entity resolution
    TRIM(referring_provider_name)                                      AS referring_provider_name,
    -- referring_provider_npi is intentionally ~20% NULL
    CASE
        WHEN referring_provider_npi IS NOT NULL
            AND REGEXP_MATCHES(TRIM(referring_provider_npi), '^[0-9]{10}$')
            THEN TRIM(referring_provider_npi)
        WHEN referring_provider_npi IS NULL THEN NULL
        ELSE NULL
    END                                                                AS referring_provider_npi,
    TRIM(receiving_facility_id)                                        AS receiving_facility_id,
    UPPER(TRIM(primary_diagnosis_icd10))                               AS primary_diagnosis_icd10,
    TRIM(requested_service_cpt)                                        AS requested_service_cpt,
    CASE
        WHEN LOWER(TRIM(priority)) IN ('routine', 'urgent')
            THEN LOWER(TRIM(priority))
        ELSE NULL
    END                                                                AS priority,
    CASE
        WHEN LOWER(TRIM(order_status)) IN ('received', 'scheduled', 'completed', 'cancelled')
            THEN LOWER(TRIM(order_status))
        ELSE NULL
    END                                                                AS order_status,
    -- counterparty_org_name kept original case — entity resolution will handle variants
    TRIM(counterparty_org_name)                                        AS counterparty_org_name,
    CASE
        WHEN LOWER(TRIM(counterparty_org_type)) IN ('law_firm', 'pi_firm', 'clinic', 'employer')
            THEN LOWER(TRIM(counterparty_org_type))
        ELSE NULL
    END                                                                AS counterparty_org_type,
    TRIM(case_reference_id)                                            AS case_reference_id,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
