WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY rcm_account_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_rcm_patient_account') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(rcm_account_id)                                               AS rcm_account_id,
    -- emr_patient_id is intentionally ~30% NULL — preserve nulls
    CASE WHEN emr_patient_id IS NOT NULL THEN TRIM(emr_patient_id) ELSE NULL END
                                                                       AS emr_patient_id,
    TRIM(patient_external_id)                                          AS patient_external_id,
    UPPER(TRIM(guarantor_name))                                        AS guarantor_name,
    CONCAT(UPPER(LEFT(TRIM(patient_first_name), 1)), LOWER(SUBSTRING(TRIM(patient_first_name), 2))) AS patient_first_name,
    CONCAT(UPPER(LEFT(TRIM(patient_last_name), 1)), LOWER(SUBSTRING(TRIM(patient_last_name), 2)))  AS patient_last_name,
    TRY_CAST(dob AS DATE)                                              AS dob,
    REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g')                    AS phone,
    UPPER(TRIM(address_line1))                                         AS address_line1,
    UPPER(TRIM(city))                                                  AS city,
    UPPER(TRIM(state))                                                 AS state,
    LEFT(TRIM(zip), 5)                                                 AS zip,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
