WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY emr_patient_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_emr_patient') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(emr_patient_id)                                               AS emr_patient_id,
    UPPER(TRIM(mrn))                                                   AS mrn,
    CONCAT(UPPER(LEFT(TRIM(first_name), 1)), LOWER(SUBSTRING(TRIM(first_name), 2))) AS first_name,
    CONCAT(UPPER(LEFT(TRIM(last_name), 1)), LOWER(SUBSTRING(TRIM(last_name), 2)))  AS last_name,
    TRY_CAST(dob AS DATE)                                              AS dob,
    CASE
        WHEN UPPER(TRIM(sex)) IN ('M', 'F', 'O', 'U') THEN UPPER(TRIM(sex))
        ELSE 'U'
    END                                                                AS sex,
    REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g')                    AS phone,
    LOWER(TRIM(email))                                                 AS email,
    UPPER(TRIM(address_line1))                                         AS address_line1,
    UPPER(TRIM(city))                                                  AS city,
    UPPER(TRIM(state))                                                 AS state,
    LEFT(TRIM(zip), 5)                                                 AS zip,
    TRIM(ssn_last4)                                                    AS ssn_last4,
    TRY_CAST(deceased_flag AS BOOLEAN)                                 AS deceased_flag,
    TRY_CAST(last_updated_at AS TIMESTAMP)                             AS last_updated_at,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
