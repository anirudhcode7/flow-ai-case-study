WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY emr_provider_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_emr_provider') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(emr_provider_id)                                              AS emr_provider_id,
    CASE
        WHEN REGEXP_MATCHES(TRIM(npi), '^[0-9]{10}$') THEN TRIM(npi)
        ELSE NULL
    END                                                                AS npi,
    CONCAT(UPPER(LEFT(TRIM(first_name), 1)), LOWER(SUBSTRING(TRIM(first_name), 2))) AS first_name,
    CONCAT(UPPER(LEFT(TRIM(last_name), 1)), LOWER(SUBSTRING(TRIM(last_name), 2)))  AS last_name,
    TRIM(specialty)                                                    AS specialty,
    UPPER(TRIM(org_name))                                              AS org_name,
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
