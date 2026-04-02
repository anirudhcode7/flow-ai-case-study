WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY referral_order_id, status, status_time
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_referral_order_status_history') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(referral_order_id)                                            AS referral_order_id,
    LOWER(TRIM(status))                                                AS status,
    TRY_CAST(status_time AS TIMESTAMP)                                 AS status_time,
    TRIM(changed_by)                                                   AS changed_by,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
