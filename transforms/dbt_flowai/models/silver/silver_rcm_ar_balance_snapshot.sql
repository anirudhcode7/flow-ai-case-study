WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, claim_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_rcm_ar_balance_snapshot') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRY_CAST(snapshot_date AS DATE)                                    AS snapshot_date,
    TRIM(claim_id)                                                     AS claim_id,
    TRIM(rcm_account_id)                                               AS rcm_account_id,
    TRY_CAST(payer_responsibility_balance AS DECIMAL(12, 2))          AS payer_responsibility_balance,
    TRY_CAST(patient_responsibility_balance AS DECIMAL(12, 2))        AS patient_responsibility_balance,
    CASE
        WHEN TRIM(aging_bucket) IN ('0-30', '31-60', '61-90', '90+')
            THEN TRIM(aging_bucket)
        ELSE NULL
    END                                                                AS aging_bucket,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
