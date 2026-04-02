WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id, line_num
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_rcm_claim_line') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(claim_id)                                                     AS claim_id,
    TRY_CAST(line_num AS INTEGER)                                      AS line_num,
    TRIM(cpt_code)                                                     AS cpt_code,
    TRY_CAST(units AS DECIMAL(10, 2))                                  AS units,
    TRY_CAST(charge_amount AS DECIMAL(12, 2))                         AS charge_amount,
    TRY_CAST(allowed_amount AS DECIMAL(12, 2))                        AS allowed_amount,
    TRY_CAST(paid_amount AS DECIMAL(12, 2))                           AS paid_amount,
    CASE WHEN denial_code IS NOT NULL THEN TRIM(denial_code) ELSE NULL END
                                                                       AS denial_code,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
