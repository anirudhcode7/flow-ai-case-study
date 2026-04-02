WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY remit_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_rcm_remittance_835') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(remit_id)                                                     AS remit_id,
    TRIM(payer_id)                                                     AS payer_id,
    TRY_CAST(payment_date AS DATE)                                     AS payment_date,
    TRIM(trace_number)                                                 AS trace_number,
    TRY_CAST(total_payment_amount AS DECIMAL(12, 2))                  AS total_payment_amount,
    TRIM(raw_835_document_ref)                                         AS raw_835_document_ref,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
