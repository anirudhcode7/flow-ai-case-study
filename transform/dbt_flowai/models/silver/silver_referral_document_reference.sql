WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY referral_order_id, doc_type, doc_uri
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_referral_document_reference') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(referral_order_id)                                            AS referral_order_id,
    CASE
        WHEN LOWER(TRIM(doc_type)) IN ('referral_form', 'imaging', 'insurance_card')
            THEN LOWER(TRIM(doc_type))
        ELSE NULL
    END                                                                AS doc_type,
    TRIM(doc_uri)                                                      AS doc_uri,
    TRY_CAST(uploaded_at AS TIMESTAMP)                                 AS uploaded_at,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
