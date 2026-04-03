-- dim_payer: Insurance payer dimension from claim headers
-- Note: payer_id is NULL in source data; keyed on payer_name instead

WITH raw_payers AS (
    SELECT DISTINCT
        payer_name
    FROM {{ ref('silver_rcm_claim_header') }}
    WHERE payer_name IS NOT NULL
)

SELECT
    'DPAY-' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY payer_name) AS VARCHAR), 4, '0')
                                        AS payer_key,
    payer_name                          AS payer_name
FROM raw_payers
