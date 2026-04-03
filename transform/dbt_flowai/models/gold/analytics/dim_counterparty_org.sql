-- dim_counterparty_org: Counterparty organizations from referral orders
-- Normalization: UPPER + strip common legal suffixes for grouping

WITH raw_counterparties AS (
    SELECT DISTINCT
        counterparty_org_name,
        counterparty_org_type
    FROM {{ ref('silver_referral_order') }}
    WHERE counterparty_org_name IS NOT NULL
),

normalized AS (
    SELECT
        counterparty_org_name AS original_name,
        counterparty_org_type,
        TRIM(REGEXP_REPLACE(
            UPPER(counterparty_org_name),
            '\s*(LLC|INC|CORP|LLP|PLLC|PA|PC|P\.A\.|P\.C\.)\s*$',
            '',
            'gi'
        )) AS normalized_name
    FROM raw_counterparties
),

grouped AS (
    SELECT
        normalized_name,
        MODE(counterparty_org_type)  AS counterparty_org_type,
        MIN(original_name)           AS canonical_name,
        COUNT(*)                     AS variant_count
    FROM normalized
    GROUP BY normalized_name
)

SELECT
    'DCPTY-' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY normalized_name) AS VARCHAR), 4, '0')
                                AS counterparty_org_key,
    canonical_name              AS counterparty_org_name,
    normalized_name,
    counterparty_org_type,
    variant_count               AS name_variant_count
FROM grouped
