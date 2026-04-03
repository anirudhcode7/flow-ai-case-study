-- dim_provider: Canonical provider dimension from EMR + external NPIs
-- Strategy: EMR providers as base, stub records for NPIs found only in claims/referrals

WITH emr_providers AS (
    SELECT
        emr_provider_id,
        npi,
        first_name,
        last_name,
        specialty,
        org_name,
        phone,
        address_line1,
        city,
        state,
        zip,
        'emr' AS source
    FROM {{ ref('silver_emr_provider') }}
),

-- All unique NPIs from claims (billing + rendering)
claim_npis AS (
    SELECT DISTINCT billing_provider_npi AS npi
    FROM {{ ref('silver_rcm_claim_header') }}
    WHERE billing_provider_npi IS NOT NULL
    UNION
    SELECT DISTINCT rendering_provider_npi
    FROM {{ ref('silver_rcm_claim_header') }}
    WHERE rendering_provider_npi IS NOT NULL
),

-- NPIs from referrals
referral_npis AS (
    SELECT DISTINCT referring_provider_npi AS npi
    FROM {{ ref('silver_referral_order') }}
    WHERE referring_provider_npi IS NOT NULL
),

-- Combined unique NPIs from non-EMR sources
all_external_npis AS (
    SELECT npi FROM claim_npis
    UNION
    SELECT npi FROM referral_npis
),

-- NPIs not already in EMR provider data
missing_npis AS (
    SELECT a.npi
    FROM all_external_npis a
    LEFT JOIN emr_providers e ON a.npi = e.npi
    WHERE e.npi IS NULL
),

-- Try to get provider names from referral free-text for missing NPIs
referral_provider_names AS (
    SELECT DISTINCT
        referring_provider_npi AS npi,
        FIRST(referring_provider_name) AS referring_name
    FROM {{ ref('silver_referral_order') }}
    WHERE referring_provider_npi IS NOT NULL
    GROUP BY referring_provider_npi
),

-- Build stub records for NPIs not in EMR
stub_providers AS (
    SELECT
        'PROV-EXT-' || m.npi       AS emr_provider_id,
        m.npi,
        COALESCE(
            CASE WHEN rn.referring_name LIKE '% %'
                 THEN TRIM(SPLIT_PART(rn.referring_name, ' ', 1))
            END,
            'Unknown'
        )                           AS first_name,
        COALESCE(
            CASE WHEN rn.referring_name LIKE '% %'
                 THEN TRIM(SPLIT_PART(rn.referring_name, ' ', -1))
            END,
            'Unknown'
        )                           AS last_name,
        CAST(NULL AS VARCHAR)       AS specialty,
        CAST(NULL AS VARCHAR)       AS org_name,
        CAST(NULL AS VARCHAR)       AS phone,
        CAST(NULL AS VARCHAR)       AS address_line1,
        CAST(NULL AS VARCHAR)       AS city,
        CAST(NULL AS VARCHAR)       AS state,
        CAST(NULL AS VARCHAR)       AS zip,
        'external'                  AS source
    FROM missing_npis m
    LEFT JOIN referral_provider_names rn ON m.npi = rn.npi
),

combined AS (
    SELECT * FROM emr_providers
    UNION ALL
    SELECT * FROM stub_providers
)

SELECT
    'DPROV-' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY npi, emr_provider_id) AS VARCHAR), 5, '0')
                                    AS provider_key,
    emr_provider_id                 AS source_provider_id,
    npi,
    first_name,
    last_name,
    first_name || ' ' || last_name  AS full_name,
    specialty,
    org_name,
    phone,
    address_line1,
    city,
    state,
    zip,
    source                          AS source_system
FROM combined
