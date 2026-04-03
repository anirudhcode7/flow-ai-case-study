-- dim_facility: Unique facilities from encounters, claims, and referrals
-- Facility IDs are opaque UUIDs; readable labels generated as "Facility {id}"

WITH encounter_facilities AS (
    SELECT DISTINCT facility_id
    FROM {{ ref('silver_emr_encounter') }}
    WHERE facility_id IS NOT NULL
),

claim_facilities AS (
    SELECT DISTINCT facility_id
    FROM {{ ref('silver_rcm_claim_header') }}
    WHERE facility_id IS NOT NULL
),

referral_facilities AS (
    SELECT DISTINCT receiving_facility_id AS facility_id
    FROM {{ ref('silver_referral_order') }}
    WHERE receiving_facility_id IS NOT NULL
),

all_facilities AS (
    SELECT facility_id FROM encounter_facilities
    UNION
    SELECT facility_id FROM claim_facilities
    UNION
    SELECT facility_id FROM referral_facilities
)

SELECT
    'DFAC-' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY facility_id) AS VARCHAR), 4, '0')
                                AS facility_key,
    facility_id                 AS source_facility_id,
    'Facility ' || facility_id  AS facility_name,
    CAST(NULL AS VARCHAR)       AS facility_type,
    CAST(NULL AS VARCHAR)       AS address,
    CAST(NULL AS VARCHAR)       AS city,
    CAST(NULL AS VARCHAR)       AS state,
    CAST(NULL AS VARCHAR)       AS zip
FROM all_facilities
