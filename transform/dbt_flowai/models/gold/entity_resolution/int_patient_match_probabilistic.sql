WITH spine AS (
    SELECT * FROM {{ ref('int_patient_spine') }}
),

candidate_pairs AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        a.first_name AS first_name_a,
        b.first_name AS first_name_b,
        a.last_name AS last_name_a,
        b.last_name AS last_name_b,
        a.dob AS dob_a,
        b.dob AS dob_b,
        a.phone AS phone_a,
        b.phone AS phone_b,
        a.email AS email_a,
        b.email AS email_b,
        a.address_line1 AS address_a,
        b.address_line1 AS address_b,
        a.city AS city_a,
        b.city AS city_b,
        a.zip AS zip_a,
        b.zip AS zip_b
    FROM spine a
    JOIN spine b
        ON a.source_system < b.source_system
    WHERE (
        (a.last_name IS NOT NULL AND b.last_name IS NOT NULL
         AND LEFT(LOWER(a.last_name), 3) = LEFT(LOWER(b.last_name), 3))
        OR (a.dob IS NOT NULL AND b.dob IS NOT NULL
            AND ABS(YEAR(a.dob) - YEAR(b.dob)) <= 1)
        OR (a.phone IS NOT NULL AND a.phone != ''
            AND b.phone IS NOT NULL AND b.phone != ''
            AND a.phone = b.phone)
        OR (a.zip IS NOT NULL AND a.zip != ''
            AND b.zip IS NOT NULL AND b.zip != ''
            AND a.zip = b.zip)
    )
),

scored AS (
    SELECT
        source_system_a,
        source_key_a,
        source_system_b,
        source_key_b,

        CASE
            WHEN last_name_a IS NULL OR last_name_b IS NULL THEN 0.0
            ELSE jaro_winkler_similarity(LOWER(last_name_a), LOWER(last_name_b))
        END AS last_name_score,

        CASE
            WHEN first_name_a IS NULL OR first_name_b IS NULL THEN 0.0
            ELSE jaro_winkler_similarity(LOWER(first_name_a), LOWER(first_name_b))
        END AS first_name_score,

        CASE
            WHEN dob_a IS NULL OR dob_b IS NULL THEN 0.0
            WHEN dob_a = dob_b THEN 1.0
            WHEN ABS(DATEDIFF('day', dob_a, dob_b)) <= 2 THEN 0.9
            WHEN MONTH(dob_a) = MONTH(dob_b)
                 AND DAY(dob_a) = DAY(dob_b)
                 AND ABS(YEAR(dob_a) - YEAR(dob_b)) = 1 THEN 0.6
            WHEN YEAR(dob_a) = YEAR(dob_b)
                 AND MONTH(dob_a) = DAY(dob_b)
                 AND DAY(dob_a) = MONTH(dob_b) THEN 0.5
            ELSE 0.0
        END AS dob_score,

        CASE
            WHEN (phone_a IS NULL OR phone_a = '') OR (phone_b IS NULL OR phone_b = '') THEN 0.0
            WHEN phone_a = phone_b THEN 1.0
            ELSE 0.0
        END AS phone_score,

        CASE
            WHEN (zip_a IS NULL OR zip_a = '') OR (zip_b IS NULL OR zip_b = '') THEN 0.0
            WHEN zip_a = zip_b AND LOWER(COALESCE(city_a, '')) = LOWER(COALESCE(city_b, ''))
                 AND LOWER(COALESCE(address_a, '')) = LOWER(COALESCE(address_b, ''))
                 AND address_a IS NOT NULL AND address_b IS NOT NULL THEN 1.0
            WHEN zip_a = zip_b AND LOWER(COALESCE(city_a, '')) = LOWER(COALESCE(city_b, '')) THEN 0.8
            WHEN zip_a = zip_b THEN 0.5
            ELSE 0.0
        END AS address_score,

        CASE
            WHEN (email_a IS NULL OR email_a = '') OR (email_b IS NULL OR email_b = '') THEN 0.0
            WHEN email_a = email_b THEN 1.0
            ELSE 0.0
        END AS email_score

    FROM candidate_pairs
),

weighted AS (
    SELECT
        *,
        ROUND(
            last_name_score * 0.20
            + first_name_score * 0.15
            + dob_score * 0.25
            + phone_score * 0.20
            + address_score * 0.10
            + email_score * 0.10
        , 3) AS confidence
    FROM scored
)

SELECT
    source_system_a,
    source_key_a,
    source_system_b,
    source_key_b,
    CAST(confidence AS DECIMAL(4,3)) AS confidence,
    'probabilistic' AS match_method,
    CAST(last_name_score AS DECIMAL(4,3)) AS last_name_score,
    CAST(first_name_score AS DECIMAL(4,3)) AS first_name_score,
    CAST(dob_score AS DECIMAL(4,3)) AS dob_score,
    CAST(phone_score AS DECIMAL(4,3)) AS phone_score,
    CAST(address_score AS DECIMAL(4,3)) AS address_score,
    CAST(email_score AS DECIMAL(4,3)) AS email_score
FROM weighted
WHERE confidence >= 0.70
