WITH spine AS (
    SELECT * FROM {{ ref('int_patient_spine') }}
),

-- Rule 1: Direct ID Link (RCM -> EMR via emr_patient_id_link)
rule_id_link AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        1.000 AS confidence,
        'deterministic_id_link' AS match_method
    FROM spine a
    JOIN spine b
        ON a.emr_patient_id_link = b.source_patient_key
    WHERE a.source_system = 'rcm'
      AND b.source_system = 'emr'
      AND a.emr_patient_id_link IS NOT NULL
),

-- Rule 2: MRN Match (same non-NULL MRN, different records)
rule_mrn AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        1.000 AS confidence,
        'deterministic_mrn' AS match_method
    FROM spine a
    JOIN spine b
        ON a.mrn = b.mrn
    WHERE a.mrn IS NOT NULL
      AND b.mrn IS NOT NULL
      AND (a.source_system < b.source_system
           OR (a.source_system = b.source_system AND a.source_patient_key < b.source_patient_key))
),

-- Rule 3: SSN-last4 + DOB
rule_ssn_dob AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        1.000 AS confidence,
        'deterministic_ssn_dob' AS match_method
    FROM spine a
    JOIN spine b
        ON a.ssn_last4 = b.ssn_last4 AND a.dob = b.dob
    WHERE a.ssn_last4 IS NOT NULL
      AND b.ssn_last4 IS NOT NULL
      AND (a.source_system < b.source_system
           OR (a.source_system = b.source_system AND a.source_patient_key < b.source_patient_key))
),

-- Rule 4: Phone + DOB (different systems)
rule_phone_dob AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        0.950 AS confidence,
        'deterministic_phone_dob' AS match_method
    FROM spine a
    JOIN spine b
        ON a.phone = b.phone AND a.dob = b.dob
    WHERE a.phone IS NOT NULL AND a.phone != ''
      AND b.phone IS NOT NULL AND b.phone != ''
      AND a.source_system != b.source_system
      AND (a.source_system < b.source_system
           OR (a.source_system = b.source_system AND a.source_patient_key < b.source_patient_key))
),

-- Rule 5: Email + DOB (different systems)
rule_email_dob AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        0.950 AS confidence,
        'deterministic_email_dob' AS match_method
    FROM spine a
    JOIN spine b
        ON a.email = b.email AND a.dob = b.dob
    WHERE a.email IS NOT NULL AND a.email != ''
      AND b.email IS NOT NULL AND b.email != ''
      AND a.source_system != b.source_system
      AND (a.source_system < b.source_system
           OR (a.source_system = b.source_system AND a.source_patient_key < b.source_patient_key))
),

-- Rule 6: Phone + Last Name (different systems)
rule_phone_lastname AS (
    SELECT
        a.source_system AS source_system_a,
        a.source_patient_key AS source_key_a,
        b.source_system AS source_system_b,
        b.source_patient_key AS source_key_b,
        0.900 AS confidence,
        'deterministic_phone_lastname' AS match_method
    FROM spine a
    JOIN spine b
        ON a.phone = b.phone AND LOWER(a.last_name) = LOWER(b.last_name)
    WHERE a.phone IS NOT NULL AND a.phone != ''
      AND b.phone IS NOT NULL AND b.phone != ''
      AND a.last_name IS NOT NULL
      AND b.last_name IS NOT NULL
      AND a.source_system != b.source_system
      AND (a.source_system < b.source_system
           OR (a.source_system = b.source_system AND a.source_patient_key < b.source_patient_key))
),

-- Union all rules
all_deterministic AS (
    SELECT * FROM rule_id_link
    UNION ALL SELECT * FROM rule_mrn
    UNION ALL SELECT * FROM rule_ssn_dob
    UNION ALL SELECT * FROM rule_phone_dob
    UNION ALL SELECT * FROM rule_email_dob
    UNION ALL SELECT * FROM rule_phone_lastname
),

-- Deduplicate: keep highest confidence per pair
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                LEAST(source_system_a || '::' || source_key_a, source_system_b || '::' || source_key_b),
                GREATEST(source_system_a || '::' || source_key_a, source_system_b || '::' || source_key_b)
            ORDER BY confidence DESC, match_method ASC
        ) AS rn
    FROM all_deterministic
)

SELECT
    source_system_a,
    source_key_a,
    source_system_b,
    source_key_b,
    CAST(confidence AS DECIMAL(4,3)) AS confidence,
    match_method
FROM ranked
WHERE rn = 1
