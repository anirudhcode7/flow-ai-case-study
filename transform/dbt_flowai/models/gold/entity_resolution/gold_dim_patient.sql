WITH patient_sources AS (
    SELECT
        f.patient_id,
        f.source_system,
        f.source_patient_key,
        s.first_name,
        s.last_name,
        s.dob,
        s.phone,
        s.email,
        s.address_line1,
        s.city,
        s.state,
        s.zip
    FROM {{ ref('int_patient_match_final') }} f
    JOIN {{ ref('int_patient_spine') }} s
        ON f.source_system = s.source_system
        AND f.source_patient_key = s.source_patient_key
),

survivorship AS (
    SELECT
        patient_id,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN first_name END),
            MAX(CASE WHEN source_system = 'rcm' THEN first_name END),
            MAX(CASE WHEN source_system = 'referral' THEN first_name END)
        ) AS best_first_name,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN last_name END),
            MAX(CASE WHEN source_system = 'rcm' THEN last_name END),
            MAX(CASE WHEN source_system = 'referral' THEN last_name END)
        ) AS best_last_name,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN dob END),
            MAX(CASE WHEN source_system = 'rcm' THEN dob END),
            MAX(CASE WHEN source_system = 'referral' THEN dob END)
        ) AS best_dob,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN phone END),
            MAX(CASE WHEN source_system = 'rcm' THEN phone END),
            MAX(CASE WHEN source_system = 'referral' THEN phone END)
        ) AS best_phone,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN email END),
            MAX(CASE WHEN source_system = 'referral' THEN email END)
        ) AS best_email,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN address_line1 END),
            MAX(CASE WHEN source_system = 'rcm' THEN address_line1 END),
            MAX(CASE WHEN source_system = 'referral' THEN address_line1 END)
        ) AS best_address_line1,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN city END),
            MAX(CASE WHEN source_system = 'rcm' THEN city END),
            MAX(CASE WHEN source_system = 'referral' THEN city END)
        ) AS best_city,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN state END),
            MAX(CASE WHEN source_system = 'rcm' THEN state END),
            MAX(CASE WHEN source_system = 'referral' THEN state END)
        ) AS best_state,

        COALESCE(
            MAX(CASE WHEN source_system = 'emr' THEN zip END),
            MAX(CASE WHEN source_system = 'rcm' THEN zip END),
            MAX(CASE WHEN source_system = 'referral' THEN zip END)
        ) AS best_zip,

        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM patient_sources
    GROUP BY patient_id
)

SELECT * FROM survivorship
