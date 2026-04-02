WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY emr_encounter_id
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_emr_encounter') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(emr_encounter_id)                                             AS emr_encounter_id,
    TRIM(emr_patient_id)                                               AS emr_patient_id,
    CASE
        WHEN LOWER(TRIM(encounter_type)) IN ('outpatient', 'inpatient', 'ed')
            THEN LOWER(TRIM(encounter_type))
        ELSE NULL
    END                                                                AS encounter_type,
    CASE
        WHEN LOWER(TRIM(status)) IN ('planned', 'in-progress', 'completed', 'cancelled')
            THEN LOWER(TRIM(status))
        ELSE NULL
    END                                                                AS status,
    TRY_CAST(start_time AS TIMESTAMP)                                  AS start_time,
    TRY_CAST(end_time AS TIMESTAMP)                                    AS end_time,
    TRIM(facility_id)                                                  AS facility_id,
    TRIM(attending_provider_id)                                        AS attending_provider_id,
    TRIM(chief_complaint)                                              AS chief_complaint,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
