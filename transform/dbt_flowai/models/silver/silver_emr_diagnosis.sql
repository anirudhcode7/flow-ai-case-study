WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY emr_encounter_id, icd10_code
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_emr_diagnosis') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(emr_encounter_id)                                             AS emr_encounter_id,
    UPPER(TRIM(icd10_code))                                            AS icd10_code,
    TRIM(diagnosis_desc)                                               AS diagnosis_desc,
    TRY_CAST(diagnosis_rank AS INTEGER)                                AS diagnosis_rank,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
