WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY emr_encounter_id, cpt_code
            ORDER BY _ingested_at DESC
        ) AS _rn
    FROM {{ ref('stg_emr_procedure') }}
),
filtered AS (
    SELECT * FROM deduped WHERE _rn = 1
)
SELECT
    TRIM(emr_encounter_id)                                             AS emr_encounter_id,
    TRIM(cpt_code)                                                     AS cpt_code,
    TRIM(procedure_desc)                                               AS procedure_desc,
    TRY_CAST(performed_time AS TIMESTAMP)                              AS performed_time,
    _ingested_at,
    _source_system,
    _source_file,
    _row_hash,
    CURRENT_TIMESTAMP                                                  AS _silver_loaded_at
FROM filtered
