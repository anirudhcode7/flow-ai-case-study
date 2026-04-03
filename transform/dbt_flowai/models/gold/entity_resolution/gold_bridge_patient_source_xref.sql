SELECT
    patient_id,
    source_system,
    source_patient_key,
    match_confidence,
    match_method,
    CURRENT_TIMESTAMP AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to
FROM {{ ref('int_patient_match_final') }}
