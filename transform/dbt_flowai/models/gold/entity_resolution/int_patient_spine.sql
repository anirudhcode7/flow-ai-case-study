WITH emr_patients AS (
    SELECT
        'emr' AS source_system,
        emr_patient_id AS source_patient_key,
        mrn,
        ssn_last4,
        first_name,
        last_name,
        dob,
        phone,
        email,
        address_line1,
        city,
        state,
        zip,
        CAST(NULL AS VARCHAR) AS emr_patient_id_link
    FROM {{ ref('silver_emr_patient') }}
),

rcm_patients AS (
    SELECT
        'rcm' AS source_system,
        rcm_account_id AS source_patient_key,
        CAST(NULL AS VARCHAR) AS mrn,
        CAST(NULL AS VARCHAR) AS ssn_last4,
        patient_first_name AS first_name,
        patient_last_name AS last_name,
        dob,
        phone,
        CAST(NULL AS VARCHAR) AS email,
        address_line1,
        city,
        state,
        zip,
        emr_patient_id AS emr_patient_id_link
    FROM {{ ref('silver_rcm_patient_account') }}
),

referral_patients AS (
    SELECT
        'referral' AS source_system,
        referral_order_id AS source_patient_key,
        CAST(NULL AS VARCHAR) AS mrn,
        CAST(NULL AS VARCHAR) AS ssn_last4,
        patient_first_name AS first_name,
        patient_last_name AS last_name,
        dob,
        phone,
        email,
        address_line1,
        city,
        state,
        zip,
        CAST(NULL AS VARCHAR) AS emr_patient_id_link
    FROM {{ ref('silver_referral_order') }}
)

SELECT * FROM emr_patients
UNION ALL
SELECT * FROM rcm_patients
UNION ALL
SELECT * FROM referral_patients
