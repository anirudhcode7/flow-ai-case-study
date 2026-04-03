SELECT * FROM {{ source('bronze', 'emr_patient') }}
