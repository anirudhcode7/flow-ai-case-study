SELECT * FROM {{ source('bronze', 'emr_provider') }}
