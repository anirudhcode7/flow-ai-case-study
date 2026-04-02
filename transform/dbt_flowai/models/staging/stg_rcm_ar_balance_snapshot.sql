SELECT * FROM {{ source('bronze', 'rcm_ar_balance_snapshot') }}
