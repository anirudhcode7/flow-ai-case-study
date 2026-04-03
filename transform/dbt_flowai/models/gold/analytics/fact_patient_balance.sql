-- fact_patient_balance: Patient-level AR aggregation
-- Grain: patient_id + snapshot_date

SELECT
    fr.snapshot_date,
    fr.patient_id,
    SUM(fr.total_ar_balance)    AS total_outstanding,
    SUM(CASE
        WHEN fr.aging_bucket IN ('61-90', '90+')
        THEN fr.total_ar_balance ELSE 0
    END)                        AS overdue_amount,
    SUM(fr.payer_balance)       AS total_payer_balance,
    SUM(fr.patient_balance)     AS total_patient_balance,
    COUNT(DISTINCT fr.claim_id) AS open_claim_count,
    CAST(NULL AS DATE)          AS last_payment_date
FROM {{ ref('fact_receivable') }} fr
WHERE fr.patient_id IS NOT NULL
GROUP BY fr.snapshot_date, fr.patient_id
