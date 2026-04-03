-- dim_date: Standard date dimension covering 2022-2027
-- Fiscal year assumes Oct 1 start (common in healthcare)

WITH date_spine AS (
    SELECT CAST(generate_series AS DATE) AS date_day
    FROM generate_series(DATE '2022-01-01', DATE '2027-12-31', INTERVAL 1 DAY)
)

SELECT
    date_day                    AS date_key,
    YEAR(date_day)              AS year,
    QUARTER(date_day)           AS quarter,
    MONTH(date_day)             AS month,
    MONTHNAME(date_day)         AS month_name,
    DAY(date_day)               AS day_of_month,
    DAYOFWEEK(date_day)         AS day_of_week,
    DAYNAME(date_day)           AS day_name,
    WEEKOFYEAR(date_day)        AS week_of_year,
    CASE WHEN DAYOFWEEK(date_day) IN (0, 6)
         THEN TRUE ELSE FALSE
    END                         AS is_weekend,
    -- Fiscal year: Oct 1 start
    CASE WHEN MONTH(date_day) >= 10
         THEN YEAR(date_day) + 1
         ELSE YEAR(date_day)
    END                         AS fiscal_year,
    CASE
        WHEN MONTH(date_day) >= 10 THEN MONTH(date_day) - 9
        ELSE MONTH(date_day) + 3
    END                         AS fiscal_quarter
FROM date_spine
