WITH raw_timeline AS (
  SELECT
    MAX(EXTRACT(DATE FROM time_stamp)) as last_date,
    MIN(EXTRACT(DATE FROM time_stamp)) as first_date
  FROM
    {{ref("stg_glamira__temp_raw_data")}}
)
, dates AS (
  SELECT day AS date
  FROM UNNEST(GENERATE_DATE_ARRAY((SELECT first_date FROM raw_timeline), (SELECT last_date FROM raw_timeline), INTERVAL 1 DAY)) AS day
)

SELECT
  FORMAT_DATE('%Y%m%d', date) AS date_key,
  date,
  EXTRACT(DAY FROM date) AS day,
  FORMAT_DATE('%A', date) AS day_name,
  EXTRACT(ISOWEEK FROM date) AS week,
  EXTRACT(MONTH FROM date) AS month,
  FORMAT_DATE('%B', date) AS month_name,
  EXTRACT(YEAR FROM date) AS year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM dates