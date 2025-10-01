WITH dim_location__load AS (
    SELECT
        *
    FROM 
        {{ ref('stg_glamira__location')}}
)
, dim_location__null_handle AS (
    SELECT
        location_key,
        COALESCE(country_name, 'XNA') AS country_name,
        COALESCE(country_short_name, 'XNA') AS country_short_name,
        COALESCE(region_name, 'XNA') AS region_name,
        COALESCE(city_name, 'XNA') AS city_name,
        COALESCE(currency_symbol, 'XNA') AS currency_symbol,
        COALESCE(exchange_rate_to_usd, 0) AS exchange_rate_to_usd
    FROM
        dim_location__load
)

, dim_location__undefined_value AS(
    SELECT
        DISTINCT location_key,
        country_name,
        country_short_name,
        region_name,
        city_name,
        currency_symbol,
        exchange_rate_to_usd
    FROM
        dim_location__null_handle
    UNION ALL
    SELECT
        -1 AS location_key,
        'XNA' AS country_name,
        'XNA' AS country_short_name,
        'XNA' AS region_name,
        'XNA' AS city_name,
        'XNA' AS currency_symbol,
        CAST(0 AS FLOAT64) AS exchange_rate_to_usd
)

SELECT
    *
FROM
    dim_location__undefined_value
