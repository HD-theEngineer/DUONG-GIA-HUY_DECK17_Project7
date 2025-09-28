WITH stg__location_rename AS (
    SELECT
        country_short AS country_short_name,
        country_long AS country_name,
        region AS region_name,
        city AS city_name,
        ip AS ip_address
    FROM 
        {{ ref("stg_glamira__raw_ip2location")}}
)

, stg_glamira__exchange_rename AS (
    SELECT
        TRIM(REGEXP_REPLACE(symbol, r"[0-9,\.\s'’]+",'')) AS currency_symbol,
        country_code AS country_short_name,
        SAFE_CAST(usd_exchange_rate AS FLOAT64) AS exchange_rate_to_usd

    FROM
        {{ ref('stg_glamira__raw_exchange_rate') }}
)

, stg_glamira__location_join_rate AS (
    SELECT
        *
    FROM
        stg__location_rename
    LEFT JOIN
        stg_glamira__exchange_rename
    USING (country_short_name)
)

, stg__location_gen_key AS (
    SELECT 
        FARM_FINGERPRINT(country_name || region_name || city_name) AS location_key,
        *
    FROM 
        stg_glamira__location_join_rate
)
, stg__location_handle_invalid AS (
    SELECT
        location_key,
        country_short_name,
        country_name,
        CASE 
            WHEN TRIM(region_name) = '-' OR TRIM(region_name) = ''
            THEN NULL 
            ELSE region_name 
        END AS region_name, 
        CASE 
            WHEN TRIM(city_name) = '-' OR TRIM(city_name) = ''
            THEN NULL 
            ELSE city_name 
        END AS city_name,        
        ip_address,
        currency_symbol,
        exchange_rate_to_usd
    FROM 
        stg__location_gen_key
)

SELECT
    *
FROM
    stg__location_handle_invalid
