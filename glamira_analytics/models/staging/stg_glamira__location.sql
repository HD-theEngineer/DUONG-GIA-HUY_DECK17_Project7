WITH stg__location_rename AS (
    SELECT
        country_short AS country_short_name,
        country_long AS country_name,
        region AS region_name,
        city AS city_name,
        ip AS ip_address
    FROM 
        {{ ref("stg_glamira__raw_ip2location")}}
    WHERE
        country_short != '-'
)

, stg__location_gen_key AS (
    SELECT 
        FARM_FINGERPRINT(country_name || region_name || city_name) AS location_key,
        country_short_name,
        country_name,
        region_name,
        city_name,
        ip_address
    FROM 
        stg__location_rename
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
        ip_address
    FROM 
        stg__location_gen_key
)

SELECT
    *
FROM
    stg__location_handle_invalid
