WITH dim_location__load AS (
    SELECT
        *
    FROM 
        {{ ref('stg_glamira__location')}}
)
, dim_location__null_handle AS (
    SELECT
        location_key,
        country_name,
        country_short_name,
        COALESCE(region_name, 'XNA') AS region_name,
        COALESCE(city_name, 'XNA') AS city_name,
        ip_address
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
        ARRAY_AGG(ip_address) AS ip_list
    FROM
        dim_location__null_handle
    GROUP BY
        location_key,
        country_name,
        country_short_name,
        region_name,
        city_name
)

SELECT
    *
FROM
    dim_location__undefined_value
